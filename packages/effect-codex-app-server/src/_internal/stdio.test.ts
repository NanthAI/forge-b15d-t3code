import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { captureStderr, makeTerminationError } from "./stdio.ts";

const encoder = new TextEncoder();
const exited = {
  pid: ChildProcessSpawner.ProcessId(52),
  exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
};

describe("Codex App Server child process termination", () => {
  it.effect.each([
    {
      name: "Node launcher error between source and stack",
      stderr: [
        "file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107",
        '  throw new Error("Missing optional dependency");',
        "        ^",
        "",
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
        "    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:9",
        "Node.js v22.14.0",
      ].join("\n"),
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "Node error code",
      stderr: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal)",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "last labelled error",
      stderr: "Error: earlier error\nTypeError: invalid configuration\n    at startup (launcher:1)",
      expected: "TypeError: invalid configuration",
    },
    {
      name: "Rust CLI error",
      stderr: "error: unexpected argument '--bad'\n\nFor more information, try '--help'.",
      expected: "error: unexpected argument '--bad'",
    },
    {
      name: "fatal error",
      stderr: "fatal: cannot start\nshutting down",
      expected: "fatal: cannot start",
    },
    {
      name: "last non-empty line",
      stderr: "starting\nlauncher could not start\n\n",
      expected: "launcher could not start",
    },
    {
      name: "terminal escapes and whitespace",
      stderr: "progress\r\u001b[31merror:\t missing   dependency\u001b[0m\r\n  ",
      expected: "error: missing dependency",
    },
    {
      name: "512 character limit",
      stderr: `Error: ${"x".repeat(600)}`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    {
      name: "8 KiB tail of a large chunk",
      stderr: `Error: discarded\n${"x".repeat(16 * 1024)}\nlast diagnostic\n`,
      expected: "last diagnostic",
    },
    { name: "empty stderr", stderr: "", expected: undefined },
    { name: "blank stderr", stderr: " \t\r\n\u001b[0m", expected: undefined },
  ])("includes a bounded diagnostic for $name", ({ stderr, expected }) =>
    Effect.gen(function* () {
      const captured = yield* captureStderr(Stream.make(encoder.encode(stderr)));
      const error = yield* makeTerminationError(exited, captured);

      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      if (error._tag !== "CodexAppServerProcessExitedError") return assert.fail("Expected exit");
      assert.equal(error.stderr, expected);
      assert.equal(error.pid, 52);
      assert.equal(
        error.message,
        `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
      );
    }),
  );

  it.effect("retains only the last 8 KiB across stderr chunks", () =>
    Effect.gen(function* () {
      const captured = yield* captureStderr(
        Stream.make(
          encoder.encode(`Error: discarded\n${"x".repeat(16 * 1024)}\nError: also discarded\n`),
          encoder.encode("\u00e9".repeat(4096)),
          encoder.encode("\nlast diagnostic\n"),
        ),
      );
      const error = yield* makeTerminationError(exited, captured);
      assert.equal(error.message, "Codex App Server process exited with code 1: last diagnostic");
    }),
  );

  it.effect("waits for stderr to finish after exit", () =>
    Effect.gen(function* () {
      const finish = yield* Deferred.make<Uint8Array>();
      const captured = yield* captureStderr(Stream.fromEffect(Deferred.await(finish)));
      const pending = yield* makeTerminationError(exited, captured).pipe(Effect.forkScoped);

      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* Deferred.succeed(finish, encoder.encode("Error: late diagnostic\n"));

      const error = yield* Fiber.join(pending);
      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: Error: late diagnostic",
      );
    }),
  );

  it.effect("uses the captured tail after 250 ms if stderr stays open", () =>
    Effect.gen(function* () {
      const captured = yield* captureStderr(
        Stream.concat(Stream.make(encoder.encode("fatal: launcher failed\n")), Stream.never),
      );
      const pending = yield* makeTerminationError(exited, captured).pipe(Effect.forkScoped);

      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* TestClock.adjust("1 millis");

      const error = yield* Fiber.join(pending);
      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: fatal: launcher failed",
      );
    }),
  );

  it.effect("retains the process identifier and exact exit-status cause", () =>
    Effect.gen(function* () {
      const rootCause = new Error("private process diagnostics");
      const cause = PlatformError.systemError({
        _tag: "Unknown",
        module: "ChildProcess",
        method: "exitCode",
        cause: rootCause,
      });
      const error = yield* makeTerminationError({
        pid: ChildProcessSpawner.ProcessId(52),
        exitCode: Effect.fail(cause),
      });

      assert.instanceOf(error, CodexError.CodexAppServerTransportError);
      assert.equal(error.pid, 52);
      assert.strictEqual(error.cause, cause);
      assert.equal(
        error.message,
        "Codex App Server transport operation 'read-process-exit-status' failed.",
      );
      assert.notInclude(error.message, rootCause.message);
    }),
  );
});
