import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { makeStderrCapture, makeTerminationError } from "./stdio.ts";

describe("Codex App Server child process termination", () => {
  const handle = {
    pid: ChildProcessSpawner.ProcessId(52),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
  };
  const encoder = new TextEncoder();

  it.effect.each([
    {
      name: "Node launcher error between source and stack frames",
      chunks: [
        "file:///opt/codex/bin/codex.js:107\n  throw new Error(\n        ^\n\nError: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n    at file:///opt/codex/bin/codex.js:107:11\n\nNode.js v24.1.0\n",
      ],
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "last error-labelled line",
      chunks: ["Error: earlier failure\nTypeError: invalid value\n    at main (app.js:1:1)\n"],
      expected: "TypeError: invalid value",
    },
    {
      name: "Node error code",
      chunks: ["Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at main (app.js:1:1)\n"],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: invalid argument\n\nUsage: codex app-server\n"],
      expected: "error: invalid argument",
    },
    {
      name: "fatal error",
      chunks: ["fatal: cannot start\ntry again\n"],
      expected: "fatal: cannot start",
    },
    {
      name: "last non-empty unlabelled line",
      chunks: ["starting\nlauncher could not start\n \n"],
      expected: "launcher could not start",
    },
    {
      name: "terminal escapes, carriage returns, whitespace and split chunks",
      chunks: [
        "loading\r\u001b[31mErr",
        "or:\t Missing   dependency\u001b[0m\r\n\u001b]0;title\u0007\n",
      ],
      expected: "Error: Missing dependency",
    },
    {
      name: "512-character cap",
      chunks: [`Error: ${"x".repeat(600)}\n    at main (app.js:1:1)\n`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", chunks: ["\n \t\r\n"], expected: undefined },
    { name: "empty stderr", chunks: [], expected: undefined },
    {
      name: "8 KiB tail across chunks, measured in bytes",
      chunks: ["Error: obsolete\n", "é".repeat(4096), "\nlast diagnostic\n"],
      expected: "last diagnostic",
    },
    {
      name: "8 KiB tail within one large chunk",
      chunks: [`Error: obsolete\n${"x".repeat(16 * 1024)}\nlast diagnostic\n`],
      expected: "last diagnostic",
    },
  ])("includes $name", ({ chunks, expected }) =>
    Effect.gen(function* () {
      const stderr = yield* makeStderrCapture(
        Stream.fromIterable(chunks.map((chunk) => encoder.encode(chunk))),
      );
      const error = yield* makeTerminationError(handle, stderr);

      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      if (error._tag === "CodexAppServerProcessExitedError") {
        assert.equal(error.stderr, expected);
        assert.equal(error.pid, 52);
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
        );
      }
    }),
  );

  it.effect("waits for stderr to finish after the process exits", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<Uint8Array>();
      const exited = yield* Deferred.make<void>();
      const stderr = yield* makeStderrCapture(Stream.fromEffect(Deferred.await(release)));
      const termination = yield* makeTerminationError(
        {
          ...handle,
          exitCode: handle.exitCode.pipe(Effect.tap(() => Deferred.succeed(exited, undefined))),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* Deferred.await(exited);
      yield* Deferred.succeed(release, encoder.encode("Error: final diagnostic\n"));
      const error = yield* Fiber.join(termination);
      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: Error: final diagnostic",
      );
    }),
  );

  it.effect("uses the captured tail after 250 ms if stderr stays open", () =>
    Effect.gen(function* () {
      const stderr = yield* makeStderrCapture(
        Stream.concat(Stream.make(encoder.encode("Error: launcher failed\n")), Stream.never),
      );
      const termination = yield* makeTerminationError(handle, stderr).pipe(Effect.forkScoped);

      yield* TestClock.adjust("250 millis");
      const error = yield* Fiber.join(termination);
      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: Error: launcher failed",
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
