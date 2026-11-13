import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { makeStderrCapture, makeTerminationError } from "./stdio.ts";

describe("Codex App Server child process termination", () => {
  it.effect.each([
    {
      name: "Node launcher source excerpt and stack",
      chunks: [
        'file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n  throw new Error("Missing optional dependency");\n        ^\n\nError: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:9\n\nNode.js v22.16.0\n',
      ],
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "last error-labelled line",
      chunks: ["Error: first error\nTypeError: last error\n    at main (index.js:1:1)\n"],
      expected: "TypeError: last error",
    },
    {
      name: "Node error code",
      chunks: [
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (index.js:1:1)\n",
      ],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: unexpected argument '--bad' found\n\nUsage: codex app-server\n"],
      expected: "error: unexpected argument '--bad' found",
    },
    {
      name: "fatal error",
      chunks: ["fatal: failed to start\nsee logs for more details\n"],
      expected: "fatal: failed to start",
    },
    {
      name: "last non-empty line",
      chunks: ["starting\nlauncher could not start\n \t\n"],
      expected: "launcher could not start",
    },
    {
      name: "terminal escapes, whitespace and carriage returns across chunks",
      chunks: ["loading\r\u001b[31mTy", "peError:  invalid\t  config\u001b[0m\r\n    at main\n"],
      expected: "TypeError: invalid config",
    },
    {
      name: "512-character limit",
      chunks: [`Error: ${"x".repeat(600)}\n    at main\n`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", chunks: [" \t\r\n"], expected: undefined },
    { name: "empty stderr", chunks: [], expected: undefined },
    {
      name: "8 KiB tail after a large chunk",
      chunks: ["Error: discarded\n", "x".repeat(8192)],
      expected: "x".repeat(512),
    },
    {
      name: "8 KiB tail across chunks",
      chunks: ["Error: discarded\n", "é".repeat(2048), "é".repeat(2048), "\nlast line\n"],
      expected: "last line",
    },
  ])("includes a bounded stderr excerpt: $name", ({ chunks, expected }) =>
    Effect.gen(function* () {
      const stderr = yield* makeStderrCapture(
        Stream.fromIterable(chunks.map((chunk) => new TextEncoder().encode(chunk))),
      );
      const error = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      );

      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      if (error._tag !== "CodexAppServerProcessExitedError") return;
      assert.equal(error.stderr, expected);
      assert.equal(error.pid, 52);
      assert.equal(
        error.message,
        `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
      );
    }),
  );

  it.effect.each([true, false])("bounds the stderr wait (stream finishes: %s)", (finishes) =>
    Effect.gen(function* () {
      const written = yield* Deferred.make<void>();
      const finish = yield* Deferred.make<void>();
      const stderr = yield* makeStderrCapture(
        Stream.make(new TextEncoder().encode("Error: launcher failed\n")).pipe(
          Stream.concat(
            Stream.fromEffect(
              Deferred.succeed(written, undefined).pipe(
                Effect.andThen(Deferred.await(finish)),
                Effect.as(new TextEncoder().encode("TypeError: final diagnostic\n")),
              ),
            ),
          ),
        ),
      );
      yield* Deferred.await(written);
      const handle = {
        pid: ChildProcessSpawner.ProcessId(52),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
      };
      const pending = yield* makeTerminationError(handle, stderr).pipe(Effect.forkChild);
      yield* TestClock.adjust("249 millis");
      assert.equal(pending.pollUnsafe(), undefined);
      if (finishes) {
        yield* Deferred.succeed(finish, undefined);
      } else {
        yield* TestClock.adjust("1 millis");
      }
      const error = yield* Fiber.join(pending);
      assert.equal(
        error.message,
        `Codex App Server process exited with code 1: ${finishes ? "TypeError: final diagnostic" : "Error: launcher failed"}`,
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
