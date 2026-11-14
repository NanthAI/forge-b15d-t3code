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
const launcherError =
  "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";

describe("Codex App Server child process termination", () => {
  for (const { name, text, expected } of [
    {
      name: "Node launcher error between source and stack frames",
      text: `file:///prefix/codex.js:107\n  throw new Error(message);\n        ^\n\n${launcherError}\n    at file:///prefix/codex.js:107:9\nNode.js v26.0.0\n`,
      expected: launcherError,
    },
    {
      name: "last error-labelled line",
      text: "Error: first failure\nTypeError: invalid argument\n    at main (app.js:1:1)\n",
      expected: "TypeError: invalid argument",
    },
    {
      name: "Node error with a code",
      text: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at main (app.js:1:1)\n",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "Rust CLI error",
      text: "error: unexpected argument '--unknown' found\nUsage: codex app-server\n",
      expected: "error: unexpected argument '--unknown' found",
    },
    {
      name: "fatal error",
      text: "fatal: cannot start\nSee the log for details\n",
      expected: "fatal: cannot start",
    },
    {
      name: "last non-empty line without a label",
      text: "starting\nlauncher could not start\n \n",
      expected: "launcher could not start",
    },
    {
      name: "terminal escapes, carriage returns and whitespace",
      text: "starting\r\u001b[31m  Error:  missing\t dependency  \u001b[0m\r\n    at main\n",
      expected: "Error: missing dependency",
    },
    {
      name: "512-character cap",
      text: `Error: ${"x".repeat(600)}\n    at main\n`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", text: " \n\r\t", expected: undefined },
    { name: "empty stderr", text: "", expected: undefined },
    {
      name: "8 KiB byte tail across chunks",
      text: `Error: discarded\n${"é".repeat(4096)}\nlatest diagnostic\n`,
      expected: "latest diagnostic",
    },
  ]) {
    it.effect(`extracts ${name}`, () =>
      Effect.gen(function* () {
        const bytes = encoder.encode(text);
        const stderr = yield* captureStderr({
          stderr: Stream.make(bytes.subarray(0, 100), bytes.subarray(100)),
        });
        const error = yield* makeTerminationError(
          {
            pid: ChildProcessSpawner.ProcessId(52),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          },
          stderr,
        );

        assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
        assert.equal(error.stderr, expected);
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
        );
      }),
    );
  }

  it.effect("waits for stderr to finish after the process exits", () =>
    Effect.gen(function* () {
      const chunk = yield* Deferred.make<Uint8Array>();
      const exited = yield* Deferred.make<void>();
      const stderr = yield* captureStderr({ stderr: Stream.fromEffect(Deferred.await(chunk)) });
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Deferred.succeed(exited, undefined).pipe(
            Effect.as(ChildProcessSpawner.ExitCode(1)),
          ),
        },
        stderr,
      ).pipe(Effect.forkChild);
      yield* Deferred.await(exited);
      yield* Deferred.succeed(chunk, encoder.encode(launcherError));
      const error = yield* Fiber.join(termination);

      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, launcherError);
    }),
  );

  it.effect("uses the captured tail after 250 ms when stderr stays open", () =>
    Effect.gen(function* () {
      const captured = yield* Deferred.make<void>();
      const stderr = yield* captureStderr({
        stderr: Stream.concat(
          Stream.succeed(encoder.encode(launcherError)),
          Stream.fromEffect(
            Deferred.succeed(captured, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        ),
      });
      yield* Deferred.await(captured);
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkChild);
      yield* TestClock.adjust("250 millis");
      const error = yield* Fiber.join(termination);

      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, launcherError);
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
