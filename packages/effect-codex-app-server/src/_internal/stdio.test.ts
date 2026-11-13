import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as Effect from "effect/Effect";
import * as PlatformError from "effect/PlatformError";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { makeStderrCapture, makeTerminationError } from "./stdio.ts";

describe("Codex App Server child process termination", () => {
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

  const launcherError =
    "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";
  for (const { name, text, expected } of [
    {
      name: "Node launcher error between source and stack frames",
      text: `file:///prefix/codex.js:107\n    throw new Error(message);\n          ^\n\n${launcherError}\n    at file:///prefix/codex.js:107:11\n\nNode.js v24.0.0\n`,
      expected: launcherError,
    },
    {
      name: "Node error code",
      text: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules:12:3)",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "TypeError",
      text: "TypeError: Cannot read properties of undefined\n    at launcher.js:10:1",
      expected: "TypeError: Cannot read properties of undefined",
    },
    {
      name: "Rust CLI error",
      text: "error: unexpected argument '--bad' found\n\nFor more information, try '--help'.",
      expected: "error: unexpected argument '--bad' found",
    },
    {
      name: "last error-labelled line",
      text: "Error: initial failure\nerror: failed to load config\nfatal: cannot start\n    at launcher.js:10:1",
      expected: "fatal: cannot start",
    },
    {
      name: "last non-empty unlabelled line",
      text: "starting server\n  unable to launch app-server  \n\n",
      expected: "unable to launch app-server",
    },
    {
      name: "terminal escapes and carriage returns",
      text: "loading\r\u001b[31merror:  missing\t dependency\u001b[0m\r\n\u001b]8;;https://example.test\u0007help\u001b]8;;\u0007\n",
      expected: "error: missing dependency",
    },
    {
      name: "512-character limit",
      text: `Error: ${"x".repeat(1024)}`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", text: " \n\r\t", expected: undefined },
    { name: "empty stderr", text: "", expected: undefined },
  ]) {
    it.effect(`includes the stderr excerpt for ${name}`, () =>
      Effect.gen(function* () {
        const stderr = yield* makeStderrCapture(Stream.make(new TextEncoder().encode(text)));
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

  it.effect("retains only the last 8 KiB of stderr across chunks", () =>
    Effect.gen(function* () {
      const encoder = new TextEncoder();
      const bytes = encoder.encode(`Error: discarded\n${"é".repeat(8192)}\nfinal diagnostic`);
      const stderr = yield* makeStderrCapture(
        Stream.make(bytes.subarray(0, 100), bytes.subarray(100, 10000), bytes.subarray(10000)),
      );
      yield* stderr.done;
      assert.equal(yield* stderr.text, new TextDecoder().decode(bytes.slice(-8192)));
      const error = yield* makeTerminationError(
        {
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          pid: ChildProcessSpawner.ProcessId(52),
        },
        stderr,
      );
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "final diagnostic");
    }),
  );

  it.effect("waits for stderr to finish after exit", () =>
    Effect.gen(function* () {
      const ready = yield* Deferred.make<void>();
      const finished = yield* Deferred.make<void>();
      let text = "source excerpt";
      const fiber = yield* makeTerminationError(
        {
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          pid: ChildProcessSpawner.ProcessId(52),
        },
        {
          done: Deferred.succeed(ready, undefined).pipe(Effect.andThen(Deferred.await(finished))),
          text: Effect.sync(() => text),
        },
      ).pipe(Effect.forkScoped);
      yield* Deferred.await(ready);
      text += "\nError: final diagnostic\n    at launcher.js:10:1";
      yield* Deferred.succeed(finished, undefined);
      const error = yield* Fiber.join(fiber);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: final diagnostic");
    }),
  );

  it.effect("uses captured stderr after 250 ms when the stream stays open", () =>
    Effect.gen(function* () {
      const ready = yield* Deferred.make<void>();
      const fiber = yield* makeTerminationError(
        {
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          pid: ChildProcessSpawner.ProcessId(52),
        },
        {
          done: Deferred.succeed(ready, undefined).pipe(Effect.andThen(Effect.never)),
          text: Effect.succeed("error: stream still open"),
        },
      ).pipe(Effect.forkScoped);
      yield* Deferred.await(ready);
      yield* TestClock.adjust("250 millis");
      const error = yield* Fiber.join(fiber);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "error: stream still open");
    }),
  );
});
