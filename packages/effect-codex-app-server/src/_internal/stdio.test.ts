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

const encoder = new TextEncoder();
const exited = {
  pid: ChildProcessSpawner.ProcessId(52),
  exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
};

describe("Codex App Server child process termination", () => {
  const launcherError =
    "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";
  for (const [name, input, expected] of [
    [
      "Node launcher",
      `file:///prefix/codex.js:107\n  throw new Error(message);\n        ^\n\n${launcherError}\n    at file:///prefix/codex.js:107:9\n\nNode.js v24.0.0\n`,
      launcherError,
    ],
    [
      "last labelled error",
      "Error: first\nTypeError: last\n    at launcher.js:1:1",
      "TypeError: last",
    ],
    [
      "Node error code",
      "Error [ERR_MODULE_NOT_FOUND]: Cannot find module\n    at launcher.js:1:1",
      "Error [ERR_MODULE_NOT_FOUND]: Cannot find module",
    ],
    ["Rust CLI", "error: invalid argument\nUsage: codex [OPTIONS]", "error: invalid argument"],
    ["fatal", "fatal: cannot start\nSee logs for details", "fatal: cannot start"],
    ["unlabelled", "starting\nCannot load the launcher\n\n", "Cannot load the launcher"],
    [
      "terminal escapes and whitespace",
      "progress\r\u001b[31mError:\t missing   dependency\u001b[0m\r\n    at launcher.js:1:1",
      "Error: missing dependency",
    ],
    ["length limit", `Error: ${"x".repeat(600)}`, `Error: ${"x".repeat(505)}`],
    ["blank", " \n\t\r\n", undefined],
    ["empty", "", undefined],
    ["8 KiB tail", `Error: discarded\n${"x".repeat(8192)}\nlast line`, "last line"],
  ] as const) {
    it.effect(`extracts bounded diagnostics from ${name} stderr`, () =>
      Effect.gen(function* () {
        const stderr = yield* makeStderrCapture(Stream.succeed(encoder.encode(input)));
        const error = yield* makeTerminationError(exited, stderr);

        assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
        assert.equal(error.stderr, expected);
        assert.equal(error.pid, 52);
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
        );
      }).pipe(Effect.scoped),
    );
  }

  it.effect("bounds the tail across chunks without splitting a retained UTF-8 character", () =>
    Effect.gen(function* () {
      const bytes = encoder.encode(`Error: discarded\n${"x".repeat(8192)}\nError: café`);
      const stderr = yield* makeStderrCapture(
        Stream.make(bytes.slice(0, 4096), bytes.slice(4096, -1), bytes.slice(-1)),
      );
      const error = yield* makeTerminationError(exited, stderr);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: café");
    }).pipe(Effect.scoped),
  );

  it.effect("waits for trailing stderr after the process exits", () =>
    Effect.gen(function* () {
      const finished = yield* Deferred.make<Uint8Array>();
      const stderr = yield* makeStderrCapture(Stream.fromEffect(Deferred.await(finished)));
      const termination = yield* makeTerminationError(exited, stderr).pipe(Effect.forkScoped);
      yield* TestClock.adjust("249 millis");
      assert.equal(termination.pollUnsafe(), undefined);
      yield* Deferred.succeed(finished, encoder.encode(launcherError));
      const error = yield* Fiber.join(termination);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, launcherError);
    }).pipe(Effect.scoped),
  );

  it.effect("uses the captured tail after 250 ms even if stderr stays open", () =>
    Effect.gen(function* () {
      const captured = yield* Deferred.make<void>();
      const stderr = yield* makeStderrCapture(
        Stream.concat(
          Stream.succeed(encoder.encode(launcherError)),
          Stream.fromEffect(
            Deferred.succeed(captured, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        ),
      );
      yield* Deferred.await(captured);
      const termination = yield* makeTerminationError(exited, stderr).pipe(Effect.forkScoped);
      yield* TestClock.adjust("250 millis");
      const error = yield* Fiber.join(termination);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, launcherError);
    }).pipe(Effect.scoped),
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
      const error = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.fail(cause),
        },
        Effect.never,
      );

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
