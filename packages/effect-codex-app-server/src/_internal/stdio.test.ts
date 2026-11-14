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

describe("Codex App Server child process termination", () => {
  const launcherError =
    "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";
  const cases = [
    {
      name: "Node launcher error between source excerpt and stack frames",
      chunks: [
        'file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n  throw new Error("Missing optional dependency");\n        ^\n\n',
        `${launcherError}\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:11\n\nNode.js v24.0.0\n`,
      ],
      expected: launcherError,
    },
    {
      name: "last error-labelled line",
      chunks: ["Error: earlier failure\nTypeError: invalid argument\n    at launcher.js:1\n"],
      expected: "TypeError: invalid argument",
    },
    {
      name: "Node error code",
      chunks: ["Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at launcher.js:1\n"],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: invalid option\nUsage: codex\n"],
      expected: "error: invalid option",
    },
    {
      name: "fatal error",
      chunks: ["fatal: cannot launch\nMore information\n"],
      expected: "fatal: cannot launch",
    },
    {
      name: "unlabelled message",
      chunks: ["starting\nlauncher could not start\n\n"],
      expected: "launcher could not start",
    },
    {
      name: "ANSI and carriage returns",
      chunks: ["progress\r\u001b[31mError:\t cannot  launch\u001b[0m\r\n    at launcher.js:1\n"],
      expected: "Error: cannot launch",
    },
    {
      name: "512 character cap",
      chunks: [`Error: ${"x".repeat(600)}`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", chunks: ["\n \t\r\u001b[0m"], expected: undefined },
    {
      name: "8 KiB tail across chunks",
      chunks: ["Error: discarded\n", "x".repeat(8192), "\nlast line\n"],
      expected: "last line",
    },
    {
      name: "8 KiB tail in one chunk",
      chunks: [`Error: discarded\n${"x".repeat(8192)}\nlast line\n`],
      expected: "last line",
    },
    {
      name: "tail bounded by bytes",
      chunks: ["Error: discarded\n", "é".repeat(4096), "\nlast line\n"],
      expected: "last line",
    },
  ];

  for (const { name, chunks, expected } of cases) {
    it.effect(name, () =>
      Effect.gen(function* () {
        const stderr = yield* captureStderr({
          stderr: Stream.fromIterable(chunks.map((chunk) => new TextEncoder().encode(chunk))),
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
      }).pipe(Effect.scoped),
    );
  }

  it.effect("waits for stderr to finish after exit", () =>
    Effect.gen(function* () {
      const completed = yield* Deferred.make<Uint8Array>();
      const stderr = yield* captureStderr({ stderr: Stream.fromEffect(Deferred.await(completed)) });
      const termination = yield* makeTerminationError(
        {
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          pid: ChildProcessSpawner.ProcessId(52),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      assert.isUndefined(termination.pollUnsafe());
      yield* Deferred.succeed(completed, new TextEncoder().encode(launcherError));
      const error = yield* Fiber.join(termination);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, launcherError);
    }).pipe(Effect.scoped),
  );

  it.effect("uses captured stderr after 250 ms when stderr stays open", () =>
    Effect.gen(function* () {
      const drained = yield* Deferred.make<void>();
      const stderr = yield* captureStderr({
        stderr: Stream.concat(
          Stream.make(new TextEncoder().encode(launcherError)),
          Stream.fromEffect(
            Deferred.succeed(drained, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        ),
      });
      yield* Deferred.await(drained);
      const termination = yield* makeTerminationError(
        {
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          pid: ChildProcessSpawner.ProcessId(52),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* TestClock.adjust("249 millis");
      assert.isUndefined(termination.pollUnsafe());
      yield* TestClock.adjust("1 millis");
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
