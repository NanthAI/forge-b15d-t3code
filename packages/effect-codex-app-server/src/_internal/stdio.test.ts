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

describe("Codex App Server child process termination", () => {
  for (const { name, chunks, expected } of [
    {
      name: "the Node launcher error between its source excerpt and stack",
      chunks: [
        "file:///opt/codex.js:107\n    throw new Error(message);\n          ^\n\n",
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n    at file:///opt/codex.js:107:11\n\nNode.js v22.0.0\n",
      ],
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "the last error-labelled line",
      chunks: ["Error: earlier failure\nType", "Error: invalid value\n    at main (app.js:1:1)"],
      expected: "TypeError: invalid value",
    },
    {
      name: "a Node error with a code",
      chunks: ["Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (app.js:1:1)\n"],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "a Rust CLI error",
      chunks: ["error: unrecognized option\n\nUsage: codex app-server\n"],
      expected: "error: unrecognized option",
    },
    {
      name: "a fatal error",
      chunks: ["fatal: failed to start\nSee the log for details.\n"],
      expected: "fatal: failed to start",
    },
    {
      name: "the last non-empty line when no error label exists",
      chunks: ["starting\nUnable to launch the app-server\n\n  \n"],
      expected: "Unable to launch the app-server",
    },
    {
      name: "terminal escapes, carriage returns and whitespace",
      chunks: ["loading\r\u001b[31mError:\t  bad   config\u001b[0m\r\n at main\n"],
      expected: "Error: bad config",
    },
    {
      name: "a diagnostic capped at 512 characters",
      chunks: [`Error: ${"x".repeat(1000)}`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    {
      name: "blank stderr",
      chunks: ["\n \t\r\n"],
      expected: undefined,
    },
    {
      name: "empty stderr",
      chunks: [],
      expected: undefined,
    },
    {
      name: "only the last 8 KiB across chunks",
      chunks: ["Error: discarded\n", "é".repeat(2048), "é".repeat(2048)],
      expected: "é".repeat(512),
    },
    {
      name: "only the last 8 KiB of a large chunk",
      chunks: [`Error: discarded\n${"x".repeat(8192)}`],
      expected: "x".repeat(512),
    },
  ]) {
    it.effect(`includes ${name}`, () =>
      Effect.gen(function* () {
        const stderr = yield* makeStderrCapture(
          Stream.fromIterable(chunks.map((chunk) => encoder.encode(chunk))),
        );
        const error = yield* makeTerminationError(
          {
            pid: ChildProcessSpawner.ProcessId(52),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          },
          stderr,
        );

        assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
        assert.equal(error.code, 1);
        assert.equal(error.pid, 52);
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
      const exited = yield* Deferred.make<void>();
      const output = yield* Deferred.make<Uint8Array>();
      const stderr = yield* makeStderrCapture(Stream.fromEffect(Deferred.await(output)));
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
      yield* Deferred.succeed(output, encoder.encode("Error: final diagnostic"));
      const error = yield* Fiber.join(termination);

      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: final diagnostic");
    }),
  );

  it.effect("returns buffered stderr after 250 ms even if the stream stays open", () =>
    Effect.gen(function* () {
      const captured = yield* Deferred.make<void>();
      const stderr = yield* makeStderrCapture(
        Stream.make(encoder.encode("fatal: launch failed")).pipe(
          Stream.concat(
            Stream.fromEffect(Deferred.succeed(captured, undefined)).pipe(
              Stream.flatMap(() => Stream.never),
            ),
          ),
        ),
      );
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
      assert.equal(error.stderr, "fatal: launch failed");
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
