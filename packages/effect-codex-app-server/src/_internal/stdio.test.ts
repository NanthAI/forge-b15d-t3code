import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { makeStderrCapture, makeTerminationError } from "./stdio.ts";

const encoder = new TextEncoder();

describe("Codex App Server child process termination", () => {
  const launcherError =
    "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";

  for (const { name, chunks, expected } of [
    {
      name: "the Node launcher's error between its source excerpt and stack",
      chunks: [
        `file:///prefix/codex.js:107\n  throw new Error(\"Missing optional dependency\");\n        ^\n\n${launcherError}\n    at file:///prefix/codex.js:107:9\n\nNode.js v22.20.0\n`,
      ],
      expected: launcherError,
    },
    {
      name: "the last error-labelled line",
      chunks: [
        "Error: earlier failure\nTypeError: invalid argument\n    at launch (codex.js:1:2)\n",
      ],
      expected: "TypeError: invalid argument",
    },
    {
      name: "a Node error with a code",
      chunks: [
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at launch (codex.js:1:2)\nNode.js v22.20.0\n",
      ],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "a Rust CLI error",
      chunks: ["error: unexpected argument '--foo' found\n\nFor more information, try '--help'.\n"],
      expected: "error: unexpected argument '--foo' found",
    },
    {
      name: "a fatal error",
      chunks: ["fatal: cannot start\nAdditional context\n"],
      expected: "fatal: cannot start",
    },
    {
      name: "the last non-empty line without an error label",
      chunks: ["Starting Codex\nUnable to load configuration\n\n"],
      expected: "Unable to load configuration",
    },
    {
      name: "terminal escapes, carriage returns and repeated whitespace",
      chunks: [
        "Starting\r\u001b[31m  Error:\t missing   dependency\u001b[0m\r\n\u001b[2K  at launch\r\n",
      ],
      expected: "Error: missing dependency",
    },
    {
      name: "the 512-character limit",
      chunks: [`Error: ${"x".repeat(600)}`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "empty stderr", chunks: [], expected: undefined },
    { name: "blank stderr", chunks: [" \t\r\n\n"], expected: undefined },
    {
      name: "the 8 KiB tail of a large chunk",
      chunks: ["Error: discarded\n" + "x".repeat(8 * 1024) + "\nRecent diagnostic\n"],
      expected: "Recent diagnostic",
    },
    {
      name: "an error at the start of the 8 KiB tail",
      chunks: ["Error: discarded\n", "\nError: retained\n", "x".repeat(8 * 1024 - 17)],
      expected: "Error: retained",
    },
    {
      name: "the 8 KiB tail across chunks counted in bytes",
      chunks: [
        "Error: discarded\n",
        "é".repeat(3 * 1024),
        "é".repeat(3 * 1024),
        "\nRecent diagnostic\n",
      ],
      expected: "Recent diagnostic",
    },
    {
      name: "an error split across chunks",
      chunks: ["Er", "ror: missing dep", "endency\n    at launch\n"],
      expected: "Error: missing dependency",
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
        assert.equal(error.pid, 52);
        assert.equal(error.code, 1);
        assert.equal(error.stderr, expected);
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
        );
      }),
    );
  }

  it.effect("waits for stderr that finishes after the process exits", () =>
    Effect.gen(function* () {
      const input = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
      const exited = yield* Deferred.make<void>();
      const stderr = yield* makeStderrCapture(Stream.fromQueue(input));
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Deferred.succeed(exited, undefined).pipe(
            Effect.as(ChildProcessSpawner.ExitCode(1)),
          ),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* Deferred.await(exited);
      assert.isUndefined(pending.pollUnsafe());
      yield* Queue.offer(input, encoder.encode(launcherError));
      yield* Queue.end(input);

      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, launcherError);
    }),
  );

  it.effect("uses the captured tail after 250 ms when stderr stays open", () =>
    Effect.gen(function* () {
      const input = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
      const stderr = yield* makeStderrCapture(Stream.fromQueue(input));
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* Queue.offer(input, encoder.encode(launcherError));
      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* TestClock.adjust("1 millis");

      const error = yield* Fiber.join(pending);
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
