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
  const encoder = new TextEncoder();
  const diagnostic =
    "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";

  for (const { name, chunks, expected } of [
    {
      name: "Node launcher error between a source excerpt and stack frames",
      chunks: [
        `file:///prefix/codex.js:107\n  throw new Error(message);\n        ^\n\n${diagnostic}\n    at file:///prefix/codex.js:107:9\n\nNode.js v22.20.0\n`,
      ],
      expected: diagnostic,
    },
    {
      name: "last error-labelled line",
      chunks: ["Error: first\nTypeError: invalid argument\n    at launcher.js:1:2\n"],
      expected: "TypeError: invalid argument",
    },
    {
      name: "Node error with a code",
      chunks: ["Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at launcher.js:1:2\n"],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: unexpected argument\nUsage: codex app-server\n"],
      expected: "error: unexpected argument",
    },
    {
      name: "fatal error",
      chunks: ["fatal: cannot start\nSee logs for details\n"],
      expected: "fatal: cannot start",
    },
    {
      name: "last non-empty line without an error label",
      chunks: ["starting\nlauncher could not start\n\n"],
      expected: "launcher could not start",
    },
    {
      name: "terminal escapes, carriage returns and whitespace",
      chunks: ["loading\r\u001b[31mError:\t Missing   dependency\u001b[0m\r\n at launcher.js:1:2"],
      expected: "Error: Missing dependency",
    },
    {
      name: "512-character cap",
      chunks: [`Error: ${"x".repeat(600)}`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "empty stderr", chunks: [], expected: undefined },
    { name: "blank stderr", chunks: [" \r\n\t\n"], expected: undefined },
    {
      name: "8 KiB tail from one large chunk",
      chunks: [`Error: discarded\n${"x".repeat(8192)}\nlast line\n`],
      expected: "last line",
    },
    {
      name: "8 KiB tail across chunks measured in bytes",
      chunks: [
        "Error: discarded\n",
        ...Array.from({ length: 4 }, () => "é".repeat(1024)),
        "\nlast line\n",
      ],
      expected: "last line",
    },
    {
      name: "error split across chunks",
      chunks: [
        "file:///launcher.js:107\nEr",
        "ror: Missing optional ",
        "dependency\n at launcher.js:107:9",
      ],
      expected: "Error: Missing optional dependency",
    },
  ]) {
    it.effect(`includes ${name}`, () =>
      Effect.gen(function* () {
        const stderr = yield* makeStderrCapture(
          Stream.make(...chunks.map((chunk) => encoder.encode(chunk))),
        );
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

  it.effect("waits for stderr to finish after process exit", () =>
    Effect.gen(function* () {
      const output = yield* Deferred.make<Uint8Array>();
      const stderr = yield* makeStderrCapture(Stream.fromEffect(Deferred.await(output)));
      const error = yield* makeTerminationError(
        {
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          pid: ChildProcessSpawner.ProcessId(52),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* TestClock.adjust("100 millis");
      assert.isUndefined(error.pollUnsafe());
      yield* Deferred.succeed(output, encoder.encode(diagnostic));
      const result = yield* Fiber.join(error);
      assert.instanceOf(result, CodexError.CodexAppServerProcessExitedError);
      assert.equal(result.stderr, diagnostic);
    }),
  );

  it.effect("uses captured stderr after 250 ms if the stream stays open", () =>
    Effect.gen(function* () {
      const consumed = yield* Deferred.make<void>();
      const stderr = yield* makeStderrCapture(
        Stream.make(encoder.encode(diagnostic)).pipe(
          Stream.tap(() => Deferred.succeed(consumed, undefined)),
          Stream.concat(Stream.never),
        ),
      );
      yield* Deferred.await(consumed);
      const error = yield* makeTerminationError(
        {
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          pid: ChildProcessSpawner.ProcessId(52),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* TestClock.adjust("249 millis");
      assert.isUndefined(error.pollUnsafe());
      yield* TestClock.adjust("1 millis");
      const result = yield* Fiber.join(error);
      assert.instanceOf(result, CodexError.CodexAppServerProcessExitedError);
      assert.equal(result.stderr, diagnostic);
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
