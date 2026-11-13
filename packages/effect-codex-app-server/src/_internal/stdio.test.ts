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
  it.effect.each([
    {
      name: "Node launcher source excerpt and stack frames",
      chunks: [
        "file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n",
        '  throw new Error("Missing optional dependency");\n        ^\n\n',
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n",
        "    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:9\n\nNode.js v24.0.0\n",
      ],
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "last error-labelled line",
      chunks: [
        "Error: first error\nTypeError: Cannot read properties of undefined\n    at main (index.js:1:1)\n",
      ],
      expected: "TypeError: Cannot read properties of undefined",
    },
    {
      name: "Node error with code",
      chunks: ["Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at main (index.js:1:1)\n"],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: unexpected argument '--bad' found\n\nFor more information, try '--help'.\n"],
      expected: "error: unexpected argument '--bad' found",
    },
    {
      name: "fatal error",
      chunks: ["fatal: failed to start\nlauncher stopped\n"],
      expected: "fatal: failed to start",
    },
    {
      name: "last non-empty line",
      chunks: ["starting\nCould not launch app-server\n\n  \n"],
      expected: "Could not launch app-server",
    },
    {
      name: "terminal escapes and carriage returns",
      chunks: ["loading\r\u001b[31mError:\t missing   dependency\u001b[0m\r\n    at main\n"],
      expected: "Error: missing dependency",
    },
    {
      name: "512-character cap",
      chunks: [`Error: ${"x".repeat(600)}\n    at main\n`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", chunks: ["\n \t\r\n"], expected: undefined },
    { name: "empty stderr", chunks: [], expected: undefined },
    {
      name: "8 KiB tail across chunks",
      chunks: ["Error: stale\n", "x".repeat(8 * 1024 - 1), "\nlatest diagnostic\n"],
      expected: "latest diagnostic",
    },
    {
      name: "8 KiB tail in one large chunk",
      chunks: [`Error: stale\n${"x".repeat(8 * 1024)}\nlatest diagnostic\n`],
      expected: "latest diagnostic",
    },
    {
      name: "tail bounded by bytes",
      chunks: ["Error: stale\n", "é".repeat(4 * 1024), "\nlatest diagnostic\n"],
      expected: "latest diagnostic",
    },
  ])("reports $name", ({ chunks, expected }) =>
    Effect.gen(function* () {
      const stderr = yield* makeStderrCapture({
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
    }),
  );

  it.effect("waits for stderr to finish after exit", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<Uint8Array>();
      const exited = yield* Deferred.make<void>();
      const stderr = yield* makeStderrCapture({
        stderr: Stream.fromEffect(Deferred.await(release)),
      });
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Deferred.succeed(exited, undefined).pipe(
            Effect.as(ChildProcessSpawner.ExitCode(1)),
          ),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* Deferred.await(exited);
      yield* Deferred.succeed(release, new TextEncoder().encode("Error: final diagnostic\n"));
      const error = yield* Fiber.join(termination);
      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: Error: final diagnostic",
      );
    }),
  );

  it.effect("uses captured stderr after 250 ms if the stream stays open", () =>
    Effect.gen(function* () {
      const consumed = yield* Deferred.make<void>();
      const finished = yield* Deferred.make<void>();
      const stderr = yield* makeStderrCapture({
        stderr: Stream.make(new TextEncoder().encode("Error: partial diagnostic\n")).pipe(
          Stream.concat(
            Stream.fromEffect(
              Deferred.succeed(consumed, undefined).pipe(Effect.andThen(Effect.never)),
            ),
          ),
        ),
      });
      yield* Deferred.await(consumed);
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(
        Effect.tap(() => Deferred.succeed(finished, undefined)),
        Effect.forkScoped,
      );

      yield* TestClock.adjust("249 millis");
      assert.isFalse(yield* Deferred.isDone(finished));
      yield* TestClock.adjust("1 millis");
      const error = yield* Fiber.join(termination);
      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: Error: partial diagnostic",
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
