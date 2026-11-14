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

describe("Codex App Server child process termination", () => {
  it.effect.each([
    {
      name: "Node launcher error between a source excerpt and stack frames",
      chunks: [
        "file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n" +
          '  throw new Error("Missing optional dependency");\n' +
          "        ^\n\n" +
          "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n" +
          "    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:9\n\n" +
          "Node.js v24.0.0\n",
      ],
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "Node error with a code",
      chunks: [
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:module:1:1)\n",
      ],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "last error-labelled line",
      chunks: ["Error: first failure\nTypeError: final failure\n    at launcher.js:1:1\n"],
      expected: "TypeError: final failure",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: invalid value for '--config'\n\nFor more information, try '--help'.\n"],
      expected: "error: invalid value for '--config'",
    },
    {
      name: "fatal error",
      chunks: ["fatal: unable to start\ncleanup complete\n"],
      expected: "fatal: unable to start",
    },
    {
      name: "last non-empty line without a label",
      chunks: ["starting\n  unable to load configuration  \n\n"],
      expected: "unable to load configuration",
    },
    {
      name: "ANSI escapes, carriage returns and whitespace",
      chunks: [
        "loading\r\u001b[31mError:\t failed   to start\u001b[0m\r\n\u001b]0;launcher\u0007\n",
      ],
      expected: "Error: failed to start",
    },
    {
      name: "512-character cap",
      chunks: [`Error: ${"x".repeat(600)}\n    at launcher.js:1:1\n`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    {
      name: "blank stderr",
      chunks: ["\n\r\t \u001b[0m\n"],
      expected: undefined,
    },
    {
      name: "8 KiB tail of a large chunk",
      chunks: [`Error: discarded\n${"x".repeat(8 * 1024)}\nlast diagnostic\n`],
      expected: "last diagnostic",
    },
    {
      name: "8 KiB tail across chunks measured in bytes",
      chunks: ["Error: discarded\n", "é".repeat(4 * 1024), "\nlast diagnostic\n"],
      expected: "last diagnostic",
    },
    {
      name: "UTF-8 text and labels split across chunks",
      chunks: ["noise\nErr", "or: démarrage impossible\n", "    at launcher.js:1:1\n"],
      expected: "Error: démarrage impossible",
    },
  ])("includes a sanitized stderr excerpt: $name", ({ chunks, expected }) =>
    Effect.gen(function* () {
      const stderr = yield* captureStderr(
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
      assert.equal(error.stderr, expected);
      assert.equal(
        error.message,
        `Codex App Server process exited with code 1${expected === undefined ? "" : `: ${expected}`}`,
      );
    }),
  );

  it.effect("waits for stderr to finish after the process exits", () =>
    Effect.gen(function* () {
      const reading = yield* Deferred.make<void>();
      const finish = yield* Deferred.make<void>();
      const stderr = yield* captureStderr(
        Stream.fromEffect(
          Deferred.succeed(reading, undefined).pipe(
            Effect.andThen(Deferred.await(finish)),
            Effect.as(encoder.encode("Error: final diagnostic\n")),
          ),
        ),
      );
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* Deferred.await(reading);
      assert.isUndefined(pending.pollUnsafe());
      yield* Deferred.succeed(finish, undefined);
      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: final diagnostic");
    }),
  );

  it.effect("uses the captured tail after 250 ms even if stderr stays open", () =>
    Effect.gen(function* () {
      const reading = yield* Deferred.make<void>();
      const stderr = yield* captureStderr(
        Stream.concat(
          Stream.make(encoder.encode("Error: launcher failed\n")),
          Stream.fromEffect(
            Deferred.succeed(reading, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        ),
      );
      yield* Deferred.await(reading);
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* TestClock.adjust("1 millis");
      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: launcher failed");
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
