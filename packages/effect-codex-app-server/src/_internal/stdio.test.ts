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
      name: "Node launcher source excerpt and stack",
      chunks: [
        "file:///opt/codex.js:107\n  throw new Error(message);\n        ^\n\nError: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n    at file:///opt/codex.js:107:9\n\nNode.js v24.0.0\n",
      ],
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "Node error with code",
      chunks: ["Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at loader.js:1:1\n"],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "last error-labelled line",
      chunks: ["Error: first failure\nTypeError: invalid configuration\n    at main.js:1:1\n"],
      expected: "TypeError: invalid configuration",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: invalid value for '--config'\n\nFor more information, try '--help'.\n"],
      expected: "error: invalid value for '--config'",
    },
    {
      name: "fatal error",
      chunks: ["fatal: unable to start\nmore details\n"],
      expected: "fatal: unable to start",
    },
    {
      name: "last non-empty unlabelled line",
      chunks: ["starting\n  launcher unavailable  \n\n"],
      expected: "launcher unavailable",
    },
    {
      name: "terminal escapes, carriage returns and whitespace",
      chunks: ["loading\r\u001b[31mError:\t missing   dependency\u001b[0m\r\n  at main.js:1:1\n"],
      expected: "Error: missing dependency",
    },
    {
      name: "512-character cap",
      chunks: [`Error: ${"x".repeat(1024)}\n`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", chunks: ["\n \t\r\n"], expected: undefined },
    { name: "empty stderr", chunks: [], expected: undefined },
    {
      name: "8 KiB tail across chunks",
      chunks: ["Error: stale failure\n", "é".repeat(4096), "\nlatest diagnostic\n"],
      expected: "latest diagnostic",
    },
    {
      name: "8 KiB tail within one oversized chunk",
      chunks: [`Error: stale failure\n${"x".repeat(8192)}\nlatest diagnostic\n`],
      expected: "latest diagnostic",
    },
    {
      name: "error at the start of the retained 8 KiB tail",
      chunks: [
        "discarded prelude\n",
        "Error: retained diagnostic\n",
        "x".repeat(8192 - "Error: retained diagnostic\n".length),
      ],
      expected: "Error: retained diagnostic",
    },
  ])("includes an excerpt from $name", ({ chunks, expected }) =>
    Effect.gen(function* () {
      const encoder = new TextEncoder();
      const stderr = yield* makeStderrCapture(
        Stream.fromArray(chunks.map((chunk) => encoder.encode(chunk))),
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
    }).pipe(Effect.scoped),
  );

  it.effect("waits for trailing stderr but uses the captured tail after 250 ms", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const stderr = yield* makeStderrCapture(
        Stream.make(new TextEncoder().encode("Error: launch failed\n")).pipe(
          Stream.concat(Stream.fromEffectDrain(Deferred.await(release))),
        ),
      );
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
      assert.equal(error.stderr, "Error: launch failed");

      yield* Deferred.succeed(release, undefined);
      yield* stderr.done;
    }).pipe(Effect.scoped),
  );

  it.effect("includes stderr that finishes after the process exits", () =>
    Effect.gen(function* () {
      const trailing = yield* Deferred.make<Uint8Array>();
      const stderr = yield* makeStderrCapture(Stream.fromEffect(Deferred.await(trailing)));
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* TestClock.adjust("100 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* Deferred.succeed(trailing, new TextEncoder().encode("Error: trailing diagnostic\n"));

      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: trailing diagnostic");
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
