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
      stderr: [
        "file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107",
        "  throw new Error(message);",
        "        ^",
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
        "    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:9",
        "Node.js v22.0.0",
      ].join("\n"),
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "last labelled error",
      stderr: "Error: earlier\nTypeError: invalid argument\n    at main (codex.js:10:2)",
      expected: "TypeError: invalid argument",
    },
    {
      name: "Node error code",
      stderr:
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules/esm/resolve:1:2)",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "Rust CLI",
      stderr: "error: invalid configuration\nUsage: codex [OPTIONS]",
      expected: "error: invalid configuration",
    },
    {
      name: "fatal error",
      stderr: "fatal: failed to start\nGoodbye",
      expected: "fatal: failed to start",
    },
    {
      name: "unlabelled output",
      stderr: "starting\nCould not load runtime\n\n",
      expected: "Could not load runtime",
    },
    {
      name: "terminal escapes and carriage returns",
      stderr: "working\r\u001b[31mError:\t  failed\u001b[0m\r\n\t at main\r\n",
      expected: "Error: failed",
    },
    {
      name: "512 character cap",
      stderr: `Error: ${"x".repeat(600)}`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", stderr: " \t\r\n", expected: undefined },
    { name: "empty stderr", stderr: "", expected: undefined },
    {
      name: "error at the start of the retained 8 KiB",
      stderr: `old output\nError: retained\n${"x".repeat(8192 - "Error: retained\n".length)}`,
      expected: "Error: retained",
    },
    {
      name: "8 KiB byte tail across chunks",
      stderr: `Error: discarded\n${"é".repeat(8192)}`,
      expected: "é".repeat(512),
    },
  ])("extracts $name", ({ stderr, expected }) =>
    Effect.gen(function* () {
      const bytes = new TextEncoder().encode(stderr);
      const capture = yield* makeStderrCapture(
        Stream.fromIterable([bytes.subarray(0, 8192), bytes.subarray(8192)]),
      );
      const error = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        capture,
      );

      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, expected);
      assert.equal(
        error.message,
        `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
      );
    }),
  );

  it.effect("waits for stderr that finishes after the process exits", () =>
    Effect.gen(function* () {
      const exited = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const capture = yield* makeStderrCapture(
        Stream.fromEffect(
          Deferred.await(release).pipe(
            Effect.as(new TextEncoder().encode("Error: late diagnostic")),
          ),
        ),
      );
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Deferred.succeed(exited, undefined).pipe(
            Effect.as(ChildProcessSpawner.ExitCode(1)),
          ),
        },
        capture,
      ).pipe(Effect.forkScoped);

      yield* Deferred.await(exited);
      yield* Deferred.succeed(release, undefined);
      const error = yield* Fiber.join(termination);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: late diagnostic");
    }),
  );

  it.effect("uses the captured tail after 250 ms when stderr stays open", () =>
    Effect.gen(function* () {
      const consumed = yield* Deferred.make<void>();
      const capture = yield* makeStderrCapture(
        Stream.make(new TextEncoder().encode("Error: failed")).pipe(
          Stream.concat(
            Stream.fromEffect(
              Deferred.succeed(consumed, undefined).pipe(Effect.andThen(Effect.never)),
            ),
          ),
        ),
      );
      yield* Deferred.await(consumed);
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        capture,
      ).pipe(Effect.forkScoped);

      yield* TestClock.adjust("249 millis");
      assert.isUndefined(termination.pollUnsafe());
      yield* TestClock.adjust("1 millis");
      const error = yield* Fiber.join(termination);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: failed");
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
