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
  const diagnostic =
    "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";

  it.effect.each([
    {
      name: "Node launcher error between source and stack frames",
      stderr: `file:///codex.js:107\n  throw new Error(message);\n        ^\n\n${diagnostic}\n    at file:///codex.js:107:9\nNode.js v26.0.0\n`,
      expected: diagnostic,
    },
    {
      name: "Node error code",
      stderr:
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules/esm/resolve:1:1)",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "last error-labelled line",
      stderr: "Error: first\nTypeError: last\n    at launch (codex.js:1:1)",
      expected: "TypeError: last",
    },
    {
      name: "Rust CLI error",
      stderr: "error: invalid option\nUsage: codex",
      expected: "error: invalid option",
    },
    {
      name: "fatal error",
      stderr: "fatal: unable to start\ncleanup complete",
      expected: "fatal: unable to start",
    },
    {
      name: "unlabelled message",
      stderr: "starting\nlauncher unavailable\n  \n",
      expected: "launcher unavailable",
    },
    {
      name: "terminal escapes and whitespace",
      stderr: "loading\r\u001b[31mError:\t bad   install\u001b[0m\r\n",
      expected: "Error: bad install",
    },
    {
      name: "512-character cap",
      stderr: `Error: ${"x".repeat(600)}`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", stderr: " \r\n\t", expected: undefined },
    {
      name: "8 KiB tail",
      stderr: `Error: discarded\n${"x".repeat(8 * 1024)}\nlast line`,
      expected: "last line",
    },
  ])("extracts $name", ({ stderr, expected }) =>
    Effect.gen(function* () {
      const bytes = new TextEncoder().encode(stderr);
      const excerpt = yield* captureStderr(
        Stream.fromIterable([bytes.subarray(0, 5), bytes.subarray(5)]),
      );
      const error = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        excerpt,
      );

      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, expected);
      assert.equal(
        error.message,
        `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
      );
    }).pipe(Effect.scoped),
  );

  it.effect("waits for final stderr after exit status is available", () =>
    Effect.gen(function* () {
      const finished = yield* Deferred.make<Uint8Array>();
      const exited = yield* Deferred.make<void>();
      const excerpt = yield* captureStderr(Stream.fromEffect(Deferred.await(finished)));
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Deferred.succeed(exited, undefined).pipe(
            Effect.as(ChildProcessSpawner.ExitCode(1)),
          ),
        },
        excerpt,
      ).pipe(Effect.forkScoped);
      yield* Deferred.await(exited);
      yield* Deferred.succeed(finished, new TextEncoder().encode(diagnostic));
      const error = yield* Fiber.join(termination);
      assert.equal(error.message, `Codex App Server process exited with code 1: ${diagnostic}`);
    }).pipe(Effect.scoped),
  );

  it.effect("uses the captured tail after 250 ms when stderr stays open", () =>
    Effect.gen(function* () {
      const captured = yield* Deferred.make<void>();
      const stderr = Stream.make(new TextEncoder().encode(diagnostic)).pipe(
        Stream.concat(
          Stream.fromEffect(
            Deferred.succeed(captured, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        ),
      );
      const excerpt = yield* captureStderr(stderr);
      yield* Deferred.await(captured);
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        excerpt,
      ).pipe(Effect.forkScoped);
      yield* TestClock.adjust("250 millis");
      const error = yield* Fiber.join(termination);
      assert.equal(error.message, `Codex App Server process exited with code 1: ${diagnostic}`);
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
