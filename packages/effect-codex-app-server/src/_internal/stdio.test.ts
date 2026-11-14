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
  for (const [name, text, expected] of [
    [
      "Node launcher error",
      `file:///codex.js:107\n  throw new Error(message);\n        ^\n\n${diagnostic}\n    at file:///codex.js:107:9\n\nNode.js v26.0.0\n`,
      diagnostic,
    ],
    [
      "last error-labelled line",
      "Error: earlier\nTypeError: invalid argument\n    at main (file:///codex.js:1:1)",
      "TypeError: invalid argument",
    ],
    [
      "Node error code",
      "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules:1:1)",
      "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    ],
    [
      "Rust CLI error",
      "error: unexpected argument\nUsage: codex app-server",
      "error: unexpected argument",
    ],
    ["fatal error", "fatal: could not start\nFor help, run codex --help", "fatal: could not start"],
    ["unlabelled diagnostic", "Starting codex\n  could not start  \n\n", "could not start"],
    [
      "terminal escapes and whitespace",
      "loading\r\u001b[31mError:\t Missing   dependency\u001b[0m\r\n   at main\n",
      "Error: Missing dependency",
    ],
    ["512-character cap", `Error: ${"x".repeat(600)}`, `Error: ${"x".repeat(505)}`],
    ["blank stderr", "\r\n \t\n", undefined],
    ["8 KiB tail", `Error: discarded\n${"é".repeat(4096)}\nlast diagnostic`, "last diagnostic"],
  ] as const) {
    it.effect(`includes a bounded stderr excerpt: ${name}`, () =>
      Effect.gen(function* () {
        const encoder = new TextEncoder();
        const stderr = yield* captureStderr(
          Stream.fromIterable([encoder.encode(text.slice(0, 10)), encoder.encode(text.slice(10))]),
        );
        const error = yield* makeTerminationError(
          {
            pid: ChildProcessSpawner.ProcessId(52),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          },
          stderr.read,
        );

        assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
        assert.equal(error.pid, 52);
        assert.equal(error.stderr, expected);
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
        );
      }).pipe(Effect.scoped),
    );
  }

  it.effect("waits for stderr to finish after the process exits", () =>
    Effect.gen(function* () {
      const exited = yield* Deferred.make<void>();
      const output = yield* Deferred.make<Uint8Array>();
      const stderr = yield* captureStderr(Stream.fromEffect(Deferred.await(output)));
      const result = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Deferred.succeed(exited, undefined).pipe(
            Effect.as(ChildProcessSpawner.ExitCode(1)),
          ),
        },
        stderr.read,
      ).pipe(Effect.forkChild);

      yield* Deferred.await(exited);
      yield* Deferred.succeed(output, new TextEncoder().encode(diagnostic));
      const error = yield* Fiber.join(result);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, diagnostic);
    }).pipe(Effect.scoped),
  );

  it.effect("uses the captured tail after 250 ms if stderr stays open", () =>
    Effect.gen(function* () {
      const captured = yield* Deferred.make<void>();
      const stderr = yield* captureStderr(
        Stream.concat(
          Stream.make(new TextEncoder().encode(diagnostic)),
          Stream.fromEffect(
            Deferred.succeed(captured, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        ),
      );
      yield* Deferred.await(captured);
      const result = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr.read,
      ).pipe(Effect.forkChild);
      yield* TestClock.adjust("250 millis");
      const error = yield* Fiber.join(result);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, diagnostic);
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
