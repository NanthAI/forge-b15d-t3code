import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { makeStderrCapture, makeTerminationError } from "./stdio.ts";

describe("Codex App Server child process termination", () => {
  it.effect("includes a bounded, normalized diagnostic from the stderr tail", () =>
    Effect.gen(function* () {
      const diagnostic =
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";
      const cases = [
        {
          chunks: [
            `file:///prefix/codex.js:107\n  throw new Error(message);\n        ^\n\n${diagnostic}\n    at file:///prefix/codex.js:107:9\n\nNode.js v24.0.0\n`,
          ],
          expected: diagnostic,
        },
        {
          chunks: ["Error: first failure\nTypeError: last failure\n    at launcher.js:1\n"],
          expected: "TypeError: last failure",
        },
        {
          chunks: ["Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at launcher.js:1\n"],
          expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
        },
        {
          chunks: ["error: invalid argument\nUsage: codex app-server\n"],
          expected: "error: invalid argument",
        },
        { chunks: ["fatal: cannot start\nmore details\n"], expected: "fatal: cannot start" },
        { chunks: ["first line\nlast diagnostic\n\n"], expected: "last diagnostic" },
        {
          chunks: [
            "progress\r\u001b[31mError:\t failed   to start\u001b[0m\r\n  at launcher.js:1\n",
          ],
          expected: "Error: failed to start",
        },
        {
          chunks: [
            "\u001b]0;launcher\u0007Error: \u001b]8;;https://example.test\u001b\\missing\u001b]8;;\u001b\\\n",
          ],
          expected: "Error: missing",
        },
        { chunks: [`Error: ${"x".repeat(600)}\n`], expected: `Error: ${"x".repeat(505)}` },
        { chunks: [" \t\r\n\u001b[0m"], expected: undefined },
        { chunks: [], expected: undefined },
        {
          chunks: ["Error: retained across chunks\n", "stack frame\n".repeat(600)],
          expected: "Error: retained across chunks",
        },
        {
          chunks: ["Error: outside tail\n", `${"x".repeat(8 * 1024)}\nlast diagnostic\n`],
          expected: "last diagnostic",
        },
        {
          chunks: [
            "Error: outside tail\n",
            "é".repeat(2048),
            "é".repeat(2048),
            "\nlast diagnostic\n",
          ],
          expected: "last diagnostic",
        },
      ];
      for (const { chunks, expected } of cases) {
        const stderr = yield* makeStderrCapture(
          Stream.fromIterable(chunks.map((chunk) => new TextEncoder().encode(chunk))),
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
        assert.equal(error.pid, 52);
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
        );
      }
    }),
  );

  it.effect("waits for stderr to finish after the process exits", () =>
    Effect.gen(function* () {
      const finalChunk = yield* Deferred.make<Uint8Array>();
      const stderr = yield* makeStderrCapture(Stream.fromEffect(Deferred.await(finalChunk)));
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkScoped);
      yield* TestClock.adjust("100 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* Deferred.succeed(finalChunk, new TextEncoder().encode("Error: final diagnostic\n"));
      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: final diagnostic");
    }),
  );

  it.effect("uses the captured tail after 250 ms if stderr stays open", () =>
    Effect.gen(function* () {
      const captured = yield* Deferred.make<void>();
      const stderr = yield* makeStderrCapture(
        Stream.concat(
          Stream.make(new TextEncoder().encode("fatal: captured diagnostic\n")),
          Stream.fromEffect(
            Deferred.succeed(captured, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        ),
      );
      yield* Deferred.await(captured);
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
      assert.equal(error.stderr, "fatal: captured diagnostic");
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
