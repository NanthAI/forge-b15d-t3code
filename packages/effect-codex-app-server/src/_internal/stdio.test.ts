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
      name: "Node launcher source excerpt and stack",
      stderr:
        'file:///prefix/codex.js:107\n  throw new Error("Missing optional dependency");\n        ^\n\nError: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n    at file:///prefix/codex.js:107:9\n\nNode.js v26.0.0\n',
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "Node error code",
      stderr:
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules:1:1)\n",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "last error label",
      stderr:
        "Error: earlier failure\nTypeError: invalid configuration\n    at main (main.js:1:1)\n",
      expected: "TypeError: invalid configuration",
    },
    {
      name: "Rust CLI error",
      stderr: "error: invalid argument\n\nFor more information, try '--help'.\n",
      expected: "error: invalid argument",
    },
    {
      name: "fatal error",
      stderr: "fatal: failed to start\nSee logs for details\n",
      expected: "fatal: failed to start",
    },
    {
      name: "unlabelled final line",
      stderr: "starting\nUnable to load configuration\n\n",
      expected: "Unable to load configuration",
    },
    {
      name: "ANSI escapes, carriage returns and whitespace",
      stderr: "loading\r\u001b[31mError:\t missing   dependency\u001b[0m\r\n\tat main\r\n",
      expected: "Error: missing dependency",
    },
    {
      name: "512 character cap",
      stderr: `Error: ${"x".repeat(600)}\n    at main\n`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "empty stderr", stderr: "", expected: undefined },
    { name: "blank stderr", stderr: " \t\r\n\n", expected: undefined },
    {
      name: "8 KiB byte tail across chunks",
      stderr: `Error: discarded\n${"é".repeat(4096)}\nlast diagnostic\n`,
      expected: "last diagnostic",
    },
    {
      name: "UTF-8 across chunks",
      stderr: "Error: échec du démarrage\n",
      expected: "Error: échec du démarrage",
    },
  ])("extracts $name", ({ stderr, expected }) =>
    Effect.gen(function* () {
      const bytes = encoder.encode(stderr);
      for (const chunkSize of [8, 16 * 1024]) {
        const chunks = Array.from({ length: Math.ceil(bytes.length / chunkSize) }, (_, index) =>
          bytes.slice(index * chunkSize, (index + 1) * chunkSize),
        );
        const captured = yield* captureStderr(Stream.fromIterable(chunks));
        const error = yield* makeTerminationError(
          {
            pid: ChildProcessSpawner.ProcessId(52),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          },
          captured,
        );

        assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
        if (error._tag !== "CodexAppServerProcessExitedError") return;
        assert.equal(error.pid, 52);
        assert.equal(error.stderr, expected);
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
        );
      }
    }),
  );

  it.effect("uses the captured tail after 250 ms even if stderr stays open", () =>
    Effect.gen(function* () {
      const captured = yield* captureStderr(
        Stream.concat(Stream.make(encoder.encode("Error: startup failed\n")), Stream.never),
      );
      const fiber = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        captured,
      ).pipe(Effect.forkChild);

      yield* TestClock.adjust("249 millis");
      assert.isUndefined(fiber.pollUnsafe());
      yield* TestClock.adjust("1 millis");
      const error = yield* Fiber.join(fiber);
      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: Error: startup failed",
      );
    }),
  );

  it.effect("waits for stderr written after the exit status becomes available", () =>
    Effect.gen(function* () {
      const finish = yield* Deferred.make<void>();
      const captured = yield* captureStderr(
        Stream.fromEffect(
          Deferred.await(finish).pipe(Effect.as(encoder.encode("Error: final diagnostic\n"))),
        ),
      );
      const fiber = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        captured,
      ).pipe(Effect.forkChild);

      yield* TestClock.adjust("100 millis");
      assert.isUndefined(fiber.pollUnsafe());
      yield* Deferred.succeed(finish, undefined);
      const error = yield* Fiber.join(fiber);
      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: Error: final diagnostic",
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
