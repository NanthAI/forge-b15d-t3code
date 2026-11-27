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
      name: "Node launcher source and stack frames",
      chunks: [
        "file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n",
        '  throw new Error("Missing optional dependency");\n        ^\n\n',
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n",
        "    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:9\n\nNode.js v22.0.0\n",
      ],
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "Node error code",
      chunks: [
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find module\n    at resolve (node:internal/modules:1:1)\n",
      ],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find module",
    },
    {
      name: "last error-labelled line",
      chunks: ["Error: earlier failure\nTypeError: invalid argument\n    at main (main.js:1:1)\n"],
      expected: "TypeError: invalid argument",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: invalid value for --config\n\nFor more information, try '--help'.\n"],
      expected: "error: invalid value for --config",
    },
    {
      name: "fatal error and terminal escapes",
      chunks: ["progress\r\u001b[31mfatal:\t could   not start\u001b[0m\r\nhelp text\n"],
      expected: "fatal: could not start",
    },
    {
      name: "last unlabelled non-empty line",
      chunks: ["starting\n\n  permission\t denied   \n\n"],
      expected: "permission denied",
    },
    {
      name: "512-character cap",
      chunks: [`Error: ${"x".repeat(600)}`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", chunks: ["\n\t \r\n\u001b[0m"], expected: undefined },
    { name: "empty stderr", chunks: [], expected: undefined },
    {
      name: "8 KiB tail across chunks",
      chunks: ["Error: discarded\n", "é".repeat(2048), "é".repeat(2048), "\nlast line\n"],
      expected: "last line",
    },
    {
      name: "8 KiB tail in one large chunk",
      chunks: [`Error: discarded\n${"x".repeat(16 * 1024)}\nlast line\n`],
      expected: "last line",
    },
  ])("includes a bounded diagnostic from $name", ({ chunks, expected }) =>
    Effect.gen(function* () {
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
      assert.equal(error.pid, 52);
      assert.equal(error.stderr, expected);
      assert.equal(
        error.message,
        `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
      );
    }),
  );

  it.effect("bounds the wait for stderr that remains open after exit", () =>
    Effect.gen(function* () {
      const drained = yield* Deferred.make<void>();
      const stderr = yield* makeStderrCapture(
        Stream.make(new TextEncoder().encode("Error: failed to launch\n")).pipe(
          Stream.concat(
            Stream.fromEffect(
              Deferred.succeed(drained, undefined).pipe(Effect.andThen(Effect.never)),
            ),
          ),
        ),
      );
      yield* Deferred.await(drained);
      const errorFiber = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkChild);
      yield* TestClock.adjust("250 millis");
      const error = yield* Fiber.join(errorFiber);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: failed to launch");
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
