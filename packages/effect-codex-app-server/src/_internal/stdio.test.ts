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
      name: "Node launcher error between source and stack frames",
      chunks: [
        "file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\nthrow new Error(message);\n      ^\n\n",
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:9\n\nNode.js v26.0.0\n",
      ],
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "Node error code",
      chunks: [
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules:1:1)\n",
      ],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "last error-labelled line",
      chunks: [
        "Error: earlier failure\nTypeError: invalid configuration\n    at main (main.js:1:1)\n",
      ],
      expected: "TypeError: invalid configuration",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: unexpected argument '--bad' found\n\nUsage: codex app-server\n"],
      expected: "error: unexpected argument '--bad' found",
    },
    {
      name: "fatal error",
      chunks: ["fatal: cannot start\nSee the logs for details\n"],
      expected: "fatal: cannot start",
    },
    {
      name: "last non-empty unlabelled line",
      chunks: ["starting\npermission denied\n\n"],
      expected: "permission denied",
    },
    {
      name: "ANSI escapes, carriage returns and whitespace",
      chunks: [
        "loading\r\u001b[31mError:\t missing",
        "   dependency\u001b[0m\r\n  at launcher.js:1\n",
      ],
      expected: "Error: missing dependency",
    },
    {
      name: "512-character limit",
      chunks: [`Error: ${"x".repeat(1024)}\n`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", chunks: [" \n\r\t\n"], expected: undefined },
    { name: "empty stderr", chunks: [], expected: undefined },
    {
      name: "8 KiB byte tail across chunks",
      chunks: ["Error: discarded\n", "é".repeat(4096), "\nlast line\n"],
      expected: "last line",
    },
    {
      name: "oversized single chunk",
      chunks: [`Error: discarded\n${"x".repeat(8192)}\nlast line\n`],
      expected: "last line",
    },
  ])("includes a bounded stderr diagnostic: $name", ({ chunks, expected }) =>
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
        `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
      );
    }).pipe(Effect.scoped),
  );

  it.effect("waits for trailing stderr but stops waiting after 250 ms if the pipe stays open", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const stderr = yield* captureStderr(
        Stream.concat(
          Stream.make(encoder.encode("starting\n")),
          Stream.fromEffect(
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(encoder.encode("Error: trailing diagnostic\n")),
            ),
          ),
        ),
      );
      yield* Deferred.await(started);
      const pending = yield* makeTerminationError(
        {
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          pid: ChildProcessSpawner.ProcessId(52),
        },
        stderr,
      ).pipe(Effect.forkScoped);
      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* Deferred.succeed(release, undefined);
      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: trailing diagnostic");

      const openStderr = yield* captureStderr(
        Stream.concat(Stream.make(encoder.encode("Error: retained\n")), Stream.never),
      );
      const timed = yield* makeTerminationError(
        {
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          pid: ChildProcessSpawner.ProcessId(52),
        },
        openStderr,
      ).pipe(Effect.forkScoped);
      yield* TestClock.adjust("249 millis");
      assert.isUndefined(timed.pollUnsafe());
      yield* TestClock.adjust("1 millis");
      const timedError = yield* Fiber.join(timed);
      assert.instanceOf(timedError, CodexError.CodexAppServerProcessExitedError);
      assert.equal(timedError.stderr, "Error: retained");
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
