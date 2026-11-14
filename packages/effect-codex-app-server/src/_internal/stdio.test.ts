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
      name: "Node launcher error between source and stack frames",
      chunks: [
        'file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\nthrow new Error("Missing optional dependency");\n      ^\n\n',
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:9\n\nNode.js v26.12.1\n",
      ],
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "Node error code",
      chunks: [
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules:10:2)\n",
      ],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "TypeError",
      chunks: ["TypeError: Invalid argument\n    at main (file:///codex.js:1:2)\n"],
      expected: "TypeError: Invalid argument",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: unexpected argument '--bad'\n\nUsage: codex app-server\n"],
      expected: "error: unexpected argument '--bad'",
    },
    {
      name: "last labelled error",
      chunks: ["Error: first failure\nfatal: final failure\nshutdown complete\n"],
      expected: "fatal: final failure",
    },
    {
      name: "last non-empty line",
      chunks: ["starting\nUnable to launch Codex\n \n"],
      expected: "Unable to launch Codex",
    },
    {
      name: "terminal escapes, carriage returns and whitespace",
      chunks: [
        "loading\r\u001b[31mError:\t Missing   dependency\u001b[0m\r\n\u001b]0;title\u0007\n",
      ],
      expected: "Error: Missing dependency",
    },
    {
      name: "512-character cap",
      chunks: [`Error: ${"x".repeat(600)}\nstack frame\n`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    {
      name: "blank stderr",
      chunks: [" \t\r\n\u001b[0m"],
      expected: undefined,
    },
    {
      name: "empty stderr",
      chunks: [],
      expected: undefined,
    },
    {
      name: "8 KiB byte tail across chunks",
      chunks: ["Error: discarded\n", "é".repeat(4096), "\nlast diagnostic\n"],
      expected: "last diagnostic",
    },
    {
      name: "8 KiB byte tail within a chunk",
      chunks: [`Error: discarded\n${"é".repeat(4096)}\nlast diagnostic\n`],
      expected: "last diagnostic",
    },
  ])("includes a bounded stderr excerpt: $name", ({ chunks, expected }) =>
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

  it.effect("waits for stderr to finish after exit", () =>
    Effect.gen(function* () {
      const output = yield* Deferred.make<Uint8Array>();
      const stderr = yield* makeStderrCapture(Stream.fromEffect(Deferred.await(output)));
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* Deferred.succeed(output, new TextEncoder().encode("Error: delayed diagnostic\n"));
      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: delayed diagnostic");
    }),
  );

  it.effect("uses captured stderr after 250 ms when the stream stays open", () =>
    Effect.gen(function* () {
      const stderr = yield* makeStderrCapture(
        Stream.concat(
          Stream.make(new TextEncoder().encode("fatal: launch failed\n")),
          Stream.never,
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
      assert.equal(error.stderr, "fatal: launch failed");
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
