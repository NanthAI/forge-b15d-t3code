import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { captureStderr, makeTerminationError } from "./stdio.ts";

describe("Codex App Server child process termination", () => {
  const launcherError =
    "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";
  const cases = [
    {
      name: "Node launcher error between source and stack frames",
      stderr: `file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n    throw new Error(message);\n          ^\n\n${launcherError}\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:11\n\nNode.js v22.0.0\n`,
      expected: launcherError,
    },
    {
      name: "Node error code",
      stderr:
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules:1:1)\n",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "TypeError",
      stderr: "TypeError: invalid argument\n    at main\n",
      expected: "TypeError: invalid argument",
    },
    {
      name: "Rust CLI error",
      stderr: "error: invalid configuration\nUsage: codex app-server\n",
      expected: "error: invalid configuration",
    },
    {
      name: "last labelled error",
      stderr: "Error: first\nfatal: could not start\ncleanup finished\n",
      expected: "fatal: could not start",
    },
    {
      name: "unlabelled message",
      stderr: "starting\nlauncher could not start\n \n",
      expected: "launcher could not start",
    },
    {
      name: "terminal escapes and carriage returns",
      stderr: "progress\r\u001b[31mError:\t missing   dependency\u001b[0m\r\n    at main\n",
      expected: "Error: missing dependency",
    },
    {
      name: "512 character limit",
      stderr: `Error: ${"x".repeat(600)}`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", stderr: " \r\n\t", expected: undefined },
    { name: "empty stderr", stderr: "", expected: undefined },
  ];

  for (const { name, stderr, expected } of cases) {
    it.effect(`reports ${name}`, () =>
      Effect.gen(function* () {
        const excerpt = yield* captureStderr(Stream.make(Buffer.from(stderr)));
        const error = yield* makeTerminationError(
          {
            pid: ChildProcessSpawner.ProcessId(52),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          },
          excerpt,
        );

        assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
        if (error._tag !== "CodexAppServerProcessExitedError") return;
        assert.equal(error.stderr, expected);
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
        );
      }),
    );
  }

  it.effect("keeps only the last 8 KiB across chunks, including split UTF-8", () =>
    Effect.gen(function* () {
      const bytes = Buffer.from(`Error: old diagnostic\n${"x".repeat(8192)}\nError: café\n`);
      const excerpt = yield* captureStderr(
        Stream.make(bytes.subarray(0, 30), bytes.subarray(30, -2), bytes.subarray(-2)),
      );
      assert.equal(yield* excerpt, "Error: café");

      const discarded = yield* captureStderr(
        Stream.make(
          Buffer.from("Error: old diagnostic\n"),
          Buffer.from(`${"é".repeat(4096)}\nlast line\n`),
        ),
      );
      assert.equal(yield* discarded, "last line");
    }),
  );

  it.effect("waits for trailing stderr after exit and bounds the wait for an open stream", () =>
    Effect.gen(function* () {
      const ending = yield* Deferred.make<void>();
      const excerpt = yield* captureStderr(
        Stream.concat(
          Stream.make(Buffer.from("Error: initial\n")),
          Stream.fromEffect(Deferred.await(ending).pipe(Effect.as(Buffer.from("Error: final\n")))),
        ),
      );
      const handle = {
        pid: ChildProcessSpawner.ProcessId(52),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
      };
      const pending = yield* makeTerminationError(handle, excerpt).pipe(Effect.forkScoped);
      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* Deferred.succeed(ending, undefined);
      const error = yield* Fiber.join(pending);
      assert.equal(error.message, "Codex App Server process exited with code 1: Error: final");

      const open = yield* captureStderr(
        Stream.concat(Stream.make(Buffer.from("Error: retained\n")), Stream.never),
      );
      const bounded = yield* makeTerminationError(handle, open).pipe(Effect.forkScoped);
      yield* TestClock.adjust("249 millis");
      assert.isUndefined(bounded.pollUnsafe());
      yield* TestClock.adjust("1 millis");
      assert.equal(
        (yield* Fiber.join(bounded)).message,
        "Codex App Server process exited with code 1: Error: retained",
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
