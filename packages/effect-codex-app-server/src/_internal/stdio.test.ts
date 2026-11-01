import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { captureStderr, makeTerminationError } from "./stdio.ts";

const launcherError =
  "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";
const encoder = new TextEncoder();

describe("Codex App Server child process termination", () => {
  it.effect.each([
    {
      name: "Node launcher source excerpt and stack",
      input: `file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n  throw new Error(\"Missing optional dependency\");\n        ^\n\n${launcherError}\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:9\n\nNode.js v24.0.0\n`,
      expected: launcherError,
    },
    {
      name: "Node error with a code",
      input:
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules:1:2)\nNode.js v24.0.0",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "last error-labelled line",
      input:
        "Error: earlier failure\nTypeError: invalid configuration\n    at startup (launcher.js:1:2)",
      expected: "TypeError: invalid configuration",
    },
    {
      name: "Rust CLI error",
      input: "error: invalid value for --config\n\nFor more information, try '--help'.",
      expected: "error: invalid value for --config",
    },
    {
      name: "fatal error",
      input: "fatal: unable to start\nsee the log for details",
      expected: "fatal: unable to start",
    },
    {
      name: "last unlabelled message",
      input: "starting\n  could not load configuration  \n\n",
      expected: "could not load configuration",
    },
    {
      name: "ANSI escapes, whitespace and carriage returns",
      input:
        "loading\r\u001b[31m  Error:\t missing   dependency \u001b[0m\r\n\u001b]0;launcher\u0007done\n",
      expected: "Error: missing dependency",
    },
    {
      name: "512-character cap",
      input: `Error: ${"x".repeat(600)}`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "empty stderr", input: "", expected: undefined },
    { name: "blank stderr", input: " \r\n\t\u001b[0m\n", expected: undefined },
    {
      name: "8 KiB byte tail",
      input: `Error: discarded\n${"é".repeat(4096)}\nlast diagnostic\n`,
      expected: "last diagnostic",
    },
    {
      name: "chunk larger than 8 KiB",
      input: `Error: discarded\n${"é".repeat(9000)}\nlast diagnostic\n`,
      expected: "last diagnostic",
    },
  ])("includes a bounded diagnostic from $name", ({ input, expected }) =>
    Effect.gen(function* () {
      const bytes = encoder.encode(input);
      const stderr = yield* captureStderr(
        Stream.make(bytes.slice(0, 31), bytes.slice(31, 8000), bytes.slice(8000)),
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
      assert.equal(error.code, 1);
      assert.equal(error.stderr, expected);
      assert.equal(
        error.message,
        `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
      );
    }),
  );

  it.effect.each(["finishes", "stays open"])(
    "waits at most 250 ms for stderr that %s after exit",
    (completion) =>
      Effect.gen(function* () {
        const input = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
        const stderr = yield* captureStderr(Stream.fromQueue(input));
        const pending = yield* makeTerminationError(
          {
            pid: ChildProcessSpawner.ProcessId(52),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          },
          stderr,
        ).pipe(Effect.forkScoped);

        yield* TestClock.adjust("249 millis");
        assert.isUndefined(pending.pollUnsafe());
        yield* Queue.offer(input, encoder.encode(launcherError));
        if (completion === "finishes") {
          yield* Queue.end(input);
        } else {
          yield* TestClock.adjust("1 millis");
        }

        const error = yield* Fiber.join(pending);
        assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
        assert.equal(error.stderr, launcherError);
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
