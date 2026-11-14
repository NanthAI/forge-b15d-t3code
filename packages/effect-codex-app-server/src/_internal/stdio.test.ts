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
      name: "the Node launcher error between its source excerpt and stack",
      chunks: [
        `file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n    throw new Error(message);\n          ^\n\n${launcherError}\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:11\n\nNode.js v24.1.0\n`,
      ],
      expected: launcherError,
    },
    {
      name: "a Node error with a code",
      chunks: [
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal)\n",
      ],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "the last error-labelled line",
      chunks: ["Error: first error\nTypeError: last error\n    at launcher.js:1:1\n"],
      expected: "TypeError: last error",
    },
    {
      name: "a Rust CLI error",
      chunks: [
        "error: unexpected argument '--unsupported'\n\nFor more information, try '--help'.\n",
      ],
      expected: "error: unexpected argument '--unsupported'",
    },
    {
      name: "a fatal error",
      chunks: ["fatal: could not start\nadditional context\n"],
      expected: "fatal: could not start",
    },
    {
      name: "the last non-empty line without a label",
      chunks: ["starting launcher\n  could not start the server  \n \n"],
      expected: "could not start the server",
    },
    {
      name: "terminal escapes, carriage returns and repeated whitespace",
      chunks: ["progress\r\u001b[31mError:\t missing   dependency\u001b[0m\r\n\n"],
      expected: "Error: missing dependency",
    },
    {
      name: "the 512-character limit",
      chunks: [`Error: ${"x".repeat(600)}\n    at launcher.js:1:1\n`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    {
      name: "empty stderr",
      chunks: [],
      expected: undefined,
    },
    {
      name: "blank stderr",
      chunks: ["\n\r\t \u001b[0m\n"],
      expected: undefined,
    },
    {
      name: "the last 8 KiB across chunks",
      chunks: ["Error: old diagnostic\n", "x".repeat(4096), "x".repeat(4096)],
      expected: "x".repeat(512),
    },
    {
      name: "a chunk larger than the tail",
      chunks: [`Error: old diagnostic\n${"x".repeat(8192)}\nlauncher failed\n`],
      expected: "launcher failed",
    },
    {
      name: "the tail limit in bytes for UTF-8 stderr",
      chunks: ["Error: old diagnostic\n", "é".repeat(4096), "\nlauncher failed\n"],
      expected: "launcher failed",
    },
  ];

  it.effect.each(cases)("includes $name", ({ chunks, expected }) =>
    Effect.gen(function* () {
      const stderr = yield* captureStderr(
        Stream.make(...chunks.map((chunk) => new TextEncoder().encode(chunk))),
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
    }),
  );

  it.effect("uses the captured tail after 250 ms when stderr stays open", () =>
    Effect.gen(function* () {
      const drained = yield* Deferred.make<void>();
      const stderr = yield* captureStderr(
        Stream.make(new TextEncoder().encode("Error: launcher failed\n")).pipe(
          Stream.concat(Stream.fromEffect(Deferred.succeed(drained, undefined)).pipe(Stream.drain)),
          Stream.concat(Stream.never),
        ),
      );
      yield* Deferred.await(drained);
      const errorFiber = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* TestClock.adjust("250 millis");
      const error = yield* Fiber.join(errorFiber);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: launcher failed");
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
