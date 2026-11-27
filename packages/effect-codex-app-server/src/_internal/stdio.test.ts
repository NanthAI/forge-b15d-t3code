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
  it.effect.each([
    {
      name: "Node launcher error between source and stack frames",
      chunks: [
        'file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n    throw new Error("Missing optional dependency");\n          ^\n\n',
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:11\n\nNode.js v22.14.0\n",
      ],
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "Node error with a code",
      chunks: [
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal)\n",
      ],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "last error-labelled line",
      chunks: ["Error: earlier failure\nTypeError: invalid configuration\n    at main\n"],
      expected: "TypeError: invalid configuration",
    },
    {
      name: "Rust CLI error",
      chunks: [
        "error: unexpected argument '--unknown' found\n\nFor more information, try '--help'.\n",
      ],
      expected: "error: unexpected argument '--unknown' found",
    },
    {
      name: "fatal diagnostic",
      chunks: ["fatal: could not start\nSee the log for details\n"],
      expected: "fatal: could not start",
    },
    {
      name: "last non-empty unlabelled line",
      chunks: ["starting\nCould not load configuration\n\n  \n"],
      expected: "Could not load configuration",
    },
    {
      name: "ANSI escapes, carriage returns and whitespace",
      chunks: ["loading\r\u001b[31mError:\t  failed\u001b[0m\r\n  at main\r\n"],
      expected: "Error: failed",
    },
    {
      name: "512 character cap",
      chunks: [`Error: ${"x".repeat(600)}\n    at main\n`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "empty stderr", chunks: [], expected: undefined },
    { name: "blank stderr", chunks: ["\n\t  \r\n"], expected: undefined },
    {
      name: "8 KiB tail across chunks",
      chunks: ["Error: discarded\n", "x".repeat(8 * 1024 - 6), "\nfinal"],
      expected: "final",
    },
    {
      name: "8 KiB tail of an oversized chunk",
      chunks: [`Error: discarded\n${"x".repeat(8 * 1024)}\nfinal`],
      expected: "final",
    },
    {
      name: "tail limit counts UTF-8 bytes",
      chunks: ["Error: discarded\n", "é".repeat(4 * 1024), "\nfinal"],
      expected: "final",
    },
  ])("includes $name in the exit error", ({ chunks, expected }) =>
    Effect.gen(function* () {
      const stderr = yield* captureStderr(
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
      if (error._tag !== "CodexAppServerProcessExitedError") return;
      assert.equal(error.stderr, expected);
      assert.equal(
        error.message,
        `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
      );
    }),
  );

  it.effect("uses the captured tail after 250 ms when stderr stays open", () =>
    Effect.gen(function* () {
      const captured = yield* Deferred.make<void>();
      const stderr = yield* captureStderr(
        Stream.concat(
          Stream.make(new TextEncoder().encode("Error: launcher failed\n")),
          Stream.fromEffect(
            Deferred.succeed(captured, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        ),
      );
      const result = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkChild);
      yield* Deferred.await(captured);
      yield* TestClock.adjust("250 millis");
      const error = yield* Fiber.join(result);

      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: Error: launcher failed",
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
