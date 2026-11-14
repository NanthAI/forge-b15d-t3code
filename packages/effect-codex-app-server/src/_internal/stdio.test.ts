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
  const cases = [
    {
      name: "Node launcher source excerpt and stack frames",
      chunks: [
        `file:///prefix/codex.js:107\n  throw new Error(message);\n        ^\n\n${diagnostic}\n    at file:///prefix/codex.js:107:9\n\nNode.js v26.0.0\n`,
      ],
      expected: diagnostic,
    },
    {
      name: "last error-labelled line",
      chunks: [
        "Error: initial failure\nTypeError: invalid configuration\n    at launch (cli.js:1:1)",
      ],
      expected: "TypeError: invalid configuration",
    },
    {
      name: "Node error code",
      chunks: ["Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (cli.js:1:1)"],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: invalid argument\nFor more information, try --help."],
      expected: "error: invalid argument",
    },
    {
      name: "fatal error",
      chunks: ["fatal: cannot start\nadditional context"],
      expected: "fatal: cannot start",
    },
    {
      name: "last non-empty unlabelled line",
      chunks: ["starting\n  cannot launch   the server  \n\n"],
      expected: "cannot launch the server",
    },
    {
      name: "terminal escapes and carriage returns across chunks",
      chunks: ["loading\r\u001b[31", "mError:\t missing   package\u001b[0m\r\n    at launch"],
      expected: "Error: missing package",
    },
    {
      name: "512-character limit",
      chunks: [`Error: ${"x".repeat(600)}\n    at launch`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", chunks: [" \n\r\t"], expected: undefined },
    { name: "empty stderr", chunks: [], expected: undefined },
    {
      name: "8 KiB byte tail across chunks",
      chunks: ["Error: discarded\n", "é".repeat(4096), "\nlatest diagnostic"],
      expected: "latest diagnostic",
    },
    {
      name: "8 KiB tail of an oversized chunk",
      chunks: [`Error: discarded\n${"x".repeat(8192)}\nlatest diagnostic`],
      expected: "latest diagnostic",
    },
  ];

  for (const { name, chunks, expected } of cases) {
    it.effect(`includes ${name} in the exit error`, () =>
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
        if (error._tag !== "CodexAppServerProcessExitedError") return assert.fail();
        assert.equal(error.stderr, expected);
        assert.equal(error.pid, 52);
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
        );
      }),
    );
  }

  it.effect("waits for final stderr arriving after the process exit", () =>
    Effect.gen(function* () {
      const exited = yield* Deferred.make<void>();
      const output = yield* Deferred.make<string>();
      const stderr = yield* captureStderr(
        Stream.fromEffect(Deferred.await(output)).pipe(
          Stream.map((text) => new TextEncoder().encode(text)),
        ),
      );
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Deferred.succeed(exited, undefined).pipe(
            Effect.as(ChildProcessSpawner.ExitCode(1)),
          ),
        },
        stderr,
      ).pipe(Effect.forkScoped);
      yield* Deferred.await(exited);
      yield* Deferred.succeed(output, diagnostic);
      const error = yield* Fiber.join(termination);

      assert.equal(error.message, `Codex App Server process exited with code 1: ${diagnostic}`);
    }),
  );

  it.effect("uses the captured tail after 250 ms when stderr remains open", () =>
    Effect.gen(function* () {
      const captured = yield* Deferred.make<void>();
      const stderr = yield* captureStderr(
        Stream.concat(
          Stream.make(new TextEncoder().encode(diagnostic)),
          Stream.fromEffect(Deferred.succeed(captured, undefined)).pipe(
            Stream.flatMap(() => Stream.never),
          ),
        ),
      );
      yield* Deferred.await(captured);
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkScoped);
      yield* TestClock.adjust("250 millis");
      const error = yield* Fiber.join(termination);

      assert.equal(error.message, `Codex App Server process exited with code 1: ${diagnostic}`);
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
