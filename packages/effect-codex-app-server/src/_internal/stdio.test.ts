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
const launcherError =
  "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";

describe("Codex App Server child process termination", () => {
  const cases = [
    {
      name: "prefers the Node launcher error over its source excerpt and stack",
      chunks: [
        `file:///opt/codex.js:107\n  throw new Error(\"Missing optional dependency\");\n        ^\n\n${launcherError}\n    at file:///opt/codex.js:107:9\n\nNode.js v24.0.0\n`,
      ],
      expected: launcherError,
    },
    {
      name: "recognizes a Node error with a code",
      chunks: [
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve\nNode.js v24.0.0",
      ],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "recognizes a TypeError",
      chunks: ["TypeError: invalid configuration\n    at initialize"],
      expected: "TypeError: invalid configuration",
    },
    {
      name: "selects the last error-labelled line from a Rust CLI",
      chunks: [
        "error: first failure\nerror: invalid value for '--config'\nFor more information, try '--help'.",
      ],
      expected: "error: invalid value for '--config'",
    },
    {
      name: "recognizes a fatal error",
      chunks: ["fatal: failed to start\nAdditional help"],
      expected: "fatal: failed to start",
    },
    {
      name: "falls back to the last non-empty line",
      chunks: ["starting\nlauncher could not run\n \n"],
      expected: "launcher could not run",
    },
    {
      name: "strips terminal escapes and collapses whitespace and carriage returns",
      chunks: ["progress\r\u001b[31merror:\t missing   dependency\u001b[0m\r\n  \t\n"],
      expected: "error: missing dependency",
    },
    {
      name: "caps the selected line at 512 characters",
      chunks: [`Error: ${"x".repeat(600)}`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "ignores blank stderr", chunks: [" \r\n\t"], expected: undefined },
    { name: "handles empty stderr", chunks: [], expected: undefined },
    {
      name: "keeps only the last 8 KiB of a large chunk",
      chunks: [`Error: discarded\n${"x".repeat(8192)}\nlast line`],
      expected: "last line",
    },
    {
      name: "bounds the tail across chunks in bytes rather than characters",
      chunks: ["Error: discarded\n", "界".repeat(2000), "界".repeat(1000), "\nlast line"],
      expected: "last line",
    },
    {
      name: "retains an error at the start of the 8 KiB tail",
      chunks: [
        "Error: discarded\nError: retained\n",
        "x".repeat(8192 - "Error: retained\n".length),
      ],
      expected: "Error: retained",
    },
  ];

  for (const { name, chunks, expected } of cases) {
    it.effect(name, () =>
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
        if (error._tag !== "CodexAppServerProcessExitedError") return;
        assert.equal(error.pid, 52);
        assert.equal(error.code, 1);
        assert.equal(error.stderr, expected);
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
        );
      }),
    );
  }

  it.effect("waits for trailing stderr after exit", () =>
    Effect.gen(function* () {
      const trailing = yield* Deferred.make<Uint8Array>();
      const stderr = yield* captureStderr(Stream.fromEffect(Deferred.await(trailing)));
      const exited = yield* Deferred.make<void>();
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
      yield* Deferred.succeed(trailing, encoder.encode(launcherError));
      const error = yield* Fiber.join(termination);
      assert.equal(error.message, `Codex App Server process exited with code 1: ${launcherError}`);
    }),
  );

  it.effect("uses the captured tail after 250 ms if stderr stays open", () =>
    Effect.gen(function* () {
      const captured = yield* Deferred.make<void>();
      const close = yield* Deferred.make<Uint8Array>();
      const stderr = yield* captureStderr(
        Stream.make(encoder.encode(launcherError)).pipe(
          Stream.concat(
            Stream.fromEffect(
              Deferred.succeed(captured, undefined).pipe(Effect.andThen(Deferred.await(close))),
            ),
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
      assert.equal(error.message, `Codex App Server process exited with code 1: ${launcherError}`);
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
