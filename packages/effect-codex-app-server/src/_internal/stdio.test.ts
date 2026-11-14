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

const launcherError =
  "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";
const encoder = new TextEncoder();
const handle = {
  pid: ChildProcessSpawner.ProcessId(52),
  exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
};

describe("Codex App Server child process termination", () => {
  for (const { name, chunks, expected } of [
    {
      name: "Node launcher source excerpt and stack frames",
      chunks: [
        `file:///opt/codex.js:107\n  throw new Error(\n        ^\n\n${launcherError}\n    at file:///opt/codex.js:107:11\n\nNode.js v24.0.0\n`,
      ],
      expected: launcherError,
    },
    {
      name: "Node error with a code",
      chunks: ["Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at moduleResolve\n"],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "last error-labelled line",
      chunks: ["Error: earlier\nTypeError: invalid configuration\n    at initialize\n"],
      expected: "TypeError: invalid configuration",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: invalid value for '--config'\n\nFor more information, try '--help'.\n"],
      expected: "error: invalid value for '--config'",
    },
    {
      name: "fatal error",
      chunks: ["fatal: could not load configuration\n    details follow\n"],
      expected: "fatal: could not load configuration",
    },
    {
      name: "last non-empty unlabelled line",
      chunks: ["loading\n\nCould not start Codex\n \n"],
      expected: "Could not start Codex",
    },
    {
      name: "terminal escapes, whitespace and carriage returns",
      chunks: ["starting\r\u001b[31mError:\t missing   dependency\u001b[0m\r\n  at launch\n"],
      expected: "Error: missing dependency",
    },
    {
      name: "512 character limit",
      chunks: [`Error: ${"x".repeat(600)}\nstack\n`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "empty stderr", chunks: [], expected: undefined },
    { name: "blank stderr", chunks: [" \n\r\t\u001b[0m"], expected: undefined },
    {
      name: "8 KiB tail from one large chunk",
      chunks: [`Error: discarded\n${"x".repeat(9 * 1024)}\nlast diagnostic\n`],
      expected: "last diagnostic",
    },
    {
      name: "8 KiB byte tail across chunks",
      chunks: ["Error: discarded\n", "é".repeat(4096), "\nlast diagnostic\n"],
      expected: "last diagnostic",
    },
    {
      name: "UTF-8 and error label split across chunks",
      chunks: ["Type", "Error: 缺少依赖\n", "    at launch\n"],
      expected: "TypeError: 缺少依赖",
    },
  ]) {
    it.effect(`includes a sanitized stderr excerpt for ${name}`, () =>
      Effect.gen(function* () {
        const stderr = yield* captureStderr(
          Stream.fromArray(chunks.map((chunk) => encoder.encode(chunk))),
        );
        const error = yield* makeTerminationError(handle, stderr);

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
  }

  it.effect("waits for stderr to finish after the process exits", () =>
    Effect.gen(function* () {
      const exited = yield* Deferred.make<void>();
      const output = yield* Deferred.make<Uint8Array>();
      const stderr = yield* captureStderr(Stream.fromEffect(Deferred.await(output)));
      const termination = yield* makeTerminationError(
        {
          ...handle,
          exitCode: Deferred.succeed(exited, undefined).pipe(
            Effect.as(ChildProcessSpawner.ExitCode(1)),
          ),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* Deferred.await(exited);
      yield* Deferred.succeed(output, encoder.encode(launcherError));
      const error = yield* Fiber.join(termination);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, launcherError);
    }),
  );

  it.effect("uses the captured tail after 250 ms even if stderr stays open", () =>
    Effect.gen(function* () {
      const drained = yield* Deferred.make<void>();
      const stderr = yield* captureStderr(
        Stream.concat(
          Stream.make(encoder.encode(launcherError)),
          Stream.fromEffect(
            Deferred.succeed(drained, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        ),
      );
      yield* Deferred.await(drained);
      const termination = yield* makeTerminationError(handle, stderr).pipe(Effect.forkScoped);

      yield* TestClock.adjust("250 millis");
      const error = yield* Fiber.join(termination);
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
