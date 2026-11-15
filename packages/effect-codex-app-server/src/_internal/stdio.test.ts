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
  const launcherError =
    "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";
  const encoder = new TextEncoder();

  it.effect.each([
    {
      name: "Node launcher source excerpt and stack",
      chunks: [
        `file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n    throw new Error(message);\n          ^\n\n${launcherError}\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:11\n\nNode.js v24.0.0\n`,
      ],
      expected: launcherError,
    },
    {
      name: "Node error code",
      chunks: [
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules/esm/resolve:1:1)\n",
      ],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "TypeError",
      chunks: ["TypeError: Invalid argument\n    at main (index.js:1:1)\n"],
      expected: "TypeError: Invalid argument",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: unexpected argument '--bad' found\nUsage: codex app-server\n"],
      expected: "error: unexpected argument '--bad' found",
    },
    {
      name: "last error-labelled line",
      chunks: ["Error: initial failure\nfatal: startup failed\nSee --help for usage\n"],
      expected: "fatal: startup failed",
    },
    {
      name: "last non-empty line",
      chunks: ["Starting app-server\nCould not start the server\n  \n"],
      expected: "Could not start the server",
    },
    {
      name: "terminal escapes, carriage returns and whitespace",
      chunks: [
        "Starting\r\u001b[31mError:\t missing   dependency\u001b[0m\r\n\u001b]8;;https://example.com\u0007help\u001b]8;;\u0007\n",
      ],
      expected: "Error: missing dependency",
    },
    {
      name: "512-character cap",
      chunks: [`Error: ${"x".repeat(600)}\n    at main (index.js:1:1)\n`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "empty stderr", chunks: [], expected: undefined },
    { name: "blank stderr", chunks: [" \r\n\t\n"], expected: undefined },
    {
      name: "error exactly at the start of the 8 KiB tail",
      chunks: ["discarded\n", `error: retained\n${"x".repeat(8192 - "error: retained\n".length)}`],
      expected: "error: retained",
    },
    {
      name: "8 KiB tail from an oversized chunk",
      chunks: [`Error: discarded\n${"x".repeat(8192)}\nlast diagnostic\n`],
      expected: "last diagnostic",
    },
    {
      name: "8 KiB tail across chunks",
      chunks: ["Error: discarded\n", "x".repeat(4096), "x".repeat(4096), "\nlast diagnostic\n"],
      expected: "last diagnostic",
    },
    {
      name: "8 KiB limit counts bytes",
      chunks: ["Error: discarded\n", "é".repeat(4096), "\nlast diagnostic\n"],
      expected: "last diagnostic",
    },
    {
      name: "UTF-8 and error labels split across chunks",
      chunks: ["Ty", "peError: dépendance manquante\n", "    at main (index.js:1:1)\n"],
      expected: "TypeError: dépendance manquante",
    },
  ])("summarizes $name", ({ chunks, expected }) =>
    Effect.gen(function* () {
      const stderr = yield* makeStderrCapture(
        Stream.make(...chunks.map((chunk) => encoder.encode(chunk))),
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

  it.effect("waits for stderr to finish after the process exits", () =>
    Effect.gen(function* () {
      const exited = yield* Deferred.make<void>();
      const releaseStderr = yield* Deferred.make<Uint8Array>();
      const stderr = yield* makeStderrCapture(Stream.fromEffect(Deferred.await(releaseStderr)));
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.as(Deferred.succeed(exited, undefined), ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkScoped);

      yield* Deferred.await(exited);
      yield* Deferred.succeed(releaseStderr, encoder.encode(launcherError));
      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, launcherError);
    }),
  );

  it.effect("uses the captured tail after 250 ms when stderr stays open", () =>
    Effect.gen(function* () {
      const stderr = yield* makeStderrCapture(
        Stream.concat(Stream.make(encoder.encode(launcherError)), Stream.never),
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
