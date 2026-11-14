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
  const launcherError =
    "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";

  for (const { name, stderr, expected } of [
    {
      name: "the Node launcher error between the source excerpt and stack frames",
      stderr: `file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n    throw new Error(\"Missing optional dependency\");\n          ^\n\n${launcherError}\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:11\n\nNode.js v22.14.0\n`,
      expected: launcherError,
    },
    {
      name: "a Node error with a code",
      stderr:
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules/esm/resolve:100:1)\n",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "the last error-labelled line",
      stderr: "Error: first error\nTypeError: invalid argument\n    at main (launcher.js:1:1)\n",
      expected: "TypeError: invalid argument",
    },
    {
      name: "a Rust CLI error",
      stderr: "error: invalid value for --config\n\nFor more information, try '--help'.\n",
      expected: "error: invalid value for --config",
    },
    {
      name: "a fatal error",
      stderr: "fatal: configuration could not be loaded\nlauncher stopped\n",
      expected: "fatal: configuration could not be loaded",
    },
    {
      name: "the last non-empty line without an error label",
      stderr: "starting launcher\nUnable to start Codex\n\n  \n",
      expected: "Unable to start Codex",
    },
    {
      name: "terminal escapes, carriage returns, and collapsed whitespace",
      stderr:
        "\u001b]0;Codex\u0007starting\r\u001b[31mError:\t Missing   dependency\u001b[0m\r\n  at launcher.js:1:1\n",
      expected: "Error: Missing dependency",
    },
    {
      name: "the 512-character excerpt limit",
      stderr: `Error: ${"x".repeat(600)}\n`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", stderr: " \r\n\t\n", expected: undefined },
    { name: "empty stderr", stderr: "", expected: undefined },
    {
      name: "the 8 KiB tail within one chunk",
      stderr: `Error: discarded diagnostic\n${"x".repeat(8 * 1024)}\nlast diagnostic\n`,
      expected: "last diagnostic",
    },
    {
      name: "the 8 KiB limit measured in bytes",
      stderr: `Error: discarded diagnostic\n${"é".repeat(4 * 1024)}\nlast diagnostic\n`,
      expected: "last diagnostic",
    },
  ]) {
    it.effect(`includes ${name}`, () =>
      Effect.gen(function* () {
        const stderrExcerpt = yield* captureStderr(Stream.make(encoder.encode(stderr)));
        const error = yield* makeTerminationError(
          {
            pid: ChildProcessSpawner.ProcessId(52),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          },
          stderrExcerpt,
        );

        assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
        if (error._tag === "CodexAppServerProcessExitedError") {
          assert.equal(error.stderr, expected);
          assert.equal(error.code, 1);
          assert.equal(error.pid, 52);
        }
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
        );
      }).pipe(Effect.scoped),
    );
  }

  it.effect("retains only the last 8 KiB across stderr chunks", () =>
    Effect.gen(function* () {
      const stderr = yield* captureStderr(
        Stream.fromIterable([
          encoder.encode("Error: discarded diagnostic\n"),
          ...Array.from({ length: 8 }, () => encoder.encode("x".repeat(1024))),
          encoder.encode("\nlast diagnostic\n"),
        ]),
      );

      assert.equal(yield* stderr, "last diagnostic");
    }).pipe(Effect.scoped),
  );

  it.effect("waits for stderr emitted after the exit status arrives", () =>
    Effect.gen(function* () {
      const exited = yield* Deferred.make<void>();
      const stderr = yield* captureStderr(
        Stream.fromEffect(
          Deferred.await(exited).pipe(Effect.as(encoder.encode("Error: late diagnostic\n"))),
        ),
      );
      const error = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Deferred.succeed(exited, undefined).pipe(
            Effect.as(ChildProcessSpawner.ExitCode(1)),
          ),
        },
        stderr,
      );

      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: Error: late diagnostic",
      );
    }).pipe(Effect.scoped),
  );

  it.effect("uses captured stderr after 250 ms if the stream stays open", () =>
    Effect.gen(function* () {
      const drained = yield* Deferred.make<void>();
      const stderr = yield* captureStderr(
        Stream.concat(
          Stream.make(encoder.encode("Error: captured diagnostic\n")),
          Stream.fromEffect(
            Deferred.succeed(drained, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        ),
      );
      yield* Deferred.await(drained);
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkChild);
      yield* TestClock.adjust("250 millis");

      assert.equal(
        (yield* Fiber.join(termination)).message,
        "Codex App Server process exited with code 1: Error: captured diagnostic",
      );
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
