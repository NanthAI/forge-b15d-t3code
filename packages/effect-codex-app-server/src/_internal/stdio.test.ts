import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { makeStderrTail, makeTerminationError } from "./stdio.ts";

describe("Codex App Server child process termination", () => {
  it.effect.each([
    {
      label: "Node launcher source excerpt and stack",
      input:
        'file:///usr/local/lib/codex.js:107\n  throw new Error("Missing optional dependency")\n        ^\n\nError: Missing optional dependency\n    at file:///usr/local/lib/codex.js:107:9\n\nNode.js v24.0.0\n',
      expected: "Error: Missing optional dependency",
    },
    {
      label: "Node error code",
      input:
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules:1:2)\n",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      label: "last error-labelled line",
      input: "Error: earlier failure\nTypeError: invalid argument\n    at main (cli.js:1:1)\n",
      expected: "TypeError: invalid argument",
    },
    {
      label: "Rust CLI error",
      input: "error: invalid value for --config\n\nFor more information, try '--help'.\n",
      expected: "error: invalid value for --config",
    },
    {
      label: "fatal error",
      input: "fatal: could not start\nSee the log for details\n",
      expected: "fatal: could not start",
    },
    {
      label: "unlabelled fallback",
      input: "starting\n  launcher could not start  \n\n",
      expected: "launcher could not start",
    },
    {
      label: "terminal escapes and whitespace",
      input: "starting\r\u001b[31mError:\t  missing dependency\u001b[0m\r\n   \n",
      expected: "Error: missing dependency",
    },
    {
      label: "512-character cap",
      input: `Error: ${"x".repeat(600)}`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    { label: "blank stderr", input: " \t\r\n", expected: undefined },
    { label: "empty stderr", input: "", expected: undefined },
  ])("extracts $label", ({ input, expected }) =>
    Effect.gen(function* () {
      const stderr = makeStderrTail();
      stderr.append(new TextEncoder().encode(input));
      const error = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        { done: Effect.void, get: stderr.get },
      );
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, expected);
      assert.equal(
        error.message,
        `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
      );
    }),
  );

  it("keeps only the last 8 KiB across stderr chunks", () => {
    const stderr = makeStderrTail();
    const encode = (text: string) => new TextEncoder().encode(text);
    stderr.append(encode(`Error: discarded\n${"x".repeat(9 * 1024)}`));
    assert.equal(stderr.get(), "x".repeat(512));
    stderr.append(encode("\nError: also discarded\n"));
    stderr.append(encode("é".repeat(2048)));
    stderr.append(encode("é".repeat(2048)));
    assert.equal(stderr.get(), "é".repeat(512));
    stderr.append(encode("\nError: retained\nstack frame\n"));
    assert.equal(stderr.get(), "Error: retained");
  });

  it.effect("waits for stderr to finish before taking the excerpt", () =>
    Effect.gen(function* () {
      const done = yield* Deferred.make<void>();
      const stderr = makeStderrTail();
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        { done: Deferred.await(done), get: stderr.get },
      ).pipe(Effect.forkChild);
      yield* TestClock.adjust("100 millis");
      stderr.append(new TextEncoder().encode("Error: late diagnostics"));
      yield* Deferred.succeed(done, undefined);

      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: late diagnostics");
    }),
  );

  it.effect("uses the captured tail after 250 ms even if stderr stays open", () =>
    Effect.gen(function* () {
      const stderr = makeStderrTail();
      stderr.append(new TextEncoder().encode("Error: captured before EOF"));
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        { done: Effect.never, get: stderr.get },
      ).pipe(Effect.forkChild);
      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* TestClock.adjust("1 millis");

      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: captured before EOF");
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
