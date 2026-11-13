import { assert, describe, it } from "@effect/vitest";
import * as TestClock from "effect/testing/TestClock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Ref from "effect/Ref";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { appendStderrTail, makeTerminationError } from "./stdio.ts";

const encoder = new TextEncoder();

describe("Codex App Server child process termination", () => {
  const diagnostic =
    "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";

  for (const { name, input, expected } of [
    {
      name: "Node launcher error between its source excerpt and stack",
      input: `file:///prefix/codex.js:107\n  throw new Error(message);\n        ^\n\n${diagnostic}\n    at file:///prefix/codex.js:107:9\n\nNode.js v24.0.0\n`,
      expected: diagnostic,
    },
    {
      name: "last error-labelled line",
      input:
        "Error: earlier error\nTypeError: invalid configuration\n    at main (launcher.js:1:1)",
      expected: "TypeError: invalid configuration",
    },
    {
      name: "Node error code",
      input: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at main (launcher.js:1:1)",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "Rust CLI error",
      input: "error: unexpected argument '--unknown' found\n\nFor more information, try '--help'.",
      expected: "error: unexpected argument '--unknown' found",
    },
    {
      name: "fatal error",
      input: "fatal: could not load configuration\nlauncher stopped",
      expected: "fatal: could not load configuration",
    },
    {
      name: "last non-empty unlabelled line",
      input: "starting\nconfiguration could not be loaded\n  \n",
      expected: "configuration could not be loaded",
    },
    {
      name: "terminal escapes, carriage returns, and repeated whitespace",
      input: "starting\r\u001b[31mError:\t missing   dependency\u001b[0m\r\n    at main\n",
      expected: "Error: missing dependency",
    },
    {
      name: "512-character cap",
      input: `Error: ${"x".repeat(600)}`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", input: " \r\n\t\u001b[0m", expected: undefined },
  ]) {
    it.effect(`reports ${name}`, () =>
      Effect.gen(function* () {
        const error = yield* makeTerminationError(
          {
            pid: ChildProcessSpawner.ProcessId(52),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          },
          { done: Effect.void, tail: Effect.succeed(encoder.encode(input)) },
        );

        assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
        assert.equal(error.pid, 52);
        assert.equal(error.stderr, expected);
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
        );
      }),
    );
  }

  it("retains only the last 8 KiB across chunks, including multibyte text", () => {
    const first = encoder.encode(`Error: discarded\n${"x".repeat(10_000)}`);
    const tail = appendStderrTail(new Uint8Array(), first);
    assert.deepEqual(tail, first.slice(-8192));

    const next = encoder.encode(`\n${"é".repeat(500)}\n${diagnostic}\n`);
    const combined = new Uint8Array(first.length + next.length);
    combined.set(first);
    combined.set(next, first.length);
    const retained = appendStderrTail(tail, next);
    assert.deepEqual(retained, combined.slice(-8192));
    assert.deepEqual(appendStderrTail(retained, new Uint8Array()), retained);
  });

  it.effect("waits for stderr to finish before reading its tail", () =>
    Effect.gen(function* () {
      const done = yield* Deferred.make<void>();
      const tail = yield* Ref.make(encoder.encode("starting"));
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        { done: Deferred.await(done), tail: Ref.get(tail) },
      ).pipe(Effect.forkScoped);

      yield* TestClock.adjust("100 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* Ref.set(tail, encoder.encode(diagnostic));
      yield* Deferred.succeed(done, undefined);

      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, diagnostic);
    }),
  );

  it.effect("uses the captured tail after 250 ms even if stderr stays open", () =>
    Effect.gen(function* () {
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        { done: Effect.never, tail: Effect.succeed(encoder.encode(diagnostic)) },
      ).pipe(Effect.forkScoped);

      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* TestClock.adjust("1 millis");

      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, diagnostic);
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
