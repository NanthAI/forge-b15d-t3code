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
  const launcherError =
    "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";
  const cases = [
    {
      name: "Node launcher error between source and stack frames",
      stderr: `file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n  throw new Error(message);\n        ^\n\n${launcherError}\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:9\n\nNode.js v22.0.0\n`,
      expected: launcherError,
    },
    {
      name: "Node error code",
      stderr:
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules/esm/resolve:1:1)\nNode.js v22.0.0",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "last labelled error",
      stderr: "Error: earlier failure\nTypeError: invalid configuration\n    at main (app.js:1:1)",
      expected: "TypeError: invalid configuration",
    },
    {
      name: "Rust CLI error",
      stderr: "error: unexpected argument '--invalid' found\n\nFor more information, try '--help'.",
      expected: "error: unexpected argument '--invalid' found",
    },
    {
      name: "fatal error",
      stderr: "fatal: cannot open configuration\nSee the log for details.",
      expected: "fatal: cannot open configuration",
    },
    {
      name: "last non-empty unlabelled line",
      stderr: "starting launcher\n  unable to open executable  \n\n",
      expected: "unable to open executable",
    },
    {
      name: "terminal escapes, whitespace and carriage returns",
      stderr: "loading\r\u001b[31mError:\t  missing   dependency\u001b[0m\r\n  at launcher\n",
      expected: "Error: missing dependency",
    },
    {
      name: "512 character cap",
      stderr: `Error: ${"x".repeat(600)}`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", stderr: " \n\r\t\n", expected: undefined },
    { name: "empty stderr", stderr: "", expected: undefined },
  ];

  for (const { name, stderr, expected } of cases) {
    it.effect(`includes a concise stderr excerpt: ${name}`, () =>
      Effect.gen(function* () {
        const tail = makeStderrTail();
        tail.append(new TextEncoder().encode(stderr));
        const error = yield* makeTerminationError(
          {
            pid: ChildProcessSpawner.ProcessId(52),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
          },
          { tail, done: Effect.void },
        );

        assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
        assert.equal(error.stderr, expected);
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
        );
      }),
    );
  }

  it("retains only the last 8 KiB across chunks, measured in bytes", () => {
    const tail = makeStderrTail();
    const encoder = new TextEncoder();
    tail.append(encoder.encode("Error: discarded\n"));
    tail.append(encoder.encode("é".repeat(4096)));
    tail.append(encoder.encode("\nlast line"));
    assert.equal(tail.excerpt(), "last line");

    const retained = `Error: retained\n${"x".repeat(8192 - 16 - 10)}\nlast line`;
    assert.equal(encoder.encode(retained).length, 8192);
    tail.append(encoder.encode(retained));
    assert.equal(tail.excerpt(), "Error: retained");
    tail.append(encoder.encode("!"));
    assert.equal(tail.excerpt(), "last line!");
  });

  it.effect("waits for stderr to finish after the process exits", () =>
    Effect.gen(function* () {
      const tail = makeStderrTail();
      const done = yield* Deferred.make<void>();
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        { tail, done: Deferred.await(done) },
      ).pipe(Effect.forkChild);

      yield* TestClock.adjust("100 millis");
      tail.append(new TextEncoder().encode(launcherError));
      yield* Deferred.succeed(done, undefined);
      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, launcherError);
    }),
  );

  it.effect("uses the captured tail after 250 ms if stderr remains open", () =>
    Effect.gen(function* () {
      const tail = makeStderrTail();
      tail.append(new TextEncoder().encode("fatal: launcher failed"));
      const completed = yield* Deferred.make<CodexError.CodexAppServerError>();
      yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        { tail, done: Effect.never },
      ).pipe(
        Effect.flatMap((error) => Deferred.succeed(completed, error)),
        Effect.forkChild,
      );

      yield* TestClock.adjust("249 millis");
      assert.isFalse(yield* Deferred.isDone(completed));
      yield* TestClock.adjust("1 millis");
      const error = yield* Deferred.await(completed);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "fatal: launcher failed");
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
