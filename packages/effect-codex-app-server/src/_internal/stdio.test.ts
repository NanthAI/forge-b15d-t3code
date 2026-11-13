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
      name: "Node launcher error between source excerpt and stack frames",
      stderr: [
        "file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107",
        '    throw new Error("Missing optional dependency")',
        "          ^",
        "",
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
        "    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:11",
        "",
        "Node.js v24.10.0",
      ].join("\n"),
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "last error-labelled line",
      stderr: "Error: first error\nTypeError: invalid argument\n    at main (main.js:1:1)\n",
      expected: "TypeError: invalid argument",
    },
    {
      name: "Node error with a code",
      stderr: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at main (main.js:1:1)\n",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "Rust CLI error",
      stderr: "error: invalid value for --config\n\nFor more information, try '--help'.\n",
      expected: "error: invalid value for --config",
    },
    {
      name: "fatal error",
      stderr: "fatal: could not start\nSee the log for details.\n",
      expected: "fatal: could not start",
    },
    {
      name: "last non-empty line without an error label",
      stderr: "starting app-server\nlauncher failed\n \t\n",
      expected: "launcher failed",
    },
    {
      name: "terminal escapes, carriage returns and whitespace",
      stderr: "loading\r\u001b[31mError:\t missing   dependency\u001b[0m\r\n\t at main\n",
      expected: "Error: missing dependency",
    },
    {
      name: "512-character limit",
      stderr: `Error: ${"x".repeat(600)}\n    at main\n`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "empty stderr", stderr: "", expected: undefined },
    { name: "blank stderr", stderr: " \r\n\t\u001b[0m", expected: undefined },
  ])("includes a concise stderr excerpt: $name", ({ stderr, expected }) =>
    Effect.gen(function* () {
      const tail = makeStderrTail();
      tail.append(new TextEncoder().encode(stderr));
      const error = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        { done: Effect.void, excerpt: tail.excerpt },
      );

      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.code, 1);
      assert.equal(error.pid, 52);
      assert.equal(error.stderr, expected);
      assert.equal(
        error.message,
        `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
      );
    }),
  );

  it("keeps only the last 8 KiB across chunks and preserves split UTF-8", () => {
    const tail = makeStderrTail();
    const encoder = new TextEncoder();
    tail.append(encoder.encode("Error: stale diagnostic\n"));
    tail.append(encoder.encode("x".repeat(512 * 1024)));
    assert.equal(tail.excerpt(), "x".repeat(512));

    tail.append(encoder.encode(`Error: must fall outside the tail\n${"x".repeat(8191)}\n`));
    assert.equal(tail.excerpt(), "x".repeat(512));

    tail.append(encoder.encode(`\nfatal: ${"é".repeat(4092)}`));
    assert.equal(tail.excerpt(), `fatal: ${"é".repeat(505)}`);

    const bytes = encoder.encode("\nTypeError: échec\n");
    const split = bytes.indexOf(0xc3) + 1;
    tail.append(bytes.subarray(0, split));
    tail.append(bytes.subarray(split));
    assert.equal(tail.excerpt(), "TypeError: échec");
  });

  it.effect("waits for stderr to finish after process exit", () =>
    Effect.gen(function* () {
      const done = yield* Deferred.make<void>();
      const tail = makeStderrTail();
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        { done: Deferred.await(done), excerpt: tail.excerpt },
      ).pipe(Effect.forkScoped);

      yield* TestClock.adjust("100 millis");
      assert.isUndefined(pending.pollUnsafe());
      tail.append(new TextEncoder().encode("Error: final diagnostic\n"));
      yield* Deferred.succeed(done, undefined);

      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: final diagnostic");
    }),
  );

  it.effect("uses the captured tail after 250 ms when stderr stays open", () =>
    Effect.gen(function* () {
      const tail = makeStderrTail();
      tail.append(new TextEncoder().encode("Error: partial diagnostic\n"));
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        { done: Effect.never, excerpt: tail.excerpt },
      ).pipe(Effect.forkScoped);

      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* TestClock.adjust("1 millis");

      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: partial diagnostic");
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
