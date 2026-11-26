import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Ref from "effect/Ref";
import { ChildProcessSpawner } from "effect/process";
import * as TestClock from "effect/testing/TestClock";

import * as CodexError from "../errors.ts";
import { appendStderrTail, makeTerminationError, stderrExcerpt } from "./stdio.ts";

const encoder = new TextEncoder();

describe("Codex App Server child process termination", () => {
  it.effect.each([
    {
      name: "Node launcher error between the source excerpt and stack",
      stderr: [
        "file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107",
        '    throw new Error("Missing optional dependency");',
        "          ^",
        "",
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
        "    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:11",
        "Node.js v24.0.0",
      ].join("\n"),
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "Node error code",
      stderr: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:module:1:1)",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "last error-labelled line",
      stderr: "Error: first failure\nTypeError: final failure\n    at main (file:///codex.js:1:1)",
      expected: "TypeError: final failure",
    },
    {
      name: "Rust CLI error",
      stderr: "error: invalid configuration\n\nFor more information, try '--help'.",
      expected: "error: invalid configuration",
    },
    {
      name: "fatal error",
      stderr: "fatal: could not start\nPlease try again.",
      expected: "fatal: could not start",
    },
    {
      name: "last non-empty unlabelled line",
      stderr: "starting\n  could not load binary  \n\n",
      expected: "could not load binary",
    },
    {
      name: "terminal escapes, carriage returns, and whitespace",
      stderr: "loading\r\u001b[31mError:\t missing   dependency\u001b[0m\r\n at main\r\n",
      expected: "Error: missing dependency",
    },
    {
      name: "512-character cap",
      stderr: `Error: ${"x".repeat(600)}\n at main`,
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "empty stderr", stderr: "", expected: undefined },
    { name: "blank stderr", stderr: "\n\t \r\n", expected: undefined },
  ])("includes a sanitized excerpt for $name", ({ stderr, expected }) =>
    Effect.gen(function* () {
      const error = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        { drained: Effect.void, tail: Effect.succeed(encoder.encode(stderr)) },
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

  it("retains only the last 8 KiB across stderr chunks", () => {
    const first = appendStderrTail(new Uint8Array(), encoder.encode("Error: stale\n"));
    const tail = appendStderrTail(first, encoder.encode("é".repeat(5 * 1024)));
    assert.equal(tail.byteLength, 8 * 1024);
    assert.deepEqual(tail, encoder.encode("é".repeat(4 * 1024)));

    const final = appendStderrTail(tail, encoder.encode("\nError: recent\n at main"));
    assert.equal(final.byteLength, 8 * 1024);
    assert.equal(stderrExcerpt(final), "Error: recent");

    const bytes = encoder.encode("Error: missing dépendance");
    const split = bytes.indexOf(0xc3) + 1;
    assert.equal(
      stderrExcerpt(appendStderrTail(bytes.slice(0, split), bytes.slice(split))),
      "Error: missing dépendance",
    );
  });

  it.effect.each([false, true])("waits for stderr with a bounded grace (timeout: %s)", (timeout) =>
    Effect.gen(function* () {
      const waiting = yield* Deferred.make<void>();
      const drained = yield* Deferred.make<void>();
      const tail = yield* Ref.make(encoder.encode("Error: initial diagnostic"));
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        {
          drained: Deferred.succeed(waiting, undefined).pipe(
            Effect.andThen(Deferred.await(drained)),
          ),
          tail: Ref.get(tail),
        },
      ).pipe(Effect.forkChild);

      yield* Deferred.await(waiting);
      if (timeout) {
        yield* TestClock.adjust("250 millis");
      } else {
        yield* Ref.set(tail, encoder.encode("Error: final diagnostic"));
        yield* Deferred.succeed(drained, undefined);
      }

      const error = yield* Fiber.join(termination);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, timeout ? "Error: initial diagnostic" : "Error: final diagnostic");
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
