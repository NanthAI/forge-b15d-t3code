import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { makeStderrTail, makeTerminationError } from "./stdio.ts";

describe("Codex App Server stderr excerpt", () => {
  it.each([
    {
      name: "Node launcher source excerpt and stack",
      stderr:
        'file:///opt/codex.js:107\n  throw new Error("Missing optional dependency");\n        ^\n\nError: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n    at file:///opt/codex.js:107:9\n\nNode.js v26.0.0\n',
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "Node error code",
      stderr:
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve (node:internal/modules)\n",
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "last labelled error",
      stderr: "Error: earlier failure\nTypeError: invalid argument\n    at start\n",
      expected: "TypeError: invalid argument",
    },
    {
      name: "Rust CLI error",
      stderr: "error: invalid value for --config\n\nFor more information, try '--help'.\n",
      expected: "error: invalid value for --config",
    },
    {
      name: "fatal error",
      stderr: "fatal: could not launch\nadditional context\n",
      expected: "fatal: could not launch",
    },
    {
      name: "last non-empty unlabelled line",
      stderr: "starting\nlauncher unavailable\n \t\n",
      expected: "launcher unavailable",
    },
    {
      name: "terminal escapes, carriage returns and whitespace",
      stderr: "loading\r\u001b[31mError:\t Missing   dependency\u001b[0m\r\nstack\n",
      expected: "Error: Missing dependency",
    },
    {
      name: "512-character cap",
      stderr: `Error: ${"x".repeat(600)}`,
      expected: `Error: ${"x".repeat(504)}…`,
    },
    { name: "blank stderr", stderr: "\n \t\r\n", expected: undefined },
  ])("selects $name", ({ stderr, expected }) => {
    const tail = makeStderrTail();
    tail.append(new TextEncoder().encode(stderr));
    assert.equal(tail.excerpt(), expected);
  });

  it("retains only the last 8 KiB across chunks, including an oversized chunk", () => {
    const tail = makeStderrTail();
    const encoder = new TextEncoder();
    tail.append(encoder.encode("Error: discarded\n"));
    tail.append(encoder.encode(`${"x".repeat(9 * 1024)}\nlast diagnostic`));
    assert.equal(tail.excerpt(), "last diagnostic");

    tail.append(encoder.encode(`\nError: retained\n${" ".repeat(8 * 1024 - 17)}`));
    assert.equal(tail.excerpt(), "Error: retained");
    tail.append(encoder.encode(" ".repeat(17)));
    assert.isUndefined(tail.excerpt());
  });
});

describe("Codex App Server child process termination", () => {
  it.effect("waits for the final stderr chunk before constructing the exit error", () =>
    Effect.gen(function* () {
      const tail = makeStderrTail();
      const draining = yield* Deferred.make<void>();
      const finished = yield* Deferred.make<void>();
      const result = yield* Deferred.make<CodexError.CodexAppServerError>();
      yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        {
          done: Deferred.succeed(draining, undefined).pipe(
            Effect.andThen(Deferred.await(finished)),
          ),
          excerpt: tail.excerpt,
        },
      ).pipe(
        Effect.flatMap((error) => Deferred.succeed(result, error)),
        Effect.forkChild,
      );
      yield* Deferred.await(draining);
      assert.isFalse(yield* Deferred.isDone(result));
      tail.append(new TextEncoder().encode("Error: final diagnostic\n"));
      yield* Deferred.succeed(finished, undefined);
      const error = yield* Deferred.await(result);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: final diagnostic");
      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: Error: final diagnostic",
      );
    }),
  );

  it.effect("uses the captured tail after 250 ms when stderr stays open", () =>
    Effect.gen(function* () {
      const draining = yield* Deferred.make<void>();
      const fiber = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        {
          done: Deferred.succeed(draining, undefined).pipe(Effect.andThen(Effect.never)),
          excerpt: () => "fatal: launcher failed",
        },
      ).pipe(Effect.forkChild);
      yield* Deferred.await(draining);
      yield* TestClock.adjust("250 millis");
      const error = yield* Fiber.join(fiber);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "fatal: launcher failed");
    }),
  );

  it.effect("keeps the exit message unchanged for blank stderr", () =>
    Effect.gen(function* () {
      const error = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        { done: Effect.void, excerpt: makeStderrTail().excerpt },
      );
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.isUndefined(error.stderr);
      assert.equal(error.message, "Codex App Server process exited with code 1");
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
