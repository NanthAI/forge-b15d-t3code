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

describe("Codex App Server child process termination", () => {
  const launcherError =
    "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";
  for (const [name, input, expected] of [
    [
      "Node launcher",
      `file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n    throw new Error(message);\n          ^\n\n${launcherError}\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:11\n\nNode.js v24.0.0\n`,
      launcherError,
    ],
    [
      "Node error code",
      "Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at moduleResolve (node:internal/modules/esm/resolve:855:18)",
      "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    ],
    [
      "last error label",
      "Error: earlier failure\nTypeError: invalid argument\n    at main (file:///codex.js:1:1)",
      "TypeError: invalid argument",
    ],
    [
      "Rust CLI",
      "error: unexpected argument '--bad' found\n\nFor more information, try '--help'.",
      "error: unexpected argument '--bad' found",
    ],
    ["fatal error", "fatal: cannot start\nmore context", "fatal: cannot start"],
    ["unlabelled output", "starting\n could not launch   codex \n\n", "could not launch codex"],
    [
      "terminal escapes and carriage returns",
      "progress\r\u001b[31mError:\u001b[0m\t missing   dependency \r\n  at main\n",
      "Error: missing dependency",
    ],
    ["excerpt limit", `Error: ${"x".repeat(600)}`, `Error: ${"x".repeat(505)}`],
    ["blank stderr", " \n\t\r\n", undefined],
    ["empty stderr", "", undefined],
    ["byte tail", `Error: discarded\n${"é".repeat(4096)}\nkept\n`, "kept"],
  ] as const) {
    it.effect(`includes a bounded stderr excerpt for ${name}`, () =>
      Effect.gen(function* () {
        const bytes = new TextEncoder().encode(input);
        const midpoint = Math.floor(bytes.length / 2);
        const stderr = yield* captureStderr(
          Stream.make(bytes.subarray(0, midpoint), bytes.subarray(midpoint)),
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
        assert.equal(error.stderr, expected);
        assert.equal(
          error.message,
          `Codex App Server process exited with code 1${expected === undefined ? "" : `: ${expected}`}`,
        );
      }).pipe(Effect.scoped),
    );
  }

  it.effect("uses the captured tail after 250 ms even when stderr stays open", () =>
    Effect.gen(function* () {
      const captured = yield* Deferred.make<void>();
      const stderr = yield* captureStderr(
        Stream.concat(
          Stream.make(new TextEncoder().encode("Error: launch failed\n")),
          Stream.fromEffect(
            Deferred.succeed(captured, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        ),
      );
      yield* Deferred.await(captured);
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkChild);
      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* TestClock.adjust("1 millis");
      const error = yield* Fiber.join(pending);
      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: Error: launch failed",
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
