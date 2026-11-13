import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";
import { TestClock } from "effect/testing";

import * as CodexError from "../errors.ts";
import { makeStderrCapture, makeTerminationError } from "./stdio.ts";

describe("Codex App Server child process termination", () => {
  it.effect.each([
    {
      name: "Node launcher source excerpt and stack",
      chunks: [
        'file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n    throw new Error("Missing optional dependency");\n          ^\n\n',
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:11\n\nNode.js v22.0.0\n",
      ],
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "Node error code",
      chunks: [
        "TypeError [ERR_INVALID_ARG_TYPE]: Invalid argument\n    at launch (codex.js:107:11)\n",
      ],
      expected: "TypeError [ERR_INVALID_ARG_TYPE]: Invalid argument",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: invalid value for --config\n\nFor more information, try '--help'.\n"],
      expected: "error: invalid value for --config",
    },
    {
      name: "last error-labelled line",
      chunks: ["Error: first failure\nfatal: could not launch\n    at launch (codex.js:107:11)\n"],
      expected: "fatal: could not launch",
    },
    {
      name: "last non-empty line without a label",
      chunks: ["starting\nconfiguration unavailable\n  \n"],
      expected: "configuration unavailable",
    },
    {
      name: "terminal escapes, whitespace and carriage returns",
      chunks: [
        "loading\r\u001b[31mError:   missing\tdependency\u001b[0m\r\n    at launch (codex.js:107:11)\n",
      ],
      expected: "Error: missing dependency",
    },
    {
      name: "512-character cap",
      chunks: [`Error: ${"x".repeat(600)}\n`],
      expected: `Error: ${"x".repeat(505)}`,
    },
    { name: "blank stderr", chunks: [" \n\r\t"], expected: undefined },
    { name: "empty stderr", chunks: [], expected: undefined },
    {
      name: "8 KiB tail across chunks",
      chunks: ["Error: discarded\n", "x".repeat(8 * 1024), "\nlast line\n"],
      expected: "last line",
    },
    {
      name: "8 KiB tail within a large multibyte chunk",
      chunks: [`Error: discarded\n${"é".repeat(4100)}\nlast line\n`],
      expected: "last line",
    },
  ])("includes a bounded diagnostic from $name", ({ chunks, expected }) =>
    Effect.gen(function* () {
      const stderr = yield* makeStderrCapture(
        Stream.fromIterable(chunks.map((chunk) => new TextEncoder().encode(chunk))),
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
      const output = yield* Deferred.make<Uint8Array>();
      const stderr = yield* makeStderrCapture(Stream.fromEffect(Deferred.await(output)));
      const fiber = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Deferred.succeed(exited, undefined).pipe(
            Effect.as(ChildProcessSpawner.ExitCode(1)),
          ),
        },
        stderr,
      ).pipe(Effect.forkChild);

      yield* Deferred.await(exited);
      yield* Deferred.succeed(output, new TextEncoder().encode("Error: late diagnostic\n"));
      const error = yield* Fiber.join(fiber);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: late diagnostic");
    }),
  );

  it.effect("uses captured stderr after 250 ms even if the pipe stays open", () =>
    Effect.gen(function* () {
      const stderr = yield* makeStderrCapture(
        Stream.concat(
          Stream.make(new TextEncoder().encode("Error: launcher exited\n")),
          Stream.never,
        ),
      );
      const fiber = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkChild);

      yield* TestClock.adjust(250);
      const error = yield* Fiber.join(fiber);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: launcher exited");
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
