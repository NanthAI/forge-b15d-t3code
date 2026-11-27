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
  it.effect.each([
    {
      name: "Node launcher error between source and stack frames",
      chunks: [
        "file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\n" +
          '  throw new Error("Missing optional dependency");\n' +
          "        ^\n\nError: Missing optional dependency @openai/codex-darwin-arm64. " +
          "Reinstall Codex: npm install -g @openai/codex@latest\n" +
          "    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:9\n\nNode.js v22.20.0\n",
      ],
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. " +
        "Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "last error-labelled line",
      chunks: ["Error: first\nTypeError: invalid value\n    at launcher.js:1:1\n"],
      expected: "TypeError: invalid value",
    },
    {
      name: "Node error code",
      chunks: ["Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at launcher.js:1:1\n"],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "Rust CLI error",
      chunks: ["error: unexpected argument '--invalid' found\n\nUsage: codex app-server\n"],
      expected: "error: unexpected argument '--invalid' found",
    },
    {
      name: "fatal diagnostic",
      chunks: ["fatal: could not start\nDetails follow\n"],
      expected: "fatal: could not start",
    },
    {
      name: "last non-empty line without an error label",
      chunks: ["Starting\nUnable to load config\n\n"],
      expected: "Unable to load config",
    },
    {
      name: "terminal escapes, carriage returns and whitespace",
      chunks: ["Starting\r\u001b[31mError:\t missing   dependency\u001b[0m\r\n\tat stack\r\n"],
      expected: "Error: missing dependency",
    },
    {
      name: "512-character limit",
      chunks: [`Error: ${"x".repeat(600)}\n`],
      expected: `Error: ${"x".repeat(504)}…`,
    },
    { name: "blank stderr", chunks: ["\n \t\r\n"], expected: undefined },
    { name: "empty stderr", chunks: [], expected: undefined },
    {
      name: "error at the start of the retained 8 KiB",
      chunks: [`Error: retained\n${"x".repeat(8192 - "Error: retained\n".length)}`],
      expected: "Error: retained",
    },
    {
      name: "8 KiB tail across chunks",
      chunks: ["Error: discarded\n", `${"x".repeat(8192 - 6)}\n`, "ready"],
      expected: "ready",
    },
    {
      name: "8 KiB tail in one large chunk",
      chunks: [`Error: discarded\n${"x".repeat(8192)}\nready`],
      expected: "ready",
    },
    {
      name: "tail bounded by UTF-8 bytes",
      chunks: ["Error: discarded\n", "é".repeat(4096), "\nready"],
      expected: "ready",
    },
    {
      name: "UTF-8 character split across chunks",
      chunks: ["Error: ", new Uint8Array([0xc3]), new Uint8Array([0xa9]), "chec\n"],
      expected: "Error: échec",
    },
  ])("includes $name in the exit error", ({ chunks, expected }) =>
    Effect.gen(function* () {
      const stderr = yield* makeStderrCapture(
        Stream.fromIterable(
          chunks.map((chunk) =>
            typeof chunk === "string" ? new TextEncoder().encode(chunk) : chunk,
          ),
        ),
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
        `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
      );
    }),
  );

  it.effect("waits for trailing stderr after the process exits", () =>
    Effect.gen(function* () {
      const trailingStderr = yield* Deferred.make<Uint8Array>();
      const stderr = yield* makeStderrCapture(Stream.fromEffect(Deferred.await(trailingStderr)));
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkScoped);
      yield* Deferred.succeed(
        trailingStderr,
        new TextEncoder().encode("Error: trailing diagnostic"),
      );
      const error = yield* Fiber.join(termination);
      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: Error: trailing diagnostic",
      );
    }),
  );

  it.effect("uses captured stderr after 250 ms even if the stream stays open", () =>
    Effect.gen(function* () {
      const captured = yield* Deferred.make<void>();
      const stderr = yield* makeStderrCapture(
        Stream.make(new TextEncoder().encode("Error: captured diagnostic")).pipe(
          Stream.tap(() => Deferred.succeed(captured, undefined)),
          Stream.concat(Stream.never),
        ),
      );
      yield* Deferred.await(captured);
      const termination = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr,
      ).pipe(Effect.forkScoped);
      yield* TestClock.adjust("250 millis");
      const error = yield* Fiber.join(termination);
      assert.equal(
        error.message,
        "Codex App Server process exited with code 1: Error: captured diagnostic",
      );
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
