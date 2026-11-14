import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { captureStderr, makeTerminationError } from "./stdio.ts";

const encoder = new TextEncoder();

describe("Codex App Server child process termination", () => {
  it.effect.each([
    {
      name: "Node launcher",
      chunks: [
        "file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107\nthrow new Error(message);\n      ^\n\nError: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\n    at file:///usr/local/lib/node_modules/@openai/codex/bin/codex.js:107:11\n\nNode.js v22.0.0\n",
      ],
      expected:
        "Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    },
    {
      name: "Node error code",
      chunks: ["Error [ERR_MODULE_NOT_FOUND]: Cannot find package\n    at resolve\n"],
      expected: "Error [ERR_MODULE_NOT_FOUND]: Cannot find package",
    },
    {
      name: "last error",
      chunks: ["Error: first\nTypeError: last\n    at main\n"],
      expected: "TypeError: last",
    },
    {
      name: "Rust CLI",
      chunks: ["error: invalid argument\n\nFor more information, try '--help'.\n"],
      expected: "error: invalid argument",
    },
    {
      name: "fatal error",
      chunks: ["fatal: cannot start\ncontext\n"],
      expected: "fatal: cannot start",
    },
    {
      name: "unlabelled output",
      chunks: ["starting\nlauncher could not start\n\n"],
      expected: "launcher could not start",
    },
    {
      name: "terminal escapes and whitespace",
      chunks: ["progress\r\u001b[31mError:\t missing   dependency\u001b[0m\r\n    at main\n"],
      expected: "Error: missing dependency",
    },
    {
      name: "excerpt cap",
      chunks: ["Error: " + "x".repeat(1024)],
      expected: "Error: " + "x".repeat(505),
    },
    { name: "blank stderr", chunks: [" \t\r\n"], expected: undefined },
    {
      name: "single oversized chunk",
      chunks: ["Error: discarded\n" + "x".repeat(8192) + "\nlast line\n"],
      expected: "last line",
    },
    {
      name: "byte tail across chunks",
      chunks: ["Error: discarded\n", "é".repeat(4096), "\nlast line\n"],
      expected: "last line",
    },
    { name: "split UTF-8", chunks: ["Error: échec\n"], expected: "Error: échec" },
  ])("extracts bounded stderr for $name", ({ name, chunks, expected }) =>
    Effect.gen(function* () {
      const bytes = encoder.encode(chunks.join(""));
      const stderr = yield* captureStderr(
        Stream.fromIterable(
          name === "split UTF-8"
            ? Array.from(bytes, (byte) => Uint8Array.of(byte))
            : chunks.map((chunk) => encoder.encode(chunk)),
        ),
      );
      const error = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr.excerpt,
      );

      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, expected);
      assert.equal(
        error.message,
        `Codex App Server process exited with code 1${expected ? `: ${expected}` : ""}`,
      );
    }),
  );

  it.effect("waits for the final stderr bytes after exit", () =>
    Effect.gen(function* () {
      const finalChunk = yield* Deferred.make<Uint8Array>();
      const stderr = yield* captureStderr(Stream.fromEffect(Deferred.await(finalChunk)));
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr.excerpt,
      ).pipe(Effect.forkScoped);
      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* Deferred.succeed(finalChunk, encoder.encode("Error: final diagnostic\n"));
      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: final diagnostic");
    }),
  );

  it.effect("stops waiting for stderr after 250 ms and uses the captured tail", () =>
    Effect.gen(function* () {
      const stderr = yield* captureStderr(
        Stream.concat(Stream.make(encoder.encode("Error: retained\n")), Stream.never),
      );
      const pending = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(52),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        stderr.excerpt,
      ).pipe(Effect.forkScoped);
      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* TestClock.adjust("1 millis");
      const error = yield* Fiber.join(pending);
      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, "Error: retained");
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
