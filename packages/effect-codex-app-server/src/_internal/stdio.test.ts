import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as PlatformError from "effect/PlatformError";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import * as Stream from "effect/Stream";
import { captureStderrTail, makeTerminationError, summarizeStderr } from "./stdio.ts";

describe("Codex App Server child process termination", () => {
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

  it.effect("adds the summarized stderr line to the exit error", () =>
    Effect.gen(function* () {
      const tail = yield* captureStderrTail(
        Stream.make(new TextEncoder().encode("boot\nError: nope\n  at x:1\n")),
      );
      const error = yield* makeTerminationError(
        {
          pid: ChildProcessSpawner.ProcessId(1),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        },
        tail,
      );
      assert.equal(error.message, "Codex App Server process exited with code 1: Error: nope");
    }).pipe(Effect.scoped),
  );

  it.effect("keeps only the last 8 KiB of stderr", () =>
    Effect.gen(function* () {
      const enc = new TextEncoder();
      const tail = yield* captureStderrTail(
        Stream.make(enc.encode("a".repeat(20000)), enc.encode("\nlast line")),
      );
      const text = yield* tail.read;
      assert.equal(text.length, 8192);
      assert.isTrue(text.endsWith("\nlast line"));
    }).pipe(Effect.scoped),
  );
});

describe("summarizeStderr", () => {
  it("picks the error line from a Node launcher trace", () => {
    assert.equal(
      summarizeStderr(
        "file:///a/codex.js:107\n  throw e;\n  ^\n\nError: Missing dep\n    at f (x:1:1)\n\nNode.js v22\n",
      ),
      "Error: Missing dep",
    );
  });
  it("handles Node errors with a code, TypeError, Rust error and fatal", () => {
    assert.equal(
      summarizeStderr("x\nError [ERR_MODULE_NOT_FOUND]: Cannot find\n  at y"),
      "Error [ERR_MODULE_NOT_FOUND]: Cannot find",
    );
    assert.equal(summarizeStderr("TypeError: bad\n  at y"), "TypeError: bad");
    assert.equal(
      summarizeStderr("error: unexpected argument\n\nUsage: codex"),
      "error: unexpected argument",
    );
    assert.equal(summarizeStderr("fatal: broken\nhint"), "fatal: broken");
  });
  it("falls back to the last non-empty line", () => {
    assert.equal(summarizeStderr("one\ntwo\n\n  \n"), "two");
  });
  it("strips escapes, handles carriage returns and collapses whitespace", () => {
    assert.equal(summarizeStderr("\u001b[31mError:\u001b[0m   a \t b\r\nmore\r"), "Error: a b");
    assert.equal(summarizeStderr("first\rsecond\r"), "second");
  });
  it("caps the line at 512 characters", () => {
    assert.equal(summarizeStderr("Error: " + "x".repeat(1000))?.length, 512);
  });
  it("returns undefined for blank stderr", () => {
    assert.isUndefined(summarizeStderr(" \n\r\n"));
  });
});
