import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as PlatformError from "effect/PlatformError";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
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
});

describe("summarizeStderr", () => {
  it("picks the error line from a Node launcher trace", () => {
    assert.equal(
      summarizeStderr("file:///a/codex.js:107\n  throw e;\n  ^\n\nError: Missing dep\n    at x\n"),
      "Error: Missing dep",
    );
  });
  it("handles coded Node errors, Rust errors and fatal", () => {
    assert.equal(
      summarizeStderr("x\nError [ERR_MODULE_NOT_FOUND]: nope\n  at y"),
      "Error [ERR_MODULE_NOT_FOUND]: nope",
    );
    assert.equal(summarizeStderr("TypeError: bad\n  at y"), "TypeError: bad");
    assert.equal(
      summarizeStderr("error: unexpected argument\nUsage: codex"),
      "error: unexpected argument",
    );
    assert.equal(summarizeStderr("fatal: oops\ntrailing"), "fatal: oops");
  });
  it("falls back to the last non-empty line", () => {
    assert.equal(summarizeStderr("first\nsecond\n\n"), "second");
  });
  it("strips escapes, collapses whitespace and caps length", () => {
    assert.equal(summarizeStderr("\u001b[31mError:\u001b[0m   a \t b\r\n"), "Error: a b");
    assert.equal(summarizeStderr("Error: " + "x".repeat(600))?.length, 512);
  });
  it("returns undefined for blank stderr", () => {
    assert.isUndefined(summarizeStderr(" \n\r\n"));
  });
});

describe("captureStderrTail", () => {
  it.effect("keeps only the last 8 KiB", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const enc = new TextEncoder();
        const capture = yield* captureStderrTail(
          Stream.make(enc.encode("a".repeat(10000)), enc.encode("b".repeat(5000))),
        );
        const text = yield* capture.read;
        assert.equal(text.length, 8192);
        assert.isTrue(text.endsWith("b".repeat(5000)));
        assert.isTrue(text.startsWith("a"));
      }),
    ),
  );
});
