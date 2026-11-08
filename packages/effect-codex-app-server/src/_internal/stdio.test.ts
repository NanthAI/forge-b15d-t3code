import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as PlatformError from "effect/PlatformError";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { captureStderrTail, excerptStderr, makeTerminationError } from "./stdio.ts";

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

  it("excerpts the error-labelled line of stderr", () => {
    assert.equal(
      excerptStderr(
        "file:///x/codex.js:107\n  throw err;\n  ^\n\nError: Missing dep\n    at foo (x:1:1)\n",
      ),
      "Error: Missing dep",
    );
    assert.equal(excerptStderr("TypeError [ERR_X]: bad\n at y"), "TypeError [ERR_X]: bad");
    assert.equal(excerptStderr("warn\nerror: boom\nhint"), "error: boom");
    assert.equal(excerptStderr("fatal: nope\nmore"), "fatal: nope");
    assert.equal(excerptStderr("first\n\nlast one\n"), "last one");
    assert.equal(excerptStderr("\u001b[31mError:\u001b[0m  a\tb\r\nz\r"), "Error: a b");
    assert.equal(excerptStderr("Error: " + "a".repeat(600))?.length, 512);
    assert.equal(excerptStderr(" \n\r\n "), undefined);
  });

  it.effect("keeps only the tail of stderr", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const enc = new TextEncoder();
        const capture = yield* captureStderrTail(
          Stream.make(enc.encode("a".repeat(20000)), enc.encode("Error: tail")),
        );
        yield* capture.done;
        const tail = yield* capture.tail;
        assert.equal(tail.length, 8 * 1024);
        assert.isTrue(tail.endsWith("Error: tail"));
      }),
    ),
  );
});
