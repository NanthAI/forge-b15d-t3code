import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as PlatformError from "effect/PlatformError";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";
import { STDERR_EXCERPT_MAX_CHARS, makeTerminationError, summarizeStderr } from "./stdio.ts";

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

  it("summarizes stderr into one line", () => {
    const launcher =
      "file:///x/codex.js:107\n  throw new Error(m);\n  ^\n\nError: Missing dep. Reinstall\n    at foo (file:///x:1:1)\n\nNode.js v22\n";
    assert.equal(summarizeStderr(launcher), "Error: Missing dep. Reinstall");
    assert.equal(
      summarizeStderr("Error [ERR_MODULE_NOT_FOUND]: Cannot find x\n  at y\n"),
      "Error [ERR_MODULE_NOT_FOUND]: Cannot find x",
    );
    assert.equal(summarizeStderr("TypeError: bad\n at z\n"), "TypeError: bad");
    assert.equal(summarizeStderr("warn\nerror: no auth\ntrailing"), "error: no auth");
    assert.equal(summarizeStderr("fatal: boom\nmore"), "fatal: boom");
    assert.equal(summarizeStderr("first\nsecond thing\n\n"), "second thing");
    assert.equal(
      summarizeStderr("\u001b[31mError:\u001b[0m  red\ttext\r\nlater\r"),
      "Error: red text",
    );
    assert.equal(summarizeStderr("x".repeat(2000))?.length, STDERR_EXCERPT_MAX_CHARS);
    assert.equal(summarizeStderr(" \n\r\n "), undefined);
  });
});
