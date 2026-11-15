import { stripVTControlCharacters } from "node:util";

import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";

const encoder = new TextEncoder();
const STDERR_TAIL_MAX_BYTES = 8 * 1024;
const STDERR_EXCERPT_MAX_CHARS = 512;

export const makeStderrTail = () => {
  let tail = new Uint8Array(0);
  return {
    append(chunk: Uint8Array) {
      const retained = tail.subarray(
        Math.max(0, tail.length + chunk.length - STDERR_TAIL_MAX_BYTES),
      );
      const incoming = chunk.subarray(-STDERR_TAIL_MAX_BYTES);
      tail = new Uint8Array(retained.length + incoming.length);
      tail.set(retained);
      tail.set(incoming, retained.length);
    },
    excerpt() {
      const lines = stripVTControlCharacters(new TextDecoder().decode(tail))
        .split(/[\r\n]+/)
        .map((line) => line.replace(/\s+/g, " ").trim())
        .filter((line) => line.length > 0);
      // Node prints the thrown error after its source excerpt and before stack frames.
      const line =
        lines.findLast((line) => /^(?:\w*Error(?: \[ERR_[^\]]+\])?|error|fatal):/.test(line)) ??
        lines.at(-1);
      return line?.slice(0, STDERR_EXCERPT_MAX_CHARS);
    },
  };
};

export const makeChildStdio = (handle: ChildProcessSpawner.ChildProcessHandle) =>
  Stdio.make({
    args: Effect.succeed([]),
    stdin: handle.stdout,
    stdout: () =>
      Sink.mapInput(handle.stdin, (chunk: string | Uint8Array) =>
        typeof chunk === "string" ? encoder.encode(chunk) : chunk,
      ),
    stderr: () => Sink.drain,
  });

export const makeInMemoryStdio = Effect.fn("makeInMemoryStdio")(function* () {
  const input = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
  const output = yield* Queue.unbounded<string>();
  const decoder = new TextDecoder();

  return {
    stdio: Stdio.make({
      args: Effect.succeed([]),
      stdin: Stream.fromQueue(input),
      stdout: () =>
        Sink.forEach((chunk: string | Uint8Array) =>
          Queue.offer(
            output,
            typeof chunk === "string" ? chunk : decoder.decode(chunk, { stream: true }),
          ),
        ),
      stderr: () => Sink.drain,
    }),
    input,
    output,
  };
});

type ChildProcessTerminationHandle = Pick<
  ChildProcessSpawner.ChildProcessHandle,
  "exitCode" | "pid"
>;

export const makeTerminationError = Effect.fn("makeTerminationError")(function* (
  handle: ChildProcessTerminationHandle,
  stderr?: {
    readonly tail: ReturnType<typeof makeStderrTail>;
    readonly done: Effect.Effect<void>;
  },
): Effect.fn.Return<CodexError.CodexAppServerError> {
  const exit = yield* Effect.result(handle.exitCode);
  if (Result.isFailure(exit)) {
    return new CodexError.CodexAppServerTransportError({
      operation: "read-process-exit-status",
      pid: handle.pid,
      cause: exit.failure,
    });
  }
  if (stderr) {
    yield* stderr.done.pipe(Effect.timeoutOption("250 millis"));
  }
  const excerpt = stderr?.tail.excerpt();
  return new CodexError.CodexAppServerProcessExitedError({
    code: exit.success,
    pid: handle.pid,
    ...(excerpt === undefined ? {} : { stderr: excerpt }),
  });
});
