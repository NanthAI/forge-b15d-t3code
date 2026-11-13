import * as NodeUtil from "node:util";

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

const STDERR_TAIL_BYTES = 8 * 1024;
const STDERR_EXCERPT_CHARS = 512;

export const appendStderrTail = (tail: Uint8Array, chunk: Uint8Array): Uint8Array => {
  if (chunk.length >= STDERR_TAIL_BYTES) return new Uint8Array(chunk.subarray(-STDERR_TAIL_BYTES));
  const retained = tail.subarray(-Math.min(tail.length, STDERR_TAIL_BYTES - chunk.length));
  const next = new Uint8Array(retained.length + chunk.length);
  next.set(retained);
  next.set(chunk, retained.length);
  return next;
};

const stderrExcerpt = (tail: Uint8Array): string | undefined => {
  const lines = NodeUtil.stripVTControlCharacters(new TextDecoder().decode(tail))
    .split(/[\r\n]+/)
    .map((line) => line.replace(/\s+/g, " ").trim())
    .filter((line) => line.length > 0);
  const line =
    lines.findLast((line) => /^(?:\w*Error(?: \[ERR_[^\]]+\])?|error|fatal):/.test(line)) ??
    lines.at(-1);
  return line?.slice(0, STDERR_EXCERPT_CHARS);
};

export const makeTerminationError = Effect.fn("makeTerminationError")(function* (
  handle: ChildProcessTerminationHandle,
  stderr?: {
    readonly done: Effect.Effect<void>;
    readonly tail: Effect.Effect<Uint8Array>;
  },
): Effect.fn.Return<CodexError.CodexAppServerError> {
  const result = yield* Effect.result(handle.exitCode);
  if (Result.isFailure(result)) {
    return new CodexError.CodexAppServerTransportError({
      operation: "read-process-exit-status",
      pid: handle.pid,
      cause: result.failure,
    });
  }
  if (stderr) yield* stderr.done.pipe(Effect.timeout("250 millis"), Effect.ignore);
  const excerpt = stderr ? stderrExcerpt(yield* stderr.tail) : undefined;
  return new CodexError.CodexAppServerProcessExitedError({
    code: result.success,
    pid: handle.pid,
    ...(excerpt === undefined ? {} : { stderr: excerpt }),
  });
});
