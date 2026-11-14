import { stripVTControlCharacters } from "node:util";

import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";

const encoder = new TextEncoder();
const STDERR_TAIL_BYTES = 8 * 1024;

export const captureStderr = Effect.fn("captureStderr")(function* (
  handle: Pick<ChildProcessSpawner.ChildProcessHandle, "stderr">,
) {
  let tail: Uint8Array = new Uint8Array();
  const fiber = yield* handle.stderr.pipe(
    Stream.runForEach((chunk) =>
      Effect.sync(() => {
        const next = new Uint8Array(Math.min(tail.length + chunk.length, STDERR_TAIL_BYTES));
        const chunkTail = chunk.subarray(-next.length);
        const retained = next.length - chunkTail.length;
        next.set(tail.subarray(tail.length - retained));
        next.set(chunkTail, retained);
        tail = next;
      }),
    ),
    Effect.ignore,
    Effect.forkScoped,
  );

  return {
    done: Fiber.await(fiber),
    excerpt: Effect.sync(() => {
      const lines = stripVTControlCharacters(new TextDecoder().decode(tail))
        .split(/[\r\n]+/)
        .map((line) => line.replace(/\s+/g, " ").trim())
        .filter(Boolean);
      const line =
        lines.findLast((line) => /^(?:\w*Error(?: \[ERR_[^\]]+\])?|error|fatal):/.test(line)) ??
        lines.at(-1);
      return line?.slice(0, 512);
    }),
  };
});

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
  stderr?: Effect.Success<ReturnType<typeof captureStderr>>,
): Effect.fn.Return<CodexError.CodexAppServerError> {
  const exit = yield* Effect.result(handle.exitCode);
  if (Result.isFailure(exit)) {
    return new CodexError.CodexAppServerTransportError({
      operation: "read-process-exit-status",
      pid: handle.pid,
      cause: exit.failure,
    });
  }
  if (stderr) yield* stderr.done.pipe(Effect.timeoutOption("250 millis"));
  return new CodexError.CodexAppServerProcessExitedError({
    code: exit.success,
    pid: handle.pid,
    stderr: stderr ? yield* stderr.excerpt : undefined,
  });
});
