import { stripVTControlCharacters } from "node:util";

import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";

import * as CodexError from "../errors.ts";

const encoder = new TextEncoder();
const STDERR_TAIL_BYTES = 8 * 1024;
const STDERR_EXCERPT_CHARACTERS = 512;

export const captureStderr = Effect.fn("captureStderr")(function* (
  stream: Stream.Stream<Uint8Array, PlatformError.PlatformError>,
) {
  let tail = Buffer.alloc(0);
  const drain = yield* stream.pipe(
    Stream.runForEach((chunk) =>
      Effect.sync(() => {
        const suffix = chunk.subarray(-STDERR_TAIL_BYTES);
        tail = Buffer.concat([
          tail.subarray(Math.max(0, tail.length + suffix.length - STDERR_TAIL_BYTES)),
          suffix,
        ]);
      }),
    ),
    Effect.ignore,
    Effect.forkScoped,
  );

  return Fiber.await(drain).pipe(
    // Descendants may keep stderr open after the app-server has exited.
    Effect.timeoutOption("250 millis"),
    Effect.map(() => {
      const lines = stripVTControlCharacters(tail.toString("utf8"))
        .split(/[\r\n]+/)
        .map((line) => line.replace(/\s+/g, " ").trim())
        .filter(Boolean);
      const line =
        lines.findLast((line) => /^(?:\w*Error(?: \[ERR_[^\]]+\])?|error|fatal):/.test(line)) ??
        lines.at(-1);
      return line?.slice(0, STDERR_EXCERPT_CHARACTERS);
    }),
  );
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
  stderr?: Effect.Effect<string | undefined>,
): Effect.fn.Return<CodexError.CodexAppServerError> {
  const exit = yield* Effect.result(handle.exitCode);
  if (Result.isFailure(exit)) {
    return new CodexError.CodexAppServerTransportError({
      operation: "read-process-exit-status",
      pid: handle.pid,
      cause: exit.failure,
    });
  }
  const excerpt = stderr === undefined ? undefined : yield* stderr;
  return new CodexError.CodexAppServerProcessExitedError({
    code: exit.success,
    pid: handle.pid,
    ...(excerpt === undefined ? {} : { stderr: excerpt }),
  });
});
