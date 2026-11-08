import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Ref from "effect/Ref";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
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

export const STDERR_TAIL_BYTES = 8 * 1024;
const STDERR_SETTLE_MS = 250;
const STDERR_EXCERPT_MAX_CHARS = 512;

export interface StderrCapture {
  readonly tail: Effect.Effect<string>;
  readonly done: Effect.Effect<void>;
}

/** Keeps the last `STDERR_TAIL_BYTES` bytes of a stream, draining the rest. */
export const captureStderrTail = Effect.fn("captureStderrTail")(function* <E>(
  stderr: Stream.Stream<Uint8Array, E>,
  maxBytes: number = STDERR_TAIL_BYTES,
) {
  const ref = yield* Ref.make<Uint8Array>(new Uint8Array(0));
  const finished = yield* Deferred.make<void>();
  yield* Stream.runForEach(stderr, (chunk) =>
    Ref.update(ref, (current) => {
      const joined = new Uint8Array(current.length + chunk.length);
      joined.set(current, 0);
      joined.set(chunk, current.length);
      return joined.length > maxBytes ? joined.slice(joined.length - maxBytes) : joined;
    }),
  ).pipe(Effect.ignore, Effect.ensuring(Deferred.succeed(finished, undefined)), Effect.forkScoped);
  return {
    tail: Effect.map(Ref.get(ref), (bytes) => new TextDecoder().decode(bytes)),
    done: Deferred.await(finished),
  } satisfies StderrCapture;
});

const ANSI_PATTERN =
  // eslint-disable-next-line no-control-regex
  /\u001b(?:\[[0-?]*[ -/]*[@-~]|\][^\u0007\u001b]*(?:\u0007|\u001b\\)|[@-Z\\-_])/g;
const ERROR_LABEL = /^(?:[A-Za-z_$][\w$.]*Error(?:\s*\[[^\]]+\])?|error|fatal):/i;

export const excerptStderr = (stderr: string): string | undefined => {
  const lines = stderr
    .replace(ANSI_PATTERN, "")
    .split(/\r\n|\r|\n/)
    .map((line) => line.replace(/\s+/g, " ").trim())
    .filter((line) => line.length > 0);
  const line = lines.findLast((candidate) => ERROR_LABEL.test(candidate)) ?? lines.at(-1);
  if (line === undefined) return undefined;
  return line.length > STDERR_EXCERPT_MAX_CHARS ? line.slice(0, STDERR_EXCERPT_MAX_CHARS) : line;
};

export const makeTerminationError = (
  handle: ChildProcessTerminationHandle,
  capture?: StderrCapture,
): Effect.Effect<CodexError.CodexAppServerError> =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(handle.exitCode);
    if (Exit.isFailure(exit)) {
      return new CodexError.CodexAppServerTransportError({
        operation: "read-process-exit-status",
        pid: handle.pid,
        cause: Cause.squash(exit.cause),
      });
    }
    let stderr: string | undefined;
    if (capture) {
      yield* capture.done.pipe(Effect.timeoutOption(`${STDERR_SETTLE_MS} millis`));
      stderr = excerptStderr(yield* capture.tail);
    }
    return new CodexError.CodexAppServerProcessExitedError({
      code: exit.value,
      pid: handle.pid,
      ...(stderr ? { stderr } : {}),
    });
  });
