import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
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

export const STDERR_TAIL_BYTES = 8 * 1024;
export const STDERR_EXCERPT_MAX_CHARS = 512;
const STDERR_WAIT = "250 millis";

// eslint-disable-next-line no-control-regex
const ANSI_PATTERN =
  /\u001b(?:\[[0-?]*[ -/]*[@-~]|\][^\u0007\u001b]*(?:\u0007|\u001b\\)|[@-Z\\-_])/g;
const ERROR_LABEL_PATTERN = /^(?:(?:[A-Za-z_$][\w$]*)?Error(?: \[[^\]]+\])?|error|fatal)\s*:/i;

const cleanLine = (line: string) => line.replace(ANSI_PATTERN, "").replace(/\s+/g, " ").trim();

/** Picks one line from stderr: the last error-labelled line, else the last non-empty line. */
export const summarizeStderr = (stderr: string): string | undefined => {
  const lines = stderr
    .split(/\r\n|\r|\n/)
    .map(cleanLine)
    .filter((line) => line.length > 0);
  if (lines.length === 0) return undefined;
  const line = lines.findLast((candidate) => ERROR_LABEL_PATTERN.test(candidate)) ?? lines.at(-1)!;
  return line.length > STDERR_EXCERPT_MAX_CHARS ? line.slice(0, STDERR_EXCERPT_MAX_CHARS) : line;
};

export interface StderrCapture {
  readonly tail: Effect.Effect<string>;
  readonly finished: Effect.Effect<void>;
}

/** Drains stderr, retaining the last STDERR_TAIL_BYTES bytes. Fork it in the client scope. */
export const captureStderr = Effect.fn("captureStderr")(function* (
  stderr: Stream.Stream<Uint8Array, unknown>,
) {
  let tail = new Uint8Array(0);
  const done = yield* Deferred.make<void>();
  yield* stderr.pipe(
    Stream.runForEach((chunk) =>
      Effect.sync(() => {
        const merged = new Uint8Array(tail.length + chunk.length);
        merged.set(tail);
        merged.set(chunk, tail.length);
        tail = merged.length > STDERR_TAIL_BYTES ? merged.slice(-STDERR_TAIL_BYTES) : merged;
      }),
    ),
    Effect.ignore,
    Effect.ensuring(Deferred.succeed(done, undefined)),
    Effect.forkScoped,
  );
  return {
    tail: Effect.sync(() => new TextDecoder().decode(tail)),
    finished: Deferred.await(done),
  } satisfies StderrCapture;
});

type ChildProcessTerminationHandle = Pick<
  ChildProcessSpawner.ChildProcessHandle,
  "exitCode" | "pid"
>;

export const makeTerminationError = (
  handle: ChildProcessTerminationHandle,
  capture?: StderrCapture,
): Effect.Effect<CodexError.CodexAppServerError> =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(handle.exitCode);
    if (exit._tag === "Failure") {
      return new CodexError.CodexAppServerTransportError({
        operation: "read-process-exit-status",
        pid: handle.pid,
        cause: Cause.squash(exit.cause),
      });
    }
    let stderr: string | undefined;
    if (capture) {
      yield* capture.finished.pipe(Effect.timeoutOption(STDERR_WAIT));
      stderr = summarizeStderr(yield* capture.tail);
    }
    return new CodexError.CodexAppServerProcessExitedError({
      code: exit.value,
      pid: handle.pid,
      ...(stderr !== undefined ? { stderr } : {}),
    });
  });
