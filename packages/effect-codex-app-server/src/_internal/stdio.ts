import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
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

const STDERR_TAIL_BYTES = 8 * 1024;
const STDERR_WAIT_MILLIS = 250;
const STDERR_LINE_MAX_CHARS = 512;

export interface StderrCapture {
  /** Waits briefly for stderr to finish, then returns the retained tail. */
  readonly read: Effect.Effect<string>;
}

/** Drains stderr while keeping its last 8 KiB. */
export const captureStderrTail = Effect.fn("captureStderrTail")(function* (
  stderr: Stream.Stream<Uint8Array, unknown>,
) {
  const tail = yield* Ref.make(new Uint8Array(0));
  const done = yield* Deferred.make<void>();
  yield* stderr.pipe(
    Stream.runForEach((chunk) =>
      Ref.update(tail, (previous) => {
        const merged = new Uint8Array(previous.length + chunk.length);
        merged.set(previous);
        merged.set(chunk, previous.length);
        return merged.length > STDERR_TAIL_BYTES
          ? merged.slice(merged.length - STDERR_TAIL_BYTES)
          : merged;
      }),
    ),
    Effect.ignore,
    Effect.ensuring(Deferred.succeed(done, undefined)),
    Effect.forkScoped,
  );
  const capture: StderrCapture = {
    read: Deferred.await(done).pipe(
      Effect.timeoutOption(STDERR_WAIT_MILLIS),
      Effect.andThen(Ref.get(tail)),
      Effect.map((bytes) => new TextDecoder().decode(bytes)),
    ),
  };
  return capture;
});

// eslint-disable-next-line no-control-regex
const ANSI_PATTERN =
  /\u001b(?:\[[0-?]*[ -/]*[@-~]|\][^\u0007\u001b]*(?:\u0007|\u001b\\)|[@-Z\\-_])/g;
const ERROR_LABELLED = /^(?:[A-Za-z]*Error(?: \[[A-Z0-9_]+\])?|error|fatal)\s*:/;

/** Picks the last error-labelled line of stderr, else the last non-empty line. */
export const summarizeStderr = (raw: string): string | undefined => {
  const lines = raw
    .replace(ANSI_PATTERN, "")
    .split(/\r\n|\r|\n/)
    .map((line) => line.replace(/\s+/g, " ").trim())
    .filter((line) => line.length > 0);
  const line = lines.findLast((candidate) => ERROR_LABELLED.test(candidate)) ?? lines.at(-1);
  if (line === undefined) return undefined;
  return line.length > STDERR_LINE_MAX_CHARS ? line.slice(0, STDERR_LINE_MAX_CHARS) : line;
};

type ChildProcessTerminationHandle = Pick<
  ChildProcessSpawner.ChildProcessHandle,
  "exitCode" | "pid"
>;

export const makeTerminationError = (
  handle: ChildProcessTerminationHandle,
  stderr?: StderrCapture,
): Effect.Effect<CodexError.CodexAppServerError> =>
  Effect.matchEffect(handle.exitCode, {
    onFailure: (cause) =>
      Effect.succeed(
        new CodexError.CodexAppServerTransportError({
          operation: "read-process-exit-status",
          pid: handle.pid,
          cause,
        }),
      ),
    onSuccess: (code) =>
      (stderr ? stderr.read : Effect.succeed("")).pipe(
        Effect.map((text) => {
          const line = summarizeStderr(text);
          return new CodexError.CodexAppServerProcessExitedError({
            code,
            pid: handle.pid,
            ...(line === undefined ? {} : { stderr: line }),
          });
        }),
      ),
  });
