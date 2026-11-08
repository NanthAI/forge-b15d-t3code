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

export interface StderrTail {
  /** Resolves with the retained stderr tail once the stream ends, or the current tail after a timeout. */
  readonly read: Effect.Effect<string>;
}

/**
 * Drains `stderr` while retaining only its last `STDERR_TAIL_BYTES` bytes.
 * Must be run in a scope; the drain fiber is forked into it.
 */
export const captureStderrTail = Effect.fn("captureStderrTail")(function* <E>(
  stderr: Stream.Stream<Uint8Array, E>,
) {
  let tail = new Uint8Array(0);
  const finished = yield* Deferred.make<void>();
  yield* Stream.runForEach(stderr, (chunk) =>
    Effect.sync(() => {
      const merged = new Uint8Array(tail.length + chunk.length);
      merged.set(tail, 0);
      merged.set(chunk, tail.length);
      tail =
        merged.length > STDERR_TAIL_BYTES
          ? merged.slice(merged.length - STDERR_TAIL_BYTES)
          : merged;
    }),
  ).pipe(Effect.ignore, Effect.ensuring(Deferred.succeed(finished, undefined)), Effect.forkScoped);
  const decode = Effect.sync(() => new TextDecoder().decode(tail));
  const read: Effect.Effect<string> = Deferred.await(finished).pipe(
    Effect.timeoutOption(STDERR_WAIT),
    Effect.andThen(decode),
  );
  return { read } satisfies StderrTail;
});

// eslint-disable-next-line no-control-regex
const ANSI_PATTERN =
  /\u001b(?:\[[0-?]*[ -/]*[@-~]|\][^\u0007\u001b]*(?:\u0007|\u001b\\)|[@-Z\\-_])/g;
const ERROR_LABEL_PATTERN =
  /^(?:[A-Za-z_$][\w$.]*)?(?:Error|Exception)(?: \[[\w.-]+\])?:\s|^(?:error|fatal)\b[^:]*:\s/i;

/** Picks the line of `stderr` that best explains why a process exited. */
export const summarizeStderr = (stderr: string): string | undefined => {
  const lines = stderr
    .replace(ANSI_PATTERN, "")
    .split(/\r\n|\r|\n/)
    .map((line) => line.replace(/\s+/g, " ").trim())
    .filter((line) => line.length > 0);
  const line = lines.findLast((candidate) => ERROR_LABEL_PATTERN.test(candidate)) ?? lines.at(-1);
  if (line === undefined) return undefined;
  return line.length > STDERR_EXCERPT_MAX_CHARS ? line.slice(0, STDERR_EXCERPT_MAX_CHARS) : line;
};

type ChildProcessTerminationHandle = Pick<
  ChildProcessSpawner.ChildProcessHandle,
  "exitCode" | "pid"
>;

export const makeTerminationError = (
  handle: ChildProcessTerminationHandle,
  stderrTail?: StderrTail,
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
      (stderrTail ? Effect.map(stderrTail.read, summarizeStderr) : Effect.void).pipe(
        Effect.map(
          (stderr) =>
            new CodexError.CodexAppServerProcessExitedError({
              code,
              pid: handle.pid,
              ...(stderr !== undefined ? { stderr } : {}),
            }),
        ),
      ),
  });
