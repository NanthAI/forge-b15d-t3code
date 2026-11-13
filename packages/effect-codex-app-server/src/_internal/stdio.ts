import { stripVTControlCharacters } from "node:util";

import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
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

export const makeStderrCapture = Effect.fn("makeStderrCapture")(function* <E>(
  stream: Stream.Stream<Uint8Array, E>,
) {
  const maxBytes = 8 * 1024;
  let tail = new Uint8Array(0);
  const fiber = yield* Stream.runForEach(stream, (chunk) =>
    Effect.sync(() => {
      const next = new Uint8Array(Math.min(maxBytes, tail.length + chunk.length));
      const retained = Math.max(0, next.length - chunk.length);
      next.set(tail.subarray(tail.length - retained));
      next.set(chunk.subarray(Math.max(0, chunk.length - maxBytes)), retained);
      tail = next;
    }),
  ).pipe(Effect.ignore, Effect.forkScoped);
  return {
    done: Fiber.await(fiber),
    text: Effect.sync(() => new TextDecoder().decode(tail)),
  };
});

const stderrExcerpt = (text: string) => {
  const lines = stripVTControlCharacters(text)
    .split(/[\r\n]+/)
    .map((line) => line.replace(/\s+/g, " ").trim())
    .filter((line) => line.length > 0);
  // Node launchers print source excerpts before the error and stack frames after it.
  const line =
    lines.findLast((line) => /^(?:[\w.]*Error(?: \[ERR_[^\]]+\])?|error|fatal):/.test(line)) ??
    lines.at(-1);
  return line?.slice(0, 512);
};

type ChildProcessTerminationHandle = Pick<
  ChildProcessSpawner.ChildProcessHandle,
  "exitCode" | "pid"
>;

export const makeTerminationError = Effect.fn("makeTerminationError")(function* (
  handle: ChildProcessTerminationHandle,
  stderr?: { readonly done: Effect.Effect<unknown>; readonly text: Effect.Effect<string> },
): Effect.fn.Return<CodexError.CodexAppServerError> {
  return yield* Effect.matchEffect(handle.exitCode, {
    onFailure: (cause) =>
      Effect.succeed(
        new CodexError.CodexAppServerTransportError({
          operation: "read-process-exit-status",
          pid: handle.pid,
          cause,
        }),
      ),
    onSuccess: Effect.fn("makeTerminationError.exited")(function* (code) {
      if (stderr) {
        yield* stderr.done.pipe(Effect.timeoutOption("250 millis"));
      }
      const excerpt = stderr ? stderrExcerpt(yield* stderr.text) : undefined;
      return new CodexError.CodexAppServerProcessExitedError({
        code,
        pid: handle.pid,
        ...(excerpt ? { stderr: excerpt } : {}),
      });
    }),
  });
});
