import * as NodeUtil from "node:util";

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

type ChildProcessTerminationHandle = Pick<
  ChildProcessSpawner.ChildProcessHandle,
  "exitCode" | "pid"
>;

export const captureStderr = Effect.fn("CodexAppServerClient.captureStderr")(function* (
  stderr: ChildProcessSpawner.ChildProcessHandle["stderr"],
) {
  const limit = 8 * 1024;
  let tail = new Uint8Array(0);
  const reader = yield* Stream.runForEach(stderr, (chunk) =>
    Effect.sync(() => {
      if (chunk.length >= limit) {
        tail = new Uint8Array(chunk.subarray(-limit));
      } else {
        const retained = tail.subarray(Math.max(0, tail.length + chunk.length - limit));
        const next = new Uint8Array(retained.length + chunk.length);
        next.set(retained);
        next.set(chunk, retained.length);
        tail = next;
      }
    }),
  ).pipe(Effect.ignore, Effect.forkScoped);

  return Fiber.await(reader).pipe(
    Effect.timeoutOption("250 millis"),
    Effect.map(() => {
      const lines = NodeUtil.stripVTControlCharacters(new TextDecoder().decode(tail))
        .split(/\r\n|\r|\n/)
        .map((line) => line.replace(/\s+/g, " ").trim())
        .filter(Boolean);
      const line =
        lines.findLast((line) => /^(?:[A-Za-z]*Error(?: \[[^\]]+\])?|error|fatal):/i.test(line)) ??
        lines.at(-1);
      return line?.slice(0, 512);
    }),
  );
});

export const makeTerminationError = (
  handle: ChildProcessTerminationHandle,
  stderr: Effect.Effect<string | void> = Effect.void,
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
      Effect.map(
        stderr,
        (stderr) =>
          new CodexError.CodexAppServerProcessExitedError({
            code,
            pid: handle.pid,
            ...(stderr ? { stderr } : {}),
          }),
      ),
  });
