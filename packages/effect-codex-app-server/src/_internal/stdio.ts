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

type ChildProcessTerminationHandle = Pick<
  ChildProcessSpawner.ChildProcessHandle,
  "exitCode" | "pid"
>;

export const makeStderrCapture = Effect.fn("CodexAppServerClient.makeStderrCapture")(function* (
  stream: ChildProcessSpawner.ChildProcessHandle["stderr"],
) {
  let tail = new Uint8Array();
  const reader = yield* stream.pipe(
    Stream.runForEach((chunk) =>
      Effect.sync(() => {
        const incoming = chunk.subarray(-8192);
        const combined = new Uint8Array(tail.length + incoming.length);
        combined.set(tail);
        combined.set(incoming, tail.length);
        tail = combined.slice(-8192);
      }),
    ),
    Effect.ignore,
    Effect.forkScoped,
  );

  return Fiber.await(reader).pipe(
    Effect.timeout("250 millis"),
    Effect.ignore,
    Effect.andThen(() => {
      const lines = stripVTControlCharacters(new TextDecoder().decode(tail))
        .split(/[\r\n]+/)
        .map((line) => line.replace(/\s+/g, " ").trim())
        .filter(Boolean);
      const line =
        lines.findLast((line) =>
          /^(?:[A-Za-z]*Error(?: \[ERR_[^\]]+\])?|error|fatal):/.test(line),
        ) ?? lines.at(-1);
      return Effect.succeed(line?.slice(0, 512));
    }),
  );
});

export const makeTerminationError = Effect.fn("CodexAppServerClient.makeTerminationError")(
  (
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
              ...(stderr === undefined ? {} : { stderr }),
            }),
        ),
    }),
);
