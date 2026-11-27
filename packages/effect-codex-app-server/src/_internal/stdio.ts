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

export const captureStderr = Effect.fn("CodexAppServerClient.captureStderr")(function* (
  stderr: ChildProcessSpawner.ChildProcessHandle["stderr"],
) {
  const maxBytes = 8 * 1024;
  let tail = new Uint8Array(0);
  const drain = yield* stderr.pipe(
    Stream.runForEach((chunk) =>
      Effect.sync(() => {
        const suffix = chunk.subarray(-maxBytes);
        const retainedBytes = Math.min(tail.length, maxBytes - suffix.length);
        const next = new Uint8Array(retainedBytes + suffix.length);
        next.set(tail.subarray(tail.length - retainedBytes));
        next.set(suffix, retainedBytes);
        tail = next;
      }),
    ),
    Effect.ignore,
    Effect.forkScoped,
  );

  return Fiber.await(drain).pipe(
    Effect.timeoutOption("250 millis"),
    Effect.map(() => {
      const lines = stripVTControlCharacters(new TextDecoder().decode(tail))
        .split(/[\r\n]+/)
        .map((line) => line.replace(/\s+/g, " ").trim())
        .filter((line) => line.length > 0);
      const excerpt =
        lines.findLast((line) => /^(?:[A-Za-z]*Error(?: \[[^\]]+\])?|error|fatal):/.test(line)) ??
        lines.at(-1);
      return excerpt?.slice(0, 512) ?? "";
    }),
  );
});

export const makeTerminationError = (
  handle: ChildProcessTerminationHandle,
  stderr: Effect.Effect<string> = Effect.succeed(""),
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
