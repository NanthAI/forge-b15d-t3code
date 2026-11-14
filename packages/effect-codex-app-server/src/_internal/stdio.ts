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
const MAX_STDERR_BYTES = 8 * 1024;
const MAX_STDERR_EXCERPT_LENGTH = 512;

const stderrExcerpt = (bytes: Uint8Array) => {
  const lines = NodeUtil.stripVTControlCharacters(new TextDecoder().decode(bytes))
    .split(/[\r\n]+/)
    .map((line) => line.replace(/\s+/g, " ").trim())
    .filter(Boolean);
  // Node launchers print source excerpts before the error and stack frames after it.
  const line =
    lines.findLast((line) => /^(?:\w*Error(?: \[ERR_[^\]]+\])?|error|fatal):/.test(line)) ??
    lines.at(-1);
  return line?.slice(0, MAX_STDERR_EXCERPT_LENGTH);
};

export const captureStderr = Effect.fn("CodexAppServerClient.captureStderr")(function* (
  stream: ChildProcessSpawner.ChildProcessHandle["stderr"],
) {
  let tail: Uint8Array = new Uint8Array();
  const drained = yield* Stream.runForEach(stream, (chunk) =>
    Effect.sync(() => {
      const retained = tail.subarray(Math.max(0, tail.length + chunk.length - MAX_STDERR_BYTES));
      const incoming = chunk.subarray(Math.max(0, chunk.length - MAX_STDERR_BYTES));
      const next = new Uint8Array(retained.length + incoming.length);
      next.set(retained);
      next.set(incoming, retained.length);
      tail = next;
    }),
  ).pipe(Effect.ignore, Effect.forkScoped);

  return Fiber.await(drained).pipe(
    Effect.timeoutOption("250 millis"),
    Effect.map(() => stderrExcerpt(tail)),
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

export const makeTerminationError = Effect.fn("CodexAppServerClient.makeTerminationError")(
  (
    handle: ChildProcessTerminationHandle,
    stderr: Effect.Effect<string | undefined> = Effect.succeed(""),
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
          (excerpt) =>
            new CodexError.CodexAppServerProcessExitedError({
              code,
              pid: handle.pid,
              ...(excerpt ? { stderr: excerpt } : {}),
            }),
        ),
    }),
);
