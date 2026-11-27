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
const stderrTailBytes = 8 * 1024;

export const captureStderr = Effect.fn("CodexAppServerClient.captureStderr")(function* (
  stream: ChildProcessSpawner.ChildProcessHandle["stderr"],
) {
  let tail = new Uint8Array();
  const drain = yield* Stream.runForEach(stream, (chunk) =>
    Effect.sync(() => {
      const kept = tail.subarray(Math.max(0, tail.length + chunk.length - stderrTailBytes));
      const incoming = chunk.subarray(Math.max(0, chunk.length - stderrTailBytes));
      tail = new Uint8Array(kept.length + incoming.length);
      tail.set(kept);
      tail.set(incoming, kept.length);
    }),
  ).pipe(Effect.ignore, Effect.forkScoped);

  return Fiber.join(drain).pipe(
    Effect.timeoutOption("250 millis"),
    Effect.andThen(() =>
      Effect.sync(() => {
        const lines = stripVTControlCharacters(new TextDecoder().decode(tail))
          .split(/[\r\n]/)
          .map((line) => line.replace(/\s+/g, " ").trim())
          .filter((line) => line.length > 0);
        // Node launchers print source excerpts before the error and stack frames after it.
        const line =
          lines.findLast((line) => /^(?:[a-z]*Error(?: \[ERR_[^\]]+\])?|fatal):/i.test(line)) ??
          lines.at(-1);
        return line?.slice(0, 512);
      }),
    ),
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

export const makeTerminationError = (
  handle: ChildProcessTerminationHandle,
  stderr: Effect.Effect<string | undefined> = Effect.undefined,
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
            ...(excerpt === undefined ? {} : { stderr: excerpt }),
          }),
      ),
  });
