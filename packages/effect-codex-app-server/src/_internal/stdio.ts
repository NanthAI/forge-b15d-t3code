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
const stderrTailBytes = 8 * 1024;

export const makeStderrCapture = Effect.fn("makeStderrCapture")(function* (
  stderr: ChildProcessSpawner.ChildProcessHandle["stderr"],
) {
  let tail = new Uint8Array();
  const drain = yield* Stream.runForEach(stderr, (chunk) =>
    Effect.sync(() => {
      const next = new Uint8Array(Math.min(stderrTailBytes, tail.length + chunk.length));
      const retained = Math.min(tail.length, Math.max(0, stderrTailBytes - chunk.length));
      next.set(tail.subarray(tail.length - retained));
      next.set(chunk.subarray(Math.max(0, chunk.length - stderrTailBytes)), retained);
      tail = next;
    }),
  ).pipe(Effect.ignore, Effect.forkScoped);

  return {
    done: Fiber.await(drain),
    excerpt: Effect.sync(() => {
      const lines = NodeUtil.stripVTControlCharacters(new TextDecoder().decode(tail))
        .split(/[\r\n]+/)
        .map((line) => line.replace(/\s+/g, " ").trim())
        .filter(Boolean);
      const line =
        lines.findLast((line) => /^(?:\w*Error(?: \[ERR_[^\]]+\])?|error|fatal):/.test(line)) ??
        lines.at(-1);
      return line?.slice(0, 512);
    }),
  };
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

export const makeTerminationError = Effect.fn("makeTerminationError")(function* (
  handle: ChildProcessTerminationHandle,
  stderr?: Effect.Success<ReturnType<typeof makeStderrCapture>>,
): Effect.fn.Return<CodexError.CodexAppServerError> {
  const status = yield* Effect.result(handle.exitCode);
  if (status._tag === "Failure") {
    return new CodexError.CodexAppServerTransportError({
      operation: "read-process-exit-status",
      pid: handle.pid,
      cause: status.failure,
    });
  }
  if (stderr) yield* stderr.done.pipe(Effect.timeoutOption("250 millis"));
  const excerpt = stderr ? yield* stderr.excerpt : undefined;
  return new CodexError.CodexAppServerProcessExitedError({
    code: status.success,
    pid: handle.pid,
    ...(excerpt === undefined ? {} : { stderr: excerpt }),
  });
});
