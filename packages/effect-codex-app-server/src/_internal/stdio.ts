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

export const makeStderrCapture = Effect.fn("makeStderrCapture")(function* (
  stderr: ChildProcessSpawner.ChildProcessHandle["stderr"],
) {
  const maxBytes = 8 * 1024;
  let tail: Uint8Array = new Uint8Array();
  const fiber = yield* Stream.runForEach(stderr, (chunk) =>
    Effect.sync(() => {
      const suffix = chunk.subarray(Math.max(0, chunk.length - maxBytes));
      const prefix = tail.subarray(Math.max(0, tail.length + suffix.length - maxBytes));
      const next = new Uint8Array(prefix.length + suffix.length);
      next.set(prefix);
      next.set(suffix, prefix.length);
      tail = next;
    }),
  ).pipe(Effect.ignore, Effect.forkScoped);

  return {
    done: Fiber.await(fiber),
    excerpt: () => {
      const lines = new TextDecoder()
        .decode(tail)
        .replace(/\u001b(?:\[[0-?]*[ -/]*[@-~]|\][^\u0007\u001b]*(?:\u0007|\u001b\\)|[@-_])/g, "")
        .split(/[\r\n]+/)
        .map((line) => line.replace(/\s+/g, " ").trim())
        .filter((line) => line.length > 0);
      const line =
        lines.findLast((line) => /^(?:[a-z]*Error(?: \[ERR_[^\]]+\])?|error|fatal):/i.test(line)) ??
        lines.at(-1);
      return line?.slice(0, 512);
    },
  };
});

export const makeTerminationError = (
  handle: ChildProcessTerminationHandle,
  stderr?: Effect.Success<ReturnType<typeof makeStderrCapture>>,
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
    onSuccess: Effect.fn("makeTerminationError")(function* (code) {
      if (stderr) {
        yield* stderr.done.pipe(Effect.timeoutOption("250 millis"));
      }
      const excerpt = stderr?.excerpt();
      return new CodexError.CodexAppServerProcessExitedError({
        code,
        pid: handle.pid,
        ...(excerpt ? { stderr: excerpt } : {}),
      });
    }),
  });
