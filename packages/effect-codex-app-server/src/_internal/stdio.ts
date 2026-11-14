import * as NodeUtil from "node:util";

import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
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
  handle: Pick<ChildProcessSpawner.ChildProcessHandle, "stderr">,
) {
  const limit = 8 * 1024;
  let tail = new Uint8Array(0);
  const fiber = yield* Stream.runForEach(handle.stderr, (chunk) =>
    Effect.sync(() => {
      if (chunk.length >= limit) {
        tail = new Uint8Array(chunk.subarray(-limit));
      } else {
        const retained = tail.subarray(Math.max(0, tail.length + chunk.length - limit));
        tail = new Uint8Array(retained.length + chunk.length);
        tail.set(retained);
        tail.set(chunk, retained.length);
      }
    }),
  ).pipe(Effect.ignore, Effect.forkScoped);

  return {
    done: Fiber.await(fiber),
    getExcerpt: () => {
      const lines = NodeUtil.stripVTControlCharacters(new TextDecoder().decode(tail))
        .split(/[\r\n]+/)
        .map((line) => line.replace(/\s+/g, " ").trim())
        .filter((line) => line.length > 0);
      const line =
        lines.findLast((line) => /^(?:\w*Error(?: \[ERR_[^\]]+\])?|error|fatal):/.test(line)) ??
        lines.at(-1);
      return line?.slice(0, 512);
    },
  };
});

export const makeTerminationError = Effect.fn("CodexAppServerClient.makeTerminationError")(
  function* (
    handle: ChildProcessTerminationHandle,
    stderr?: Effect.Success<ReturnType<typeof captureStderr>>,
  ): Effect.fn.Return<CodexError.CodexAppServerError> {
    const status = yield* Effect.result(handle.exitCode);
    if (Result.isFailure(status)) {
      return new CodexError.CodexAppServerTransportError({
        operation: "read-process-exit-status",
        pid: handle.pid,
        cause: status.failure,
      });
    }
    if (stderr) {
      yield* stderr.done.pipe(Effect.timeoutOption("250 millis"));
    }
    const excerpt = stderr?.getExcerpt();
    return new CodexError.CodexAppServerProcessExitedError({
      code: status.success,
      pid: handle.pid,
      ...(excerpt ? { stderr: excerpt } : {}),
    });
  },
);
