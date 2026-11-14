import { stripVTControlCharacters } from "node:util";

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
  stream: ChildProcessSpawner.ChildProcessHandle["stderr"],
) {
  const tail = new Uint8Array(8 * 1024);
  let length = 0;
  const fiber = yield* Stream.runForEach(stream, (chunk) =>
    Effect.sync(() => {
      const bytes = chunk.subarray(-tail.length);
      const retained = Math.min(length, tail.length - bytes.length);
      tail.copyWithin(0, length - retained, length);
      tail.set(bytes, retained);
      length = retained + bytes.length;
    }),
  ).pipe(Effect.ignore, Effect.forkScoped);

  return {
    done: Fiber.await(fiber),
    excerpt: () => {
      const lines = stripVTControlCharacters(new TextDecoder().decode(tail.subarray(0, length)))
        .split(/[\r\n]+/)
        .map((line) => line.replace(/\s+/g, " ").trim())
        .filter((line) => line.length > 0);
      const errorLine = lines.findLast((line) =>
        /^(?:\w*Error(?: \[ERR_[^\]]+\])?|error|fatal):/.test(line),
      );
      return (errorLine ?? lines.at(-1))?.slice(0, 512);
    },
  };
});

export const makeTerminationError = Effect.fn("CodexAppServerClient.makeTerminationError")(
  function* (
    handle: ChildProcessTerminationHandle,
    stderr?: Effect.Success<ReturnType<typeof captureStderr>>,
  ): Effect.fn.Return<CodexError.CodexAppServerError> {
    const result = yield* Effect.result(handle.exitCode);
    if (Result.isFailure(result)) {
      return new CodexError.CodexAppServerTransportError({
        operation: "read-process-exit-status",
        pid: handle.pid,
        cause: result.failure,
      });
    }
    if (stderr) {
      yield* stderr.done.pipe(Effect.timeoutOption("250 millis"));
    }
    const excerpt = stderr?.excerpt();
    return new CodexError.CodexAppServerProcessExitedError({
      code: result.success,
      pid: handle.pid,
      ...(excerpt ? { stderr: excerpt } : {}),
    });
  },
);
