import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { checkPiProviderStatus, MINIMUM_PI_VERSION } from "./status.ts";

const encoder = new TextEncoder();
const decodeRpcRequest = Schema.decodeSync(
  Schema.fromJsonString(Schema.Struct({ id: Schema.String, type: Schema.String })),
);
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function processHandle(input: {
  readonly stdout?: string;
  readonly stderr?: string;
  readonly exitCode?: number;
}) {
  const bytes = (value: string | undefined) =>
    value === undefined || value.length === 0
      ? Stream.empty
      : Stream.succeed(encoder.encode(value));
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(900_000_001),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(input.exitCode ?? 0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: bytes(input.stdout),
    stderr: bytes(input.stderr),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

function piProbeSpawner(version: string) {
  return ChildProcessSpawner.make((command) => {
    const args = ChildProcess.isStandardCommand(command) ? command.args : [];
    return Effect.succeed(
      args.includes("--version")
        ? processHandle({ stdout: `pi ${version}\n` })
        : processHandle({ stderr: "RPC startup failed", exitCode: 1 }),
    );
  });
}

const piDiscoverySpawner = (
  inventory: unknown,
  options: {
    readonly state?: unknown;
    readonly version?: string;
    readonly authCheck?: { readonly stdout: string; readonly exitCode?: number };
    readonly authCalls?: Array<ReadonlyArray<string>>;
  } = {},
) =>
  Effect.gen(function* () {
    const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
    let stdinBuffer = "";
    const stdin = Sink.forEach((chunk: Uint8Array) =>
      Effect.gen(function* () {
        stdinBuffer += new TextDecoder().decode(chunk);
        while (true) {
          const newline = stdinBuffer.indexOf("\n");
          if (newline === -1) return;
          const line = stdinBuffer.slice(0, newline);
          stdinBuffer = stdinBuffer.slice(newline + 1);
          if (line.length === 0) continue;
          const request = decodeRpcRequest(line);
          const data =
            request.type === "get_available_models"
              ? inventory
              : request.type === "get_commands"
                ? { commands: [] }
                : request.type === "get_state"
                  ? (options.state ?? {})
                  : {};
          yield* Queue.offer(
            stdout,
            encoder.encode(
              `${encodeJsonLine({ type: "response", id: request.id, command: request.type, success: true, data })}\n`,
            ),
          );
        }
      }),
    );
    return ChildProcessSpawner.make((command) => {
      const args = ChildProcess.isStandardCommand(command) ? command.args : [];
      if (args[0] === "auth") {
        options.authCalls?.push(args);
        return Effect.succeed(processHandle(options.authCheck ?? { exitCode: 1 }));
      }
      return Effect.succeed(
        args.includes("--version")
          ? processHandle({ stdout: `pi ${options.version ?? "0.84.3"}\n` })
          : ChildProcessSpawner.makeHandle({
              ...processHandle({}),
              exitCode: Effect.never,
              isRunning: Effect.succeed(true),
              stdin,
              stdout: Stream.fromQueue(stdout),
            }),
      );
    });
  });

const settings = {
  enabled: true,
  binaryPath: "pi",
  launchArgs: "",
  customModels: [],
} as const;

describe("PiProvider", () => {
  it.effect("requires the first published Pi version with entries and settlement hooks", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(settings).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, piProbeSpawner("0.80.3")),
      );
      assert.equal(snapshot.status, "error");
      assert.equal(snapshot.version, "0.80.3");
      assert.include(snapshot.message ?? "", `Pi ${MINIMUM_PI_VERSION} or newer`);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps compatible Pi selectable when optional discovery fails", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(settings).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, piProbeSpawner("0.84.3")),
      );
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.auth.status, "unknown");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["default"],
      );
      assert.include(snapshot.message ?? "", "could not refresh its models and commands");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reports a known unauthenticated inventory when Pi returns no usable models", () =>
    Effect.gen(function* () {
      const spawner = yield* piDiscoverySpawner({ models: [] });
      const snapshot = yield* checkPiProviderStatus(settings).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      assert.equal(snapshot.status, "warning");
      assert.equal(snapshot.auth.status, "unauthenticated");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["default"],
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  // regression: #18498 a malformed model inventory must not restore stale Pi models
  it.effect.each([
    ["missing models", {}],
    ["missing model id", { models: [{ provider: "anthropic" }] }],
    [
      "mixed valid and invalid models",
      { models: [{ provider: "anthropic", id: "claude-sonnet-4" }, { provider: "openrouter" }] },
    ],
  ] as const)("reports unknown authentication when Pi discovery returns %s", (_label, inventory) =>
    Effect.gen(function* () {
      const spawner = yield* piDiscoverySpawner(inventory);
      const snapshot = yield* checkPiProviderStatus(settings).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.auth.status, "unknown");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  describe("default model credentials", () => {
    const state = { model: { provider: "openai", id: "gpt-5" } };
    const inventory = { models: [{ provider: "anthropic", id: "claude-sonnet-4" }] };
    const run = (
      options: Parameters<typeof piDiscoverySpawner>[1],
      launchArgs = "--model openai/gpt-5",
      inv: unknown = inventory,
    ) =>
      Effect.gen(function* () {
        const spawner = yield* piDiscoverySpawner(inv, options);
        return yield* checkPiProviderStatus({ ...settings, launchArgs }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        );
      }).pipe(Effect.provide(NodeServices.layer));

    it.effect("names the model when auth check reports missing credentials", () =>
      Effect.gen(function* () {
        const authCalls: Array<ReadonlyArray<string>> = [];
        const snapshot = yield* run({
          state,
          authCalls,
          authCheck: { stdout: JSON.stringify({ provider: "openai", status: "missing" }) },
        });
        assert.equal(snapshot.status, "ready");
        assert.equal(snapshot.auth.status, "authenticated");
        assert.include(snapshot.message ?? "", "openai/gpt-5");
        assert.include(snapshot.message ?? "", "has no credentials configured for openai");
        assert.deepEqual(authCalls, [
          ["auth", "check", "--provider", "openai", "--json", "--no-refresh"],
        ]);
      }),
    );

    it.effect("skips auth check with a runtime --api-key", () =>
      Effect.gen(function* () {
        const authCalls: Array<ReadonlyArray<string>> = [];
        const snapshot = yield* run({ state, authCalls }, "--model openai/gpt-5 --api-key x");
        assert.equal(snapshot.status, "ready");
        assert.include(snapshot.message ?? "", "openai/gpt-5");
        assert.deepEqual(authCalls, []);
      }),
    );

    it.effect("reports provider_not_found distinctly", () =>
      Effect.gen(function* () {
        const snapshot = yield* run({
          state,
          authCheck: { stdout: JSON.stringify({ error: "provider_not_found" }), exitCode: 1 },
        });
        assert.include(snapshot.message ?? "", "does not recognize");
      }),
    );

    it.effect("falls back to a generic message on malformed auth check output", () =>
      Effect.gen(function* () {
        const snapshot = yield* run({ state, authCheck: { stdout: "not json" } });
        assert.equal(snapshot.status, "ready");
        assert.include(snapshot.message ?? "", "launch arguments");
      }),
    );

    it.effect("gives a generic message without running auth check on Pi older than 0.84.1", () =>
      Effect.gen(function* () {
        const authCalls: Array<ReadonlyArray<string>> = [];
        const snapshot = yield* run({ state, authCalls, version: "0.84.0" });
        assert.include(snapshot.message ?? "", "launch arguments");
        assert.deepEqual(authCalls, []);
      }),
    );

    it.effect("stays silent when the active provider has a listed model", () =>
      Effect.gen(function* () {
        const authCalls: Array<ReadonlyArray<string>> = [];
        const snapshot = yield* run({ state, authCalls }, "--model openai/gpt-99", {
          models: [{ provider: "openai", id: "gpt-5" }],
        });
        assert.equal(snapshot.message, undefined);
        assert.deepEqual(authCalls, []);
      }),
    );
  });
});
