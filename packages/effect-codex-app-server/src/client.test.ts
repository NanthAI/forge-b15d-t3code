import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Queue from "effect/Queue";
import * as Path from "effect/Path";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as References from "effect/References";
import * as Scope from "effect/Scope";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";

import * as CodexClient from "./client.ts";
import { makeInMemoryStdio } from "./_internal/stdio.ts";
import { CodexAppServerProcessExitedError } from "./errors.ts";

const mockPeerPath = Effect.map(Effect.service(Path.Path), (path) =>
  path.join(import.meta.dirname, "../test/fixtures/codex-app-server-mock-peer.ts"),
);
const mockPeerArgs = (path: string) => [path];

it.layer(NodeServices.layer)("effect-codex-app-server client", (it) => {
  const makeHandle = (env?: Record<string, string>) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const path = yield* Path.Path;
      const peerCwd = path.join(import.meta.dirname, "..");
      const command = ChildProcess.make(process.execPath, mockPeerArgs(yield* mockPeerPath), {
        cwd: peerCwd,
        ...(env ? { env: { ...process.env, ...env } } : {}),
      });
      return yield* spawner.spawn(command);
    });

  it.effect("initializes, handles typed server requests, and reads account and skills data", () =>
    Effect.gen(function* () {
      const userInputRequests = yield* Ref.make<Array<unknown>>([]);
      const messageDeltas = yield* Ref.make<Array<unknown>>([]);
      const handle = yield* makeHandle();
      const scope = yield* Scope.make();
      const layerClient = CodexClient.layerChildProcess(handle);
      const context = yield* Layer.buildWithScope(layerClient, scope);

      const result = yield* Effect.gen(function* () {
        const client = yield* CodexClient.CodexAppServerClient;

        yield* client.handleServerRequest("item/tool/requestUserInput", (payload) =>
          Ref.update(userInputRequests, (current) => [...current, payload]).pipe(
            Effect.as({
              answers: {
                approved: {
                  answers: ["yes"],
                },
              },
            }),
          ),
        );

        yield* client.handleServerNotification("item/agentMessage/delta", (payload) =>
          Ref.update(messageDeltas, (current) => [...current, payload]),
        );

        const initialized = yield* client.request("initialize", {
          clientInfo: {
            name: "effect-codex-app-server-test",
            title: "Effect Codex App Server Test",
            version: "0.0.0",
          },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: null,
          },
        });
        assert.equal(initialized.userAgent, "mock-codex-app-server");

        yield* client.notify("initialized", undefined);

        const account = yield* client.request("account/read", {});
        assert.equal(account.requiresOpenaiAuth, false);
        assert.deepEqual(account.account, {
          type: "chatgpt",
          email: "mock@example.com",
          planType: "plus",
        });

        const path = yield* Path.Path;
        const peerCwd = path.join(import.meta.dirname, "..");
        const skills = yield* client.request("skills/list", { cwds: [peerCwd] });
        assert.equal(skills.data.length, 1);
        assert.equal(skills.data[0]?.cwd, peerCwd);

        return {
          account,
          skills,
        };
      }).pipe(Effect.provide(context), Effect.ensuring(Scope.close(scope, Exit.void)));

      assert.equal(result.skills.data[0]?.skills.length, 0);
      assert.deepEqual(yield* Ref.get(userInputRequests), [
        {
          isBlocking: true,
          itemId: "item-approval-1",
          threadId: "thread-1",
          turnId: "turn-1",
          questions: [
            {
              id: "approved",
              header: "Approve",
              question: "Continue with the mock skills request?",
              options: [
                {
                  label: "yes",
                  description: "Approve the request",
                },
              ],
            },
          ],
        },
      ]);
      assert.deepEqual(yield* Ref.get(messageDeltas), [
        {
          delta: "Mock server is ready.",
          itemId: "item-1",
          threadId: "thread-1",
          turnId: "turn-1",
        },
      ]);
    }),
  );
  it.effect("drains child stderr so large diagnostics cannot block protocol responses", () =>
    Effect.gen(function* () {
      const handle = yield* makeHandle({
        CODEX_APP_SERVER_TEST_STDERR_BYTES: String(512 * 1024),
      });
      const scope = yield* Scope.make();
      const layerClient = CodexClient.layerChildProcess(handle);
      const context = yield* Layer.buildWithScope(layerClient, scope);

      const initialized = yield* Effect.gen(function* () {
        const client = yield* CodexClient.CodexAppServerClient;
        return yield* client.request("initialize", {
          clientInfo: {
            name: "effect-codex-app-server-test",
            title: "Effect Codex App Server Test",
            version: "0.0.0",
          },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: null,
          },
        });
      }).pipe(
        Effect.timeout("5 seconds"),
        Effect.provide(context),
        Effect.ensuring(Scope.close(scope, Exit.void)),
      );

      assert.equal(initialized.userAgent, "mock-codex-app-server");
    }),
  );
  it.effect("warns once about a notification that fails to decode and keeps dispatching", () =>
    Effect.gen(function* () {
      const warnings: Array<string> = [];
      const logger = Logger.make(({ logLevel, message, fiber }) => {
        if (logLevel !== "Warn") return;
        const annotations = fiber.getRef(References.CurrentLogAnnotations);
        warnings.push(JSON.stringify({ message: [message].flat(), annotations }));
      });
      const delivered = yield* Queue.unbounded<unknown>();
      const { stdio, input } = yield* makeInMemoryStdio();
      const scope = yield* Scope.make();
      const client = yield* CodexClient.make(stdio).pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.provide(Logger.layer([logger])),
      );
      yield* client.handleServerNotification("item/agentMessage/delta", (payload) =>
        Queue.offer(delivered, payload).pipe(Effect.asVoid),
      );
      const sentinel = "SECRET-SENTINEL";
      const send = (params: unknown) =>
        Queue.offer(
          input,
          new TextEncoder().encode(
            `${JSON.stringify({ method: "item/agentMessage/delta", params })}\n`,
          ),
        );
      const bad = { threadId: "t", turnId: sentinel, itemId: "i" };
      yield* send(bad);
      yield* send(bad);
      yield* send({ ...bad, turnId: "turn", delta: "ok" });
      const payload = yield* Queue.take(delivered).pipe(Effect.timeout("2 seconds"));
      assert.equal((payload as { delta: string }).delta, "ok");
      const logged = warnings;
      assert.equal(logged.length, 1);
      assert.include(logged[0]!, "item/agentMessage/delta");
      assert.include(logged[0]!, "MissingKey");
      assert.notInclude(logged[0]!, sentinel);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("reports the launcher's stderr line when the process exits before initialize", () =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const script = [
        "process.stderr.write('file:///x/codex.js:107\\n  throw new Error(\"boom\");\\n  ^\\n\\n');",
        "process.stderr.write('Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest\\n    at file:///x/codex.js:107:9\\n');",
        "process.exit(1);",
      ].join("");
      const handle = yield* spawner.spawn(ChildProcess.make(process.execPath, ["-e", script]));
      const scope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(CodexClient.layerChildProcess(handle), scope);
      const exit = yield* Effect.gen(function* () {
        const client = yield* CodexClient.CodexAppServerClient;
        return yield* client.request("initialize", {
          clientInfo: { name: "t", title: "t", version: "0.0.0" },
          capabilities: { experimentalApi: true, optOutNotificationMethods: null },
        });
      }).pipe(
        Effect.timeout("5 seconds"),
        Effect.provide(context),
        Effect.exit,
        Effect.ensuring(Scope.close(scope, Exit.void)),
      );
      assert.isTrue(Exit.isFailure(exit));
      const error = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;
      assert.instanceOf(error, CodexAppServerProcessExitedError);
      assert.equal(
        (error as Error).message,
        "Codex App Server process exited with code 1: Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
      );
    }),
  );
});
