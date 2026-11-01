import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Path from "effect/Path";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as References from "effect/References";
import * as Scope from "effect/Scope";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";

import * as CodexClient from "./client.ts";
import * as CodexError from "./errors.ts";
import { makeInMemoryStdio } from "./_internal/stdio.ts";

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

  it.effect("warns once about a notification that fails to decode and keeps dispatching", () =>
    Effect.gen(function* () {
      const secret = "codex-notification-secret-sentinel";
      const { stdio, input } = yield* makeInMemoryStdio();
      const warnings: Array<unknown> = [];
      const client = yield* CodexClient.make(stdio).pipe(
        Effect.provide(
          Logger.layer([
            Logger.make(({ logLevel, message, fiber }) => {
              if (logLevel === "Warn") {
                warnings.push({
                  message,
                  annotations: fiber.getRef(References.CurrentLogAnnotations),
                });
              }
            }),
          ]),
        ),
      );
      const received = yield* Deferred.make<unknown>();
      const handled: Array<unknown> = [];
      yield* client.handleServerNotification("item/agentMessage/delta", (payload) =>
        Effect.sync(() => handled.push(payload)).pipe(
          Effect.andThen(Deferred.succeed(received, payload)),
          Effect.asVoid,
        ),
      );

      const send = (params: unknown) =>
        Queue.offer(
          input,
          new TextEncoder().encode(
            `${JSON.stringify({ method: "item/agentMessage/delta", params })}\n`,
          ),
        );
      const malformed = { itemId: "item-1", threadId: "thread-1", turnId: secret };
      const valid = { ...malformed, turnId: "turn-1", delta: "Hello" };
      yield* send(malformed);
      yield* send(malformed);
      yield* send(valid);

      assert.deepEqual(yield* Deferred.await(received), valid);
      assert.deepEqual(handled, [valid]);
      assert.lengthOf(warnings, 1);
      assert.deepEqual(warnings[0], {
        message: ["Dropped a Codex App Server notification that failed to decode"],
        annotations: {
          operation: "decode-notification-payload",
          method: "item/agentMessage/delta",
          issueCount: 3,
          issueKinds: ["Composite", "Pointer", "MissingKey"],
          maximumPathDepth: 1,
        },
      });
      assert.notInclude(JSON.stringify(warnings), secret);
    }),
  );

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
  it.effect("reports the launcher's error when the child exits before initialize", () =>
    Effect.gen(function* () {
      const diagnostic =
        "Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest";
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const handle = yield* spawner.spawn(
        ChildProcess.make(process.execPath, [
          "--input-type=module",
          "-e",
          `throw new Error(${JSON.stringify(diagnostic)})`,
        ]),
      );
      const error = yield* Effect.gen(function* () {
        const client = yield* CodexClient.CodexAppServerClient;
        return yield* Effect.flip(
          client.request("initialize", {
            clientInfo: { name: "test", title: "Test", version: "0.0.0" },
            capabilities: { experimentalApi: true, optOutNotificationMethods: null },
          }),
        );
      }).pipe(Effect.provide(CodexClient.layerChildProcess(handle)));

      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.stderr, `Error: ${diagnostic}`);
      assert.equal(
        error.message,
        `Codex App Server process exited with code 1: Error: ${diagnostic}`,
      );
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
});
