import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  type ModelSelection,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  type RunId,
  type ServerProvider,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { McpSchema, McpServer } from "effect/unstable/ai";

import { ClaudeProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { OrchestratorV2, type OrchestratorV2Shape } from "../orchestration-v2/Orchestrator.ts";
import {
  type ProviderAdapterV2Event,
  ProviderAdapterProtocolError,
  type ProviderAdapterV2Shape,
  type ProviderAdapterV2TurnInput,
} from "../orchestration-v2/ProviderAdapter.ts";
import { makeLayer as makeProviderAdapterRegistryLayer } from "../orchestration-v2/ProviderAdapterRegistry.ts";
import { ProviderContinuationRequests } from "../orchestration-v2/ProviderContinuationRequests.ts";
import { layer as threadManagementServiceLayer } from "../orchestration-v2/ThreadManagementService.ts";
import { checkpointWorkspace } from "../orchestration-v2/testkit/ReplayFixtureWorkspace.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { makeProviderRegistryLayer } from "../provider/testUtils/providerRegistryMock.ts";
import { ScheduledTaskService } from "../scheduledTasks/ScheduledTaskService.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";

const parentThreadId = ThreadId.make("thread:mcp-inbox-parent");
const projectId = ProjectId.make("project:mcp-inbox");
const parentInstanceId = ProviderInstanceId.make("codex");
const childInstanceId = ProviderInstanceId.make("claudeAgent");
const parentSelection = { instanceId: parentInstanceId, model: "gpt-5.4" } satisfies ModelSelection;
const childSelection = {
  instanceId: childInstanceId,
  model: "claude-sonnet-4-6",
} satisfies ModelSelection;
const parentPrompt = "Hold this turn open while the inbox is exercised.";

/** Codex with native steering switched off, the capability shape of ACP and Cursor. */
const nonSteeringCapabilities: OrchestrationV2ProviderCapabilities = {
  ...CodexProviderCapabilitiesV2,
  turns: {
    ...CodexProviderCapabilitiesV2.turns,
    supportsActiveSteering: false,
    supportsSteeringByInterruptRestart: true,
  },
};

interface CapturedTurn {
  readonly threadId: ThreadId;
  readonly text: string;
}

function makeAdapter(input: {
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly capabilities: OrchestrationV2ProviderCapabilities;
  readonly capturedTurns: Ref.Ref<ReadonlyArray<CapturedTurn>>;
  readonly steers: Ref.Ref<ReadonlyArray<string>>;
  readonly gate: (turn: ProviderAdapterV2TurnInput) => Deferred.Deferred<void> | undefined;
}): ProviderAdapterV2Shape {
  const unsupported = (detail: string) =>
    Effect.fail(new ProviderAdapterProtocolError({ driver: input.driver, detail }));
  return {
    instanceId: input.instanceId,
    driver: input.driver,
    getCapabilities: () => Effect.succeed(input.capabilities),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (sessionInput) =>
      Effect.gen(function* () {
        const events = yield* PubSub.unbounded<ProviderAdapterV2Event>();
        const now = yield* DateTime.now;
        const providerSession: OrchestrationV2ProviderSession = {
          id: sessionInput.providerSessionId,
          driver: input.driver,
          providerInstanceId: input.instanceId,
          status: "ready",
          cwd: sessionInput.runtimePolicy.cwd ?? process.cwd(),
          model: sessionInput.modelSelection.model,
          capabilities: input.capabilities,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        };
        const publish = (providerEvents: ReadonlyArray<ProviderAdapterV2Event>) =>
          Effect.forEach(providerEvents, (event) => PubSub.publish(events, event), {
            discard: true,
          });
        const providerTurn = (
          turnInput: ProviderAdapterV2TurnInput,
          status: "running" | "completed",
          at: DateTime.Utc,
        ) => ({
          id: ProviderTurnId.make(
            `provider-turn:${input.instanceId}:${turnInput.threadId}:${turnInput.runOrdinal}`,
          ),
          providerThreadId: turnInput.providerThread.id,
          nodeId: turnInput.rootNodeId,
          runAttemptId: turnInput.attemptId,
          nativeTurnRef: {
            driver: input.driver,
            nativeId: `native-turn:${turnInput.threadId}:${turnInput.runOrdinal}`,
            strength: "strong" as const,
          },
          ordinal: turnInput.providerTurnOrdinal,
          status,
          startedAt: at,
          completedAt: status === "completed" ? at : null,
        });
        return {
          instanceId: input.instanceId,
          driver: input.driver,
          providerSessionId: sessionInput.providerSessionId,
          providerSession,
          events: Stream.fromPubSub(events),
          ensureThread: (threadInput) =>
            Effect.gen(function* () {
              const createdAt = yield* DateTime.now;
              const nativeThreadId = `${input.driver}:${threadInput.threadId}`;
              return {
                id: ProviderThreadId.make(`provider-thread:${nativeThreadId}`),
                driver: input.driver,
                providerInstanceId: input.instanceId,
                providerSessionId: sessionInput.providerSessionId,
                appThreadId: threadInput.threadId,
                ownerNodeId: null,
                nativeThreadRef: {
                  driver: input.driver,
                  nativeId: nativeThreadId,
                  strength: "strong",
                },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt,
                updatedAt: createdAt,
              } satisfies OrchestrationV2ProviderThread;
            }),
          resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
          startTurn: (turnInput) =>
            Effect.gen(function* () {
              yield* Ref.update(input.capturedTurns, (turns) => [
                ...turns,
                { threadId: turnInput.threadId, text: turnInput.message.text },
              ]);
              const startedAt = yield* DateTime.now;
              yield* publish([
                {
                  type: "provider_turn.updated",
                  driver: input.driver,
                  providerTurn: providerTurn(turnInput, "running", startedAt),
                },
              ]);
              const gate = input.gate(turnInput);
              if (gate !== undefined) yield* Deferred.await(gate);
              const completedAt = yield* DateTime.now;
              const completed = providerTurn(turnInput, "completed", completedAt);
              yield* publish([
                { type: "provider_turn.updated", driver: input.driver, providerTurn: completed },
                {
                  type: "turn_item.updated",
                  driver: input.driver,
                  turnItem: {
                    id: TurnItemId.make(
                      `turn-item:${input.instanceId}:${turnInput.threadId}:${turnInput.runOrdinal}:assistant`,
                    ),
                    threadId: turnInput.threadId,
                    runId: turnInput.runId,
                    nodeId: turnInput.rootNodeId,
                    providerThreadId: turnInput.providerThread.id,
                    providerTurnId: completed.id,
                    nativeItemRef: null,
                    parentItemId: null,
                    ordinal: turnInput.runOrdinal * 100 + 1,
                    status: "completed",
                    title: null,
                    startedAt: completedAt,
                    completedAt,
                    updatedAt: completedAt,
                    type: "assistant_message",
                    messageId: MessageId.make(
                      `message:${input.instanceId}:${turnInput.threadId}:${turnInput.runOrdinal}:assistant`,
                    ),
                    text: `Completed: ${turnInput.message.text}`,
                    streaming: false,
                  },
                },
                {
                  type: "turn.terminal",
                  driver: input.driver,
                  providerThreadId: turnInput.providerThread.id,
                  providerTurnId: completed.id,
                  runOrdinal: turnInput.runOrdinal,
                  status: "completed",
                  failure: null,
                  threadDisposition: "reusable",
                },
              ]);
            }),
          steerTurn: (steerInput) =>
            Ref.update(input.steers, (steers) => [...steers, steerInput.message.text]),
          interruptTurn: () => Effect.void,
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => unsupported("readThreadSnapshot is unused in this test"),
          rollbackThread: () => unsupported("rollbackThread is unused in this test"),
          forkThread: () => unsupported("forkThread is unused in this test"),
        };
      }),
  };
}

function providerSnapshot(instanceId: ProviderInstanceId, driver: string, model: string) {
  return {
    instanceId,
    driver: ProviderDriverKind.make(driver),
    enabled: true,
    installed: true,
    version: "test",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-06-17T00:00:00.000Z",
    models: [{ slug: model, name: model, isCustom: false, capabilities: null }],
    slashCommands: [],
    skills: [],
  } satisfies ServerProvider;
}

function waitForProjection(
  orchestrator: OrchestratorV2Shape,
  predicate: (projection: OrchestrationV2ThreadProjection) => boolean,
) {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 1_000; attempt += 1) {
      const projection = yield* orchestrator.getThreadProjection(parentThreadId);
      if (predicate(projection)) return projection;
      yield* Effect.sleep("5 millis");
    }
    return yield* Effect.die(new Error("Timed out waiting for the parent projection."));
  });
}

const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "thread-inbox-test", version: "1.0.0" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "thread-inbox-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

const scheduledTaskStubLayer = Layer.succeed(
  ScheduledTaskService,
  ScheduledTaskService.of({
    list: () => Effect.succeed({ tasks: [] }),
    subscribeList: () => Stream.succeed({ tasks: [] }),
    upsert: () => Effect.die("unused"),
    setEnabled: () => Effect.die("unused"),
    delete: () => Effect.die("unused"),
    runNow: () => Effect.die("unused"),
  }),
);

const invocation: McpInvocationContext.McpInvocationScope = {
  environmentId: EnvironmentId.make("environment:mcp-inbox"),
  threadId: parentThreadId,
  providerSessionId: "mcp-inbox-provider-session",
  providerInstanceId: parentInstanceId,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};

interface Harness {
  readonly orchestrator: OrchestratorV2Shape;
  readonly parentRun: OrchestrationV2Run;
  readonly capturedTurns: Ref.Ref<ReadonlyArray<CapturedTurn>>;
  readonly steers: Ref.Ref<ReadonlyArray<string>>;
  readonly releaseParent: Effect.Effect<void>;
  readonly invoke: (
    name: string,
    args: Record<string, unknown>,
  ) => Effect.Effect<McpSchema.CallToolResult, unknown>;
  readonly queueMessage: (suffix: string, text: string) => Effect.Effect<OrchestrationV2Run>;
  readonly queueDelegatedCompletion: (
    suffix: string,
  ) => Effect.Effect<{ readonly queuedRun: OrchestrationV2Run; readonly taskId: string }>;
  readonly run: (runId: RunId) => Effect.Effect<OrchestrationV2Run | undefined>;
}

/**
 * Runs `body` against the production MCP registration with the inbox-delivering
 * server, while a parent turn on `parentCapabilities` stays open.
 */
function withInboxHarness<A>(
  name: string,
  parentCapabilities: OrchestrationV2ProviderCapabilities,
  body: (harness: Harness) => Effect.Effect<A, unknown>,
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(name);
      const capturedTurns = yield* Ref.make<ReadonlyArray<CapturedTurn>>([]);
      const steers = yield* Ref.make<ReadonlyArray<string>>([]);
      const parentGate = yield* Deferred.make<void>();
      const gate = (turn: ProviderAdapterV2TurnInput) =>
        turn.threadId === parentThreadId && turn.message.text === parentPrompt
          ? parentGate
          : undefined;
      const registryLayer = makeProviderAdapterRegistryLayer([
        makeAdapter({
          instanceId: parentInstanceId,
          driver: ProviderDriverKind.make("codex"),
          capabilities: parentCapabilities,
          capturedTurns,
          steers,
          gate,
        }),
        makeAdapter({
          instanceId: childInstanceId,
          driver: ProviderDriverKind.make("claudeAgent"),
          capabilities: ClaudeProviderCapabilitiesV2,
          capturedTurns,
          steers,
          gate,
        }),
      ]);
      // The continuation worker is held so each test dispatches the completion itself.
      const continuationProbeLayer = Layer.succeed(ProviderContinuationRequests, {
        offer: () => Effect.void,
        take: Effect.never,
      });
      const orchestratorLayer = makeOrchestratorV2ReplayLayerWithRegistry(
        {
          name,
          runtimePolicyOverride: {
            cwd,
            approvalPolicy: "never",
            sandboxPolicy: {
              type: "readOnly",
              access: { type: "fullAccess" },
              networkAccess: false,
            },
          },
        },
        registryLayer,
      ).pipe(Layer.provide(continuationProbeLayer));
      const orchestrationLayer = Layer.merge(
        orchestratorLayer,
        threadManagementServiceLayer.pipe(Layer.provide(orchestratorLayer)),
      );
      const testLayer = McpHttpServer.ThreadToolkitRegistrationLive.pipe(
        Layer.provide(McpHttpServer.InboxDeliveryMcpServerLive),
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provideMerge(orchestrationLayer),
        Layer.provide(
          makeProviderRegistryLayer([
            providerSnapshot(parentInstanceId, "codex", parentSelection.model),
            providerSnapshot(childInstanceId, "claudeAgent", childSelection.model),
          ]),
        ),
        Layer.provide(scheduledTaskStubLayer),
        Layer.provide(NodeServices.layer),
      );

      return yield* Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        const server = yield* McpServer.McpServer;
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:inbox:create"),
          threadId: parentThreadId,
          projectId,
          title: "Inbox parent",
          modelSelection: parentSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:inbox:start"),
          threadId: parentThreadId,
          messageId: MessageId.make("message:inbox:start"),
          text: parentPrompt,
          attachments: [],
          modelSelection: parentSelection,
          dispatchMode: { type: "start_immediately" },
        });
        const started = yield* waitForProjection(
          orchestrator,
          (projection) =>
            projection.runs.some((run) => run.status === "running") &&
            projection.providerTurns.some((turn) => turn.status === "running"),
        );
        const parentRun = started.runs[0];
        if (parentRun === undefined || parentRun.rootNodeId === null) {
          return yield* Effect.die(new Error("Parent run missing."));
        }
        const parentRootNodeId = parentRun.rootNodeId;

        const invoke = (toolName: string, args: Record<string, unknown>) =>
          server
            .callTool({ name: toolName, arguments: args })
            .pipe(
              Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
              Effect.provideService(McpSchema.McpServerClient, client),
            );
        const run = (runId: RunId) =>
          orchestrator.getThreadProjection(parentThreadId).pipe(
            Effect.map((projection) => projection.runs.find((candidate) => candidate.id === runId)),
            Effect.orDie,
          );
        const queueMessage = (suffix: string, text: string) =>
          Effect.gen(function* () {
            const messageId = MessageId.make(`message:inbox:${suffix}`);
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              createdBy: "user",
              creationSource: "web",
              commandId: CommandId.make(`command:inbox:${suffix}`),
              threadId: parentThreadId,
              messageId,
              text,
              attachments: [],
              modelSelection: parentSelection,
              dispatchMode: { type: "queue_after_active" },
            });
            const queued = yield* waitForProjection(orchestrator, (projection) =>
              projection.runs.some(
                (candidate) =>
                  candidate.userMessageId === messageId && candidate.status === "queued",
              ),
            );
            const queuedRun = queued.runs.find(
              (candidate) => candidate.userMessageId === messageId,
            );
            if (queuedRun === undefined) return yield* Effect.die(new Error("Run not queued."));
            return queuedRun;
          }).pipe(Effect.orDie);
        const queueDelegatedCompletion = (suffix: string) =>
          Effect.gen(function* () {
            const delegated = yield* orchestrator.dispatch({
              type: "delegated_task.request",
              createdBy: "agent",
              creationSource: "mcp",
              commandId: CommandId.make(`command:inbox:${suffix}:delegate`),
              parentThreadId,
              parentRunId: parentRun.id,
              parentNodeId: parentRootNodeId,
              task: `Finish the ${suffix} child task.`,
              modelSelection: childSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              completionWake: "always",
            });
            const taskEvent = delegated.storedEvents.find(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.origin === "app_owned",
            );
            if (taskEvent?.event.type !== "subagent.updated") {
              return yield* Effect.die(new Error("Delegated task was not created."));
            }
            const task = taskEvent.event.payload;
            const reserved = yield* waitForProjection(orchestrator, (projection) => {
              const delivery = projection.runs.find((candidate) => candidate.id === parentRun.id)
                ?.delegatedCompletion?.delivery;
              return (
                projection.subagents.find((candidate) => candidate.id === task.id)?.status ===
                  "completed" &&
                delivery !== undefined &&
                delivery !== null &&
                delivery.taskIds.includes(task.id)
              );
            });
            const delivery = reserved.runs.find((candidate) => candidate.id === parentRun.id)
              ?.delegatedCompletion?.delivery;
            if (delivery === undefined || delivery === null) {
              return yield* Effect.die(new Error("Delegated completion was not reserved."));
            }
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              createdBy: "agent",
              creationSource: "server",
              commandId: CommandId.make(`command:inbox:${suffix}:dispatch`),
              threadId: parentThreadId,
              messageId: delivery.messageId,
              text: "Delegated task reached a terminal state.",
              attachments: [],
              modelSelection: parentSelection,
              dispatchMode: { type: "queue_after_active" },
              delegatedCompletion: {
                parentRunId: parentRun.id,
                generation: delivery.generation,
                taskIds: delivery.taskIds,
              },
            });
            const queued = yield* waitForProjection(orchestrator, (projection) =>
              projection.runs.some(
                (candidate) =>
                  candidate.userMessageId === delivery.messageId && candidate.status === "queued",
              ),
            );
            const queuedRun = queued.runs.find(
              (candidate) => candidate.userMessageId === delivery.messageId,
            );
            if (queuedRun === undefined) {
              return yield* Effect.die(new Error("Delegated completion was not queued."));
            }
            return { queuedRun, taskId: task.id };
          }).pipe(Effect.orDie);

        return yield* body({
          orchestrator,
          parentRun,
          capturedTurns,
          steers,
          releaseParent: Deferred.succeed(parentGate, undefined).pipe(Effect.asVoid),
          invoke,
          queueMessage,
          queueDelegatedCompletion,
          run,
        });
      }).pipe(Effect.provide(testLayer));
    }),
  );
}

const inboxBlocks = (result: McpSchema.CallToolResult) =>
  result.content.filter(
    (block): block is Extract<typeof block, { readonly type: "text" }> =>
      block.type === "text" && block.text.includes("===== T3 INBOX"),
  );

/** Samples an absence repeatedly instead of trusting one sleep to outlast a late event. */
const expectToStay = (check: Effect.Effect<void, unknown>) =>
  Effect.gen(function* () {
    for (let sample = 0; sample < 4; sample += 1) {
      yield* Effect.sleep("50 millis");
      yield* check;
    }
  });

describe("thread inbox delivery", () => {
  it.live("t3_inbox_wait wakes on a queued message and consumes it into the active run", () =>
    withInboxHarness("mcp-inbox-wait", nonSteeringCapabilities, (harness) =>
      Effect.gen(function* () {
        const text = "Stop the CI wait and look at the new failure.";
        const waiting = yield* harness
          .invoke("t3_inbox_wait", { timeoutMs: 10_000 })
          .pipe(Effect.forkChild);
        yield* Effect.sleep("100 millis");
        const queuedRun = yield* harness.queueMessage("wait", text);
        const result = yield* Fiber.join(waiting);

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          messages: [{ queuedRunId: queuedRun.id, text, truncated: false, createdBy: "user" }],
          notices: [],
          timedOut: false,
        });
        expect((yield* harness.run(queuedRun.id))?.status).toBe("cancelled");
        const projection = yield* harness.orchestrator.getThreadProjection(parentThreadId);
        const turnItem = projection.turnItems.find(
          (item) => item.type === "user_message" && item.messageId === queuedRun.userMessageId,
        );
        expect(turnItem).toMatchObject({
          runId: harness.parentRun.id,
          inputIntent: "promoted_queued_to_steer",
          text,
        });

        // The delivered message must not start a turn of its own afterwards.
        yield* harness.releaseParent;
        yield* waitForProjection(
          harness.orchestrator,
          (current) =>
            current.runs.find((candidate) => candidate.id === harness.parentRun.id)?.status ===
            "completed",
        );
        yield* expectToStay(
          Effect.gen(function* () {
            expect((yield* Ref.get(harness.capturedTurns)).map((turn) => turn.text)).toEqual([
              parentPrompt,
            ]);
          }),
        );
        expect(yield* Ref.get(harness.steers)).toEqual([]);
      }),
    ),
  );

  it.live("t3_inbox_wait returns empty after the timeout when nothing is queued", () =>
    withInboxHarness("mcp-inbox-timeout", nonSteeringCapabilities, (harness) =>
      Effect.gen(function* () {
        const startedAt = Date.now();
        const result = yield* harness.invoke("t3_inbox_wait", { timeoutMs: 300 });
        expect(Date.now() - startedAt).toBeGreaterThanOrEqual(290);
        expect(result.structuredContent).toEqual({ messages: [], notices: [], timedOut: true });
      }),
    ),
  );

  it.live("appends a queued message to one tool result and never to a second", () =>
    withInboxHarness("mcp-inbox-piggyback", nonSteeringCapabilities, (harness) =>
      Effect.gen(function* () {
        // Seed the hint and let the subscription settle, so the queued message
        // below is noticed through the domain-event path, not a projection read.
        const before = yield* harness.invoke("t3_thread_configuration", {});
        expect(inboxBlocks(before)).toEqual([]);
        yield* Effect.sleep("1200 millis");
        const settled = yield* harness.invoke("t3_thread_configuration", {});
        expect(inboxBlocks(settled)).toEqual([]);
        const text = "Also update the changelog.";
        const queuedRun = yield* harness.queueMessage("piggyback", text);

        const first = yield* harness.invoke("t3_thread_configuration", {});
        expect(first.structuredContent).toMatchObject({ threadId: parentThreadId });
        const blocks = inboxBlocks(first);
        expect(blocks).toHaveLength(1);
        expect(blocks[0]?.text).toContain(text);
        expect(blocks[0]?.text).toContain(queuedRun.id);
        expect((yield* harness.run(queuedRun.id))?.status).toBe("cancelled");

        const second = yield* harness.invoke("t3_thread_configuration", {});
        expect(inboxBlocks(second)).toEqual([]);
        expect(second.content).toHaveLength(1);
      }),
    ),
  );

  it.live("leaves providers with native steering untouched", () =>
    withInboxHarness("mcp-inbox-steering", CodexProviderCapabilitiesV2, (harness) =>
      Effect.gen(function* () {
        const queuedRun = yield* harness.queueMessage(
          "steering",
          "Queued behind a steerable turn.",
        );
        const result = yield* harness.invoke("t3_thread_configuration", {});
        expect(inboxBlocks(result)).toEqual([]);
        expect(result.content).toHaveLength(1);
        expect((yield* harness.run(queuedRun.id))?.status).toBe("queued");
      }),
    ),
  );

  it.live("surfaces a delegated-completion notice once and leaves its delivery queued", () =>
    withInboxHarness("mcp-inbox-notice", nonSteeringCapabilities, (harness) =>
      Effect.gen(function* () {
        const { queuedRun, taskId } = yield* harness.queueDelegatedCompletion("notice");

        const first = yield* harness.invoke("t3_thread_configuration", {});
        const blocks = inboxBlocks(first);
        expect(blocks).toHaveLength(1);
        expect(blocks[0]?.text).toContain(taskId);
        expect(blocks[0]?.text).not.toContain("delivered into this turn");

        const second = yield* harness.invoke("t3_thread_configuration", {});
        expect(inboxBlocks(second)).toEqual([]);
        const waited = yield* harness.invoke("t3_inbox_wait", { timeoutMs: 200 });
        expect(waited.structuredContent).toEqual({ messages: [], notices: [], timedOut: true });
        expect((yield* harness.run(queuedRun.id))?.status).toBe("queued");
        const rejected = yield* harness.orchestrator
          .dispatch({
            type: "queued-message.deliver-in-turn",
            commandId: CommandId.make("command:inbox:notice:deliver"),
            threadId: parentThreadId,
            queuedRunId: queuedRun.id,
            targetRunId: harness.parentRun.id,
          })
          .pipe(Effect.flip);
        expect(rejected).toMatchObject({
          cause: expect.stringContaining("keep their own delivery path"),
        });
        expect((yield* harness.run(queuedRun.id))?.status).toBe("queued");

        // The notice keeps its own delivery: exactly one turn once the parent settles.
        yield* harness.releaseParent;
        yield* waitForProjection(
          harness.orchestrator,
          (projection) =>
            projection.runs.find((candidate) => candidate.id === queuedRun.id)?.status ===
            "completed",
        );
        const deliveries = (yield* Ref.get(harness.capturedTurns)).filter(
          (turn) => turn.threadId === parentThreadId && turn.text.startsWith("Delegated task"),
        );
        expect(deliveries).toHaveLength(1);
      }),
    ),
  );
});
