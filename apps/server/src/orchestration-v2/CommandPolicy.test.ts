import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ThreadProjection,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { CursorProviderCapabilitiesV2 } from "./Adapters/CursorAdapterV2.ts";
import { GrokProviderCapabilitiesV2 } from "./Adapters/GrokAdapterV2.ts";
import {
  CommandPolicyCapabilityUnsupportedError,
  CommandPolicyUnsupportedError,
  CommandPolicyV2,
  layer as commandPolicyLayer,
  resolveMessageDispatchIntent,
} from "./CommandPolicy.ts";

const commandId = CommandId.make("command-policy-test");
const threadId = ThreadId.make("command-policy-thread");
const activeRunId = RunId.make("command-policy-active-run");
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
};

const baseCapabilities: OrchestrationV2ProviderCapabilities = CodexProviderCapabilitiesV2;

function capabilities(
  override: (current: OrchestrationV2ProviderCapabilities) => OrchestrationV2ProviderCapabilities,
): OrchestrationV2ProviderCapabilities {
  return override(baseCapabilities);
}

function dispatchProjection(
  sessionCapabilities?: OrchestrationV2ProviderCapabilities,
): OrchestrationV2ThreadProjection {
  const providerThreadId = ProviderThreadId.make("command-policy-provider-thread");
  const providerSessionId = ProviderSessionId.make("command-policy-provider-session");
  return {
    runs:
      sessionCapabilities === undefined
        ? []
        : [{ id: activeRunId, status: "running", providerThreadId }],
    providerThreads:
      sessionCapabilities === undefined ? [] : [{ id: providerThreadId, providerSessionId }],
    providerSessions:
      sessionCapabilities === undefined
        ? []
        : [{ id: providerSessionId, capabilities: sessionCapabilities }],
  } as unknown as OrchestrationV2ThreadProjection;
}

function decisionProjection(input: {
  readonly text: string;
  readonly attachments?: ReadonlyArray<unknown>;
  readonly status?: "preparing" | "starting" | "running" | "waiting";
  readonly withProviderTurn?: boolean;
}): OrchestrationV2ThreadProjection {
  const activeAttemptId = RunAttemptId.make("command-policy-decision-attempt");
  const activeMessageId = MessageId.make("command-policy-decision-message");
  const withProviderTurn = input.withProviderTurn ?? true;
  return {
    thread: { id: threadId, modelSelection },
    runs: [
      {
        id: activeRunId,
        status: input.status ?? "running",
        activeAttemptId: withProviderTurn ? activeAttemptId : null,
        userMessageId: activeMessageId,
      },
    ],
    messages: [
      {
        id: activeMessageId,
        role: "user",
        text: input.text,
        attachments: input.attachments ?? [],
      },
    ],
    providerTurns: withProviderTurn
      ? [
          {
            id: ProviderTurnId.make("command-policy-decision-turn"),
            runAttemptId: activeAttemptId,
            status: "running",
          },
        ]
      : [],
  } as unknown as OrchestrationV2ThreadProjection;
}

it("resolves automatic message delivery from authoritative provider capabilities", () => {
  assert.deepEqual(
    resolveMessageDispatchIntent(dispatchProjection(), { type: "start_immediately" }, "auto"),
    { type: "start_immediately" },
  );
  assert.deepEqual(
    resolveMessageDispatchIntent(
      dispatchProjection(baseCapabilities),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "steer_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    resolveMessageDispatchIntent(
      dispatchProjection(
        capabilities((current) => ({
          ...current,
          turns: {
            ...current.turns,
            supportsActiveSteering: false,
            supportsQueuedMessages: true,
            supportsSteeringByInterruptRestart: true,
          },
        })),
      ),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "queue_after_active" },
  );
  assert.deepEqual(
    resolveMessageDispatchIntent(
      dispatchProjection(
        capabilities((current) => ({
          ...current,
          turns: {
            ...current.turns,
            supportsActiveSteering: false,
            supportsQueuedMessages: false,
            supportsSteeringByInterruptRestart: true,
          },
        })),
      ),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "restart_active", targetRunId: activeRunId },
  );
});

it("targets the latest active run for explicit steer and restart intent", () => {
  const projection = dispatchProjection(baseCapabilities);
  assert.deepEqual(
    resolveMessageDispatchIntent(projection, { type: "start_immediately" }, "steer"),
    { type: "steer_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    resolveMessageDispatchIntent(projection, { type: "start_immediately" }, "restart"),
    { type: "restart_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    resolveMessageDispatchIntent(dispatchProjection(), { type: "start_immediately" }, "steer"),
    { type: "start_immediately" },
  );
});

const layer = it.layer(commandPolicyLayer);

layer("CommandPolicyV2", (it) => {
  it.effect("queues a targeted message while context compaction is active", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const result = yield* policy.decideMessageDispatch({
        commandId,
        projection: decisionProjection({ text: " /COMPACT " }),
        requestedMode: { type: "steer_active", targetRunId: activeRunId },
        capabilities: baseCapabilities,
      });

      assert.deepEqual(result, { type: "queue_after_active", activeRunId });
    }),
  );

  it.effect("queues behind preparing and starting compaction without a provider turn", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      for (const status of ["preparing", "starting"] as const) {
        for (const requestedMode of [
          { type: "steer_active" as const, targetRunId: activeRunId },
          { type: "restart_active" as const, targetRunId: activeRunId },
          { type: "start_immediately" as const },
        ]) {
          const result = yield* policy.decideMessageDispatch({
            commandId,
            projection: decisionProjection({
              text: "/compact",
              status,
              withProviderTurn: false,
            }),
            requestedMode,
            capabilities: baseCapabilities,
          });

          assert.deepEqual(result, { type: "queue_after_active", activeRunId });
        }
      }
    }),
  );

  it.effect("rejects stale explicit targets before compact queue conversion", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;
      const staleRunId = RunId.make("command-policy-stale-run");

      for (const requestedMode of [
        { type: "steer_active" as const, targetRunId: staleRunId },
        { type: "restart_active" as const, targetRunId: staleRunId },
      ]) {
        const error = yield* policy
          .decideMessageDispatch({
            commandId,
            projection: decisionProjection({ text: "/compact" }),
            requestedMode,
            capabilities: baseCapabilities,
          })
          .pipe(Effect.flip);

        assert.instanceOf(error, CommandPolicyUnsupportedError);
        assert.equal(error.requestedMode, requestedMode.type);
      }
    }),
  );

  it.effect("does not treat logout, compact arguments, or compact attachments as compaction", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      for (const input of [
        { text: "/logout", attachments: [] },
        { text: "/compact preserve the summary", attachments: [] },
        { text: "/compact", attachments: [{}] },
      ]) {
        const result = yield* policy.decideMessageDispatch({
          commandId,
          projection: decisionProjection(input),
          requestedMode: { type: "steer_active", targetRunId: activeRunId },
          capabilities: baseCapabilities,
        });

        assert.equal(result.type, "steer_active");
      }
    }),
  );

  it.effect("starts immediately when no run is active", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;
      const projection = {
        thread: { id: threadId, modelSelection },
        runs: [],
        messages: [],
        providerTurns: [],
      } as unknown as OrchestrationV2ThreadProjection;

      const result = yield* policy.decideMessageDispatch({
        commandId,
        projection,
        requestedMode: { type: "start_immediately" },
        capabilities: baseCapabilities,
      });

      assert.deepEqual(result, { type: "start_run", modelSelection });
    }),
  );

  it.effect("rejects compact queue conversion without queued-message support", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;
      const unsupportedCapabilities = capabilities((current) => ({
        ...current,
        turns: {
          ...current.turns,
          supportsQueuedMessages: false,
        },
      }));

      const error = yield* policy
        .decideMessageDispatch({
          commandId,
          projection: decisionProjection({ text: "/compact" }),
          requestedMode: { type: "restart_active", targetRunId: activeRunId },
          capabilities: unsupportedCapabilities,
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "queued_messages");
    }),
  );

  it.effect("prefers direct active steering when the provider supports it", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const result = yield* policy.decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: baseCapabilities,
      });

      assert.equal(result, "active_steering");
    }),
  );

  it.effect("uses interrupt-and-restart steering when direct steering is unavailable", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const result = yield* policy.decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: capabilities((current) => ({
          ...current,
          turns: {
            ...current.turns,
            supportsActiveSteering: false,
            supportsInterrupt: true,
            supportsSteeringByInterruptRestart: true,
          },
        })),
      });

      assert.equal(result, "interrupt_restart");
    }),
  );

  it.effect("uses interrupt-and-restart steering for Grok ACP", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const result = yield* policy.decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("grok"),
        capabilities: GrokProviderCapabilitiesV2,
      });

      assert.equal(result, "interrupt_restart");
    }),
  );

  it.effect("honors an explicit interrupt-and-restart request", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const result = yield* policy.decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: CodexProviderCapabilitiesV2,
        forceRestart: true,
      });

      assert.equal(result, "interrupt_restart");
    }),
  );

  it.effect("returns typed capability errors for unsupported active steering", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const error = yield* policy
        .decideSteeringExecution({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          capabilities: capabilities((current) => ({
            ...current,
            turns: {
              ...current.turns,
              supportsActiveSteering: false,
              supportsInterrupt: false,
              supportsSteeringByInterruptRestart: false,
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "active_steering");
    }),
  );

  it.effect("guards native fork behind fork and identity capabilities", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const error = yield* policy
        .ensureNativeFork({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          fromSpecificTurn: true,
          capabilities: capabilities((current) => ({
            ...current,
            identity: {
              ...current.identity,
              nativeThreadIds: "weak",
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "native_fork");
    }),
  );

  it.effect("uses a native fork when the provider supports the requested source point", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const result = yield* policy.decideForkExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: CodexProviderCapabilitiesV2,
        sameProvider: true,
        hasStrongNativeSource: true,
        fromSpecificTurn: true,
      });

      assert.equal(result, "native_fork");
    }),
  );

  it.effect("falls back to portable context when Cursor cannot fork natively", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const result = yield* policy.decideForkExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("cursor"),
        capabilities: CursorProviderCapabilitiesV2,
        sameProvider: true,
        hasStrongNativeSource: true,
        fromSpecificTurn: true,
      });

      assert.equal(result, "portable_context");
    }),
  );

  it.effect("falls back to portable context when Grok ACP cannot fork natively", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const result = yield* policy.decideForkExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("grok"),
        capabilities: GrokProviderCapabilitiesV2,
        sameProvider: true,
        hasStrongNativeSource: true,
        fromSpecificTurn: true,
      });

      assert.equal(result, "portable_context");
    }),
  );

  it.effect("returns a typed error when neither native nor portable fork is available", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const error = yield* policy
        .decideForkExecution({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("cursor"),
          capabilities: capabilities((current) => ({
            ...current,
            threads: {
              ...current.threads,
              canForkThread: false,
            },
            context: {
              ...current.context,
              canConsumeHandoffSummaries: false,
            },
          })),
          sameProvider: true,
          hasStrongNativeSource: true,
          fromSpecificTurn: true,
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "context_handoff");
    }),
  );

  it.effect("guards rollback behind provider rollback snapshot support", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const error = yield* policy
        .ensureRollback({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          capabilities: capabilities((current) => ({
            ...current,
            checkpointing: {
              ...current.checkpointing,
              providerRollbackReturnsSnapshot: false,
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "rollback_snapshot");
    }),
  );

  it.effect("guards fork-delta handoff behind context handoff capabilities", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const error = yield* policy
        .ensureContextHandoff({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          strategy: "fork_delta_context",
          capabilities: capabilities((current) => ({
            ...current,
            context: {
              ...current.context,
              supportsDeltaHandoff: false,
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "context_handoff");
    }),
  );

  it.effect("guards queued turns behind queued-message support", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicyV2;

      const error = yield* policy
        .ensureQueuedMessages({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          capabilities: capabilities((current) => ({
            ...current,
            turns: {
              ...current.turns,
              supportsQueuedMessages: false,
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "queued_messages");
    }),
  );
});
