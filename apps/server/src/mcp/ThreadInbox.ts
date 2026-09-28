import {
  CommandId,
  type MessageId,
  type OrchestrationV2Actor,
  type OrchestrationV2CreationSource,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import { isNativeMaintenanceCommand } from "../orchestration-v2/Orchestrator.ts";
import { queuedRunsInDeliveryOrder } from "../orchestration-v2/QueuedRunOrder.ts";
import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";
import type { McpInvocationScope } from "./McpInvocationContext.ts";

/** Full text is returned up to this many characters per message. */
export const INBOX_MESSAGE_TEXT_LIMIT = 16_000;
const MAX_MESSAGES_PER_DELIVERY = 20;
/** Hints seeded before the event subscription has settled are re-read once. */
const SUBSCRIPTION_SETTLE = Duration.seconds(1);
/** A waiter re-reads the projection this often even without an event. */
const WAIT_RECHECK = Duration.seconds(30);

export interface ThreadInboxMessage {
  readonly queuedRunId: RunId;
  readonly messageId: MessageId;
  readonly text: string;
  readonly truncated: boolean;
  readonly createdBy: OrchestrationV2Actor;
  readonly creationSource: OrchestrationV2CreationSource;
}

export interface ThreadInboxNotice {
  readonly queuedRunId: RunId;
  readonly kind: "delegated_completion" | "notification";
  readonly taskIds: ReadonlyArray<string>;
  readonly summary: string | null;
}

export interface ThreadInboxDelivery {
  readonly messages: ReadonlyArray<ThreadInboxMessage>;
  readonly notices: ReadonlyArray<ThreadInboxNotice>;
}

export interface ThreadInboxShape {
  /**
   * Block until the calling thread has queued input or `timeoutMs` elapses.
   * Queued user and agent messages are consumed into the active run; notices
   * are surfaced once and keep their own delivery.
   */
  readonly wait: (input: {
    readonly scope: McpInvocationScope;
    readonly timeoutMs: number;
    readonly includeNotices: boolean;
  }) => Effect.Effect<ThreadInboxDelivery & { readonly timedOut: boolean }>;
  /**
   * Collect queued input for a tool result from a live caller whose provider
   * cannot steer. Returns null on the common no-input path without any
   * projection read.
   */
  readonly piggyback: (scope: McpInvocationScope) => Effect.Effect<ThreadInboxDelivery | null>;
}

export class ThreadInbox extends Context.Service<ThreadInbox, ThreadInboxShape>()(
  "t3/mcp/ThreadInbox",
) {}

const ThreadInboxConfig = Config.all({
  piggyback: Config.boolean("T3CODE_MCP_INBOX_PIGGYBACK").pipe(Config.withDefault(true)),
});

/** Per-thread hint kept current from domain events; the projection stays the truth. */
interface ThreadHint {
  readonly queued: Set<RunId>;
  /** Queued runs already examined and left queued (surfaced notices, blocked messages). */
  readonly inert: Set<RunId>;
  /** Notices already surfaced to the agent once. */
  readonly surfaced: Set<RunId>;
  activeRunId: RunId | null;
  /** Active run whose provider steers natively, so piggyback stays out of it. */
  steeringRunId: RunId | null;
  seeded: boolean;
  version: number;
}

const ACTIVE_RUN_STATUSES = new Set<OrchestrationV2Run["status"]>([
  "preparing",
  "starting",
  "running",
  "waiting",
]);

export const emptyDelivery: ThreadInboxDelivery = { messages: [], notices: [] };

export function isEmptyDelivery(delivery: ThreadInboxDelivery): boolean {
  return delivery.messages.length === 0 && delivery.notices.length === 0;
}

function truncate(text: string): { readonly text: string; readonly truncated: boolean } {
  const characters = Array.from(text);
  return characters.length > INBOX_MESSAGE_TEXT_LIMIT
    ? { text: characters.slice(0, INBOX_MESSAGE_TEXT_LIMIT).join(""), truncated: true }
    : { text, truncated: false };
}

function activeRunSteers(
  projection: OrchestrationV2ThreadProjection,
  run: OrchestrationV2Run,
): boolean {
  const providerThread = projection.providerThreads.find(
    (candidate) => candidate.id === run.providerThreadId,
  );
  const session = projection.providerSessions.find(
    (candidate) => candidate.id === providerThread?.providerSessionId,
  );
  return session?.capabilities.turns.supportsActiveSteering === true;
}

function delegatedCompletionSummary(
  projection: OrchestrationV2ThreadProjection,
  taskIds: ReadonlyArray<string>,
): string {
  const tasks = projection.subagents.filter((task) => taskIds.includes(task.id));
  const outcome = tasks.some((task) => task.status === "failed")
    ? "failed"
    : tasks.some((task) => task.status === "cancelled" || task.status === "interrupted")
      ? "stopped"
      : "finished";
  const title = tasks[0]?.title?.trim();
  return taskIds.length === 1
    ? `Delegated task${title ? ` "${title}"` : ""} ${outcome}.`
    : `${taskIds.length} delegated tasks ${outcome}.`;
}

/** Text block appended to a tool result that carried queued input. */
export function formatPiggybackBlock(delivery: ThreadInboxDelivery): string {
  const lines: Array<string> = [];
  if (delivery.messages.length > 0) {
    const count = delivery.messages.length;
    lines.push(
      `===== T3 INBOX: ${count} queued message${count === 1 ? "" : "s"} delivered into this turn =====`,
      "These arrived while you were working. Treat them as new input from the sender; they will not be sent again.",
    );
    delivery.messages.forEach((message, index) => {
      lines.push(
        "",
        `--- message ${index + 1} of ${count} (from ${message.createdBy}, queued run ${message.queuedRunId}) ---`,
        message.text,
      );
      if (message.truncated) {
        lines.push(`[truncated at ${INBOX_MESSAGE_TEXT_LIMIT} characters]`);
      }
    });
  }
  if (delivery.notices.length > 0) {
    lines.push(
      "",
      "===== T3 INBOX: completion notices (still queued; the full notice arrives as its own turn after this one ends) =====",
    );
    for (const notice of delivery.notices) {
      const tasks = notice.taskIds.length === 0 ? "" : ` taskIds: ${notice.taskIds.join(", ")}.`;
      lines.push(`- ${notice.summary ?? "Background work finished."}${tasks}`);
    }
  }
  lines.push("===== END T3 INBOX =====");
  return lines.join("\n");
}

export const make = Effect.gen(function* () {
  const threads = yield* ThreadManagementService;
  const crypto = yield* Crypto.Crypto;
  const config = yield* ThreadInboxConfig;
  const layerScope = yield* Effect.scope;
  const signals = yield* PubSub.unbounded<ThreadId>();
  const collectLock = yield* Semaphore.make(1);
  const hints = new Map<ThreadId, ThreadHint>();
  let started = false;
  let settled = false;

  const hintFor = (threadId: ThreadId): ThreadHint => {
    const existing = hints.get(threadId);
    if (existing !== undefined) return existing;
    const created: ThreadHint = {
      queued: new Set(),
      inert: new Set(),
      surfaced: new Set(),
      activeRunId: null,
      steeringRunId: null,
      seeded: false,
      version: 0,
    };
    hints.set(threadId, created);
    return created;
  };

  const onEvent = (event: OrchestrationV2DomainEvent) =>
    Effect.suspend(() => {
      if (event.type !== "run.created" && event.type !== "run.updated") return Effect.void;
      const run = event.payload;
      const hint = hintFor(run.threadId);
      hint.version += 1;
      const wasQueued = hint.queued.has(run.id);
      let becameQueued = false;
      if (run.status === "queued") {
        if (!wasQueued) {
          hint.queued.add(run.id);
          hint.inert.clear();
          becameQueued = true;
        }
      } else if (wasQueued) {
        hint.queued.delete(run.id);
        hint.surfaced.delete(run.id);
        hint.inert.clear();
      }
      if (ACTIVE_RUN_STATUSES.has(run.status)) {
        hint.activeRunId = run.id;
      } else if (hint.activeRunId === run.id) {
        hint.activeRunId = null;
        hint.steeringRunId = null;
      }
      return becameQueued ? PubSub.publish(signals, run.threadId).pipe(Effect.asVoid) : Effect.void;
    });

  const onSubscriptionStart = Effect.suspend(() => {
    settled = false;
    for (const hint of hints.values()) hint.seeded = false;
    return Effect.sleep(SUBSCRIPTION_SETTLE).pipe(
      Effect.andThen(
        Effect.sync(() => {
          settled = true;
        }),
      ),
      Effect.forkIn(layerScope),
      Effect.asVoid,
    );
  });

  const subscription = Stream.runForEach(
    threads.streamDomainEvents.pipe(Stream.onStart(onSubscriptionStart)),
    onEvent,
  ).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("mcp.thread-inbox.subscription-ended", { cause }),
    ),
    Effect.andThen(Effect.sleep(Duration.seconds(1))),
    Effect.forever,
  );

  // Subscribed lazily so registration layers stay inert until a caller uses the inbox.
  const ensureStarted = Effect.suspend(() => {
    if (started) return Effect.void;
    started = true;
    return subscription.pipe(Effect.forkIn(layerScope), Effect.asVoid);
  });

  const refreshHint = (
    threadId: ThreadId,
    projection: OrchestrationV2ThreadProjection,
    versionAtRead: number,
  ) => {
    const hint = hintFor(threadId);
    const exact = hint.version === versionAtRead;
    const queued = projection.runs.filter((run) => run.status === "queued").map((run) => run.id);
    if (exact) hint.queued.clear();
    for (const runId of queued) hint.queued.add(runId);
    for (const runId of hint.surfaced) if (!hint.queued.has(runId)) hint.surfaced.delete(runId);
    const active = projection.runs.find((run) => ACTIVE_RUN_STATUSES.has(run.status));
    if (exact) hint.activeRunId = active?.id ?? null;
    hint.seeded = exact && settled;
    return { hint, exact };
  };

  const newCommandId = crypto.randomUUIDv4.pipe(
    Effect.orDie,
    Effect.map((uuid) => CommandId.make(`mcp-inbox:${uuid}`)),
  );

  const collect = (
    scope: McpInvocationScope,
    options: { readonly mode: "wait" | "piggyback"; readonly includeNotices: boolean },
  ): Effect.Effect<ThreadInboxDelivery> =>
    collectLock
      .withPermit(
        Effect.gen(function* () {
          const versionAtRead = hintFor(scope.threadId).version;
          const projection = yield* threads.getThreadProjection(scope.threadId).pipe(
            Effect.map(Option.some),
            Effect.catchCause((cause) =>
              Effect.logWarning("mcp.thread-inbox.projection-read-failed", {
                threadId: scope.threadId,
                cause,
              }).pipe(Effect.as(Option.none<OrchestrationV2ThreadProjection>())),
            ),
          );
          if (Option.isNone(projection)) return emptyDelivery;
          const current = projection.value;
          const { hint, exact } = refreshHint(scope.threadId, current, versionAtRead);
          if (current.thread.archivedAt !== null || current.thread.deletedAt !== null) {
            return emptyDelivery;
          }
          const targetRun = current.runs.find(
            (run) =>
              run.status === "running" && run.providerInstanceId === scope.providerInstanceId,
          );
          const steers = targetRun !== undefined && activeRunSteers(current, targetRun);
          hint.steeringRunId = steers && targetRun !== undefined ? targetRun.id : null;
          if (options.mode === "piggyback" && (targetRun === undefined || steers)) {
            return emptyDelivery;
          }

          const notices: Array<ThreadInboxNotice> = [];
          const messages: Array<ThreadInboxMessage> = [];
          const inert: Array<RunId> = [];
          let blocked = targetRun === undefined || targetRun.rootNodeId === null;
          for (const run of queuedRunsInDeliveryOrder(current)) {
            const message = current.messages.find(
              (candidate) => candidate.id === run.userMessageId,
            );
            if (message === undefined) continue;
            if (message.delegatedCompletion !== undefined || message.notification !== undefined) {
              if (hint.surfaced.has(run.id)) {
                inert.push(run.id);
                continue;
              }
              if (!options.includeNotices) continue;
              hint.surfaced.add(run.id);
              inert.push(run.id);
              notices.push(
                message.delegatedCompletion !== undefined
                  ? {
                      queuedRunId: run.id,
                      kind: "delegated_completion",
                      taskIds: message.delegatedCompletion.taskIds,
                      summary: delegatedCompletionSummary(
                        current,
                        message.delegatedCompletion.taskIds,
                      ),
                    }
                  : {
                      queuedRunId: run.id,
                      kind: "notification",
                      taskIds: [],
                      summary: message.notification?.summary ?? null,
                    },
              );
              continue;
            }
            // Deliver in queue order; stop at the first message that must stay a turn of its own.
            if (
              blocked ||
              targetRun === undefined ||
              message.attachments.length > 0 ||
              isNativeMaintenanceCommand(message) ||
              messages.length >= MAX_MESSAGES_PER_DELIVERY
            ) {
              blocked = true;
              inert.push(run.id);
              continue;
            }
            const delivered = yield* threads
              .dispatch({
                type: "queued-message.deliver-in-turn",
                commandId: yield* newCommandId,
                threadId: scope.threadId,
                queuedRunId: run.id,
                targetRunId: targetRun.id,
              })
              .pipe(
                Effect.as(true),
                Effect.catchCause((cause) =>
                  Effect.logInfo("mcp.thread-inbox.delivery-skipped", {
                    threadId: scope.threadId,
                    queuedRunId: run.id,
                    cause,
                  }).pipe(Effect.as(false)),
                ),
              );
            if (!delivered) {
              blocked = true;
              continue;
            }
            hint.queued.delete(run.id);
            messages.push({
              queuedRunId: run.id,
              messageId: message.id,
              ...truncate(message.text),
              createdBy: message.createdBy,
              creationSource: message.creationSource,
            });
          }
          if (exact) for (const runId of inert) hint.inert.add(runId);
          return { messages, notices };
        }),
      )
      .pipe(Effect.uninterruptible);

  const needsTruthRead = (scope: McpInvocationScope): boolean => {
    const hint = hints.get(scope.threadId);
    if (hint === undefined || !hint.seeded) return true;
    if (hint.activeRunId === null || hint.steeringRunId === hint.activeRunId) return false;
    for (const runId of hint.queued) if (!hint.inert.has(runId)) return true;
    return false;
  };

  const piggyback: ThreadInboxShape["piggyback"] = (scope) =>
    Effect.gen(function* () {
      if (!config.piggyback || !scope.capabilities.has("orchestration")) return null;
      yield* ensureStarted;
      if (!needsTruthRead(scope)) return null;
      const delivery = yield* collect(scope, { mode: "piggyback", includeNotices: true });
      return isEmptyDelivery(delivery) ? null : delivery;
    });

  const wait: ThreadInboxShape["wait"] = ({ scope, timeoutMs, includeNotices }) =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* ensureStarted;
        const subscriptionForThread = yield* PubSub.subscribe(signals);
        const nextSignal = Effect.gen(function* () {
          while ((yield* PubSub.take(subscriptionForThread)) !== scope.threadId) {
            // Signals for other threads are not ours.
          }
        });
        const startedAt = yield* Clock.currentTimeMillis;
        while (true) {
          const delivery = yield* collect(scope, { mode: "wait", includeNotices });
          if (!isEmptyDelivery(delivery)) return { ...delivery, timedOut: false };
          const now = yield* Clock.currentTimeMillis;
          const remaining = timeoutMs - (now - startedAt);
          if (remaining <= 0) return { ...emptyDelivery, timedOut: true };
          yield* nextSignal.pipe(
            Effect.timeoutOption(Duration.min(Duration.millis(remaining), WAIT_RECHECK)),
          );
        }
      }),
    );

  return ThreadInbox.of({ wait, piggyback });
});

export const layer: Layer.Layer<
  ThreadInbox,
  Config.ConfigError,
  ThreadManagementService | Crypto.Crypto
> = Layer.effect(ThreadInbox, make);
