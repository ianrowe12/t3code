import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2TurnItem,
  ProjectId,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { LockTimeoutError, SqlError, UnknownError } from "effect/unstable/sql/SqlError";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { EventSinkV2, layer as eventSinkLayer } from "./EventSink.ts";
import { EventStoreV2, layer as eventStoreLayer } from "./EventStore.ts";
import {
  ProjectionStoreApplyEventError,
  ProjectionStoreV2,
  layer as projectionStoreLayer,
} from "./ProjectionStore.ts";

const providerInstanceId = ProviderInstanceId.make("codex");

// Fails projection writes after the events were appended inside the
// transaction, so a retry that did not roll back would duplicate them.
const injectedApplyFailures = {
  remaining: 0,
  attempts: 0,
  reason: "lock" as "lock" | "other",
};

const faultyProjectionStoreLayer = Layer.effect(
  ProjectionStoreV2,
  Effect.gen(function* () {
    const projectionStore = yield* ProjectionStoreV2;
    return ProjectionStoreV2.of({
      ...projectionStore,
      apply: (event) =>
        Effect.gen(function* () {
          injectedApplyFailures.attempts += 1;
          if (injectedApplyFailures.remaining > 0) {
            injectedApplyFailures.remaining -= 1;
            const driverError = new Error("database is locked");
            return yield* new ProjectionStoreApplyEventError({
              eventType: event.type,
              cause: new SqlError({
                reason:
                  injectedApplyFailures.reason === "lock"
                    ? new LockTimeoutError({ cause: driverError, operation: "execute" })
                    : new UnknownError({ cause: driverError, operation: "execute" }),
              }),
            });
          }
          return yield* projectionStore.apply(event);
        }),
    });
  }),
).pipe(Layer.provide(projectionStoreLayer));

const storesProvided = Layer.mergeAll(
  SqlitePersistenceMemory,
  eventStoreLayer.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
  faultyProjectionStoreLayer.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);
const TestLayer = Layer.mergeAll(
  storesProvided,
  eventSinkLayer.pipe(Layer.provide(storesProvided)),
);

function threadEvents(key: string, now: DateTime.Utc): ReadonlyArray<OrchestrationV2DomainEvent> {
  const threadId = ThreadId.make(`thread:event-sink-lock:${key}`);
  const thread: OrchestrationV2AppThread = {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: ProjectId.make(`project:${threadId}`),
    title: `Thread ${threadId}`,
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  return [
    {
      id: EventId.make(`event:event-sink-lock:${key}:created`),
      type: "thread.created",
      threadId,
      providerInstanceId,
      occurredAt: now,
      payload: thread,
    },
    {
      id: EventId.make(`event:event-sink-lock:${key}:renamed`),
      type: "thread.metadata-updated",
      threadId,
      providerInstanceId,
      occurredAt: now,
      payload: { ...thread, title: "Renamed" },
    },
  ];
}

const turnItemPersistThrottleEnv = "T3CODE_TURN_ITEM_PERSIST_THROTTLE_MS";

function withTurnItemPersistThrottleEnv<A, E, R>(
  value: string | undefined,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> {
  return Effect.acquireUseRelease(
    Effect.sync(() => {
      const previous = process.env[turnItemPersistThrottleEnv];
      if (value === undefined) {
        delete process.env[turnItemPersistThrottleEnv];
      } else {
        process.env[turnItemPersistThrottleEnv] = value;
      }
      return previous;
    }),
    () => effect,
    (previous) =>
      Effect.sync(() => {
        if (previous === undefined) {
          delete process.env[turnItemPersistThrottleEnv];
        } else {
          process.env[turnItemPersistThrottleEnv] = previous;
        }
      }),
  );
}

function makeThrottleThread(key: string, now: DateTime.Utc): OrchestrationV2AppThread {
  const threadId = ThreadId.make(`thread:turn-item-persist-throttle:${key}`);
  return {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: ProjectId.make(`project:${threadId}`),
    title: `Throttle ${key}`,
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}

function threadCreatedEvent(input: {
  readonly key: string;
  readonly thread: OrchestrationV2AppThread;
  readonly now: DateTime.Utc;
}): OrchestrationV2DomainEvent {
  return {
    id: EventId.make(`event:turn-item-persist-throttle:${input.key}:thread`),
    type: "thread.created",
    threadId: input.thread.id,
    providerInstanceId,
    occurredAt: input.now,
    payload: input.thread,
  };
}

function makeTextTurnItem(input: {
  readonly key: string;
  readonly threadId: ThreadId;
  readonly now: DateTime.Utc;
  readonly text: string;
  readonly type?: "reasoning" | "assistant_message";
  readonly status?: OrchestrationV2TurnItem["status"];
  readonly streaming?: boolean;
}): OrchestrationV2TurnItem {
  const type = input.type ?? "reasoning";
  const status = input.status ?? "running";
  const completedAt = status === "running" ? null : input.now;
  const base = {
    id: TurnItemId.make(`turn-item:turn-item-persist-throttle:${input.key}`),
    threadId: input.threadId,
    runId: RunId.make(`run:turn-item-persist-throttle:${input.key}`),
    nodeId: NodeId.make(`node:turn-item-persist-throttle:${input.key}`),
    providerThreadId: ProviderThreadId.make(
      `provider-thread:turn-item-persist-throttle:${input.key}`,
    ),
    providerTurnId: ProviderTurnId.make(`provider-turn:turn-item-persist-throttle:${input.key}`),
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status,
    title: null,
    startedAt: input.now,
    completedAt,
    updatedAt: input.now,
    text: input.text,
    streaming: input.streaming ?? status === "running",
  };
  if (type === "assistant_message") {
    return {
      ...base,
      type,
      messageId: MessageId.make(`message:turn-item-persist-throttle:${input.key}`),
      attachments: [],
    };
  }
  return { ...base, type };
}

function makeCommandTurnItem(input: {
  readonly key: string;
  readonly threadId: ThreadId;
  readonly now: DateTime.Utc;
  readonly output: string;
}): OrchestrationV2TurnItem {
  return {
    id: TurnItemId.make(`turn-item:turn-item-persist-throttle:${input.key}`),
    threadId: input.threadId,
    runId: RunId.make(`run:turn-item-persist-throttle:${input.key}`),
    nodeId: NodeId.make(`node:turn-item-persist-throttle:${input.key}`),
    providerThreadId: ProviderThreadId.make(
      `provider-thread:turn-item-persist-throttle:${input.key}`,
    ),
    providerTurnId: ProviderTurnId.make(`provider-turn:turn-item-persist-throttle:${input.key}`),
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "running",
    title: "Command",
    startedAt: input.now,
    completedAt: null,
    updatedAt: input.now,
    type: "command_execution",
    input: "echo test",
    output: input.output,
  };
}

function turnItemEvent(input: {
  readonly id: string;
  readonly item: OrchestrationV2TurnItem;
  readonly now: DateTime.Utc;
}): OrchestrationV2DomainEvent {
  return {
    id: EventId.make(`event:turn-item-persist-throttle:${input.id}`),
    type: "turn-item.updated",
    threadId: input.item.threadId,
    ...(input.item.runId === null ? {} : { runId: input.item.runId }),
    ...(input.item.nodeId === null ? {} : { nodeId: input.item.nodeId }),
    providerInstanceId,
    occurredAt: input.now,
    payload: input.item,
  };
}

function providerTurnTerminalEvent(input: {
  readonly key: string;
  readonly item: OrchestrationV2TurnItem;
  readonly now: DateTime.Utc;
  readonly status: OrchestrationV2ProviderTurn["status"];
}): OrchestrationV2DomainEvent {
  return {
    id: EventId.make(`event:turn-item-persist-throttle:${input.key}:provider-turn-terminal`),
    type: "provider-turn.updated",
    threadId: input.item.threadId,
    ...(input.item.runId === null ? {} : { runId: input.item.runId }),
    ...(input.item.nodeId === null ? {} : { nodeId: input.item.nodeId }),
    providerInstanceId,
    occurredAt: input.now,
    payload: {
      id: input.item.providerTurnId ?? ProviderTurnId.make(`provider-turn:${input.key}`),
      providerThreadId:
        input.item.providerThreadId ?? ProviderThreadId.make(`provider-thread:${input.key}`),
      nodeId: input.item.nodeId ?? NodeId.make(`node:${input.key}`),
      runAttemptId: null,
      nativeTurnRef: null,
      ordinal: 1,
      status: input.status,
      startedAt: input.item.startedAt,
      completedAt: input.now,
    },
  };
}

const persistedTurnItems = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const eventStore = yield* EventStoreV2;
    const stored = yield* eventStore.read({ threadId }).pipe(Stream.runCollect);
    return Array.from(stored).flatMap((event) =>
      event.event.type === "turn-item.updated" ? [event.event.payload] : [],
    );
  });

const writeWithInjectedFailures = (input: {
  readonly key: string;
  readonly failures: number;
  readonly reason: "lock" | "other";
}) =>
  Effect.gen(function* () {
    const eventSink = yield* EventSinkV2;
    const events = threadEvents(input.key, yield* DateTime.now);
    injectedApplyFailures.remaining = input.failures;
    injectedApplyFailures.attempts = 0;
    injectedApplyFailures.reason = input.reason;
    const fiber = yield* eventSink.write({ events }).pipe(Effect.exit, Effect.forkChild);
    // Retry backoff sleeps on the test clock.
    yield* TestClock.adjust("1 minute");
    const exit = yield* Fiber.join(fiber);
    const persisted = yield* (yield* EventStoreV2)
      .read({ threadId: events[0]!.threadId })
      .pipe(Stream.runCollect);
    return {
      exit,
      threadId: events[0]!.threadId,
      persisted: Array.from(persisted),
      attempts: injectedApplyFailures.attempts,
    };
  });

it.layer(TestLayer)("EventSink SQLite lock contention", (it) => {
  it.effect("retries a write that hit a transient database lock and commits it once", () =>
    Effect.gen(function* () {
      const result = yield* writeWithInjectedFailures({
        key: "transient",
        failures: 2,
        reason: "lock",
      });

      assert.equal(result.exit._tag, "Success");
      assert.deepEqual(
        result.persisted.map((stored) => stored.event.type),
        ["thread.created", "thread.metadata-updated"],
      );
      const projection = yield* (yield* ProjectionStoreV2).getThreadProjection(result.threadId);
      assert.equal(projection.thread.title, "Renamed");
    }),
  );

  it.effect("gives up after a bounded number of attempts when the lock persists", () =>
    Effect.gen(function* () {
      const result = yield* writeWithInjectedFailures({
        key: "persistent",
        failures: Number.POSITIVE_INFINITY,
        reason: "lock",
      });

      assert.equal(result.exit._tag, "Failure");
      assert.equal(result.attempts, 7);
      assert.lengthOf(result.persisted, 0);
    }),
  );

  it.effect("does not retry failures other than lock contention", () =>
    Effect.gen(function* () {
      const result = yield* writeWithInjectedFailures({
        key: "other",
        failures: 1,
        reason: "other",
      });

      assert.equal(result.exit._tag, "Failure");
      assert.equal(result.attempts, 1);
      assert.lengthOf(result.persisted, 0);
    }),
  );
});

it.layer(TestLayer)("EventSink streaming turn-item persistence throttle", (it) => {
  it.effect(
    "persists rapid streaming reasoning updates only at the throttle cadence plus final",
    () =>
      withTurnItemPersistThrottleEnv(
        "2000",
        Effect.gen(function* () {
          const sink = yield* EventSinkV2;
          const startedAt = yield* DateTime.now;
          const thread = makeThrottleThread("cadence", startedAt);
          const first = makeTextTurnItem({
            key: "cadence",
            threadId: thread.id,
            now: startedAt,
            text: "a",
          });

          yield* sink.write({
            events: [
              threadCreatedEvent({ key: "cadence", thread, now: startedAt }),
              turnItemEvent({ id: "cadence:1", item: first, now: startedAt }),
            ],
          });

          for (const [index, text] of ["ab", "abc", "abcd", "abcde"].entries()) {
            const now = yield* DateTime.now;
            const stored = yield* sink.write({
              events: [
                turnItemEvent({
                  id: `cadence:rapid:${index}`,
                  item: makeTextTurnItem({
                    key: "cadence",
                    threadId: thread.id,
                    now,
                    text,
                  }),
                  now,
                }),
              ],
            });
            assert.lengthOf(stored, 0);
          }

          let items = yield* persistedTurnItems(thread.id);
          assert.deepEqual(
            items.map((item) => (item.type === "reasoning" ? item.text : null)),
            ["a"],
          );

          yield* TestClock.adjust("2 seconds");
          items = yield* persistedTurnItems(thread.id);
          assert.deepEqual(
            items.map((item) => (item.type === "reasoning" ? item.text : null)),
            ["a", "abcde"],
          );

          const completedAt = yield* DateTime.now;
          yield* sink.write({
            events: [
              turnItemEvent({
                id: "cadence:final",
                item: makeTextTurnItem({
                  key: "cadence",
                  threadId: thread.id,
                  now: completedAt,
                  text: "abcdef",
                  status: "completed",
                  streaming: false,
                }),
                now: completedAt,
              }),
            ],
          });

          items = yield* persistedTurnItems(thread.id);
          assert.deepEqual(
            items.map((item) => (item.type === "reasoning" ? item.text : null)),
            ["a", "abcde", "abcdef"],
          );
        }),
      ),
  );

  it.effect(
    "persists the completed full text even when completion lands inside the throttle window",
    () =>
      withTurnItemPersistThrottleEnv(
        "2000",
        Effect.gen(function* () {
          const sink = yield* EventSinkV2;
          const startedAt = yield* DateTime.now;
          const thread = makeThrottleThread("final", startedAt);
          const first = makeTextTurnItem({
            key: "final",
            threadId: thread.id,
            now: startedAt,
            text: "partial",
            type: "assistant_message",
          });
          yield* sink.write({
            events: [
              threadCreatedEvent({ key: "final", thread, now: startedAt }),
              turnItemEvent({ id: "final:1", item: first, now: startedAt }),
            ],
          });

          const pendingAt = yield* DateTime.now;
          const pendingStored = yield* sink.write({
            events: [
              turnItemEvent({
                id: "final:pending",
                item: makeTextTurnItem({
                  key: "final",
                  threadId: thread.id,
                  now: pendingAt,
                  text: "partial plus pending",
                  type: "assistant_message",
                }),
                now: pendingAt,
              }),
            ],
          });
          assert.lengthOf(pendingStored, 0);

          const completedAt = yield* DateTime.now;
          yield* sink.write({
            events: [
              turnItemEvent({
                id: "final:completed",
                item: makeTextTurnItem({
                  key: "final",
                  threadId: thread.id,
                  now: completedAt,
                  text: "complete final answer",
                  type: "assistant_message",
                  status: "completed",
                  streaming: false,
                }),
                now: completedAt,
              }),
            ],
          });

          let items = yield* persistedTurnItems(thread.id);
          assert.deepEqual(
            items.map((item) => (item.type === "assistant_message" ? item.text : null)),
            ["partial", "complete final answer"],
          );
          assert.equal(items.at(-1)?.status, "completed");

          yield* TestClock.adjust("2 seconds");
          items = yield* persistedTurnItems(thread.id);
          assert.deepEqual(
            items.map((item) => (item.type === "assistant_message" ? item.text : null)),
            ["partial", "complete final answer"],
          );
        }),
      ),
  );

  it.effect("flushes a pending streaming text update before a terminal provider turn", () =>
    withTurnItemPersistThrottleEnv(
      "2000",
      Effect.gen(function* () {
        const sink = yield* EventSinkV2;
        const startedAt = yield* DateTime.now;
        const thread = makeThrottleThread("terminal-flush", startedAt);
        const first = makeTextTurnItem({
          key: "terminal-flush",
          threadId: thread.id,
          now: startedAt,
          text: "partial",
        });
        yield* sink.write({
          events: [
            threadCreatedEvent({ key: "terminal-flush", thread, now: startedAt }),
            turnItemEvent({ id: "terminal-flush:1", item: first, now: startedAt }),
          ],
        });

        const pendingAt = yield* DateTime.now;
        const pendingStored = yield* sink.write({
          events: [
            turnItemEvent({
              id: "terminal-flush:pending",
              item: makeTextTurnItem({
                key: "terminal-flush",
                threadId: thread.id,
                now: pendingAt,
                text: "latest pending before interrupt",
              }),
              now: pendingAt,
            }),
          ],
        });
        assert.lengthOf(pendingStored, 0);

        const terminalAt = yield* DateTime.now;
        yield* sink.write({
          events: [
            providerTurnTerminalEvent({
              key: "terminal-flush",
              item: first,
              now: terminalAt,
              status: "interrupted",
            }),
          ],
        });

        const items = yield* persistedTurnItems(thread.id);
        assert.deepEqual(
          items.map((item) => (item.type === "reasoning" ? item.text : null)),
          ["partial", "latest pending before interrupt"],
        );
      }),
    ),
  );

  it.effect("does not throttle running tool item updates", () =>
    withTurnItemPersistThrottleEnv(
      "2000",
      Effect.gen(function* () {
        const sink = yield* EventSinkV2;
        const startedAt = yield* DateTime.now;
        const thread = makeThrottleThread("tool", startedAt);
        yield* sink.write({
          events: [threadCreatedEvent({ key: "tool", thread, now: startedAt })],
        });

        for (const [index, output] of ["one", "two", "three"].entries()) {
          const now = yield* DateTime.now;
          const stored = yield* sink.write({
            events: [
              turnItemEvent({
                id: `tool:${index}`,
                item: makeCommandTurnItem({
                  key: "tool",
                  threadId: thread.id,
                  now,
                  output,
                }),
                now,
              }),
            ],
          });
          assert.lengthOf(stored, 1);
        }

        const items = yield* persistedTurnItems(thread.id);
        assert.deepEqual(
          items.map((item) => (item.type === "command_execution" ? item.output : null)),
          ["one", "two", "three"],
        );
      }),
    ),
  );

  it.effect("disables streaming text persistence throttling when the env override is zero", () =>
    withTurnItemPersistThrottleEnv(
      "0",
      Effect.gen(function* () {
        const sink = yield* EventSinkV2;
        const startedAt = yield* DateTime.now;
        const thread = makeThrottleThread("disabled", startedAt);
        yield* sink.write({
          events: [threadCreatedEvent({ key: "disabled", thread, now: startedAt })],
        });

        for (const [index, text] of ["a", "ab", "abc"].entries()) {
          const now = yield* DateTime.now;
          const stored = yield* sink.write({
            events: [
              turnItemEvent({
                id: `disabled:${index}`,
                item: makeTextTurnItem({
                  key: "disabled",
                  threadId: thread.id,
                  now,
                  text,
                }),
                now,
              }),
            ],
          });
          assert.lengthOf(stored, 1);
        }

        const items = yield* persistedTurnItems(thread.id);
        assert.deepEqual(
          items.map((item) => (item.type === "reasoning" ? item.text : null)),
          ["a", "ab", "abc"],
        );
      }),
    ),
  );
});
