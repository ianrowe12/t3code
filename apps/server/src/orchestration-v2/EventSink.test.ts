import { assert, it } from "@effect/vitest";
import {
  EventId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
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
