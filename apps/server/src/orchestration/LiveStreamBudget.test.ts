import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { ThreadId } from "@t3tools/contracts";
import { describe, expect } from "vite-plus/test";

import {
  shellApplicationEventKey,
  shellStreamItemKey,
  type ShellApplicationEvent,
} from "../orchestration-v2/ShellStream.ts";
import { toPersistenceSqlError } from "../persistence/Errors.ts";
import {
  bufferLiveStream,
  isLiveStreamBufferError,
  LIVE_STREAM_MAX_ITEMS,
  LiveStreamBufferError,
  makeLiveStreamBudget,
  replayAndBufferLiveEvents,
  replayAndBufferProjectedLiveEvents,
  type RetainedLiveItem,
} from "./LiveStreamBudget.ts";

describe("LiveStreamBudget", () => {
  it.effect("closes the source without releasing a batch still waiting for an ACK", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const budget = yield* makeLiveStreamBudget({ maxItems: 3 });
        const queue = yield* Queue.unbounded<RetainedLiveItem<{ text: string }>>();
        const sourceClosed = yield* Deferred.make<void>();
        const first = yield* budget.retain({ text: "first" });
        const second = yield* budget.retain({ text: "second" });
        const third = yield* budget.retain({ text: "third" });
        yield* Queue.offerAll(queue, [first, second, third]);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const pull = yield* Stream.toPull(
              budget.deliver(
                Stream.fromQueue(queue).pipe(
                  Stream.rechunk(1),
                  Stream.ensuring(Deferred.succeed(sourceClosed, undefined)),
                ),
              ),
            );
            expect(yield* pull).toEqual([{ text: "first" }]);
            // The other two items are in the source's pull state, not its queue.
            expect(yield* Queue.size(queue)).toBe(0);
            expect((yield* budget.usage).retainedItems).toBe(3);
            const overflow = yield* budget.retain({ text: "fourth" }).pipe(Effect.result);
            expect(overflow._tag).toBe("Failure");
            // Do not resume the consumer. Its source scope must close now.
            yield* Deferred.await(sourceClosed);
            yield* budget.closed;
            expect((yield* budget.usage).retainedItems).toBe(1);
          }),
        );
        expect(yield* budget.usage).toEqual({ retainedItems: 0, retainedSerializedBytes: 0 });
      }),
    ),
  );
});

it.effect("stops draining a slow subscriber when its unacknowledged tail fills", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const input = yield* Queue.unbounded<{ text: string }>();
      const sourceClosed = yield* Deferred.make<void>();
      const pull = yield* Stream.toPull(
        bufferLiveStream(
          Stream.fromQueue(input).pipe(Stream.ensuring(Deferred.succeed(sourceClosed, undefined))),
          { maxItems: 2 },
        ),
      );
      yield* Queue.offer(input, { text: "first" });
      expect(yield* pull).toEqual([{ text: "first" }]);
      yield* Queue.offerAll(input, [{ text: "second" }, { text: "third" }]);
      yield* Deferred.await(sourceClosed);
      const next = yield* pull.pipe(Effect.result);
      expect(next._tag).toBe("Failure");
      if (next._tag === "Failure") expect(next.failure._tag).toBe("LiveStreamBufferError");
    }),
  ),
);

type Event = { readonly sequence: number; readonly text: string; readonly threadId?: string };

describe("replayAndBufferLiveEvents", () => {
  for (const phase of ["high-water", "replay"] as const) {
    it.effect(`unsubscribes and cancels a blocked ${phase} read on live overflow`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const pubsub = yield* PubSub.unbounded<Event>();
          const subscribed = yield* Deferred.make<PubSub.Subscription<Event>>();
          const readStarted = yield* Deferred.make<void>();
          const readClosed = yield* Deferred.make<void>();
          const blockedRead = Deferred.succeed(readStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(readClosed, undefined)),
          );
          const pull = yield* Stream.toPull(
            replayAndBufferLiveEvents(
              {
                subscribe: PubSub.subscribe(pubsub).pipe(
                  Effect.tap((subscription) => Deferred.succeed(subscribed, subscription)),
                ),
                latestSequence:
                  phase === "high-water" ? blockedRead.pipe(Effect.as(0)) : Effect.succeed(0),
                replay: () => (phase === "replay" ? Stream.fromEffect(blockedRead) : Stream.empty),
              },
              { maxItems: 2 },
            ),
          );
          const reader = yield* pull.pipe(Effect.result, Effect.forkScoped);
          yield* Deferred.await(readStarted);
          yield* PubSub.publishAll(pubsub, [
            { sequence: 1, text: "one" },
            { sequence: 2, text: "two" },
            { sequence: 3, text: "overflow" },
          ]);
          yield* (yield* Deferred.await(subscribed)).shutdownHook.await;
          yield* Deferred.await(readClosed);
          const result = yield* Fiber.join(reader);
          expect(result._tag).toBe("Failure");
          if (result._tag === "Failure") expect(result.failure._tag).toBe("LiveStreamBufferError");
          expect(yield* PubSub.size(pubsub)).toBe(0);
          // Further publications do not accumulate for the abandoned subscriber.
          yield* PubSub.publish(pubsub, { sequence: 4, text: "after overflow" });
          expect(yield* PubSub.size(pubsub)).toBe(0);
        }),
      ),
    );
  }

  for (const limit of ["items", "bytes"] as const) {
    it.effect(`counts an unacknowledged replay batch toward the live ${limit} limit`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const pubsub = yield* PubSub.unbounded<Event>();
          const subscribed = yield* Deferred.make<PubSub.Subscription<Event>>();
          const replayed = { sequence: 1, text: "replay" };
          const live = { sequence: 2, text: "live" };
          const pull = yield* Stream.toPull(
            replayAndBufferLiveEvents(
              {
                subscribe: PubSub.subscribe(pubsub).pipe(
                  Effect.tap((subscription) => Deferred.succeed(subscribed, subscription)),
                ),
                latestSequence: Effect.succeed(1),
                replay: () => Stream.succeed(replayed),
              },
              limit === "items"
                ? { maxItems: 1 }
                : {
                    maxSerializedBytes: 40,
                  },
            ),
          );
          expect(yield* pull).toEqual([replayed]);
          yield* PubSub.publish(pubsub, live);
          // The client has not acknowledged replay; cleanup must still finish.
          yield* (yield* Deferred.await(subscribed)).shutdownHook.await;
          expect(yield* PubSub.size(pubsub)).toBe(0);
          const result = yield* pull.pipe(Effect.result);
          expect(result._tag).toBe("Failure");
          if (result._tag === "Failure") expect(result.failure._tag).toBe("LiveStreamBufferError");
        }),
      ),
    );
  }

  it.effect("lets a fast reader replay a database page larger than both buffer limits", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const pubsub = yield* PubSub.unbounded<Event>();
        const events = Array.from({ length: 20 }, (_, index) => ({
          sequence: index + 1,
          text: "historical event",
        }));
        const replayed = yield* replayAndBufferLiveEvents(
          {
            subscribe: PubSub.subscribe(pubsub),
            latestSequence: Effect.succeed(20),
            replay: () => Stream.fromIterable(events),
          },
          { maxItems: 2, maxSerializedBytes: 100 },
        ).pipe(Stream.take(20), Stream.runCollect);
        expect(replayed).toEqual(events);
        expect(yield* PubSub.size(pubsub)).toBe(0);
      }),
    ),
  );

  it.effect(
    "filters unrelated events before buffering and deduplicates the replay/live overlap",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const pubsub = yield* PubSub.unbounded<Event>();
          const highWaterStarted = yield* Deferred.make<void>();
          const highWater = yield* Deferred.make<number>();
          const first = { sequence: 1, text: "first", threadId: "selected" };
          const overlap = { sequence: 2, text: "overlap", threadId: "selected" };
          const last = { sequence: 10, text: "last", threadId: "selected" };
          const reader = yield* replayAndBufferLiveEvents(
            {
              subscribe: PubSub.subscribe(pubsub),
              latestSequence: Deferred.succeed(highWaterStarted, undefined).pipe(
                Effect.andThen(Deferred.await(highWater)),
              ),
              replay: () => Stream.make(first, overlap),
              filter: (event) => event.threadId === "selected",
            },
            { maxItems: 4 },
          ).pipe(Stream.take(3), Stream.runCollect, Effect.forkScoped);
          yield* Deferred.await(highWaterStarted);
          yield* PubSub.publishAll(pubsub, [
            overlap,
            ...Array.from({ length: 6 }, (_, index) => ({
              sequence: index + 3,
              text: "unrelated",
              threadId: "other",
            })),
            last,
          ]);
          yield* Deferred.succeed(highWater, 2);
          expect(yield* Fiber.join(reader)).toEqual([first, overlap, last]);
          expect(yield* PubSub.size(pubsub)).toBe(0);
        }),
      ),
  );

  it.effect(
    "caps raw and projected stages while projection and client acknowledgement are blocked",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const pubsub = yield* PubSub.unbounded<Event>();
          const subscribed = yield* Deferred.make<PubSub.Subscription<Event>>();
          const projectionStarted = yield* Deferred.make<void>();
          const projectionClosed = yield* Deferred.make<void>();
          const releaseProjection = yield* Deferred.make<void>();
          const source = replayAndBufferLiveEvents(
            {
              subscribe: PubSub.subscribe(pubsub).pipe(
                Effect.tap((subscription) => Deferred.succeed(subscribed, subscription)),
              ),
              latestSequence: Effect.succeed(0),
              replay: () => Stream.empty,
            },
            { maxItems: 2 },
          ).pipe(
            Stream.mapEffect((event) =>
              event.sequence === 3
                ? Deferred.succeed(projectionStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseProjection)),
                    Effect.as(event),
                  )
                : Effect.succeed(event),
            ),
            Stream.ensuring(Deferred.succeed(projectionClosed, undefined)),
          );
          const pull = yield* Stream.toPull(bufferLiveStream(source, { maxItems: 2 }));
          const firstPull = yield* pull.pipe(Effect.forkScoped);
          const subscription = yield* Deferred.await(subscribed);
          yield* PubSub.publish(pubsub, { sequence: 1, text: "awaiting ACK" });
          expect(yield* Fiber.join(firstPull)).toEqual([{ sequence: 1, text: "awaiting ACK" }]);
          yield* PubSub.publish(pubsub, { sequence: 2, text: "projected tail" });
          yield* PubSub.publish(pubsub, { sequence: 3, text: "projecting" });
          yield* Deferred.await(projectionStarted);
          yield* PubSub.publishAll(pubsub, [
            { sequence: 4, text: "raw tail" },
            { sequence: 5, text: "overflow" },
          ]);
          // Two projected slots and two raw slots cannot retain a fifth event.
          yield* subscription.shutdownHook.await;
          expect(yield* PubSub.size(pubsub)).toBe(0);
          yield* Deferred.succeed(releaseProjection, undefined);
          yield* Deferred.await(projectionClosed);
          const result = yield* pull.pipe(Effect.result);
          expect(result._tag).toBe("Failure");
          if (result._tag === "Failure") expect(result.failure._tag).toBe("LiveStreamBufferError");
        }),
      ),
  );
});

describe("coalesced live streams", () => {
  const drain = (subscription: PubSub.Subscription<unknown>) =>
    Effect.gen(function* () {
      while ((yield* PubSub.remaining(subscription)) > 0) yield* Effect.yieldNow;
    });

  it.effect("delivers the newest value per key in sequence order after a stalled read", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const pubsub = yield* PubSub.unbounded<Event>();
        const subscribed = yield* Deferred.make<PubSub.Subscription<Event>>();
        const pull = yield* Stream.toPull(
          replayAndBufferProjectedLiveEvents(
            {
              subscribe: PubSub.subscribe(pubsub).pipe(
                Effect.tap((subscription) => Deferred.succeed(subscribed, subscription)),
              ),
              latestSequence: Effect.succeed(0),
              replay: () => Stream.empty,
              project: (event: Event) => event,
              coalesceKey: (event) => event.threadId ?? "",
            },
            { maxItems: 3 },
          ),
        );
        const first = yield* pull.pipe(Effect.forkScoped);
        const subscription = yield* Deferred.await(subscribed);
        yield* PubSub.publish(pubsub, { sequence: 1, text: "a1", threadId: "a" });
        expect(yield* Fiber.join(first)).toEqual([{ sequence: 1, text: "a1", threadId: "a" }]);
        // The first batch waits for its ACK while two keys receive five updates.
        yield* PubSub.publishAll(pubsub, [
          { sequence: 2, text: "b1", threadId: "b" },
          { sequence: 3, text: "a2", threadId: "a" },
          { sequence: 4, text: "b2", threadId: "b" },
          { sequence: 5, text: "a3", threadId: "a" },
          { sequence: 6, text: "b3", threadId: "b" },
        ]);
        yield* drain(subscription);
        expect(yield* pull).toEqual([
          { sequence: 5, text: "a3", threadId: "a" },
          { sequence: 6, text: "b3", threadId: "b" },
        ]);
      }),
    ),
  );

  it.effect(
    "keeps a shell burst for a few threads within both stage budgets while projection and ACK stall",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const threadIds = ["thread-a", "thread-b", "thread-c"].map((id) => ThreadId.make(id));
          const burst = (fromSequence: number) =>
            Array.from(
              { length: LIVE_STREAM_MAX_ITEMS + 500 },
              (_, index): ShellApplicationEvent => ({
                sequence: fromSequence + index,
                event: { threadId: threadIds[index % threadIds.length]! },
              }),
            );
          const pubsub = yield* PubSub.unbounded<ShellApplicationEvent>();
          const subscribed = yield* Deferred.make<PubSub.Subscription<ShellApplicationEvent>>();
          const projectionStarted = yield* Deferred.make<void>();
          const releaseProjection = yield* Deferred.make<void>();
          const raw = replayAndBufferProjectedLiveEvents({
            subscribe: PubSub.subscribe(pubsub).pipe(
              Effect.tap((subscription) => Deferred.succeed(subscribed, subscription)),
            ),
            latestSequence: Effect.succeed(0),
            replay: () => Stream.empty,
            project: (event: ShellApplicationEvent) => event,
            coalesceKey: shellApplicationEventKey,
          });
          let projections = 0;
          // Stands in for the shell read. The first read blocks like a slow
          // getThreadShell while the raw tail keeps filling.
          const projected = raw.pipe(
            Stream.mapEffect((event) =>
              (projections++ === 0
                ? Deferred.succeed(projectionStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseProjection)),
                  )
                : Effect.void
              ).pipe(
                Effect.as({
                  kind: "thread.removed" as const,
                  sequence: event.sequence,
                  location: "active" as const,
                  threadId: "event" in event ? event.event.threadId : threadIds[0]!,
                }),
              ),
            ),
          );
          const pull = yield* Stream.toPull(
            bufferLiveStream(projected, undefined, shellStreamItemKey),
          );
          const first = yield* pull.pipe(Effect.forkScoped);
          const subscription = yield* Deferred.await(subscribed);

          const firstBurst = burst(1);
          yield* PubSub.publishAll(pubsub, firstBurst);
          yield* Deferred.await(projectionStarted);
          yield* drain(subscription);
          yield* Deferred.succeed(releaseProjection, undefined);
          const delivered = Array.from(yield* Fiber.join(first));

          // The client holds its ACK while a second burst arrives.
          const secondBurst = burst(firstBurst.length + 1);
          yield* PubSub.publishAll(pubsub, secondBurst);
          yield* drain(subscription);

          const finalSequence = secondBurst.at(-1)!.sequence;
          while (delivered.at(-1)!.sequence < finalSequence) {
            delivered.push(...(yield* pull));
          }
          const sequences = delivered.map((item) => item.sequence);
          expect(sequences).toEqual([...sequences].sort((left, right) => left - right));
          expect(delivered.length).toBeLessThan(20);
          const lastByThread = new Map(
            delivered.map((item) => [item.kind === "thread.removed" ? item.threadId : "", item]),
          );
          for (const threadId of threadIds) {
            expect(lastByThread.get(threadId)?.sequence).toBe(
              secondBurst.findLast((event) => "event" in event && event.event.threadId === threadId)
                ?.sequence,
            );
          }
        }),
      ),
  );
});

describe("isLiveStreamBufferError", () => {
  it("recognizes an overflow wrapped by the event store", () => {
    const overflow = new LiveStreamBufferError({ message: "full" });
    expect(isLiveStreamBufferError(overflow)).toBe(true);
    expect(isLiveStreamBufferError(toPersistenceSqlError("stream:buffer")(overflow))).toBe(true);
    expect(isLiveStreamBufferError(toPersistenceSqlError("stream:query")(new Error("io")))).toBe(
      false,
    );
  });
});
