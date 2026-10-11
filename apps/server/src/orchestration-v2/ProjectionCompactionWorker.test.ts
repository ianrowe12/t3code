import { assert, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";

import { ProjectionMaintenanceError, ProjectionMaintenanceV2 } from "./ProjectionMaintenance.ts";
import * as ProjectionCompactionWorker from "./ProjectionCompactionWorker.ts";

const summary = { deletedEventCount: 1, deletedReceiptCount: 0, reclaimableBytes: 0 };

it.effect("waits for the initial delay, repeats, and recovers after a failed compaction", () => {
  let runs = 0;
  const layer = ProjectionCompactionWorker.layer.pipe(
    Layer.provide(
      Layer.mock(ProjectionMaintenanceV2)({
        compactEventStore: Effect.suspend(() => {
          runs += 1;
          return runs === 1
            ? Effect.fail(new ProjectionMaintenanceError({ operation: "test" }))
            : Effect.succeed(summary);
        }),
      }),
    ),
    Layer.provide(
      ConfigProvider.layer(
        ConfigProvider.fromUnknown({
          T3CODE_EVENT_COMPACTION_INITIAL_DELAY_MINUTES: 2,
          T3CODE_EVENT_COMPACTION_INTERVAL_MINUTES: 3,
        }),
      ),
    ),
  );
  return Effect.gen(function* () {
    yield* Layer.build(layer);
    yield* TestClock.adjust("119 seconds");
    assert.equal(runs, 0);
    yield* TestClock.adjust("1 second");
    assert.equal(runs, 1);
    yield* TestClock.adjust("179 seconds");
    assert.equal(runs, 1);
    yield* TestClock.adjust("1 second");
    assert.equal(runs, 2);
  }).pipe(Effect.scoped);
});

it.effect("does not run when disabled", () => {
  let runs = 0;
  const layer = ProjectionCompactionWorker.layer.pipe(
    Layer.provide(
      Layer.mock(ProjectionMaintenanceV2)({
        compactEventStore: Effect.sync(() => {
          runs += 1;
          return summary;
        }),
      }),
    ),
    Layer.provide(
      ConfigProvider.layer(
        ConfigProvider.fromUnknown({
          T3CODE_EVENT_COMPACTION_ENABLED: false,
        }),
      ),
    ),
  );
  return Effect.gen(function* () {
    yield* Layer.build(layer);
    yield* TestClock.adjust("24 hours");
    assert.equal(runs, 0);
  }).pipe(Effect.scoped);
});

it.effect("rejects invalid scheduling settings rather than spinning or silently clamping", () =>
  Effect.gen(function* () {
    for (const interval of [0, -1, Number.POSITIVE_INFINITY, Number.NaN]) {
      const layer = ProjectionCompactionWorker.layer.pipe(
        Layer.provide(Layer.mock(ProjectionMaintenanceV2)({})),
        Layer.provide(
          ConfigProvider.layer(
            ConfigProvider.fromUnknown({
              T3CODE_EVENT_COMPACTION_INTERVAL_MINUTES: interval,
            }),
          ),
        ),
      );
      const result = yield* Effect.scoped(Layer.build(layer)).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
    }
  }),
);
