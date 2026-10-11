import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { ProjectionMaintenanceV2 } from "./ProjectionMaintenance.ts";

const settings = Config.all({
  enabled: Config.Boolean("T3CODE_EVENT_COMPACTION_ENABLED").pipe(Config.withDefault(true)),
  initialDelayMinutes: Config.schema(
    Schema.Number.check(Schema.isFinite(), Schema.isGreaterThanOrEqualTo(0)),
    "T3CODE_EVENT_COMPACTION_INITIAL_DELAY_MINUTES",
  ).pipe(Config.withDefault(20)),
  intervalMinutes: Config.schema(
    Schema.Number.check(Schema.isFinite(), Schema.isGreaterThanOrEqualTo(1)),
    "T3CODE_EVENT_COMPACTION_INTERVAL_MINUTES",
  ).pipe(Config.withDefault(360)),
});

export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* settings;
    return Layer.effectDiscard(
      Effect.gen(function* () {
        if (!config.enabled) return;
        const maintenance = yield* ProjectionMaintenanceV2;
        const run = maintenance.compactEventStore.pipe(
          Effect.tap((summary) =>
            Effect.logInfo("orchestration-v2.event-compaction.completed", summary),
          ),
          Effect.catchCause((cause) =>
            Effect.logWarning("orchestration-v2.event-compaction.failed", { cause }),
          ),
        );
        yield* Effect.sleep(`${config.initialDelayMinutes} minutes`).pipe(
          Effect.andThen(
            Effect.forever(
              run.pipe(Effect.andThen(Effect.sleep(`${config.intervalMinutes} minutes`))),
            ),
          ),
          Effect.forkScoped,
        );
      }),
    );
  }),
);
