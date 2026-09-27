import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { ProjectionMaintenanceV2 } from "./ProjectionMaintenance.ts";

const ProjectionCompactionEnvConfig = Config.all({
  enabled: Config.boolean("T3CODE_EVENT_COMPACTION_ENABLED").pipe(Config.withDefault(true)),
  initialDelayMinutes: Config.number("T3CODE_EVENT_COMPACTION_INITIAL_DELAY_MINUTES").pipe(
    Config.withDefault(20),
  ),
  intervalMinutes: Config.number("T3CODE_EVENT_COMPACTION_INTERVAL_MINUTES").pipe(
    Config.withDefault(360),
  ),
});

type ProjectionCompactionSettings = Config.Success<typeof ProjectionCompactionEnvConfig>;

/**
 * Runs compactEventStore in the background: once after the initial delay,
 * then once per interval. Superseded events otherwise pile up forever and the
 * event log grows with every streamed update. Failures are logged and the
 * next run still happens. Deleted rows free pages SQLite reuses, so the file
 * stops growing; it only shrinks with an offline VACUUM.
 */
export const makeWorkerLayer = (settings: ProjectionCompactionSettings) =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      if (!settings.enabled) {
        yield* Effect.logInfo("orchestration-v2.event-compaction.disabled");
        return;
      }
      const maintenance = yield* ProjectionMaintenanceV2;
      const intervalMinutes = Math.max(1, settings.intervalMinutes);
      const runOnce = maintenance.compactEventStore.pipe(
        Effect.tap((summary) =>
          Effect.logInfo("orchestration-v2.event-compaction.completed", summary),
        ),
        Effect.catchCause((cause) =>
          Effect.logWarning("orchestration-v2.event-compaction.failed", { cause }),
        ),
      );
      yield* Effect.sleep(`${settings.initialDelayMinutes} minutes`).pipe(
        Effect.andThen(
          Effect.forever(runOnce.pipe(Effect.andThen(Effect.sleep(`${intervalMinutes} minutes`)))),
        ),
        Effect.forkScoped,
      );
    }),
  );

/** Production layer; reads T3CODE_EVENT_COMPACTION_* from the environment. */
export const workerLive = Layer.unwrap(
  Effect.map(ProjectionCompactionEnvConfig, (settings) => makeWorkerLayer(settings)),
);
