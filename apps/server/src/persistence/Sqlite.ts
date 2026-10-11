import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { runMigrations } from "./Migrations.ts";
import { initializeV2Database } from "./initializeV2Database.ts";
import * as ServerConfig from "../config.ts";
import { acquireDatabaseAccess } from "../serverStateOwnership.ts";
import { SqlError, ConnectionError } from "effect/sql/SqlError";

// Size the -wal file is cut back to on the first commit after a WAL reset.
export const WAL_SIZE_LIMIT_BYTES = 32 * 1024 * 1024;
export const MigrationsEnabled = Context.Reference<boolean>("t3/sqlite/migrations-enabled", {
  defaultValue: () => true,
});

const layerSetup = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    // CLI and server write from separate processes; wait rather than fail with SQLITE_BUSY.
    yield* sql`PRAGMA busy_timeout = 5000;`;
    yield* sql`PRAGMA foreign_keys = ON;`;
    const migrationsEnabled = yield* MigrationsEnabled;
    if (migrationsEnabled) yield* sql`PRAGMA journal_mode = WAL;`;
    // PASSIVE checkpoints never shrink the -wal file, so it otherwise keeps its
    // largest size until the last connection closes.
    yield* sql.unsafe(`PRAGMA journal_size_limit = ${WAL_SIZE_LIMIT_BYTES};`);
    if (migrationsEnabled) yield* runMigrations();
  }),
);

export const layerFromPath = Effect.fn("makeSqlitePersistenceLive")(function* (dbPath: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fs.makeDirectory(path.dirname(dbPath), { recursive: true });
  const layerAccess = Layer.effectDiscard(
    Effect.acquireRelease(
      Effect.tryPromise({
        try: () => acquireDatabaseAccess(path.dirname(dbPath)),
        catch: (cause) =>
          new SqlError({
            reason: new ConnectionError({
              cause,
              operation: "acquire-state-access",
              message: "Application SQLite is unavailable during state maintenance.",
            }),
          }),
      }),
      (release) => Effect.sync(release),
    ),
  );

  return Layer.provideMerge(
    layerSetup,
    NodeSqliteClient.layer({
      filename: dbPath,
      spanAttributes: {
        "db.name": path.basename(dbPath),
        "service.name": "t3code-server",
      },
    }),
  ).pipe(Layer.provide(layerAccess));
}, Layer.unwrap);

export const layerMemory = Layer.provideMerge(
  layerSetup,
  NodeSqliteClient.layer({ filename: ":memory:" }),
);

export const layerConfig = Layer.unwrap(
  Effect.gen(function* () {
    const { dbPath } = yield* ServerConfig.ServerConfig;
    if (yield* MigrationsEnabled) {
      const path = yield* Path.Path;
      yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: () => acquireDatabaseAccess(path.dirname(dbPath)),
          catch: (cause) =>
            new SqlError({
              reason: new ConnectionError({
                cause,
                operation: "initialize-state",
                message: "Application SQLite is unavailable during state maintenance.",
              }),
            }),
        }),
        (release) => Effect.sync(release),
      );
      yield* initializeV2Database(dbPath);
    }
    return layerFromPath(dbPath);
  }),
);
