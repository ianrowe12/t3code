import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as NetService from "@t3tools/shared/Net";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as TestConsole from "effect/testing/TestConsole";
import { Command } from "effect/unstable/cli";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { makeCli } from "../binCli.ts";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../config.ts";
import { acquireServerStateLock, ServerStateLockError } from "../serverStateLock.ts";
import {
  makeSqlitePersistenceLive,
  SqliteMigrationsEnabled,
} from "../persistence/Layers/Sqlite.ts";
import {
  makePersistedServerRuntimeState,
  persistServerRuntimeState,
} from "../serverRuntimeState.ts";

const CliRuntime = Layer.mergeAll(NodeServices.layer, NetService.layer, TestConsole.layer);
const cli = makeCli({ cloudEnabled: true });
const runCli = (args: ReadonlyArray<string>) =>
  Command.runWith(cli, { version: "0.0.0" })(args).pipe(Effect.provide(CliRuntime));
const isStateLockError = Schema.is(ServerStateLockError);

for (const metadata of [
  ["legacy", `{"version":1,"pid":${process.pid}}`],
  ["unsupported", `{"version":1,"pid":${process.pid},"stateLockVersion":99}`],
  ["malformed", "{"],
]) {
  it.effect(`refuses ${metadata[0]} discovery instead of initializing auth unsafely`, () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fs = yield* FileSystem.FileSystem;
      yield* fs.makeDirectory(config.stateDir, { recursive: true });
      yield* fs.writeFileString(config.serverRuntimeStatePath, metadata[1]!);
      const result = yield* runCli([
        "auth",
        "session",
        "list",
        "--json",
        "--base-dir",
        config.baseDir,
      ]).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.isTrue(isStateLockError(result.failure));
      assert.isFalse(yield* fs.exists(config.dbPath));
      yield* fs.remove(config.serverRuntimeStatePath);
      yield* acquireServerStateLock(config);
    }).pipe(
      Effect.provide(
        ServerConfig.layerTest(".", { prefix: "t3-auth-cli-untrusted-" }).pipe(
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
    ),
  );
}

for (const command of [
  ["auth", "pairing", "create", "--json"],
  ["auth", "session", "list", "--json"],
  ["connect", "status", "--json"],
]) {
  it.effect(`${command.join(" ")} cannot migrate before a state owner publishes discovery`, () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fs = yield* FileSystem.FileSystem;
      yield* acquireServerStateLock(config);
      const result = yield* runCli([...command, "--base-dir", config.baseDir]).pipe(
        Effect.provideService(HostProcessEnvironment, { PATH: "", HOME: config.baseDir }),
        Effect.result,
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure")
        assert.isTrue(isStateLockError(result.failure), String(result.failure));
      assert.isFalse(yield* fs.exists(config.dbPath));
    }).pipe(
      Effect.provide(
        ServerConfig.layerTest(".", { prefix: "t3-auth-cli-admission-" }).pipe(
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
    ),
  );
}

for (const command of [
  ["auth", "pairing", "create", "--json"],
  ["auth", "session", "list", "--json"],
  ["connect", "status", "--json"],
]) {
  it.effect(`${command.join(" ")} shares a live owner's persistence without migrations`, () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      yield* acquireServerStateLock(config);
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`DROP TABLE effect_sql_migrations`;
      }).pipe(Effect.provide(makeSqlitePersistenceLive(config.dbPath)));
      yield* persistServerRuntimeState({
        path: config.serverRuntimeStatePath,
        state: yield* makePersistedServerRuntimeState({ config, port: 3773, stateLockVersion: 1 }),
      });

      yield* runCli([...command, "--base-dir", config.baseDir]).pipe(
        Effect.provideService(HostProcessEnvironment, { PATH: "", HOME: config.baseDir }),
      );

      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const rows = yield* sql<{
          count: number;
        }>`SELECT count(*) AS count FROM sqlite_schema WHERE name = 'effect_sql_migrations'`;
        assert.equal(rows[0]?.count, 0);
      }).pipe(
        Effect.provide(makeSqlitePersistenceLive(config.dbPath)),
        Effect.provideService(SqliteMigrationsEnabled, false),
      );
    }).pipe(
      Effect.provide(
        ServerConfig.layerTest(".", { prefix: "t3-auth-cli-live-" }).pipe(
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
    ),
  );
}

it.effect("fresh-home pairing initializes under ownership and releases it after completion", () =>
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    assert.isFalse(yield* fs.exists(config.dbPath));
    yield* runCli(["auth", "pairing", "create", "--base-dir", config.baseDir, "--json"]);
    assert.isTrue(yield* fs.exists(config.dbPath));
    yield* acquireServerStateLock(config);
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql<{
        count: number;
      }>`SELECT count(*) AS count FROM effect_sql_migrations`;
      assert.equal(rows[0]?.count, 51);
    }).pipe(Effect.provide(makeSqlitePersistenceLive(config.dbPath)));
  }).pipe(
    Effect.provide(
      ServerConfig.layerTest(".", { prefix: "t3-auth-cli-fresh-" }).pipe(
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
  ),
);

it.effect("finishes owned auth initialization before a cloud command starts its server", () =>
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    yield* Effect.gen(function* () {
      const auth = yield* EnvironmentAuth.EnvironmentAuth;
      yield* acquireServerStateLock(config);
      yield* SqlClient.SqlClient.pipe(
        Effect.asVoid,
        Effect.provide(makeSqlitePersistenceLive(config.dbPath)),
      );
      const session = yield* auth.issueSession({ label: "server-start regression" });
      yield* auth.revokeSession(session.sessionId);
    }).pipe(Effect.provide(EnvironmentAuth.runtimeLayer));
  }).pipe(
    Effect.provide(
      ServerConfig.layerTest(".", { prefix: "t3-auth-before-server-" }).pipe(
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
  ),
);
