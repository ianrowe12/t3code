// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it, vi } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory, makeSqlitePersistenceLive } from "./Sqlite.ts";
import { acquireServerStateLock } from "../../serverStateLock.ts";
import {
  acquireDatabaseAccess,
  SERVER_DATABASE_ACCESS_LOCK_FILE,
} from "../../serverStateOwnership.ts";

const lockHolderSource = `
const { DatabaseSync } = require("node:sqlite");
const db = new DatabaseSync(process.argv[1]);
db.exec("BEGIN IMMEDIATE");
process.stdout.write("locked\\n");
setTimeout(() => {
  db.exec("COMMIT");
  db.close();
}, Number(process.argv[2]));
`;

const spawnWriteLockHolder = (dbPath: string, holdMs: number) =>
  Effect.promise(
    () =>
      new Promise<void>((resolve, reject) => {
        const holder = NodeChildProcess.spawn(
          process.execPath,
          ["-e", lockHolderSource, dbPath, String(holdMs)],
          { stdio: ["ignore", "pipe", "ignore"] },
        );
        holder.stdout.once("data", () => resolve());
        holder.on("error", reject);
        holder.on("exit", () =>
          reject(new Error("lock holder exited before acquiring the write lock")),
        );
      }),
  );

it.effect("waits out a concurrent writer instead of failing with SQLITE_BUSY", () => {
  const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-sqlite-busy-"));
  const dbPath = NodePath.join(tempDir, "state.sqlite");

  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE busy_probe(id INTEGER PRIMARY KEY)`;
    yield* spawnWriteLockHolder(dbPath, 300);
    yield* sql`INSERT INTO busy_probe(id) VALUES (${1})`;
    const rows = yield* sql<{ readonly id: number }>`SELECT id FROM busy_probe`;
    assert.deepEqual([...rows], [{ id: 1 }]);
  }).pipe(
    Effect.provide(makeSqlitePersistenceLive(dbPath).pipe(Layer.provide(NodeServices.layer))),
    Effect.ensuring(Effect.sync(() => NodeFS.rmSync(tempDir, { recursive: true, force: true }))),
  );
});

it.effect("applies busy_timeout in the shared persistence setup", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{ readonly timeout: number }>`PRAGMA busy_timeout`;
    assert.equal(rows[0]?.timeout, 5000);
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);

for (const failBuild of [false, true]) {
  it.effect(
    `protects SQLite close and WAL checkpoint after ${failBuild ? "failed" : "successful"} layer construction`,
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-close-lease-" });
        const dbPath = NodePath.join(stateDir, "state.sqlite");
        yield* acquireServerStateLock({
          stateDir,
          serverRuntimeStatePath: NodePath.join(stateDir, "server-runtime.json"),
        });
        const close = NodeSqlite.DatabaseSync.prototype.close;
        const protectedCloses: boolean[] = [];
        const checkpointableCloses: boolean[] = [];
        yield* Effect.acquireRelease(
          Effect.sync(() =>
            vi
              .spyOn(NodeSqlite.DatabaseSync.prototype, "close")
              .mockImplementation(function (this: NodeSqlite.DatabaseSync) {
                const application = this.prepare("PRAGMA database_list")
                  .all()
                  .some((database) => database.file === dbPath);
                if (application) {
                  const probe = new NodeSqlite.DatabaseSync(
                    NodePath.join(stateDir, SERVER_DATABASE_ACCESS_LOCK_FILE),
                  );
                  let blocked = false;
                  try {
                    probe.exec("BEGIN EXCLUSIVE");
                  } catch (cause) {
                    blocked = cause instanceof Error && "errcode" in cause && cause.errcode === 5;
                  } finally {
                    close.call(probe);
                  }
                  protectedCloses.push(blocked);
                  checkpointableCloses.push(NodeFS.existsSync(`${dbPath}-wal`));
                }
                close.call(this);
              }),
          ),
          (spy) => Effect.sync(() => spy.mockRestore()),
        );
        const persistence = makeSqlitePersistenceLive(dbPath);
        const layer = failBuild
          ? persistence.pipe(Layer.tap(() => Effect.fail("consumer initialization failed")))
          : persistence;
        const result = yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TABLE close_probe (id INTEGER PRIMARY KEY)`;
          yield* sql`INSERT INTO close_probe VALUES (1)`;
          assert.isTrue(yield* fs.exists(`${dbPath}-wal`));
        }).pipe(Effect.provide(layer), Effect.result);
        assert.equal(result._tag, failBuild ? "Failure" : "Success");
        assert.isAbove(protectedCloses.length, 0);
        assert.isTrue(protectedCloses.every(Boolean));
        assert.isTrue(checkpointableCloses.every(Boolean));
        assert.isFalse(yield* fs.exists(`${dbPath}-wal`));
        yield* Effect.acquireRelease(
          Effect.promise(() => acquireDatabaseAccess(stateDir, { exclusive: true })),
          (release) => Effect.sync(release),
        );
      }).pipe(Effect.provide(NodeServices.layer)),
  );
}
