import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { Launcher, readServiceState, writeServiceState } from "./serviceLauncher.ts";
import { acquireServerStateLock } from "./serverStateLock.ts";
import { acquireDatabaseAccess, acquireRuntimeStateOwnership } from "./serverStateOwnership.ts";
import {
  compareExactServiceVersions,
  decodeServiceState,
  isExactServiceVersion,
  SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_STOP_MARKER_FILE,
} from "./cloud/serviceProtocol.ts";

it("accepts only exact semantic versions", () => {
  for (const version of ["0.0.0", "1.2.3", "1.2.3-alpha.1", "1.2.3-0", "1.2.3+001"]) {
    assert.isTrue(isExactServiceVersion(version), version);
  }
  for (const version of ["latest", "01.2.3", "1.2.3-01", "1.2.3-alpha..1", "1.2.3+."]) {
    assert.isFalse(isExactServiceVersion(version), version);
  }
});

it("orders exact semantic versions without treating build metadata as precedence", () => {
  assert.equal(compareExactServiceVersions("1.2.3", "1.2.3"), 0);
  assert.equal(compareExactServiceVersions("1.2.4", "1.2.3"), 1);
  assert.equal(compareExactServiceVersions("2.0.0-alpha.1", "2.0.0-alpha.2"), -1);
  assert.equal(compareExactServiceVersions("2.0.0-alpha.2", "2.0.0-alpha.beta"), -1);
  assert.equal(compareExactServiceVersions("2.0.0-alpha-beta", "2.0.0-alpha-alpha"), 1);
  assert.equal(compareExactServiceVersions("2.0.0", "2.0.0-rc.1"), 1);
  assert.equal(compareExactServiceVersions("2.0.0+one", "2.0.0+two"), 0);
});

it("rejects contradictory service state", () => {
  assert.isUndefined(
    decodeServiceState({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      activeVersion: "0.0.31",
      update: {
        id: "update-1",
        fromVersion: "0.0.30",
        targetVersion: "0.0.32",
        dbPath: "/tmp/state.sqlite",
        status: "pending",
      },
    }),
  );

  assert.isUndefined(
    decodeServiceState({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      activeVersion: "1.0.0",
      update: {
        id: "update-3",
        fromVersion: "1.0.0",
        targetVersion: "1.1.0",
        status: "pending",
      },
    }),
  );

  assert.isUndefined(
    decodeServiceState({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      activeVersion: "1.0.0",
      update: {
        id: "update-2",
        fromVersion: "1.0.0",
        targetVersion: "0.9.0",
        dbPath: "/tmp/state.sqlite",
        status: "pending",
      },
    }),
  );
});

it.layer(NodeServices.layer)("service state persistence", (it) => {
  it.effect("starts a child while a CLI retains database access but no runtime ownership", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-launch-with-auth-" });
      yield* Effect.acquireRelease(
        Effect.promise(() => acquireDatabaseAccess(path.join(root, "userdata"))),
        (release) => Effect.sync(release),
      );
      const versionDir = path.join(root, "runtime", "versions", "1.0.0");
      const entryPath = path.join(versionDir, "node_modules", "t3", "dist", "bin.mjs");
      yield* fs.makeDirectory(path.dirname(entryPath), { recursive: true });
      yield* fs.writeFileString(
        entryPath,
        `
import { writeFileSync } from "node:fs";
writeFileSync(new URL("../../../../../../child-started", import.meta.url), "started");
process.exit(0);
`,
      );
      yield* fs.writeFileString(path.join(versionDir, ".install-complete"), "1.0.0\n");
      const state = { protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: "1.0.0" } as const;
      yield* Effect.promise(() =>
        writeServiceState(path.join(root, "runtime", "service-state.json"), state),
      );
      yield* Effect.tryPromise(() => new Launcher(root, state).run()).pipe(Effect.result);
      assert.isTrue(yield* fs.exists(path.join(root, "child-started")));
    }),
  );

  for (const target of ["outside", "database-symlink", "wal-symlink"] as const) {
    it.effect(`rejects a pending restore through an unowned ${target}`, () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-unowned-rollback-" });
        const otherDatabase = path.join(root, "other-state", "state.sqlite");
        yield* fs.makeDirectory(path.dirname(otherDatabase), { recursive: true });
        yield* fs.writeFileString(otherDatabase, "other owner's work");
        const stateDir = path.join(root, "userdata");
        yield* fs.makeDirectory(stateDir, { recursive: true });
        const databasePath =
          target === "outside" ? otherDatabase : path.join(stateDir, "state.sqlite");
        if (target === "database-symlink") yield* fs.symlink(otherDatabase, databasePath);
        if (target === "wal-symlink") {
          yield* fs.writeFileString(databasePath, "owned database");
          yield* fs.symlink(otherDatabase, `${databasePath}-wal`);
        }
        const backup = path.join(root, "runtime", "db-backup", "pending-1");
        yield* fs.makeDirectory(backup, { recursive: true });
        yield* fs.writeFileString(path.join(backup, "database"), "older backup");
        yield* fs.writeFileString(path.join(backup, "database-wal"), "older wal");
        const state = {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
          update: {
            id: "pending-1",
            fromVersion: "1.0.0",
            targetVersion: "1.1.0",
            dbPath: databasePath,
            status: "pending",
          },
        } as const;
        const statePath = path.join(root, "runtime", "service-state.json");
        yield* Effect.promise(() => writeServiceState(statePath, state));

        const result = yield* Effect.tryPromise(() => new Launcher(root, state).run()).pipe(
          Effect.result,
        );

        assert.equal(result._tag, "Failure");
        assert.equal(yield* fs.readFileString(otherDatabase), "other owner's work");
        assert.deepEqual(yield* Effect.promise(() => readServiceState(statePath)), state);
      }),
    );
  }

  it.effect("rejects an unowned update request without scheduling a handoff", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-unowned-request-" });
      const outside = path.join(root, "other-state", "state.sqlite");
      yield* fs.makeDirectory(path.dirname(outside), { recursive: true });
      yield* fs.writeFileString(outside, "other owner's work");
      const responsePath = path.join(root, "response");
      // @effect-diagnostics-next-line preferSchemaOverJson:off - embeds a path in fake child source.
      const encodedResponse = JSON.stringify(responsePath);
      // @effect-diagnostics-next-line preferSchemaOverJson:off - embeds a path in fake child source.
      const encodedOutside = JSON.stringify(outside);
      const childSource = `
import { writeFileSync } from "node:fs";
process.on("SIGTERM", () => { writeFileSync(${encodedResponse}, "terminated"); process.exit(1); });
process.on("message", (message) => {
  writeFileSync(${encodedResponse}, message.type);
  process.exit(0);
});
process.send({ type: "request-update", targetVersion: "1.1.0", dbPath: ${encodedOutside} });
`;
      for (const version of ["1.0.0", "1.1.0"]) {
        const versionDir = path.join(root, "runtime", "versions", version);
        const entryPath = path.join(versionDir, "node_modules", "t3", "dist", "bin.mjs");
        yield* fs.makeDirectory(path.dirname(entryPath), { recursive: true });
        yield* fs.writeFileString(entryPath, childSource);
        yield* fs.writeFileString(path.join(versionDir, ".install-complete"), `${version}\n`);
      }
      const state = { protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: "1.0.0" } as const;
      const statePath = path.join(root, "runtime", "service-state.json");
      yield* Effect.promise(() => writeServiceState(statePath, state));
      yield* Effect.tryPromise(() => new Launcher(root, state).run()).pipe(Effect.result);
      assert.equal(yield* fs.readFileString(responsePath), "update-rejected");
      assert.deepEqual(yield* Effect.promise(() => readServiceState(statePath)), state);
      assert.equal(yield* fs.readFileString(outside), "other owner's work");
    }),
  );

  for (const owner of ["standalone", "orphan-child", "sqlite-access"] as const) {
    it.effect(`does not restore a pending backup over a ${owner}'s work`, () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-owned-rollback-" });
        const stateDir = path.join(root, "userdata");
        const databasePath = path.join(stateDir, "state.sqlite");
        if (owner === "standalone") {
          yield* acquireServerStateLock({
            stateDir,
            serverRuntimeStatePath: path.join(stateDir, "server-runtime.json"),
          });
        } else {
          yield* Effect.acquireRelease(
            Effect.promise(() =>
              owner === "sqlite-access"
                ? acquireDatabaseAccess(stateDir)
                : acquireRuntimeStateOwnership(stateDir),
            ),
            (release) => Effect.sync(release),
          );
        }
        yield* fs.writeFileString(databasePath, "new owner's work");
        const backup = path.join(root, "runtime", "db-backup", "pending-1");
        yield* fs.makeDirectory(backup, { recursive: true });
        yield* fs.writeFileString(path.join(backup, "database"), "older backup");
        const state = {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
          update: {
            id: "pending-1",
            fromVersion: "1.0.0",
            targetVersion: "1.1.0",
            dbPath: databasePath,
            status: "pending",
          },
        } as const;
        const statePath = path.join(root, "runtime", "service-state.json");
        yield* Effect.promise(() => writeServiceState(statePath, state));

        const result = yield* Effect.tryPromise(() => new Launcher(root, state).run()).pipe(
          Effect.result,
        );

        assert.equal(result._tag, "Failure");
        assert.equal(yield* fs.readFileString(databasePath), "new owner's work");
        assert.deepEqual(yield* Effect.promise(() => readServiceState(statePath)), state);
      }),
    );
  }

  it.effect("durably replaces and strictly reads one state document", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-test-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      const state = {
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: "0.0.31",
      } as const;

      yield* Effect.promise(() => writeServiceState(statePath, state));
      assert.deepEqual(yield* Effect.promise(() => readServiceState(statePath)), state);
    }),
  );

  it.effect("serializes shutdown with launcher recovery", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-stop-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      const versionDir = path.join(root, "runtime", "versions", "1.0.0");
      const entryPath = path.join(versionDir, "node_modules", "t3", "dist", "bin.mjs");
      yield* fs.makeDirectory(path.dirname(entryPath), { recursive: true });
      yield* fs.writeFileString(entryPath, "setInterval(() => {}, 1_000);\n");
      yield* fs.writeFileString(path.join(versionDir, ".install-complete"), "1.0.0\n");
      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
        }),
      );

      const launcher = new Launcher(root, yield* Effect.promise(() => readServiceState(statePath)));
      const running = launcher.run();
      const stopping = launcher.stop("SIGTERM");
      // An explicit stop leaves the marker that tells a child shutting down
      // mid-update that no replacement server is coming. It is present as
      // soon as stop() returns its promise, before queued transitions run.
      assert.isTrue(yield* fs.exists(path.join(root, "runtime", SERVICE_STOP_MARKER_FILE)));
      yield* Effect.promise(() => stopping);
      yield* Effect.promise(() => running);
    }),
  );

  it.effect("commits only after the trial reports prepared", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-flow-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      const databasePath = path.join(root, "userdata", "state.sqlite");
      yield* fs.makeDirectory(path.dirname(databasePath), { recursive: true });
      yield* fs.writeFileString(databasePath, "before trial");
      // @effect-diagnostics-next-line preferSchemaOverJson:off - embeds a path in fake child source.
      const encodedDatabasePath = JSON.stringify(databasePath);
      const childSource = `
const context = JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
if (context.update?.status === "pending") {
  process.send({ type: "prepared", updateId: context.update.id });
  process.on("message", (message) => {
    if (message.type === "committed") process.exit(0);
  });
} else if (context.update === undefined) {
  process.send({ type: "request-update", targetVersion: "1.1.0", dbPath: ${encodedDatabasePath} });
  setInterval(() => {}, 1_000);
} else {
  process.exit(0);
}
`;
      for (const version of ["1.0.0", "1.1.0"]) {
        const versionDir = path.join(root, "runtime", "versions", version);
        const entryPath = path.join(versionDir, "node_modules", "t3", "dist", "bin.mjs");
        yield* fs.makeDirectory(path.dirname(entryPath), { recursive: true });
        yield* fs.writeFileString(entryPath, childSource);
        yield* fs.writeFileString(path.join(versionDir, ".install-complete"), `${version}\n`);
      }
      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
        }),
      );

      const launcher = new Launcher(root, yield* Effect.promise(() => readServiceState(statePath)));
      yield* Effect.promise(() =>
        launcher.run().then(
          () => Promise.reject(new Error("launcher unexpectedly completed")),
          () => Promise.resolve(),
        ),
      );

      const state = yield* Effect.promise(() => readServiceState(statePath));
      assert.equal(state.activeVersion, "1.1.0");
      assert.equal(state.update?.status, "committed");
    }),
  );

  it.effect("rolls back a trial that reports the wrong update ID", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-rollback-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      const databasePath = path.join(root, "userdata", "state.sqlite");
      yield* fs.makeDirectory(path.dirname(databasePath), { recursive: true });
      yield* fs.writeFileString(databasePath, "before trial");
      // @effect-diagnostics-next-line preferSchemaOverJson:off - embeds a path in fake child source.
      const encodedDatabasePath = JSON.stringify(databasePath);
      const childSource = `
const context = JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
if (context.update?.status === "pending") {
  process.send({ type: "prepared", updateId: "wrong-update" });
} else if (context.update === undefined) {
  process.send({ type: "request-update", targetVersion: "1.1.0", dbPath: ${encodedDatabasePath} });
  setInterval(() => {}, 1_000);
} else {
  process.exit(0);
}
`;
      for (const version of ["1.0.0", "1.1.0"]) {
        const versionDir = path.join(root, "runtime", "versions", version);
        const entryPath = path.join(versionDir, "node_modules", "t3", "dist", "bin.mjs");
        yield* fs.makeDirectory(path.dirname(entryPath), { recursive: true });
        yield* fs.writeFileString(entryPath, childSource);
        yield* fs.writeFileString(path.join(versionDir, ".install-complete"), `${version}\n`);
      }
      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
        }),
      );

      const launcher = new Launcher(root, yield* Effect.promise(() => readServiceState(statePath)));
      yield* Effect.promise(() =>
        launcher.run().then(
          () => Promise.reject(new Error("launcher unexpectedly completed")),
          () => Promise.resolve(),
        ),
      );

      const state = yield* Effect.promise(() => readServiceState(statePath));
      assert.equal(state.activeVersion, "1.0.0");
      assert.equal(state.update?.status, "rolled-back");
      assert.equal(
        state.update?.status === "rolled-back" ? state.update.reason : undefined,
        "invalid-prepared",
      );
    }),
  );

  it.effect("restores the database when a migrating trial exits", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-db-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      const databasePath = path.join(root, "userdata", "state.sqlite");
      const original = "database before migration";
      yield* fs.makeDirectory(path.dirname(databasePath), { recursive: true });
      yield* fs.writeFileString(databasePath, original);
      // @effect-diagnostics-next-line preferSchemaOverJson:off - embeds a path in fake child source.
      const encodedDatabasePath = JSON.stringify(databasePath);
      const childSource = `
import { writeFileSync } from "node:fs";
const context = JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
if (context.update?.status === "pending") {
  writeFileSync(context.update.dbPath, "database after migration");
  writeFileSync(context.update.dbPath + "-wal", "trial wal");
  writeFileSync(context.update.dbPath + "-shm", "trial shm");
  process.exit(1);
} else if (context.update === undefined) {
  process.send({ type: "request-update", targetVersion: "1.1.0", dbPath: ${encodedDatabasePath} });
  setInterval(() => {}, 1_000);
} else {
  process.exit(0);
}
`;
      for (const version of ["1.0.0", "1.1.0"]) {
        const versionDir = path.join(root, "runtime", "versions", version);
        const entryPath = path.join(versionDir, "node_modules", "t3", "dist", "bin.mjs");
        yield* fs.makeDirectory(path.dirname(entryPath), { recursive: true });
        yield* fs.writeFileString(entryPath, childSource);
        yield* fs.writeFileString(path.join(versionDir, ".install-complete"), `${version}\n`);
      }
      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
        }),
      );

      const launcher = new Launcher(root, yield* Effect.promise(() => readServiceState(statePath)));
      yield* Effect.promise(() =>
        launcher.run().then(
          () => Promise.reject(new Error("launcher unexpectedly completed")),
          () => Promise.resolve(),
        ),
      );

      const state = yield* Effect.promise(() => readServiceState(statePath));
      assert.equal(state.activeVersion, "1.0.0");
      assert.equal(state.update?.status, "rolled-back");
      assert.equal(yield* fs.readFileString(databasePath), original);
      assert.isFalse(yield* fs.exists(`${databasePath}-wal`));
      assert.isFalse(yield* fs.exists(`${databasePath}-shm`));
      const updateId = state.update?.id;
      assert.isDefined(updateId);
      assert.isFalse(yield* fs.exists(path.join(root, "runtime", "db-backup", updateId)));
    }),
  );
});
