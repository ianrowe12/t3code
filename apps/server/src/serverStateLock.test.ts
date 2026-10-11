import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { acquireServerStateLock } from "./serverStateLock.ts";
import {
  acquireDatabaseAccess,
  acquireRuntimeStateOwnership,
  acquireStateLaunchGate,
  hasRuntimeStateOwner,
} from "./serverStateOwnership.ts";

it.effect("excludes duplicate and alias owners and releases ownership with its scope", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-state-lock-" });
    const alias = path.join(dir, "alias");
    const stateDir = path.join(dir, "state");
    yield* fs.makeDirectory(stateDir);
    yield* fs.symlink(stateDir, alias);
    yield* Effect.scoped(
      Effect.gen(function* () {
        yield* acquireServerStateLock(stateDir);
        assert.isTrue(yield* Effect.promise(() => hasRuntimeStateOwner(alias)));
        const duplicate = yield* Effect.scoped(acquireServerStateLock(alias)).pipe(Effect.result);
        assert.equal(duplicate._tag, "Failure");
      }),
    );
    assert.isFalse(yield* Effect.promise(() => hasRuntimeStateOwner(stateDir)));
    yield* acquireServerStateLock(alias);
  }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);

it.effect("permits a launcher child but rejects an orphan's replacement", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-launch-lock-" });
    const releaseGate = yield* Effect.promise(() => acquireStateLaunchGate(dir));
    yield* Effect.scoped(acquireServerStateLock(dir, dir));
    const releaseRuntime = yield* Effect.promise(() => acquireRuntimeStateOwnership(dir));
    releaseGate();
    const duplicate = yield* Effect.scoped(acquireServerStateLock(dir)).pipe(Effect.result);
    assert.equal(duplicate._tag, "Failure");
    releaseRuntime();
    yield* acquireServerStateLock(dir);
  }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);

it.effect("allows concurrent database readers but excludes backup and restore", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-access-lock-" });
    const first = yield* Effect.promise(() => acquireDatabaseAccess(dir));
    const second = yield* Effect.promise(() => acquireDatabaseAccess(dir));
    const exclusive = yield* Effect.tryPromise(() =>
      acquireDatabaseAccess(dir, { exclusive: true }),
    ).pipe(Effect.result);
    assert.equal(exclusive._tag, "Failure");
    first();
    second();
    const maintenance = yield* Effect.promise(() =>
      acquireDatabaseAccess(dir, { exclusive: true }),
    );
    const reader = yield* Effect.tryPromise(() => acquireDatabaseAccess(dir)).pipe(Effect.result);
    assert.equal(reader._tag, "Failure");
    maintenance();
  }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);

it.effect("fails closed on unknown or malformed ownership metadata", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-invalid-lock-" });
    const metadata = path.join(dir, "server-runtime.json");
    for (const value of ["{", '{"version":1,"pid":1,"stateLockVersion":2}']) {
      yield* fs.writeFileString(metadata, value);
      const result = yield* Effect.scoped(acquireServerStateLock(dir)).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
    }
    yield* fs.remove(metadata);
    yield* acquireServerStateLock(dir);
  }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);
