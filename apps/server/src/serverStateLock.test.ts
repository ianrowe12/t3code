// @effect-diagnostics nodeBuiltinImport:off - Junctions exercise state-directory aliases on Windows too.
import * as NodeFSP from "node:fs/promises";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";

import packageJson from "../package.json" with { type: "json" };
import * as ServiceLauncherClient from "./cloud/serviceLauncherClient.ts";
import {
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_LAUNCHER_PROTOCOL,
} from "./cloud/serviceProtocol.ts";
import * as ServerConfig from "./config.ts";
import { runServer } from "./server.ts";
import { acquireServerStateLock, ServerStateLockError } from "./serverStateLock.ts";
import { acquireStateLaunchGate } from "./serverStateOwnership.ts";
import {
  makePersistedServerRuntimeState,
  persistServerRuntimeState,
} from "./serverRuntimeState.ts";

const isServerStateLockError = Schema.is(ServerStateLockError);

for (const metadata of ["empty", "malformed", "unreadable", "unsupported"] as const) {
  it.effect(`fails closed on ${metadata} runtime ownership metadata`, () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-invalid-owner-" });
      const config = {
        stateDir,
        serverRuntimeStatePath: path.join(stateDir, "server-runtime.json"),
      };
      if (metadata === "unreadable") yield* fs.makeDirectory(config.serverRuntimeStatePath);
      else
        yield* fs.writeFileString(
          config.serverRuntimeStatePath,
          metadata === "empty"
            ? ""
            : metadata === "malformed"
              ? "{"
              : '{"version":1,"pid":1,"stateLockVersion":2}',
        );
      const result = yield* Effect.scoped(acquireServerStateLock(config)).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      yield* fs.remove(config.serverRuntimeStatePath, { recursive: true });
      yield* acquireServerStateLock(config);
    }).pipe(Effect.provide(NodeServices.layer)),
  );
}

for (const failure of ["parent-death", "ipc-disconnect"] as const) {
  it.effect(`rejects managed admission after ${failure} during startup`, () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-orphan-admission-" });
      const config = {
        stateDir,
        serverRuntimeStatePath: path.join(stateDir, "server-runtime.json"),
      };
      const host = {
        connected: true,
        parentPid: 123,
        send: () => true,
        on: () => undefined,
        off: () => undefined,
      };
      yield* Effect.gen(function* () {
        const mode = yield* ServiceLauncherClient.resolveServiceLauncherMode();
        assert.isTrue(mode.managed);
        if (failure === "parent-death") host.parentPid = 1;
        else host.connected = false;
        let recoveryStarted = false;
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* acquireServerStateLock(config, { launcherStateDir: stateDir });
            recoveryStarted = true;
          }),
        ).pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        assert.isFalse(recoveryStarted);
        yield* acquireServerStateLock(config);
      }).pipe(
        Effect.provideService(ServiceLauncherClient.ServiceLauncherHostProcess, host),
        Effect.provideService(HostProcessEnvironment, {
          // @effect-diagnostics-next-line preferSchemaOverJson:off - standalone launcher wire fixture.
          [SERVICE_LAUNCHER_CONTEXT_ENV]: JSON.stringify({
            protocol: SERVICE_LAUNCHER_PROTOCOL,
            childVersion: packageJson.version,
            stateDir,
            launcherPid: 123,
          }),
        }),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );
}

it.effect("allows managed children while excluding independent starts throughout a handoff", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-launch-handoff-" });
    const config = { stateDir, serverRuntimeStatePath: path.join(stateDir, "server-runtime.json") };
    yield* Effect.acquireRelease(
      Effect.promise(() => acquireStateLaunchGate(stateDir)),
      (release) => Effect.sync(release),
    );
    yield* Effect.gen(function* () {
      yield* Effect.scoped(acquireServerStateLock(config, { launcherStateDir: stateDir }));
      const error = yield* Effect.scoped(acquireServerStateLock(config)).pipe(Effect.flip);
      assert.isTrue(isServerStateLockError(error));
      yield* acquireServerStateLock(config, { launcherStateDir: stateDir });
      const duplicate = yield* Effect.scoped(
        acquireServerStateLock(config, { launcherStateDir: stateDir }),
      ).pipe(Effect.flip);
      assert.isTrue(isServerStateLockError(duplicate));
    }).pipe(
      Effect.provideService(ServiceLauncherClient.ServiceLauncherHostProcess, {
        connected: true,
        parentPid: 123,
        send: () => true,
        on: () => undefined,
        off: () => undefined,
      }),
      Effect.provideService(HostProcessEnvironment, {
        // @effect-diagnostics-next-line preferSchemaOverJson:off - standalone launcher wire fixture.
        [SERVICE_LAUNCHER_CONTEXT_ENV]: JSON.stringify({
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          childVersion: packageJson.version,
          stateDir,
          launcherPid: 123,
        }),
      }),
    );
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("keeps an orphan child's runtime exclusive after its launcher is killed", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-orphan-owner-" });
    const ownershipModule = new URL("./serverStateOwnership.ts", import.meta.url).href;
    const childSource = `
const { acquireRuntimeStateOwnership } = await import(process.argv[1]);
await acquireRuntimeStateOwnership(process.argv[2]);
setInterval(() => {}, 60_000);
process.send("owned");
`;
    // @effect-diagnostics-next-line preferSchemaOverJson:off - embeds source in a subprocess fixture.
    const encodedChildSource = JSON.stringify(childSource);
    const launcherSource = `
import { spawn } from "node:child_process";
const { acquireStateLaunchGate } = await import(process.argv[1]);
await acquireStateLaunchGate(process.argv[2]);
const child = spawn(process.execPath, ["--input-type=module", "-e", ${encodedChildSource}, process.argv[1], process.argv[2]], {
  stdio: ["ignore", "ignore", "inherit", "ipc"],
});
child.on("message", () => process.stdout.write(String(child.pid) + "\\n"));
process.on("SIGTERM", () => { child.kill("SIGKILL"); process.exit(0); });
`;
    const launcher = yield* spawner.spawn(
      ChildProcess.make(
        process.execPath,
        ["--input-type=module", "-e", launcherSource, ownershipModule, stateDir],
        { detached: false },
      ),
    );
    const ready = yield* launcher.stdout.pipe(Stream.decodeText(), Stream.runHead);
    if (Option.isNone(ready))
      assert.fail(yield* launcher.stderr.pipe(Stream.decodeText(), Stream.mkString));
    const childPid = Number(ready.value.trim());
    assert.isAbove(childPid, 0);
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        try {
          process.kill(childPid, "SIGKILL");
        } catch {}
      }),
    );
    yield* launcher.kill({ killSignal: "SIGKILL" });
    yield* launcher.exitCode.pipe(Effect.result);
    assert.doesNotThrow(() => process.kill(childPid, 0));
    // The launch gate is gone, but the independent runtime lock survives.
    yield* Effect.acquireRelease(
      Effect.promise(() => acquireStateLaunchGate(stateDir)),
      (release) => Effect.sync(release),
    ).pipe(Effect.scoped);
    const error = yield* Effect.scoped(
      acquireServerStateLock({
        stateDir,
        serverRuntimeStatePath: path.join(stateDir, "server-runtime.json"),
      }),
    ).pipe(Effect.flip);
    assert.isTrue(isServerStateLockError(error));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("permits restart with a reused PID after a guarded server has exited", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-reused-owner-pid-" });
    const config = { stateDir, serverRuntimeStatePath: path.join(stateDir, "server-runtime.json") };
    const previous = yield* makePersistedServerRuntimeState({
      config: { host: "127.0.0.1", devUrl: undefined },
      port: 3773,
    });
    yield* persistServerRuntimeState({
      path: config.serverRuntimeStatePath,
      state: { ...previous, stateLockVersion: 1 },
    });

    yield* acquireServerStateLock(config);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("rejects a different-port server before opening its application database", () =>
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    yield* acquireServerStateLock(config);
    yield* fs.makeDirectory(config.dbPath);
    const state = yield* makePersistedServerRuntimeState({ config, port: 3773 });
    yield* persistServerRuntimeState({ path: config.serverRuntimeStatePath, state });
    const before = yield* fs.readFileString(config.serverRuntimeStatePath);

    const error = yield* runServer.pipe(
      Effect.provideService(ServerConfig.ServerConfig, { ...config, port: 3774 }),
      Effect.flip,
    );

    assert.isTrue(isServerStateLockError(error));
    assert.equal(yield* fs.readFileString(config.serverRuntimeStatePath), before);
    assert.deepEqual(yield* fs.readDirectory(config.dbPath), []);
  }).pipe(
    Effect.provide(
      ServerConfig.layerTest(".", { prefix: "t3-server-admission-" }).pipe(
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
  ),
);

const childOwnerSource = `
import * as Effect from "effect/Effect";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as path from "node:path";
const { acquireServerStateLock } = await import(process.argv[1]);
const { makePersistedServerRuntimeState, persistServerRuntimeState } =
  await import(new URL("./serverRuntimeState.ts", process.argv[1]));
process.stdin.resume();
await Effect.runPromise(Effect.gen(function* () {
  const config = {
    stateDir: process.argv[2],
    serverRuntimeStatePath: path.join(process.argv[2], "server-runtime.json")
  };
  yield* acquireServerStateLock(config);
  const state = yield* makePersistedServerRuntimeState({
    config: { host: "127.0.0.1", devUrl: undefined }, port: 3773, stateLockVersion: 1
  });
  yield* persistServerRuntimeState({ path: config.serverRuntimeStatePath, state });
  process.stdout.write("owned\\n");
  yield* Effect.never;
}).pipe(Effect.provide(NodeServices.layer), Effect.scoped));
`;

it.effect("excludes a separate process and recovers after its ungraceful exit", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-process-owner-" });
    const config = {
      stateDir,
      serverRuntimeStatePath: path.join(stateDir, "server-runtime.json"),
    };
    const child = yield* spawner.spawn(
      ChildProcess.make(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          childOwnerSource,
          new URL("./serverStateLock.ts", import.meta.url).href,
          stateDir,
        ],
        {
          cwd: yield* path.fromFileUrl(new URL(".", import.meta.url)),
          stdin: "pipe",
          detached: false,
        },
      ),
    );
    const ready = yield* child.stdout.pipe(Stream.decodeText(), Stream.runHead);
    if (Option.isNone(ready)) {
      assert.fail(yield* child.stderr.pipe(Stream.decodeText(), Stream.mkString));
    }
    assert.equal(ready.value, "owned\n");
    const before = yield* fs.readFileString(config.serverRuntimeStatePath);
    const error = yield* Effect.scoped(acquireServerStateLock(config)).pipe(Effect.flip);
    assert.isTrue(isServerStateLockError(error));
    assert.equal(yield* fs.readFileString(config.serverRuntimeStatePath), before);
    assert.isTrue(yield* child.isRunning);

    yield* child.kill({ killSignal: "SIGKILL" });
    const exit = yield* child.exitCode.pipe(Effect.result);
    if (exit._tag === "Failure") assert.include(String(exit.failure.cause), "SIGKILL");
    else assert.notEqual(exit.success, 0);
    assert.isFalse(yield* child.isRunning);
    yield* acquireServerStateLock(config);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("rejects a second state owner before it can run recovery, then permits restart", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-state-owner-" });
    const config = {
      stateDir,
      serverRuntimeStatePath: path.join(stateDir, "server-runtime.json"),
    };
    let recoveryCount = 0;
    const startup = Effect.gen(function* () {
      yield* acquireServerStateLock(config);
      recoveryCount += 1;
    });

    yield* Effect.scoped(
      Effect.gen(function* () {
        yield* startup;
        const error = yield* Effect.scoped(startup).pipe(Effect.flip);
        assert.isTrue(isServerStateLockError(error));
        assert.equal(recoveryCount, 1);
      }),
    );

    yield* startup;
    assert.equal(recoveryCount, 2);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("admits exactly one concurrent startup", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-concurrent-owner-" });
    const config = { stateDir, serverRuntimeStatePath: path.join(stateDir, "server-runtime.json") };
    const results = yield* Effect.all(
      [
        acquireServerStateLock(config).pipe(Effect.result),
        acquireServerStateLock(config).pipe(Effect.result),
      ],
      { concurrency: "unbounded" },
    );
    assert.equal(results.filter((result) => result._tag === "Success").length, 1);
    assert.equal(results.filter((result) => result._tag === "Failure").length, 1);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("shares ownership across directory aliases but allows independent homes", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-owner-alias-" });
    const first = {
      stateDir: path.join(root, "first"),
      serverRuntimeStatePath: path.join(root, "first", "server-runtime.json"),
    };
    yield* acquireServerStateLock(first);
    const alias = path.join(root, "alias");
    yield* Effect.promise(() => NodeFSP.symlink(first.stateDir, alias, "junction"));
    const error = yield* acquireServerStateLock({
      stateDir: alias,
      serverRuntimeStatePath: path.join(alias, "server-runtime.json"),
    }).pipe(Effect.flip);
    assert.isTrue(isServerStateLockError(error));

    yield* acquireServerStateLock({
      stateDir: path.join(root, "second"),
      serverRuntimeStatePath: path.join(root, "second", "server-runtime.json"),
    });
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("refuses to take state from a live server that predates the lock", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-legacy-state-owner-" });
    const config = {
      stateDir,
      serverRuntimeStatePath: path.join(stateDir, "server-runtime.json"),
    };
    const state = yield* makePersistedServerRuntimeState({
      config: { host: "127.0.0.1", devUrl: undefined },
      port: 3773,
    });
    yield* persistServerRuntimeState({ path: config.serverRuntimeStatePath, state });
    const before = yield* fs.readFileString(config.serverRuntimeStatePath);

    const error = yield* acquireServerStateLock(config).pipe(Effect.flip);

    assert.isTrue(isServerStateLockError(error));
    assert.equal(yield* fs.readFileString(config.serverRuntimeStatePath), before);
  }).pipe(Effect.provide(NodeServices.layer)),
);
