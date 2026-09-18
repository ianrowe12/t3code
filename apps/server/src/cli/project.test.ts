// @effect-diagnostics nodeBuiltinImport:off - CLI integration uses temporary Node paths.
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentInternalError,
  EventId,
  Project,
  ProjectSnapshot,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
  type ProjectId,
} from "@t3tools/contracts";
import * as NetService from "@t3tools/shared/Net";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { Command } from "effect/unstable/cli";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { cli } from "../binCli.ts";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../config.ts";
import { EventSinkV2 } from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import {
  OrchestrationV2EventSinkLayerLive,
  ProjectServiceLayerLive,
} from "../orchestration-v2/runtimeLayer.ts";
import { layerConfig as SqlitePersistenceLayerLive } from "../persistence/Layers/Sqlite.ts";
import * as ProjectEnrichmentService from "../project/ProjectEnrichmentService.ts";
import * as ProjectFaviconResolver from "../project/ProjectFaviconResolver.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as T3ProjectFileLoader from "../project/T3ProjectFileLoader.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { acquireServerStateLock } from "../serverStateLock.ts";
import { acquireDatabaseAccess, acquireRuntimeStateOwnership } from "../serverStateOwnership.ts";
import {
  makePersistedServerRuntimeState,
  persistServerRuntimeState,
  readPersistedServerRuntimeState,
} from "../serverRuntimeState.ts";
import {
  ProjectLiveServerDeclaredResponseError,
  ProjectLiveServerRequestError,
  projectCommandErrorFromLiveServerRequest,
} from "./project.ts";

const CliRuntimeLayer = Layer.mergeAll(NodeServices.layer, NetService.layer);
const encodeProjectSnapshot = Schema.encodeEffect(Schema.fromJsonString(ProjectSnapshot));
const encodeProject = Schema.encodeEffect(Schema.fromJsonString(Project));
const runCli = (args: ReadonlyArray<string>) =>
  Command.runWith(cli, { version: "0.0.0" })(args).pipe(Effect.provide(CliRuntimeLayer));

const makeConfig = (baseDir: string) =>
  Effect.gen(function* () {
    const derivedPaths = yield* ServerConfig.deriveServerPaths(baseDir, undefined);
    return {
      logLevel: "Info",
      traceMinLevel: "Info",
      traceTimingEnabled: true,
      traceBatchWindowMs: 200,
      traceMaxBytes: 10 * 1024 * 1024,
      traceMaxFiles: 10,
      otlpTracesUrl: undefined,
      otlpMetricsUrl: undefined,
      otlpExportIntervalMs: 10_000,
      otlpServiceName: "t3-server",
      mode: "web",
      port: 0,
      host: "127.0.0.1",
      cwd: process.cwd(),
      baseDir,
      ...derivedPaths,
      staticDir: undefined,
      devUrl: undefined,
      devAllowedOrigins: [],
      noBrowser: true,
      startupPresentation: "browser",
      desktopBootstrapToken: undefined,
      autoBootstrapProjectFromCwd: false,
      logWebSocketEvents: false,
      tailscaleServeEnabled: false,
      tailscaleServePort: 443,
    } satisfies ServerConfig.ServerConfig["Service"];
  });

const readProjects = (baseDir: string) =>
  Effect.gen(function* () {
    const config = yield* makeConfig(baseDir);
    const layer = ProjectServiceLayerLive.pipe(
      Layer.provideMerge(ProjectEnrichmentService.layer),
      Layer.provideMerge(RepositoryIdentityResolver.layer),
      Layer.provideMerge(ProjectFaviconResolver.layer),
      Layer.provideMerge(T3ProjectFileLoader.layer),
      Layer.provideMerge(WorkspacePaths.layer),
      Layer.provideMerge(SqlitePersistenceLayerLive),
      Layer.provideMerge(NodeServices.layer),
      Layer.provide(ServerConfig.layer(config)),
      Layer.provide(Layer.succeed(References.MinimumLogLevel, config.logLevel)),
    );
    return yield* ProjectService.ProjectService.pipe(
      Effect.flatMap((projects) => projects.snapshot),
      Effect.provide(layer),
    );
  });

it("maps declared server failures into structural project command errors", () => {
  const cause = new EnvironmentInternalError({
    code: "internal_error",
    reason: "access_token_issuance_failed",
    traceId: "trace-123",
  });

  const error = projectCommandErrorFromLiveServerRequest(cause);

  assert.instanceOf(error, ProjectLiveServerDeclaredResponseError);
  assert.strictEqual(error.operation, "callLiveServer");
  assert.strictEqual(error.code, "internal_error");
  assert.strictEqual(error.traceId, "trace-123");
  assert.strictEqual(error.message, "Server request failed (internal_error, trace trace-123).");
  assert.strictEqual(error.cause, cause);
});

it("preserves unexpected server failures without deriving the message from them", () => {
  const cause = new Error("credential abc123 was rejected");

  const error = projectCommandErrorFromLiveServerRequest(cause);

  assert.instanceOf(error, ProjectLiveServerRequestError);
  assert.strictEqual(error.operation, "callLiveServer");
  assert.strictEqual(error.message, "Failed to call the running server.");
  assert.strictEqual(error.cause, cause);
});

it.effect("adds, renames, and removes projects through the V2 project CLI domain", () =>
  Effect.gen(function* () {
    const baseDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-v2-project-cli-"));
    const workspaceRoot = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "t3-v2-project-workspace-"),
    );

    yield* runCli(["project", "add", workspaceRoot, "--title", "Alpha", "--base-dir", baseDir]);
    const added = (yield* readProjects(baseDir)).projects[0];
    assert.equal(added?.title, "Alpha");
    assert.equal(added?.workspaceRoot, workspaceRoot);

    yield* runCli(["project", "rename", workspaceRoot, "Beta", "--base-dir", baseDir]);
    assert.equal((yield* readProjects(baseDir)).projects[0]?.title, "Beta");

    yield* runCli(["project", "remove", added?.id ?? "", "--base-dir", baseDir]);
    assert.deepEqual((yield* readProjects(baseDir)).projects, []);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("does not clear a live owner's discovery or fall back to offline mutations", () =>
  Effect.gen(function* () {
    const fixture = yield* makeProjectLookupFixture();
    const config = yield* makeConfig(fixture.baseDir);
    const net = yield* NetService.NetService;
    const port = yield* net.reserveLoopbackPort();
    const state = yield* makePersistedServerRuntimeState({ config, port });
    yield* persistServerRuntimeState({ path: config.serverRuntimeStatePath, state });

    const result = yield* runCli([
      "project",
      "rename",
      fixture.workspaceRoot,
      "Must not change",
      "--base-dir",
      fixture.baseDir,
    ]).pipe(Effect.result);

    assert.equal(result._tag, "Failure");
    assert.equal((yield* readProjects(fixture.baseDir)).projects[0]?.title, fixture.project.title);
    assert.deepEqual(
      Option.getOrThrow(yield* readPersistedServerRuntimeState(config.serverRuntimeStatePath)),
      state,
    );
  }).pipe(Effect.provide(CliRuntimeLayer)),
);

it.effect("does not mutate offline while an owner has not published discovery yet", () =>
  Effect.gen(function* () {
    const fixture = yield* makeProjectLookupFixture();
    const config = yield* makeConfig(fixture.baseDir);
    yield* acquireServerStateLock(config);

    const result = yield* runCli([
      "project",
      "rename",
      fixture.workspaceRoot,
      "Must not change",
      "--base-dir",
      fixture.baseDir,
    ]).pipe(Effect.result);

    assert.equal(result._tag, "Failure");
    assert.equal((yield* readProjects(fixture.baseDir)).projects[0]?.title, fixture.project.title);
  }).pipe(Effect.provide(CliRuntimeLayer)),
);

it.effect("does not initialize auth persistence before state ownership admission", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-project-auth-admission-" });
    const config = yield* makeConfig(NodePath.join(root, "state"));
    const workspace = NodePath.join(root, "workspace");
    yield* fs.makeDirectory(workspace);
    yield* acquireServerStateLock(config);
    assert.isFalse(yield* fs.exists(config.dbPath));

    const result = yield* runCli(["project", "add", workspace, "--base-dir", config.baseDir]).pipe(
      Effect.result,
    );

    assert.equal(result._tag, "Failure");
    assert.isFalse(
      yield* fs.exists(config.dbPath),
      "rejected CLI startup must not migrate SQLite for auth",
    );
  }).pipe(Effect.provide(CliRuntimeLayer)),
);

it.effect(
  "uses offline ownership rather than auth when a guarded discovery PID has been recycled",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-project-recycled-pid-" });
      const config = yield* makeConfig(NodePath.join(root, "state"));
      const workspace = NodePath.join(root, "workspace");
      yield* fs.makeDirectory(workspace);
      yield* fs.makeDirectory(config.stateDir, { recursive: true });
      const net = yield* NetService.NetService;
      const port = yield* net.reserveLoopbackPort();
      yield* persistServerRuntimeState({
        path: config.serverRuntimeStatePath,
        state: yield* makePersistedServerRuntimeState({ config, port, stateLockVersion: 1 }),
      });

      yield* runCli(["project", "add", workspace, "--base-dir", config.baseDir]);

      assert.equal((yield* readProjects(config.baseDir)).projects.length, 1);
      const database = new NodeSqlite.DatabaseSync(config.dbPath);
      try {
        assert.equal(
          database.prepare("SELECT count(*) AS count FROM auth_sessions").get()?.count,
          0,
        );
      } finally {
        database.close();
      }
    }).pipe(Effect.provide(CliRuntimeLayer)),
);

it.effect("keeps live auth leased through cleanup and close after its server exits", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-auth-lease-lifetime-" });
    const config = yield* makeConfig(root);
    yield* fs.makeDirectory(config.stateDir, { recursive: true });
    const serverScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
      Scope.close(scope, Exit.void),
    );
    yield* acquireServerStateLock(config).pipe(Effect.provideService(Scope.Scope, serverScope));
    yield* SqlClient.SqlClient.pipe(
      Effect.asVoid,
      Effect.provide(SqlitePersistenceLayerLive.pipe(Layer.provide(ServerConfig.layer(config)))),
    );
    yield* persistServerRuntimeState({
      path: config.serverRuntimeStatePath,
      state: yield* makePersistedServerRuntimeState({ config, port: 3773, stateLockVersion: 1 }),
    });
    const maintenance = Effect.tryPromise(async () => {
      const release = await acquireRuntimeStateOwnership(config.stateDir);
      try {
        const releaseDatabase = await acquireDatabaseAccess(config.stateDir, { exclusive: true });
        releaseDatabase();
      } finally {
        release();
      }
    });
    yield* Effect.gen(function* () {
      const auth = yield* EnvironmentAuth.EnvironmentAuth;
      yield* Scope.close(serverScope, Exit.void);
      const session = yield* auth.issueSession({ label: "ownership regression" });
      assert.equal((yield* maintenance.pipe(Effect.result))._tag, "Failure");
      yield* auth.revokeSession(session.sessionId);
      assert.equal((yield* maintenance.pipe(Effect.result))._tag, "Failure");
    }).pipe(
      Effect.provide(EnvironmentAuth.runtimeLayer.pipe(Layer.provide(ServerConfig.layer(config)))),
    );
    yield* maintenance;
  }).pipe(Effect.provide(CliRuntimeLayer)),
);

it.effect(
  "authenticates live project commands without running migrations or leaving sessions active",
  () =>
    Effect.gen(function* () {
      const fixture = yield* makeProjectLookupFixture();
      const config = yield* makeConfig(fixture.baseDir);
      yield* acquireServerStateLock(config);
      const snapshot = yield* encodeProjectSnapshot({
        projects: [fixture.project],
        updatedAt: "2026-09-18T12:00:00.000Z",
      });
      const mutated = yield* encodeProject({
        ...fixture.project,
        title: "Through live server",
      });
      // Missing migration history makes an accidental migration observable even on a current schema.
      const database = new NodeSqlite.DatabaseSync(config.dbPath);
      try {
        database.exec("DROP TABLE effect_sql_migrations");
      } finally {
        database.close();
      }
      const requests: Array<{ method: string | undefined; authenticated: boolean }> = [];
      const server = yield* Effect.acquireRelease(
        Effect.promise(
          () =>
            new Promise<NodeHttp.Server>((resolve) => {
              const server = NodeHttp.createServer((request, response) => {
                requests.push({
                  method: request.method,
                  authenticated: request.headers.authorization?.startsWith("Bearer ") === true,
                });
                request.resume();
                response.setHeader("content-type", "application/json");
                if (request.method === "GET") {
                  response.end(snapshot);
                } else {
                  response.end(mutated);
                }
              });
              server.listen(0, "127.0.0.1", () => resolve(server));
            }),
        ),
        (server) =>
          Effect.promise(() => new Promise<void>((resolve) => server.close(() => resolve()))),
      );
      const address = server.address();
      assert.isNotNull(address);
      assert.notEqual(typeof address, "string");
      if (address === null || typeof address === "string") return;
      yield* persistServerRuntimeState({
        path: config.serverRuntimeStatePath,
        state: yield* makePersistedServerRuntimeState({
          config,
          port: address.port,
          stateLockVersion: 1,
        }),
      });

      yield* runCli([
        "project",
        "rename",
        fixture.project.id,
        "Through live server",
        "--base-dir",
        config.baseDir,
      ]);

      assert.deepEqual(
        requests.map((request) => request.method),
        ["GET", "POST"],
      );
      assert.isTrue(requests.every((request) => request.authenticated));
      const after = new NodeSqlite.DatabaseSync(config.dbPath);
      try {
        assert.equal(
          after
            .prepare(
              "SELECT count(*) AS count FROM sqlite_schema WHERE name = 'effect_sql_migrations'",
            )
            .get()?.count,
          0,
        );
        assert.equal(
          after
            .prepare("SELECT count(*) AS count FROM auth_sessions WHERE revoked_at IS NULL")
            .get()?.count,
          0,
        );
      } finally {
        after.close();
      }
    }).pipe(Effect.provide(CliRuntimeLayer)),
);

const makeProjectLookupFixture = Effect.fn("ProjectCliTest.makeProjectLookupFixture")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-v2-project-lookup-" });
  const baseDir = NodePath.join(root, "state");
  const workspaceRoot = NodePath.join(root, "workspace");
  yield* fs.makeDirectory(workspaceRoot);
  yield* runCli(["project", "add", workspaceRoot, "--base-dir", baseDir]);
  const project = (yield* readProjects(baseDir)).projects[0];
  assert.isDefined(project);
  return { baseDir, workspaceRoot, project: project! };
});

const makeThreadPersistenceLayer = Effect.fn("ProjectCliTest.makeThreadPersistenceLayer")(
  function* (baseDir: string) {
    const config = yield* makeConfig(baseDir);
    return Layer.mergeAll(
      OrchestrationV2EventSinkLayerLive,
      ProjectionStore.layer,
      EventStore.layer,
    ).pipe(
      Layer.provideMerge(SqlitePersistenceLayerLive),
      Layer.provideMerge(NodeServices.layer),
      Layer.provide(ServerConfig.layer(config)),
      Layer.provide(Layer.succeed(References.MinimumLogLevel, config.logLevel)),
    );
  },
);

const seedNativeThreads = Effect.fn("ProjectCliTest.seedNativeThreads")(function* (
  baseDir: string,
  threads: ReadonlyArray<{
    readonly id: ThreadId;
    readonly projectId: ProjectId;
    readonly archived: boolean;
  }>,
) {
  const layer = yield* makeThreadPersistenceLayer(baseDir);
  const createdAt = DateTime.makeUnsafe("2026-09-04T12:00:00.000Z");
  const providerInstanceId = ProviderInstanceId.make("codex");
  yield* Effect.gen(function* () {
    const eventSink = yield* EventSinkV2;
    yield* eventSink.write({
      commandId: CommandId.make("project-cli-seed-threads"),
      events: threads.map(({ id, projectId, archived }) => {
        const payload: OrchestrationV2AppThread = {
          createdBy: "user",
          creationSource: "web",
          id,
          projectId,
          title: id,
          providerInstanceId,
          modelSelection: { instanceId: providerInstanceId, model: "gpt-5" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
          forkedFrom: null,
          createdAt,
          updatedAt: createdAt,
          archivedAt: archived ? createdAt : null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        };
        return {
          id: EventId.make(`project-cli-create-${id}`),
          type: "thread.created" as const,
          threadId: id,
          providerInstanceId,
          occurredAt: createdAt,
          payload,
        };
      }),
    });
  }).pipe(Effect.provide(layer));
});

const readNativeThreadState = Effect.fn("ProjectCliTest.readNativeThreadState")(function* (
  baseDir: string,
  threadId: ThreadId,
) {
  const layer = yield* makeThreadPersistenceLayer(baseDir);
  return yield* Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const events = yield* EventStore.EventStoreV2;
    return {
      thread: yield* projections.getThread(threadId),
      events: yield* events.read({ threadId }).pipe(Stream.runCollect),
    };
  }).pipe(Effect.provide(layer));
});

it.layer(NodeServices.layer)("project deletion with native V2 threads", (it) => {
  it.effect.each([
    { label: "an active thread", archived: false, missing: false },
    { label: "an archived thread", archived: true, missing: false },
    {
      label: "an active thread after its workspace disappears",
      archived: false,
      missing: true,
    },
    {
      label: "an archived thread after its workspace disappears",
      archived: true,
      missing: true,
    },
  ])("rejects unforced removal of a project with $label", ({ archived, missing }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const { baseDir, workspaceRoot, project } = yield* makeProjectLookupFixture();
      const threadId = ThreadId.make("project-cli-preserved-thread");
      yield* seedNativeThreads(baseDir, [{ id: threadId, projectId: project.id, archived }]);
      const before = yield* readNativeThreadState(baseDir, threadId);
      if (missing) yield* fs.rename(workspaceRoot, `${workspaceRoot}-removed`);

      const error = yield* runCli([
        "project",
        "remove",
        missing ? workspaceRoot : project.id,
        "--base-dir",
        baseDir,
      ]).pipe(
        Effect.match({
          onFailure: (error) => error,
          onSuccess: () => assert.fail("Removing a nonempty project must require --force."),
        }),
      );

      assert.include(error.message, "not empty");
      assert.deepEqual(
        (yield* readProjects(baseDir)).projects.map((entry) => entry.id),
        [project.id],
      );
      assert.deepEqual(yield* readNativeThreadState(baseDir, threadId), before);
      assert.equal(yield* fs.exists(workspaceRoot), !missing);
    }),
  );

  it.effect.each(["present", "missing"] as const)(
    "force-removes active and archived V2 threads with the workspace %s, preserving unrelated projects",
    (workspace) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const { baseDir, workspaceRoot, project } = yield* makeProjectLookupFixture();
        const otherWorkspace = `${workspaceRoot}-other`;
        yield* fs.makeDirectory(otherWorkspace);
        yield* runCli(["project", "add", otherWorkspace, "--base-dir", baseDir]);
        const otherProject = (yield* readProjects(baseDir)).projects.find(
          (entry) => entry.workspaceRoot === otherWorkspace,
        );
        assert.isDefined(otherProject);
        const activeId = ThreadId.make("project-cli-deleted-active");
        const archivedId = ThreadId.make("project-cli-deleted-archived");
        const unrelatedId = ThreadId.make("project-cli-unrelated-thread");
        yield* seedNativeThreads(baseDir, [
          { id: activeId, projectId: project.id, archived: false },
          { id: archivedId, projectId: project.id, archived: true },
          { id: unrelatedId, projectId: otherProject!.id, archived: false },
        ]);
        const unrelatedBefore = yield* readNativeThreadState(baseDir, unrelatedId);
        if (workspace === "missing") {
          yield* fs.rename(workspaceRoot, `${workspaceRoot}-removed`);
        }

        yield* runCli([
          "project",
          "remove",
          workspace === "missing" ? workspaceRoot : project.id,
          "--force",
          "--base-dir",
          baseDir,
        ]);

        assert.deepEqual(
          (yield* readProjects(baseDir)).projects.map((entry) => entry.id),
          [otherProject!.id],
        );
        for (const threadId of [activeId, archivedId]) {
          const state = yield* readNativeThreadState(baseDir, threadId);
          assert.isNotNull(state.thread.deletedAt);
          assert.lengthOf(
            state.events.filter((record) => record.event.type === "thread.deleted"),
            1,
          );
        }
        assert.deepEqual(yield* readNativeThreadState(baseDir, unrelatedId), unrelatedBefore);
        assert.equal(yield* fs.exists(workspaceRoot), workspace === "present");
        assert.isTrue(yield* fs.exists(otherWorkspace));
      }),
  );
});

it.layer(NodeServices.layer)("project lookup with unavailable workspaces", (it) => {
  it.effect.each(["id", "stored path"] as const)(
    "removes an empty project by %s after its directory is gone",
    (identifier) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const { baseDir, workspaceRoot, project } = yield* makeProjectLookupFixture();
        yield* fs.rename(workspaceRoot, `${workspaceRoot}-removed`);
        yield* runCli([
          "project",
          "remove",
          identifier === "id" ? project.id : workspaceRoot,
          "--base-dir",
          baseDir,
        ]);
        assert.deepEqual((yield* readProjects(baseDir)).projects, []);
        assert.isFalse(yield* fs.exists(workspaceRoot));
      }),
  );

  it.effect("renames by ID and stored path after the directory is gone", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const { baseDir, workspaceRoot, project } = yield* makeProjectLookupFixture();
      yield* fs.rename(workspaceRoot, `${workspaceRoot}-removed`);
      for (const [identifier, title] of [
        [project.id, "Renamed by ID"],
        [workspaceRoot, "Renamed by stored path"],
      ] as const) {
        yield* runCli(["project", "rename", identifier, title, "--base-dir", baseDir]);
        assert.equal((yield* readProjects(baseDir)).projects[0]?.title, title);
      }
      assert.isFalse(yield* fs.exists(workspaceRoot));
    }),
  );

  it.effect("does not resolve another environment's project ID in an empty database", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const { baseDir, workspaceRoot, project } = yield* makeProjectLookupFixture();
      yield* fs.rename(workspaceRoot, `${workspaceRoot}-removed`);
      const replacementDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-v2-project-empty-" });
      const error = yield* runCli([
        "project",
        "remove",
        project.id,
        "--force",
        "--base-dir",
        replacementDir,
      ]).pipe(Effect.flip);
      assert.include(error.message, "No active project found");
      assert.deepEqual(
        (yield* readProjects(baseDir)).projects.map((entry) => entry.id),
        [project.id],
      );
      assert.deepEqual((yield* readProjects(replacementDir)).projects, []);
    }),
  );

  it.effect("normalizes existing paths without conflating separately registered symlinks", () =>
    Effect.gen(function* () {
      const { baseDir, workspaceRoot, project } = yield* makeProjectLookupFixture();
      yield* runCli([
        "project",
        "rename",
        `${workspaceRoot}${NodePath.sep}.`,
        "Normalized",
        "--base-dir",
        baseDir,
      ]);
      assert.equal((yield* readProjects(baseDir)).projects[0]?.title, "Normalized");
      const aliasPath = `${workspaceRoot}-alias`;
      NodeFS.symlinkSync(workspaceRoot, aliasPath, "junction");
      const unknownAlias = yield* runCli([
        "project",
        "remove",
        aliasPath,
        "--base-dir",
        baseDir,
      ]).pipe(Effect.flip);
      assert.include(unknownAlias.message, "No active project found");
      yield* runCli(["project", "add", aliasPath, "--base-dir", baseDir]);
      const added = (yield* readProjects(baseDir)).projects;
      assert.equal(added.length, 2);
      const aliasProject = added.find((entry) => entry.workspaceRoot === aliasPath);
      assert.isDefined(aliasProject);
      assert.notEqual(aliasProject?.id, project.id);
      yield* runCli(["project", "remove", `${aliasPath}${NodePath.sep}.`, "--base-dir", baseDir]);
      assert.deepEqual(
        (yield* readProjects(baseDir)).projects.map((entry) => entry.id),
        [project.id],
      );
      assert.isTrue(NodeFS.existsSync(workspaceRoot));
    }),
  );
});
