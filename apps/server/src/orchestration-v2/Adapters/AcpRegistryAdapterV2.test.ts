import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CheckpointId,
  EnvironmentId,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Crypto from "effect/Crypto";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import type {
  AcpRegistryAvailableCommands,
  AcpRegistryLiveConfiguration,
} from "../../provider/acp/AcpRegistryProbe.ts";
import { makeAcpRegistryCatalog } from "../../provider/acp/AcpRegistrySupport.ts";
import { layer as idAllocatorLayer, IdAllocatorV2 } from "../IdAllocator.ts";
import { ProviderAdapterV2RuntimePolicy } from "../ProviderAdapter.ts";
import { BUILT_IN_PROVIDER_ADAPTER_DRIVER_KINDS_V2 } from "../builtInProviderAdapterDrivers.ts";
import {
  ACP_REGISTRY_PROVIDER,
  AcpRegistryAdapterV2Driver,
  makeAcpRegistryAdapterV2,
} from "./AcpRegistryAdapterV2.ts";

const registryUrl = "https://registry.test/registry.json";
const decodeAcpRegistryAdapterSettings = Schema.decodeUnknownEffect(
  AcpRegistryAdapterV2Driver.configSchema,
);

const serverConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-acp-registry-v2-adapter-",
}).pipe(Layer.provide(NodeServices.layer));

const registryLayer = Layer.succeed(
  HttpClient.HttpClient,
  HttpClient.make((request) =>
    Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        Response.json({
          version: "1.0.0",
          agents: [
            {
              id: "fixture-agent",
              name: "Fixture Agent",
              version: "1.0.0",
              description: "ACP V2 adapter fixture",
              distribution: {
                binary: {
                  "darwin-aarch64": {
                    archive: "https://registry.test/unused",
                    cmd: "fixture-agent",
                    args: [],
                  },
                  "linux-x86_64": {
                    archive: "https://registry.test/unused",
                    cmd: "fixture-agent",
                    args: [],
                  },
                },
              },
            },
          ],
        }),
      ),
    ),
  ),
);

const testLayer = Layer.mergeAll(
  NodeServices.layer,
  idAllocatorLayer,
  serverConfigLayer,
  registryLayer,
);

const SpawnRecord = Schema.Struct({
  args: Schema.Array(Schema.String),
  endpoint: Schema.optionalKey(Schema.String),
  authorization: Schema.optionalKey(Schema.String),
  node: Schema.optionalKey(Schema.String),
  entrypoint: Schema.optionalKey(Schema.String),
  providerVariable: Schema.optionalKey(Schema.String),
});

const existingAgentArgs = ["--acp", "--existing-fixture-argument"];
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

function assertNativeMcpArgs(args: ReadonlyArray<string>, mcpBridgeArgs: ReadonlyArray<string>) {
  assert.lengthOf(args, existingAgentArgs.length + 2);
  assert.deepEqual(args.slice(0, -1), [...existingAgentArgs, "--additional-mcp-config"]);
  assert.deepEqual(decodeJson(args.at(-1)), {
    mcpServers: {
      "t3-code": {
        command: process.execPath,
        args: mcpBridgeArgs,
        env: { ELECTRON_RUN_AS_NODE: "1" },
      },
    },
  });
}

const AcpRequestRecord = Schema.Struct({
  method: Schema.optionalKey(Schema.String),
  params: Schema.optionalKey(
    Schema.Struct({
      sessionId: Schema.optionalKey(Schema.String),
      configId: Schema.optionalKey(Schema.String),
      value: Schema.optionalKey(Schema.Union([Schema.String, Schema.Boolean])),
      mcpServers: Schema.optionalKey(Schema.Array(Schema.Unknown)),
    }),
  ),
});
const decodeSpawnRecords = Schema.decodeUnknownEffect(
  Schema.Array(Schema.fromJsonString(SpawnRecord)),
);
const decodeRequestRecords = Schema.decodeUnknownEffect(
  Schema.Array(Schema.fromJsonString(AcpRequestRecord)),
);

const makeMcpLaunchFixture = Effect.fnUntraced(function* (
  agentId: string,
  customAgent?: string,
  rejectConfig = false,
) {
  const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fileSystem = yield* FileSystem.FileSystem;
  const idAllocator = yield* IdAllocatorV2;
  const path = yield* Path.Path;
  const mcpBridgeArgs = [
    process.argv[1] === undefined ? "t3" : path.resolve(process.argv[1]),
    "acp-mcp-bridge",
  ];
  const serverConfig = yield* ServerConfig;
  const mockAgentPath = yield* path.fromFileUrl(
    new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
  );
  const spawnLog = path.join(serverConfig.providerStatusCacheDir, "mcp-spawns.jsonl");
  const requestLog = path.join(serverConfig.providerStatusCacheDir, "mcp-requests.jsonl");
  yield* fileSystem.makeDirectory(serverConfig.providerStatusCacheDir, { recursive: true });
  const recordSpawn = `import { appendFileSync } from "node:fs";
    appendFileSync(process.env.T3_TEST_MCP_SPAWN_LOG, JSON.stringify({
      args: process.argv.slice(2),
      endpoint: process.env.T3_ACP_MCP_ENDPOINT,
      authorization: process.env.T3_ACP_MCP_AUTHORIZATION,
      node: process.env.T3_ACP_MCP_NODE,
      entrypoint: process.env.T3_ACP_MCP_ENTRYPOINT,
      providerVariable: process.env.T3_TEST_PROVIDER_VARIABLE
    }) + "\\n");`;
  const args = [
    "--import",
    `data:text/javascript,${encodeURIComponent(recordSpawn)}`,
    mockAgentPath,
    ...existingAgentArgs,
  ];
  const instanceId = ProviderInstanceId.make("acp-registry-mcp-launch");
  const settings = yield* decodeAcpRegistryAdapterSettings({ agentId });
  const adapter = makeAcpRegistryAdapterV2({
    crypto: yield* Crypto.Crypto,
    instanceId,
    settings,
    environment: {},
    childProcessSpawner,
    fileSystem,
    idAllocator,
    resolver: {
      resolve: (_settings, cwd) =>
        Effect.succeed({
          agent: {
            id: agentId,
            name: "MCP launch fixture",
            version: "1.0.83",
            description: "Records the native process MCP configuration",
            distribution: { npx: { package: "fixture", args: ["--acp"] } },
          },
          distribution: "npx",
          spawn: {
            command: process.execPath,
            args,
            cwd,
            env: {
              T3_ACP_SESSION_LIFECYCLE: "1",
              ...(customAgent === undefined ? {} : { T3_ACP_CUSTOM_AGENT: customAgent }),
              ...(rejectConfig ? { T3_ACP_FAIL_SET_CONFIG_OPTION: "1" } : {}),
              T3_TEST_MCP_SPAWN_LOG: spawnLog,
              T3_ACP_REQUEST_LOG_PATH: requestLog,
              T3_ACP_MCP_ENDPOINT: "http://127.0.0.1:1/stale",
              T3_ACP_MCP_AUTHORIZATION: "stale-provider-authorization",
              T3_TEST_PROVIDER_VARIABLE: "private-provider-setting",
            },
          },
        }),
    },
    serverConfig,
  });
  const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
    runtimeMode: "full-access",
    interactionMode: "default",
    cwd: process.cwd(),
  });
  const modelSelection = { instanceId, model: "default" } as const;
  const registerMcp = Effect.fnUntraced(function* (
    threadId: ThreadId,
    authorization: string,
    endpoint = "http://127.0.0.1:43123/mcp",
  ) {
    McpProviderSession.setMcpProviderSession({
      environmentId: EnvironmentId.make("environment-registry-mcp-launch"),
      threadId,
      providerSessionId: `mcp-${threadId}`,
      providerInstanceId: instanceId,
      endpoint,
      authorizationHeader: authorization,
      browserToolsAvailable: false,
    });
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => McpProviderSession.clearMcpProviderSession(threadId)),
    );
  });
  const readSpawns = fileSystem
    .readFileString(spawnLog)
    .pipe(Effect.flatMap((text) => decodeSpawnRecords(text.trim().split("\n"))));
  const readRequests = fileSystem
    .readFileString(requestLog)
    .pipe(Effect.flatMap((text) => decodeRequestRecords(text.trim().split("\n"))));
  return {
    adapter,
    instanceId,
    modelSelection,
    runtimePolicy,
    registerMcp,
    readSpawns,
    readRequests,
    mcpBridgeArgs,
  };
});

describe("AcpRegistryAdapterV2", () => {
  it.effect("fails session activation if Copilot rejects the default reset", () =>
    Effect.gen(function* () {
      const fixture = yield* makeMcpLaunchFixture("github-copilot-cli", "researcher", true);
      const error = yield* Effect.flip(
        fixture.adapter.openSession({
          threadId: ThreadId.make("thread-rejected-default"),
          providerSessionId: ProviderSessionId.make("session-rejected-default"),
          modelSelection: fixture.modelSelection,
          runtimePolicy: fixture.runtimePolicy,
        }),
      );
      assert.equal(error._tag, "ProviderAdapterOpenSessionError");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("sends an explicit default reset even when discovery already reports the default", () =>
    Effect.gen(function* () {
      const fixture = yield* makeMcpLaunchFixture("github-copilot-cli", "");
      yield* fixture.adapter.openSession({
        threadId: ThreadId.make("thread-explicit-default"),
        providerSessionId: ProviderSessionId.make("session-explicit-default"),
        modelSelection: fixture.modelSelection,
        runtimePolicy: fixture.runtimePolicy,
      });
      const requests = yield* fixture.readRequests;
      assert.deepInclude(
        requests.find(
          (request) =>
            request.method === "session/set_config_option" && request.params?.configId === "agent",
        )?.params,
        { configId: "agent", value: "" },
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect(
    "resets a customized Copilot session on open and cold resume without changing other options",
    () =>
      Effect.gen(function* () {
        const fixture = yield* makeMcpLaunchFixture("github-copilot-cli", "researcher");
        const threadId = ThreadId.make("thread-copilot-default-agent");
        const selection = {
          threadId,
          modelSelection: {
            ...fixture.modelSelection,
            options: [
              { id: "agent", value: "researcher" },
              { id: "mode", value: "code" },
            ],
          },
          runtimePolicy: fixture.runtimePolicy,
        };
        yield* fixture.registerMcp(threadId, "test-default-agent-credential");
        const providerThread = yield* Effect.gen(function* () {
          const runtime = yield* fixture.adapter.openSession({
            ...selection,
            providerSessionId: ProviderSessionId.make("session-customized-copilot"),
          });
          return yield* runtime.ensureThread(selection);
        }).pipe(Effect.scoped);
        const nativeId = providerThread.nativeThreadRef?.nativeId;
        if (nativeId == null) assert.fail("Expected the original native session");
        yield* fixture.adapter.openSession({
          ...selection,
          modelSelection: {
            ...fixture.modelSelection,
            options: [{ id: "mode", value: "code" }],
          },
          providerSessionId: ProviderSessionId.make("session-resumed-copilot"),
          initialNativeThreadId: nativeId,
        });
        const requests = yield* fixture.readRequests;
        const configs = requests.filter(
          (request) => request.method === "session/set_config_option",
        );
        assert.equal(configs[0]?.params?.configId, "agent");
        assert.deepEqual(
          configs
            .filter((request) => request.params?.configId === "agent")
            .map((request) => request.params?.value),
          ["", ""],
        );
        assert.deepEqual(
          configs
            .filter((request) => request.params?.configId === "mode")
            .map((request) => request.params?.value),
          ["code", "code"],
        );
        assert.deepInclude(
          requests.find((request) => request.method === "session/resume")?.params,
          {
            sessionId: nativeId,
          },
        );
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("exposes T3 MCP to the native Copilot process without credentials in argv", () =>
    Effect.gen(function* () {
      const fixture = yield* makeMcpLaunchFixture("github-copilot-cli");
      const threadId = ThreadId.make("thread-copilot-native-mcp");
      yield* fixture.registerMcp(threadId, "******");
      yield* fixture.adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("session-copilot-native-mcp"),
        modelSelection: fixture.modelSelection,
        runtimePolicy: fixture.runtimePolicy,
      });
      const spawns = yield* fixture.readSpawns;
      assert.lengthOf(spawns, 1);
      const spawn = spawns[0]!;
      assertNativeMcpArgs(spawn.args, fixture.mcpBridgeArgs);
      assert.equal(spawn.node, process.execPath);
      assert.equal(spawn.entrypoint, fixture.mcpBridgeArgs[0]);
      assert.equal(spawn.providerVariable, "private-provider-setting");
      assert.equal(spawn.endpoint, "http://127.0.0.1:43123/mcp");
      assert.equal(spawn.authorization, "******");
      assert.notInclude(spawn.args.join("\n"), "fixture-scoped-authorization");
      assert.notInclude(spawn.args.join("\n"), "http://127.0.0.1:43123/mcp");
      const requests = yield* fixture.readRequests;
      assert.deepEqual(
        requests.find((request) => request.method === "session/new")?.params?.mcpServers,
        [
          {
            type: "stdio",
            name: "t3-code",
            command: process.execPath,
            args: fixture.mcpBridgeArgs,
            env: [
              { name: "ELECTRON_RUN_AS_NODE", value: "1" },
              { name: "T3_ACP_MCP_ENDPOINT", value: "http://127.0.0.1:43123/mcp" },
              { name: "T3_ACP_MCP_AUTHORIZATION", value: "******" },
            ],
          },
        ],
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("does not inject native MCP without T3 context", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-copilot-without-mcp");
      const fixture = yield* makeMcpLaunchFixture("github-copilot-cli");
      yield* fixture.adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("session-copilot-without-mcp"),
        modelSelection: fixture.modelSelection,
        runtimePolicy: fixture.runtimePolicy,
      });
      const spawns = yield* fixture.readSpawns;
      assert.lengthOf(spawns, 1);
      assert.deepEqual(spawns[0]!.args, existingAgentArgs);
      assert.equal(spawns[0]!.providerVariable, "private-provider-setting");
      assert.equal(spawns[0]!.endpoint, "http://127.0.0.1:1/stale");
      assert.equal(spawns[0]!.authorization, "stale-provider-authorization");
      const requests = yield* fixture.readRequests;
      assert.deepEqual(
        requests.find((request) => request.method === "session/new")?.params?.mcpServers,
        [],
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("keeps other registry agents on the normal ACP MCP configuration", () =>
    Effect.gen(function* () {
      const fixture = yield* makeMcpLaunchFixture("fixture-agent");
      const threadId = ThreadId.make("thread-other-agent-mcp");
      const authorization = "other-agent-credential";
      const endpoint = "http://127.0.0.1:43124/mcp";
      yield* fixture.registerMcp(threadId, authorization, endpoint);
      yield* fixture.adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("session-other-agent-mcp"),
        modelSelection: fixture.modelSelection,
        runtimePolicy: fixture.runtimePolicy,
      });
      const spawns = yield* fixture.readSpawns;
      assert.lengthOf(spawns, 1);
      assert.deepEqual(spawns[0]!.args, existingAgentArgs);
      assert.equal(spawns[0]!.authorization, authorization);
      assert.equal(spawns[0]!.endpoint, endpoint);
      const requests = yield* fixture.readRequests;
      assert.deepEqual(
        requests.find((request) => request.method === "session/new")?.params?.mcpServers,
        [
          {
            type: "stdio",
            name: "t3-code",
            command: process.execPath,
            args: fixture.mcpBridgeArgs,
            env: [
              { name: "ELECTRON_RUN_AS_NODE", value: "1" },
              { name: "T3_ACP_MCP_ENDPOINT", value: endpoint },
              { name: "T3_ACP_MCP_AUTHORIZATION", value: authorization },
            ],
          },
        ],
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("refreshes inherited MCP credentials when cold-resuming a Copilot session", () =>
    Effect.gen(function* () {
      const fixture = yield* makeMcpLaunchFixture("github-copilot-cli");
      const threadId = ThreadId.make("thread-copilot-cold-resume");
      const selection = {
        threadId,
        modelSelection: fixture.modelSelection,
        runtimePolicy: fixture.runtimePolicy,
      };
      yield* fixture.registerMcp(threadId, "first-session-credential");
      const providerThread = yield* Effect.gen(function* () {
        const runtime = yield* fixture.adapter.openSession({
          ...selection,
          providerSessionId: ProviderSessionId.make("session-copilot-before-resume"),
        });
        return yield* runtime.ensureThread(selection);
      }).pipe(Effect.scoped);
      const nativeThreadId = providerThread.nativeThreadRef?.nativeId;
      if (nativeThreadId === null || nativeThreadId === undefined) {
        assert.fail("Expected a persisted native session ID");
      }
      const endpoint = "http://127.0.0.1:43125/mcp";
      yield* fixture.registerMcp(threadId, "resumed-session-credential", endpoint);
      yield* fixture.adapter.openSession({
        ...selection,
        providerSessionId: ProviderSessionId.make("session-copilot-after-resume"),
        initialNativeThreadId: nativeThreadId,
      });
      const spawns = yield* fixture.readSpawns;
      assert.lengthOf(spawns, 2);
      for (const spawn of spawns) assertNativeMcpArgs(spawn.args, fixture.mcpBridgeArgs);
      assert.equal(spawns[0]!.authorization, "first-session-credential");
      assert.equal(spawns[1]!.authorization, "resumed-session-credential");
      assert.equal(spawns[1]!.endpoint, endpoint);
      const requests = yield* fixture.readRequests;
      assert.deepInclude(requests.find((request) => request.method === "session/resume")?.params, {
        sessionId: nativeThreadId,
      });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("uses the target thread's MCP credentials for a Copilot replacement process", () =>
    Effect.gen(function* () {
      const fixture = yield* makeMcpLaunchFixture("github-copilot-cli");
      const sourceThreadId = ThreadId.make("thread-copilot-replacement-source");
      const targetThreadId = ThreadId.make("thread-copilot-replacement-target");
      yield* fixture.registerMcp(sourceThreadId, "source-thread-credential");
      yield* fixture.registerMcp(
        targetThreadId,
        "target-thread-credential",
        "http://127.0.0.1:43126/mcp",
      );
      const selection = {
        threadId: sourceThreadId,
        modelSelection: fixture.modelSelection,
        runtimePolicy: fixture.runtimePolicy,
      };
      const runtime = yield* fixture.adapter.openSession({
        ...selection,
        providerSessionId: ProviderSessionId.make("session-copilot-replacement"),
      });
      const providerThread = yield* runtime.ensureThread(selection);
      yield* runtime.rollbackThread({
        providerThread: { ...providerThread, appThreadId: targetThreadId },
        providerThreadTurns: [],
        target: {
          type: "thread_start",
          checkpointId: CheckpointId.make("checkpoint-copilot-replacement"),
          appRunOrdinal: 0,
        },
      });
      const spawns = yield* fixture.readSpawns;
      assert.lengthOf(spawns, 2);
      for (const spawn of spawns) assertNativeMcpArgs(spawn.args, fixture.mcpBridgeArgs);
      assert.equal(spawns[0]!.authorization, "source-thread-credential");
      assert.equal(spawns[1]!.authorization, "target-thread-credential");
      assert.equal(spawns[1]!.endpoint, "http://127.0.0.1:43126/mcp");
      const requests = yield* fixture.readRequests;
      assert.lengthOf(
        requests.filter((request) => request.method === "session/new"),
        2,
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it("is registered as a generic provider driver with schema defaults", () => {
    assert.isTrue(BUILT_IN_PROVIDER_ADAPTER_DRIVER_KINDS_V2.has(ACP_REGISTRY_PROVIDER));
    assert.equal(AcpRegistryAdapterV2Driver.driverKind, ACP_REGISTRY_PROVIDER);
    assert.deepEqual(AcpRegistryAdapterV2Driver.defaultConfig(), {
      enabled: true,
      agentId: "",
      commandPath: "",
      authMethodId: "",
      distribution: "auto",
      customModels: [],
    });
  });

  it.effect("opens a real ACP child process resolved from registry configuration", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const idAllocator = yield* IdAllocatorV2;
      const path = yield* Path.Path;
      const serverConfig = yield* ServerConfig;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      const resolver = yield* makeAcpRegistryCatalog({
        cacheDir: serverConfig.providerStatusCacheDir,
        registryUrl,
      });
      const settings = yield* decodeAcpRegistryAdapterSettings({
        agentId: "fixture-agent",
        commandPath: process.execPath,
        authMethodId: "test",
      });
      let startupActive = false;
      let startupCount = 0;
      const instanceId = ProviderInstanceId.make("acp-registry-fixture");
      const commandsPublished = yield* Deferred.make<{
        readonly instanceId: ProviderInstanceId;
        readonly commands: AcpRegistryAvailableCommands;
      }>();
      const configurationPublished = yield* Deferred.make<AcpRegistryLiveConfiguration>();
      const adapter = makeAcpRegistryAdapterV2({
        crypto: yield* Crypto.Crypto,
        instanceId,
        settings,
        environment: {
          T3_ACP_SESSION_LIFECYCLE: "1",
          T3_ACP_COMMAND_ADVERTISEMENT_DELAY_MS: "750",
        },
        childProcessSpawner,
        fileSystem,
        idAllocator,
        runtimeCoordinator: {
          withForegroundStartup: (agentId, effect) =>
            Effect.acquireUseRelease(
              Effect.sync(() => {
                assert.equal(agentId, "fixture-agent");
                startupActive = true;
                startupCount += 1;
              }),
              () => effect,
              () =>
                Effect.sync(() => {
                  startupActive = false;
                }),
            ),
          runBackgroundProbe: (_agentId, effect) => effect.pipe(Effect.map(Option.some)),
          withSessionMutation: (effect) => effect,
          clearAvailableCommands: () => Effect.void,
          publishAvailableCommands: (publishedInstanceId, commands) =>
            Deferred.succeed(commandsPublished, {
              instanceId: publishedInstanceId,
              commands,
            }).pipe(Effect.asVoid),
          getAvailableCommands: () => Effect.succeed(Option.none()),
          watchAvailableCommands: () => Effect.never,
          clearLiveConfiguration: () => Effect.void,
          publishLiveConfiguration: (_publishedInstanceId, configuration) =>
            Deferred.succeed(configurationPublished, configuration).pipe(Effect.asVoid),
          getLiveConfiguration: () => Effect.succeed(Option.none()),
          watchLiveConfiguration: () => Effect.never,
          requestUrlAuthentication: () => Effect.succeed(false),
          acceptUrlAuthentication: () => Effect.succeed(false),
          getUrlAuthAction: () => Effect.succeed(Option.none()),
          watchUrlAuthAction: () => Effect.never,
        },
        resolver: {
          resolve: (configuredSettings, cwd, environment) =>
            Effect.sync(() => assert.isTrue(startupActive)).pipe(
              Effect.andThen(resolver.resolve(configuredSettings, cwd, environment)),
              Effect.map((resolved) => ({
                ...resolved,
                spawn: {
                  ...resolved.spawn,
                  args: [mockAgentPath],
                },
              })),
            ),
        },
        serverConfig,
      });
      const threadId = ThreadId.make("thread-acp-registry-fixture");
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: process.cwd(),
      });
      const modelSelection = { instanceId, model: "default" } as const;
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-acp-registry-fixture"),
        modelSelection,
        runtimePolicy,
      });
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy,
      });

      assert.equal(runtime.providerSession.driver, "acpRegistry");
      assert.equal(startupCount, 1);
      assert.isFalse(startupActive);
      assert.equal(providerThread.nativeThreadRef?.nativeId, "mock-session-1");
      assert.equal(providerThread.nativeMetadata?.itemIdentityVersion, 2);
      assert.isTrue(runtime.providerSession.capabilities.threads.canReadThreadSnapshot);
      assert.isTrue(runtime.providerSession.capabilities.threads.canForkThread);
      assert.deepEqual(yield* Deferred.await(commandsPublished), {
        instanceId,
        commands: {
          slashCommands: [
            {
              name: "review",
              description: "Review the current changes",
              input: { hint: "focus" },
            },
          ],
          skills: [
            {
              name: "workspace-skill",
              description: "Run the workspace skill",
              path: "acp://skill/workspace-skill",
              scope: "agent",
              enabled: true,
            },
          ],
        },
      });
      const configuration = yield* Deferred.await(configurationPublished);
      assert.equal(configuration.currentModelId, "default");
      assert.deepInclude(configuration.models[0], {
        id: "default",
        name: "Auto",
        description: null,
      });
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );
});
