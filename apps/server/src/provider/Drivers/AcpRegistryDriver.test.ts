import {
  AcpRegistryOperationError,
  AcpRegistrySettings,
  ProviderInstanceId,
  type TextGenerationError,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import * as ServerConfig from "../../config.ts";
import * as IdAllocator from "../../orchestration-v2/IdAllocator.ts";
import * as ServerSettings from "../../serverSettings.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import type { TextGeneration } from "../../textGeneration/TextGeneration.ts";
import { NoOpProviderEventLoggers, ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { AcpRegistryCatalog, type AcpRegistryInspection } from "../acp/AcpRegistrySupport.ts";
import {
  acpRegistrySnapshotReadiness,
  AcpRegistryDriver,
  applyAcpRegistryAvailableCommands,
  applyAcpRegistryLiveConfiguration,
  buildCheckedAcpRegistrySnapshot,
  checkAcpRegistryProviderReadiness,
  checkAcpRegistryProviderStatus,
} from "./AcpRegistryDriver.ts";

const decodeSettings = Schema.decodeSync(AcpRegistrySettings);
const identity = {
  instanceId: ProviderInstanceId.make("acpRegistry_test"),
  displayName: "Test ACP",
  accentColor: undefined,
  continuationKey: "acpRegistry:instance:acpRegistry_test",
};
const noSessionManagement = {
  canList: false,
  canLoad: false,
  canResume: false,
  canLogout: false,
  canDelete: false,
  canConfigureProviders: false,
} as const;

function catalogWithInspection(inspection: AcpRegistryInspection): AcpRegistryCatalog["Service"] {
  return {
    search: () => Effect.die("unused search"),
    prepare: () => Effect.die("unused prepare"),
    inspect: () => Effect.succeed(inspection),
    resolve: () => Effect.die("unused resolve"),
    uninstallManagedBinary: () => Effect.die("unused uninstall"),
  };
}

describe("acpRegistrySnapshotReadiness", () => {
  it.each(["github-copilot-cli", "other-agent"])(
    "publishes only user-selectable options for %s after discovery and live refresh",
    (agentId) => {
      const configOptions = [
        {
          id: "agent",
          label: "Agent",
          type: "select" as const,
          currentValue: "researcher",
          options: [
            { id: "", label: "Copilot" },
            { id: "researcher", label: "Researcher" },
          ],
        },
        {
          id: "reasoning_effort",
          label: "Reasoning Effort",
          type: "select" as const,
          currentValue: "max",
          options: [{ id: "max", label: "Max" }],
        },
      ];
      const configuration = {
        models: [{ id: "gpt-6-astra", name: "GPT-6 Astra", description: null }],
        currentModelId: "gpt-6-astra",
        configOptions,
      };
      const provider = buildCheckedAcpRegistrySnapshot({
        ...identity,
        settings: decodeSettings({ agentId }),
        checkedAt: "2026-09-21T20:00:00.000Z",
        inspection: { status: "ready", agentId, version: "1.0.87", distribution: "npx" },
        probe: {
          probe: {
            ...configuration,
            instanceId: identity.instanceId,
            ready: true,
            icon: null,
            authMethods: [],
            sessionManagement: noSessionManagement,
          },
          slashCommands: [],
          skills: [],
        },
      });
      for (const snapshot of [
        provider,
        applyAcpRegistryLiveConfiguration(provider, configuration, [], agentId),
      ]) {
        expect(
          snapshot.models[0]?.capabilities?.optionDescriptors?.map((option) => option.id),
        ).toEqual(
          agentId === "github-copilot-cli" ? ["reasoning_effort"] : ["agent", "reasoning_effort"],
        );
        expect(snapshot.models[0]?.slug).toBe("gpt-6-astra");
      }
    },
  );

  it.effect("binds Copilot helpers to each instance without enabling unrelated ACP helpers", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        directory: process.cwd(),
        prefix: ".copilot-driver-test-",
      });
      const commandPath = writeFakeCli({
        directory,
        name: "configured-copilot",
        source: `
          import assert from "node:assert/strict";
          const args = process.argv.slice(2);
          assert.equal(args.includes("--acp"), false);
          const model = args[args.indexOf("--model") + 1];
          assert.equal(process.env.COPILOT_GITHUB_TOKEN, "synthetic-" + model);
          const prompt = args[args.indexOf("--prompt") + 1];
          if (prompt.includes("Staged patch:")) console.log('{"subject":"Repair state","body":""}');
          else if (prompt.includes("Base branch:")) console.log('{"title":"Repair state","body":"Summary"}');
          else if (prompt.includes("key: branch")) console.log('{"branch":"repair-state"}');
          else console.log('{"title":"Repair state"}');
        `,
      });
      const catalog = AcpRegistryCatalog.of({
        ...catalogWithInspection({
          status: "unprepared",
          agentId: "github-copilot-cli",
          version: "1.0.83",
          distribution: "binary",
        }),
        resolve: (config, cwd, environment) => {
          expect(config.commandPath).toBe(commandPath);
          return Effect.succeed({
            agent: {
              id: "github-copilot-cli",
              name: "Copilot",
              version: "1.0.83",
              description: "",
              distribution: {},
            },
            distribution: "binary" as const,
            spawn: { command: commandPath, args: ["--acp"], cwd, env: environment ?? {} },
          });
        },
      });
      yield* Effect.gen(function* () {
        for (const [id, agentId] of [
          ["copilot_personal", "github-copilot-cli"],
          ["copilot_work", "github-copilot-cli"],
          ["gemini", "gemini-cli"],
        ] as const) {
          const instanceId = ProviderInstanceId.make(id);
          const instance = yield* AcpRegistryDriver.create({
            ...identity,
            instanceId,
            enabled: true,
            config: decodeSettings({ agentId, commandPath }),
            environment: [
              { name: "COPILOT_GITHUB_TOKEN", value: `synthetic-${id}`, sensitive: true },
            ],
          });
          const common = {
            cwd: "/untrusted-workspace",
            modelSelection: { instanceId, model: id },
          };
          const operations: ReadonlyArray<{
            readonly effect: Effect.Effect<
              Effect.Success<
                ReturnType<TextGeneration["Service"][keyof TextGeneration["Service"]]>
              >,
              TextGenerationError
            >;
            readonly expected: unknown;
          }> = [
            {
              effect: instance.textGeneration.generateThreadTitle({
                ...common,
                message: "Repair state",
              }),
              expected: { title: "Repair state" },
            },
            {
              effect: instance.textGeneration.generateBranchName({
                ...common,
                message: "Repair state",
              }),
              expected: { branch: "repair-state" },
            },
            {
              effect: instance.textGeneration.generateCommitMessage({
                ...common,
                branch: "main",
                stagedSummary: "",
                stagedPatch: "",
              }),
              expected: { subject: "Repair state", body: "" },
            },
            {
              effect: instance.textGeneration.generatePrContent({
                ...common,
                baseBranch: "main",
                headBranch: "fix",
                commitSummary: "",
                diffSummary: "",
                diffPatch: "",
              }),
              expected: { title: "Repair state", body: "Summary" },
            },
          ];
          for (const operation of operations) {
            if (agentId === "github-copilot-cli") {
              expect(yield* operation.effect).toEqual(operation.expected);
            } else {
              expect((yield* Effect.flip(operation.effect)).detail).toContain(
                "do not provide application text generation",
              );
            }
          }
        }
      }).pipe(
        Effect.provideService(AcpRegistryCatalog, catalog),
        Effect.provideService(ProviderEventLoggers, NoOpProviderEventLoggers),
        Effect.provideService(HostProcessEnvironment, {
          PATH: process.env.PATH,
          COPILOT_GITHUB_TOKEN: "synthetic-host-token",
        }),
        Effect.provide(
          Layer.mergeAll(
            ServerConfig.layerTest(directory, path.join(directory, "t3-home")),
            ServerSettings.layerTest(),
            IdAllocator.layer,
            Layer.mock(BackgroundPolicy.BackgroundPolicy)({
              shouldRunScopeWork: () => Effect.succeed(false),
            }),
          ),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it("offers app text generation only for the Copilot registry agent", () => {
    for (const agentId of ["github-copilot-cli", "gemini-cli", ""]) {
      const snapshot = buildCheckedAcpRegistrySnapshot({
        ...identity,
        settings: decodeSettings({ agentId }),
        checkedAt: "2026-09-13T00:00:00.000Z",
        inspection: { status: "ready", agentId, version: "1.0.83", distribution: "binary" },
      });
      expect(snapshot.supportsTextGeneration).toBe(agentId === "github-copilot-cli");
    }
  });

  it("treats a live empty command advertisement as an authoritative replacement", () => {
    const provider = buildCheckedAcpRegistrySnapshot({
      ...identity,
      settings: decodeSettings({ agentId: "test-agent" }),
      checkedAt: "2026-08-13T10:00:00.000Z",
      inspection: {
        status: "ready",
        agentId: "test-agent",
        version: "1.0.0",
        distribution: "npx",
      },
      probe: {
        probe: {
          instanceId: identity.instanceId,
          ready: true,
          icon: null,
          authMethods: [],
          models: [],
          currentModelId: null,
          configOptions: [],
          sessionManagement: noSessionManagement,
        },
        slashCommands: [{ name: "stale" }],
        skills: [{ name: "stale-skill", path: "stale", enabled: true }],
      },
    });

    const replaced = applyAcpRegistryAvailableCommands(
      provider,
      Option.some({ slashCommands: [], skills: [] }),
    );
    expect(replaced.slashCommands).toEqual([]);
    expect(replaced.skills).toEqual([]);
    expect(applyAcpRegistryAvailableCommands(provider, Option.none()).slashCommands).toEqual([
      { name: "stale" },
    ]);
    expect(provider.iconUrl).toBe(
      "https://cdn.agentclientprotocol.com/registry/v1/latest/test-agent.svg",
    );
  });

  it("overlays live configuration without dropping probe-owned session capabilities", () => {
    const provider = buildCheckedAcpRegistrySnapshot({
      ...identity,
      settings: decodeSettings({ agentId: "test-agent" }),
      checkedAt: "2026-08-13T10:00:00.000Z",
      inspection: {
        status: "ready",
        agentId: "test-agent",
        version: "1.0.0",
        distribution: "npx",
      },
      probe: {
        probe: {
          instanceId: identity.instanceId,
          ready: true,
          icon: null,
          authMethods: [],
          models: [{ id: "probe-model", name: "Probe model", description: null }],
          currentModelId: "probe-model",
          configOptions: [],
          sessionManagement: {
            canList: true,
            canLoad: true,
            canResume: true,
            canLogout: true,
            canDelete: true,
            canConfigureProviders: true,
          },
        },
        slashCommands: [],
        skills: [],
      },
    });

    expect(
      applyAcpRegistryLiveConfiguration(
        provider,
        {
          models: [{ id: "live-model", name: "Live model", description: null }],
          currentModelId: "live-model",
          configOptions: [],
        },
        [],
      ),
    ).toMatchObject({
      auth: { status: "authenticated", canLogout: true },
      nativeSessions: { canList: true, canLoad: true, canResume: true },
      models: [{ slug: "live-model", isDefault: true }],
    });
  });

  it("keeps the discovered catalog when a live update reports no model selector", () => {
    const configOptions = [
      {
        id: "reasoning_effort",
        label: "Reasoning Effort",
        type: "select" as const,
        currentValue: "xhigh",
        options: [{ id: "xhigh", label: "Extra High" }],
      },
    ];
    const provider = buildCheckedAcpRegistrySnapshot({
      ...identity,
      settings: decodeSettings({ agentId: "github-copilot-cli" }),
      checkedAt: "2026-09-24T18:12:36.340Z",
      inspection: {
        status: "ready",
        agentId: "github-copilot-cli",
        version: "1.0.83",
        distribution: "npx",
      },
      probe: {
        probe: {
          instanceId: identity.instanceId,
          ready: true,
          icon: null,
          authMethods: [],
          models: [
            { id: "auto", name: "Auto", description: null },
            { id: "claude-opus-5.5", name: "Claude Opus 5.5", description: null },
          ],
          currentModelId: "auto",
          configOptions,
          sessionManagement: noSessionManagement,
        },
        slashCommands: [],
        skills: [],
      },
    });

    const refreshed = applyAcpRegistryLiveConfiguration(
      provider,
      { models: [], currentModelId: null, configOptions: [] },
      [],
      "github-copilot-cli",
    );

    expect(refreshed.models.map((model) => model.slug)).toEqual(["auto", "claude-opus-5.5"]);
    expect(refreshed.models[0]?.isDefault).toBe(true);
    expect(
      refreshed.models[1]?.capabilities?.optionDescriptors?.map((option) => option.id),
    ).toEqual(["reasoning_effort"]);
  });

  it("maps registry inspection status to provider readiness", () => {
    expect(
      acpRegistrySnapshotReadiness({
        status: "ready",
        agentId: "gemini-cli",
        version: "1.2.3",
        distribution: "npx",
      }),
    ).toEqual({ installed: true, version: "1.2.3", status: "ready" });

    expect(
      acpRegistrySnapshotReadiness({
        status: "missing_runner",
        agentId: "gemini-cli",
        version: "1.2.3",
        distribution: "npx",
        runner: "npx",
      }),
    ).toMatchObject({ installed: false, version: "1.2.3", status: "error" });

    expect(
      acpRegistrySnapshotReadiness({
        status: "unprepared",
        agentId: "zed-agent",
        version: "2.0.0",
        distribution: "binary",
      }),
    ).toMatchObject({ installed: false, version: "2.0.0", status: "warning" });

    expect(
      acpRegistrySnapshotReadiness({ status: "failed", message: "Registry unavailable." }),
    ).toEqual({
      installed: false,
      version: null,
      status: "error",
      message: "Registry unavailable.",
    });
  });

  it("projects authenticated probes, discovered models, custom models, and commands", () => {
    const snapshot = buildCheckedAcpRegistrySnapshot({
      ...identity,
      settings: decodeSettings({
        agentId: "test-agent",
        customModels: [" custom-model ", "gpt-discovered"],
      }),
      checkedAt: "2026-08-13T10:00:00.000Z",
      inspection: {
        status: "ready",
        agentId: "test-agent",
        version: "1.0.0",
        distribution: "npx",
      },
      probe: {
        probe: {
          instanceId: identity.instanceId,
          ready: true,
          icon: null,
          authMethods: [],
          models: [{ id: "gpt-discovered", name: "GPT Discovered", description: null }],
          currentModelId: "gpt-discovered",
          configOptions: [],
          sessionManagement: noSessionManagement,
        },
        slashCommands: [{ name: "plan", description: "Create a plan", input: { hint: "topic" } }],
        skills: [{ name: "workspace-skill", path: "acp://skill/workspace-skill", enabled: true }],
      },
    });

    expect(snapshot.auth).toEqual({ status: "authenticated", canLogout: false });
    expect(snapshot.supportsTextGeneration).toBe(false);
    expect(
      snapshot.models.map(({ slug, name, isCustom, isDefault }) => ({
        slug,
        name,
        isCustom,
        isDefault,
      })),
    ).toEqual([
      {
        slug: "gpt-discovered",
        name: "GPT Discovered",
        isCustom: false,
        isDefault: true,
      },
      {
        slug: "custom-model",
        name: "custom-model",
        isCustom: true,
        isDefault: undefined,
      },
    ]);
    expect(snapshot.slashCommands).toEqual([
      { name: "plan", description: "Create a plan", input: { hint: "topic" } },
    ]);
    expect(snapshot.skills).toEqual([
      { name: "workspace-skill", path: "acp://skill/workspace-skill", enabled: true },
    ]);
  });

  it("keeps a default model only when the agent advertises none", () => {
    const snapshot = buildCheckedAcpRegistrySnapshot({
      ...identity,
      settings: decodeSettings({ agentId: "test-agent" }),
      checkedAt: "2026-08-13T10:00:00.000Z",
      inspection: {
        status: "ready",
        agentId: "test-agent",
        version: "1.0.0",
        distribution: "uvx",
      },
      probe: {
        probe: {
          instanceId: identity.instanceId,
          ready: true,
          icon: null,
          authMethods: [],
          models: [],
          currentModelId: null,
          configOptions: [],
          sessionManagement: noSessionManagement,
        },
        slashCommands: [],
        skills: [],
      },
    });

    expect(snapshot.models.map((model) => model.slug)).toEqual(["default"]);
    expect(snapshot.models[0]?.isDefault).toBe(true);
  });

  it("reports failed authentication without hiding successful local inspection", () => {
    const snapshot = buildCheckedAcpRegistrySnapshot({
      ...identity,
      settings: decodeSettings({ agentId: "test-agent", authMethodId: "grok-login" }),
      checkedAt: "2026-08-13T10:00:00.000Z",
      inspection: {
        status: "ready",
        agentId: "test-agent",
        version: "1.0.0",
        distribution: "binary",
      },
      probeError: new AcpRegistryOperationError({
        reason: "authentication_failed",
        message: "Login required.",
        authMethods: [
          {
            id: "api-key",
            name: "API key",
            description: null,
            type: "env_var",
          },
          {
            id: "grok-login",
            name: "Log in with Grok",
            description: null,
            type: "agent",
          },
        ],
      }),
    });

    expect(snapshot).toMatchObject({
      installed: true,
      version: "1.0.0",
      status: "warning",
      auth: {
        status: "unauthenticated",
        type: "agent",
        label: "Log in with Grok",
      },
      message:
        'Complete the advertised "Log in with Grok" authentication method on the server. T3 Code will detect it automatically on the next provider refresh.',
    });
  });

  it.effect("runs the disposable probe only after local inspection is ready", () =>
    Effect.gen(function* () {
      const settings = decodeSettings({ agentId: "test-agent" });
      const environment = { PATH: "/provider/bin" };
      let receivedEnvironment: NodeJS.ProcessEnv | undefined;
      const snapshot = yield* checkAcpRegistryProviderStatus(
        {
          ...identity,
          settings,
          cwd: "/workspace",
          environment,
        },
        (input) =>
          Effect.sync(() => {
            receivedEnvironment = input.environment;
            return {
              probe: {
                instanceId: identity.instanceId,
                ready: true as const,
                icon: null,
                authMethods: [],
                models: [{ id: "agent-model", name: "Agent Model", description: null }],
                currentModelId: "agent-model",
                configOptions: [],
                sessionManagement: noSessionManagement,
              },
              slashCommands: [{ name: "review" }],
              skills: [],
            };
          }),
      ).pipe(
        Effect.provideService(
          AcpRegistryCatalog,
          catalogWithInspection({
            status: "ready",
            agentId: "test-agent",
            version: "1.0.0",
            distribution: "npx",
          }),
        ),
      );

      expect(receivedEnvironment).toBe(environment);
      expect(snapshot).toMatchObject({
        auth: { status: "authenticated" },
        models: [{ slug: "agent-model" }],
        slashCommands: [{ name: "review" }],
        skills: [],
      });
    }),
  );

  it.effect("publishes concrete local readiness before background ACP discovery", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkAcpRegistryProviderReadiness({
        ...identity,
        settings: decodeSettings({ agentId: "test-agent" }),
        environment: { PATH: "/provider/bin" },
      }).pipe(
        Effect.provideService(
          AcpRegistryCatalog,
          catalogWithInspection({
            status: "ready",
            agentId: "test-agent",
            version: "1.0.0",
            distribution: "npx",
          }),
        ),
      );

      expect(snapshot).toMatchObject({
        installed: true,
        status: "ready",
        version: "1.0.0",
        auth: { status: "unknown" },
        message: "Checking ACP authentication, models, and commands in the background...",
      });
    }),
  );
});
