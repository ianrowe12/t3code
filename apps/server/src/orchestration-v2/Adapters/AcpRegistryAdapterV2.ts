import {
  normalizeDevinSessionUpdate,
  normalizeDevinToolCall,
  extractDevinSubagentUpdate,
} from "./DevinAcp.ts";
import { extractCopilotSubagentEndNotice, extractCopilotSubagentUpdates } from "./CopilotAcp.ts";
import {
  AcpRegistrySettings,
  defaultInstanceIdForDriver,
  ProviderDriverKind,
} from "@t3tools/contracts";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as EffectAcpErrors from "effect-acp/errors";

import { ServerConfig } from "../../config.ts";
import {
  normalizeAcpRegistryCommands,
  normalizeAcpRegistryLiveConfiguration,
  normalizeAcpRegistryWebUrl,
} from "../../provider/acp/AcpRegistryProbe.ts";
import { AcpRegistryCatalog } from "../../provider/acp/AcpRegistrySupport.ts";
import { AcpRegistryRuntimeCoordinator } from "../../provider/acp/AcpRegistryRuntimeCoordinator.ts";
import * as AcpSessionRuntime from "../../provider/acp/AcpSessionRuntime.ts";
import {
  copilotCompletionCapabilities,
  copilotPromptFinalAnswer,
  makeCopilotPromptCompletionRuntime,
} from "../../provider/acp/CopilotPromptCompletion.ts";
import { makeAcpNativeLoggerFactory } from "../../provider/acp/AcpNativeLogging.ts";
import { ProviderEventLoggers } from "../../provider/Layers/ProviderEventLoggers.ts";
import { mergeProviderInstanceEnvironment } from "../../provider/ProviderInstanceEnvironment.ts";
import { IdAllocatorV2 } from "../IdAllocator.ts";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "../ProviderAdapterDriver.ts";
import {
  AcpProviderCapabilitiesV2,
  makeAcpAdapterV2,
  type AcpAdapterV2Flavor,
  type AcpAdapterV2RuntimeInput,
} from "./AcpAdapterV2.ts";

export const ACP_REGISTRY_PROVIDER = ProviderDriverKind.make("acpRegistry");
export const ACP_REGISTRY_DEFAULT_INSTANCE_ID = defaultInstanceIdForDriver(ACP_REGISTRY_PROVIDER);

const DEFAULT_ACP_REGISTRY_SETTINGS = Schema.decodeSync(AcpRegistrySettings)({});
const CopilotMcpServer = Schema.Struct({
  type: Schema.optionalKey(Schema.Literal("stdio")),
  command: Schema.String,
  args: Schema.optionalKey(Schema.Array(Schema.String)),
});
const isCopilotMcpServer = Schema.is(CopilotMcpServer);
const encodeCopilotMcpConfig = Schema.encodeSync(
  Schema.fromJsonString(
    Schema.Struct({
      mcpServers: Schema.Struct({
        "t3-code": Schema.Struct({
          ...CopilotMcpServer.fields,
          env: Schema.Struct({ ELECTRON_RUN_AS_NODE: Schema.Literal("1") }),
        }),
      }),
    }),
  ),
);
const isAcpRequestError = Schema.is(EffectAcpErrors.AcpRequestError);
const isCopilotSessionErrorData = Schema.is(Schema.Struct({ details: Schema.String }));

function copilotLostSessionDetails(error: unknown): string | undefined {
  if (
    isAcpRequestError(error) &&
    error.method === "session/prompt" &&
    error.code === -32603 &&
    isCopilotSessionErrorData(error.data) &&
    error.data.details.startsWith(
      "Request session.send failed with message: session event delivery failed: session not found: ",
    )
  ) {
    return error.data.details;
  }
  return undefined;
}

export interface AcpRegistryAdapterV2Options {
  readonly instanceId: Parameters<typeof makeAcpAdapterV2>[0]["instanceId"];
  readonly settings: AcpRegistrySettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly crypto: Crypto.Crypto;
  readonly fileSystem: FileSystem.FileSystem;
  readonly idAllocator: IdAllocatorV2["Service"];
  readonly resolver: Pick<AcpRegistryCatalog["Service"], "resolve">;
  readonly runtimeCoordinator?: AcpRegistryRuntimeCoordinator["Service"];
  readonly serverConfig: ServerConfig["Service"];
  readonly nativeLogging?: Parameters<typeof makeAcpAdapterV2>[0]["nativeLogging"];
  readonly makeRuntime?: (
    input: AcpAdapterV2RuntimeInput,
  ) => Effect.Effect<
    AcpSessionRuntime.AcpSessionRuntime["Service"],
    EffectAcpErrors.AcpError,
    Crypto.Crypto | Scope.Scope
  >;
  readonly assertComplete?: Effect.Effect<void, EffectAcpErrors.AcpError>;
}

function makeAcpRegistryRuntime(options: AcpRegistryAdapterV2Options) {
  return (
    input: AcpAdapterV2RuntimeInput,
  ): Effect.Effect<
    AcpSessionRuntime.AcpSessionRuntime["Service"],
    EffectAcpErrors.AcpError,
    Crypto.Crypto | Scope.Scope
  > =>
    Effect.gen(function* () {
      const { processEnvironment, ...runtimeInput } = input;
      const resolved = yield* options.resolver
        .resolve(options.settings, input.cwd, options.environment)
        .pipe(
          Effect.mapError(
            (cause) =>
              new EffectAcpErrors.AcpSpawnError({
                command: options.settings.agentId || ACP_REGISTRY_PROVIDER,
                cause,
              }),
          ),
        );
      const t3McpServer =
        resolved.agent.id === "github-copilot-cli"
          ? runtimeInput.mcpServers.find((server) => server.name === "t3-code")
          : undefined;
      // Copilot 1.0.83 drops session-supplied MCP servers, but its native CLI
      // accepts additional config. Credentials stay in the inherited environment.
      const spawn = {
        ...resolved.spawn,
        ...(t3McpServer !== undefined && isCopilotMcpServer(t3McpServer)
          ? {
              args: [
                ...resolved.spawn.args,
                "--additional-mcp-config",
                encodeCopilotMcpConfig({
                  mcpServers: {
                    "t3-code": {
                      command: t3McpServer.command,
                      ...(t3McpServer.args === undefined ? {} : { args: t3McpServer.args }),
                      env: { ELECTRON_RUN_AS_NODE: "1" },
                    },
                  },
                }),
              ],
            }
          : {}),
        ...(processEnvironment === undefined
          ? {}
          : { env: { ...resolved.spawn.env, ...processEnvironment } }),
      };
      const context = yield* Layer.build(
        AcpSessionRuntime.layer({
          ...runtimeInput,
          spawn,
          ...(resolved.agent.id === "github-copilot-cli"
            ? { cancelBehavior: "wait-for-prompt" as const }
            : {}),
          ...(options.settings.authMethodId ? { authMethodId: options.settings.authMethodId } : {}),
        }).pipe(
          Layer.provide(
            Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, options.childProcessSpawner),
          ),
        ),
      );
      const runtime = yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
        Effect.provide(context),
      );
      return resolved.agent.id === "github-copilot-cli"
        ? yield* makeCopilotPromptCompletionRuntime(
            runtime,
            runtimeInput.onTermination(
              new EffectAcpErrors.AcpRequestError({
                code: -32603,
                errorMessage: "Copilot session recovery failed; a replacement runtime is required.",
              }),
            ),
          )
        : runtime;
    });
}

export function makeAcpRegistryAdapterV2(options: AcpRegistryAdapterV2Options) {
  const runtimeCoordinator = options.runtimeCoordinator;
  const isDevin = options.settings.agentId === "devin";
  const isCopilot = options.settings.agentId === "github-copilot-cli";
  const flavor: AcpAdapterV2Flavor = {
    driver: ACP_REGISTRY_PROVIDER,
    capabilities: AcpProviderCapabilitiesV2,
    ...(isCopilot
      ? {
          clientCapabilitiesMeta: copilotCompletionCapabilities,
          extractSubagentUpdates: extractCopilotSubagentUpdates,
          extractSubagentEndNotice: extractCopilotSubagentEndNotice,
          retainSubagentsAcrossTurns: true,
          extractPromptFinalAnswer: copilotPromptFinalAnswer,
          restartRuntimeOnPromptError: (error: unknown) =>
            copilotLostSessionDetails(error) !== undefined,
          formatPromptError: (error: unknown) => {
            const details = copilotLostSessionDetails(error);
            return details === undefined
              ? undefined
              : `${details}\nSend your message again to reload the saved conversation in a new provider process.`;
          },
        }
      : {}),
    ...(isDevin
      ? {
          clientCapabilitiesMeta: {
            "cognition.ai/subagentSupport": true,
            "cognition.ai/messageGrouping": true,
          },
          normalizeSessionUpdate: normalizeDevinSessionUpdate,
          normalizeToolCall: normalizeDevinToolCall,
          extractSubagentUpdate: extractDevinSubagentUpdate,
        }
      : {}),
    makeRuntime: options.makeRuntime ?? makeAcpRegistryRuntime(options),
    ...(runtimeCoordinator === undefined
      ? {}
      : {
          onAvailableCommandsUpdate: (commands) =>
            runtimeCoordinator.publishAvailableCommands(
              options.instanceId,
              normalizeAcpRegistryCommands(commands),
            ),
          onSessionConfigurationUpdate: (configOptions, modeState) =>
            runtimeCoordinator.publishLiveConfiguration(
              options.instanceId,
              normalizeAcpRegistryLiveConfiguration(configOptions, modeState),
            ),
          onUrlElicitation: ({ elicitationId, url, message }) => {
            const normalizedUrl = normalizeAcpRegistryWebUrl(url);
            if (normalizedUrl === undefined || elicitationId.trim().length === 0) {
              return Effect.succeed(false);
            }
            return runtimeCoordinator.requestUrlAuthentication(options.instanceId, {
              elicitationId: elicitationId.trim().slice(0, 256),
              url: normalizedUrl,
              message: message.trim().slice(0, 1_024),
            });
          },
          withRuntimeStartup: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
            runtimeCoordinator.withForegroundStartup(options.settings.agentId, effect),
        }),
    ...(options.assertComplete === undefined ? {} : { assertComplete: options.assertComplete }),
  };
  return makeAcpAdapterV2({
    instanceId: options.instanceId,
    flavor,
    crypto: options.crypto,
    fileSystem: options.fileSystem,
    idAllocator: options.idAllocator,
    serverConfig: options.serverConfig,
    clientTerminals: {
      childProcessSpawner: options.childProcessSpawner,
      environment: options.environment,
      shellCommands: isDevin,
    },
    ...(options.nativeLogging === undefined ? {} : { nativeLogging: options.nativeLogging }),
  });
}

export type AcpRegistryAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | AcpRegistryCatalog
  | IdAllocatorV2
  | ProviderEventLoggers
  | ServerConfig;

export const AcpRegistryAdapterV2Driver: ProviderAdapterDriver<
  AcpRegistrySettings,
  AcpRegistryAdapterV2DriverEnv
> = {
  driverKind: ACP_REGISTRY_PROVIDER,
  configSchema: AcpRegistrySettings,
  defaultConfig: (): AcpRegistrySettings => DEFAULT_ACP_REGISTRY_SETTINGS,
  create: Effect.fn("AcpRegistryAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<AcpRegistrySettings>) {
      const hostEnvironment = yield* HostProcessEnvironment;
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const crypto = yield* Crypto.Crypto;
      const fileSystem = yield* FileSystem.FileSystem;
      const idAllocator = yield* IdAllocatorV2;
      const providerEventLoggers = yield* ProviderEventLoggers;
      const serverConfig = yield* ServerConfig;
      const makeNativeLogger = yield* makeAcpNativeLoggerFactory();
      const resolver = yield* AcpRegistryCatalog;
      const runtimeCoordinator = yield* Effect.serviceOption(AcpRegistryRuntimeCoordinator);
      return makeAcpRegistryAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
        childProcessSpawner,
        crypto,
        fileSystem,
        idAllocator,
        resolver,
        ...(Option.isSome(runtimeCoordinator)
          ? { runtimeCoordinator: runtimeCoordinator.value }
          : {}),
        serverConfig,
        nativeLogging: (threadId) =>
          makeNativeLogger({
            nativeEventLogger: providerEventLoggers.native,
            provider: ACP_REGISTRY_PROVIDER,
            threadId,
          }),
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: ACP_REGISTRY_PROVIDER,
              instanceId: input.instanceId,
              detail: "Failed to create ACP Registry adapter.",
              cause,
            }),
        ),
      ),
  ),
};
