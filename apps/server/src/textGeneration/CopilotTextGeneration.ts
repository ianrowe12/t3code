import {
  type AcpRegistrySettings,
  type ModelSelection,
  TextGenerationError,
} from "@t3tools/contracts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { extractJsonObject, fromLenientJson } from "@t3tools/shared/schemaJson";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { AcpRegistryCatalog } from "../provider/acp/AcpRegistrySupport.ts";
import type * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";

const MAX_OUTPUT_BYTES = 128_000;
const isTextGenerationError = Schema.is(TextGenerationError);
const Configuration = Schema.Record(Schema.String, Schema.Unknown);
const decodeConfiguration = Schema.decodeEffect(fromLenientJson(Configuration));
const encodeConfiguration = Schema.encodeSync(Schema.fromJsonString(Configuration));
const decodeFields = Schema.decodeUnknownEffect(Configuration);
const isTextLabel = Schema.is(Schema.String.check(Schema.isPattern(/[\p{L}\p{N}]/u)));
const isBranchLabel = Schema.is(Schema.String.check(Schema.isPattern(/[a-z0-9]/iu)));
const isReasoningEffort = Schema.is(
  Schema.Literals(["none", "minimal", "low", "medium", "high", "xhigh", "max"]),
);
const isContextTier = Schema.is(Schema.Literals(["default", "long_context"]));

// Never copy execution settings, MCP servers, plugins, hooks, or session history.
const AUTHENTICATION_KEYS = [
  "lastLoggedInUser",
  "loggedInUsers",
  "authTokens",
  "copilotTokens",
  "storeTokenPlaintext",
] as const;
const ENVIRONMENT_KEYS = [
  "PATH",
  "Path",
  "SystemRoot",
  "SYSTEMROOT",
  "COMSPEC",
  "PATHEXT",
  "LANG",
  "LC_ALL",
  "LC_CTYPE",
  "COPILOT_GITHUB_TOKEN",
  "GH_TOKEN",
  "GITHUB_TOKEN",
  "COPILOT_GH_HOST",
  "GH_HOST",
  "HTTP_PROXY",
  "HTTPS_PROXY",
  "ALL_PROXY",
  "NO_PROXY",
  "http_proxy",
  "https_proxy",
  "all_proxy",
  "no_proxy",
  "NODE_EXTRA_CA_CERTS",
  "SSL_CERT_FILE",
  "SSL_CERT_DIR",
] as const;

export interface CopilotTextGenerationOptions {
  readonly settings: AcpRegistrySettings;
  readonly environment: NodeJS.ProcessEnv;
  /** App-owned scratch root, never the conversation workspace or native Copilot home. */
  readonly helperDirectory: string;
}

/** Copilot's prompt CLI is a separate, tool-free helper, not an ACP conversation. */
export const makeCopilotTextGeneration = Effect.fn("makeCopilotTextGeneration")(function* (
  options: CopilotTextGenerationOptions,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const catalog = yield* AcpRegistryCatalog;

  const runJson = Effect.fn("CopilotTextGeneration.runJson")(
    function* <S extends Schema.Top>(input: {
      readonly operation: keyof TextGeneration.TextGeneration["Service"];
      readonly prompt: string;
      readonly outputSchema: S;
      readonly modelSelection: ModelSelection;
    }) {
      const fail = (detail: string) =>
        new TextGenerationError({ operation: input.operation, detail });
      if (!options.settings.enabled || options.settings.agentId !== "github-copilot-cli") {
        return yield* fail("Copilot application text generation is not enabled for this instance.");
      }
      const modelArgs: string[] = [];
      for (const option of input.modelSelection.options ?? []) {
        switch (option.id) {
          case "reasoningEffort":
          case "reasoning_effort":
            if (!isReasoningEffort(option.value)) {
              return yield* fail("Invalid Copilot reasoning effort for text generation.");
            }
            modelArgs.push("--reasoning-effort", option.value);
            break;
          case "context":
          case "contextWindow":
            if (!isContextTier(option.value)) {
              return yield* fail("Invalid Copilot context tier for text generation.");
            }
            modelArgs.push("--context", option.value);
            break;
          default:
            return yield* fail(
              `Copilot text generation does not support model option '${option.id}'.`,
            );
        }
      }
      yield* fs.makeDirectory(options.helperDirectory, { recursive: true, mode: 0o700 });
      const root = yield* fs.makeTempDirectoryScoped({
        directory: options.helperDirectory,
        prefix: "copilot-",
      });
      const cwd = path.join(root, "workspace");
      const home = path.join(root, "home");
      const copilotHome = path.join(home, ".copilot");
      yield* fs.makeDirectory(cwd);
      // Stop repository discovery at the helper, even when T3 home is inside a worktree.
      yield* fs.makeDirectory(path.join(cwd, ".git", "objects"), { recursive: true });
      yield* fs.makeDirectory(path.join(cwd, ".git", "refs"));
      yield* fs.writeFileString(path.join(cwd, ".git", "HEAD"), "ref: refs/heads/main\n");
      yield* fs.makeDirectory(copilotHome, { recursive: true, mode: 0o700 });

      const resolved = yield* catalog.resolve(options.settings, cwd, options.environment);
      // Registry launch arguments must not reintroduce remote sessions, tools or config.
      if (
        resolved.agent.id !== "github-copilot-cli" ||
        resolved.spawn.args.some((arg) => arg !== "--acp")
      ) {
        return yield* fail(
          "Copilot text generation requires a direct CLI launch with only the registry --acp argument.",
        );
      }
      const sourceEnvironment = resolved.spawn.env ?? options.environment;
      const userHome = sourceEnvironment.HOME?.trim() || sourceEnvironment.USERPROFILE?.trim();
      const sourceHome =
        sourceEnvironment.COPILOT_HOME?.trim() ||
        (userHome ? path.join(userHome, ".copilot") : undefined);
      if (sourceHome && !path.isAbsolute(sourceHome)) {
        return yield* fail("Copilot authentication configuration requires an absolute path.");
      }
      const configuration: Record<string, unknown> = {
        disableAllHooks: true,
        hooks: {},
        ide: { autoConnect: false },
        remoteExport: false,
        autoUpdate: false,
        includeCoAuthoredBy: false,
        notifications: false,
        keepAlive: "off",
      };
      if (sourceHome) {
        const sourceConfig = path.join(sourceHome, "config.json");
        if (yield* fs.exists(sourceConfig)) {
          const info = yield* fs.stat(sourceConfig);
          if (info.type !== "File" || info.size > 1_048_576n) {
            return yield* fail(
              "Copilot authentication configuration is not a bounded regular file.",
            );
          }
          const source = yield* fs.readFileString(sourceConfig).pipe(
            Effect.flatMap(decodeConfiguration),
            Effect.mapError(() => fail("Could not read Copilot authentication configuration.")),
          );
          for (const key of AUTHENTICATION_KEYS) {
            if (source[key] !== undefined) configuration[key] = source[key];
          }
        }
      }
      yield* fs.writeFileString(
        path.join(copilotHome, "config.json"),
        encodeConfiguration(configuration),
        { mode: 0o600 },
      );
      yield* fs.writeFileString(
        path.join(copilotHome, "settings.json"),
        encodeConfiguration({
          disableAllHooks: true,
          ide: { autoConnect: false },
          remoteExport: false,
        }),
        { mode: 0o600 },
      );
      const environment: NodeJS.ProcessEnv = {};
      for (const key of ENVIRONMENT_KEYS) {
        if (sourceEnvironment[key] !== undefined) environment[key] = sourceEnvironment[key];
      }
      Object.assign(environment, {
        HOME: home,
        USERPROFILE: home,
        COPILOT_HOME: copilotHome,
        XDG_CONFIG_HOME: path.join(home, ".config"),
        XDG_CACHE_HOME: path.join(home, ".cache"),
        XDG_DATA_HOME: path.join(home, ".local", "share"),
        XDG_STATE_HOME: path.join(home, ".local", "state"),
        APPDATA: path.join(home, "AppData", "Roaming"),
        LOCALAPPDATA: path.join(home, "AppData", "Local"),
        TMPDIR: root,
        TMP: root,
        TEMP: root,
        COPILOT_DISABLE_LOGIN_SHELL_ENV: "1",
        COPILOT_AUTO_UPDATE: "false",
        GIT_CONFIG_NOSYSTEM: "1",
        GIT_CONFIG_GLOBAL: path.join(home, ".gitconfig"),
        GIT_CEILING_DIRECTORIES: cwd,
      });
      const args = [
        // Copilot 1.0.83 treats an empty allowlist as its default toolset. A
        // nonempty list with no registered tool removes every tool instead.
        "--available-tools=t3-text-generation-no-tools",
        "--deny-tool=shell",
        "--deny-tool=write",
        "--deny-tool=read",
        "--disable-builtin-mcps",
        "--no-custom-instructions",
        "--no-remote-export",
        "--no-remote",
        "--no-auto-update",
        "--no-ask-user",
        "--no-bash-env",
        "--no-eager-powershell-resolution",
        "--no-experimental",
        "--log-level",
        "none",
        "--silent",
        "--stream",
        "off",
        "--output-format",
        "text",
        ...(input.modelSelection.model === "default"
          ? []
          : ["--model", input.modelSelection.model]),
        ...modelArgs,
        "--prompt",
        input.prompt,
      ];
      const launch = yield* resolveSpawnCommand(resolved.spawn.command, args).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
      );
      const child = yield* spawner.spawn(
        ChildProcess.make(launch.command, launch.args, {
          cwd,
          env: environment,
          extendEnv: false,
          shell: launch.shell,
          stdin: "ignore",
          killSignal: "SIGTERM",
          forceKillAfter: "2 seconds",
        }),
      );
      const collect = (stream: typeof child.stdout) =>
        stream.pipe(
          Stream.runFoldEffect(
            () => ({ chunks: [] as Uint8Array[], bytes: 0 }),
            (state, chunk) => {
              if (state.bytes + chunk.byteLength > MAX_OUTPUT_BYTES) {
                return Effect.fail(fail("Copilot text generation exceeded the output limit."));
              }
              state.chunks.push(chunk);
              return Effect.succeed({
                chunks: state.chunks,
                bytes: state.bytes + chunk.byteLength,
              });
            },
          ),
        );
      const [stdout, , code] = yield* Effect.all(
        [collect(child.stdout), collect(child.stderr), child.exitCode],
        { concurrency: "unbounded" },
      );
      if (code !== 0) return yield* fail(`Copilot text generation exited with code ${code}.`);
      const text = new TextDecoder("utf-8", { fatal: true });
      const output = yield* Effect.try({
        try: () => text.decode(Buffer.concat(stdout.chunks, stdout.bytes)).trim(),
        catch: () => fail("Copilot returned invalid UTF-8 output."),
      });
      if (!output) return yield* fail("Copilot returned empty output.");
      // oxlint-disable-next-line t3code/no-inline-schema-compile -- Each prompt supplies its output schema.
      const generated = yield* Schema.decodeEffect(Schema.fromJsonString(input.outputSchema))(
        extractJsonObject(output),
      ).pipe(Effect.mapError(() => fail("Copilot returned invalid structured output.")));
      const fields = yield* decodeFields(generated).pipe(
        Effect.mapError(() => fail("Copilot returned invalid structured output.")),
      );
      for (const key of ["title", "subject", "branch"]) {
        const value = fields[key];
        if (typeof value !== "string") continue;
        const label = value.trim().split(/\r?\n/u)[0] ?? "";
        if (!(key === "branch" ? isBranchLabel(label) : isTextLabel(label))) {
          return yield* fail(`Copilot returned an empty ${key}.`);
        }
      }
      return generated;
    },
    (effect, input) =>
      effect.pipe(
        Effect.timeoutOrElse({
          duration: "180 seconds",
          orElse: () =>
            Effect.fail(
              new TextGenerationError({
                operation: input.operation,
                detail: "Copilot text generation timed out.",
              }),
            ),
        }),
        Effect.scoped,
        Effect.mapError((cause) =>
          isTextGenerationError(cause)
            ? cause
            : new TextGenerationError({
                operation: input.operation,
                detail:
                  "Copilot text generation failed. Check this provider instance's executable and sign-in.",
              }),
        ),
      ),
  );

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("CopilotTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt(input);
      const generated = yield* runJson({
        operation: "generateThreadTitle",
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return { title: sanitizeThreadTitle(generated.title) };
    });
  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("CopilotTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt(input);
      const generated = yield* runJson({
        operation: "generateBranchName",
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return { branch: sanitizeBranchFragment(generated.branch) };
    });
  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("CopilotTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt(input);
      const generated = yield* runJson({
        operation: "generateCommitMessage",
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });
  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("CopilotTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt(input);
      const generated = yield* runJson({
        operation: "generatePrContent",
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return { title: sanitizePrTitle(generated.title), body: generated.body.trim() };
    });
  return {
    generateThreadTitle,
    generateBranchName,
    generateCommitMessage,
    generatePrContent,
  } satisfies TextGeneration.TextGeneration["Service"];
});
