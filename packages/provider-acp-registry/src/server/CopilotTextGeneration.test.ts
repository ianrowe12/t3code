import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { expect } from "vite-plus/test";

import { AcpRegistrySettings } from "../settings.ts";
import { AcpRegistryCatalog } from "./AcpRegistrySupport.ts";
import { makeCopilotTextGeneration } from "./CopilotTextGeneration.ts";
import { AcpRegistryDriver } from "./driver.ts";

const settings = Schema.decodeSync(AcpRegistrySettings)({ agentId: "github-copilot-cli" });
const modelSelection = {
  instanceId: ProviderInstanceId.make("acpRegistry_github_copilot"),
  model: "gpt-6.1-sol",
};
const titleInput = { cwd: "/untrusted-workspace", message: "Fix reconnect", modelSelection };

const makeFixture = Effect.fnUntraced(function* (
  source: string,
  options: { readonly args?: ReadonlyArray<string>; readonly environment?: NodeJS.ProcessEnv } = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-copilot-text-" });
  const command = writeFakeCli({ directory, name: "copilot", source });
  const helperDirectory = path.join(directory, "helpers");
  const nativeHome = path.join(directory, "native-copilot");
  const catalog = AcpRegistryCatalog.of({
    search: () => Effect.die("unused search"),
    prepare: () => Effect.die("unused prepare"),
    inspect: () =>
      Effect.succeed({
        status: "ready",
        agentId: "github-copilot-cli",
        version: "1.0.96",
        distribution: "binary",
      }),
    uninstallManagedBinary: () => Effect.die("unused uninstall"),
    resolve: (config, cwd, environment) => {
      expect(config.commandPath).toBe(command);
      return Effect.succeed({
        agent: {
          id: "github-copilot-cli",
          name: "Copilot",
          version: "1.0.96",
          description: "",
          distribution: {},
        },
        distribution: "binary" as const,
        spawn: { command, args: options.args ?? ["--acp"], cwd, env: environment ?? {} },
      });
    },
  });
  const generation = yield* makeCopilotTextGeneration({
    settings: { ...settings, commandPath: command },
    environment: { PATH: process.env.PATH, COPILOT_HOME: nativeHome, ...options.environment },
    helperDirectory,
  }).pipe(Effect.provideService(AcpRegistryCatalog, catalog));
  return { generation, fs, path, directory, helperDirectory, nativeHome, command, catalog };
});

it.layer(NodeServices.layer)("Copilot application text generation", (it) => {
  it.effect("generates titles through the actual ACP Registry provider instance", () =>
    Effect.gen(function* () {
      const { command, catalog, nativeHome } = yield* makeFixture(
        `console.log('{"title":"Provider instance title"}');`,
      );
      const instance = yield* AcpRegistryDriver.create({
        instanceId: modelSelection.instanceId,
        displayName: "GitHub Copilot",
        accentColor: undefined,
        environment: [],
        enabled: true,
        config: { ...settings, commandPath: command },
      }).pipe(
        Effect.provideService(AcpRegistryCatalog, catalog),
        Effect.provideService(HostProcess.Environment, {
          PATH: process.env.PATH,
          COPILOT_HOME: nativeHome,
        }),
        Effect.provideService(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
        Effect.provide(
          Layer.mergeAll(
            TestProviderHost.layer({ runBackgroundWork: false }),
            IdAllocator.layer,
            McpProviderSessions.layer,
          ),
        ),
      );
      expect(
        yield* instance.textGeneration.generateThreadTitle(titleInput).pipe(Effect.result),
      ).toEqual(Result.succeed({ title: "Provider instance title" }));
    }),
  );

  it.effect("generates an automatic title with the configured executable and model", () =>
    Effect.gen(function* () {
      const { generation, fs, helperDirectory } = yield* makeFixture(`
        import assert from "node:assert/strict";
        const args = process.argv.slice(2);
        assert.equal(args[args.indexOf("--model") + 1], "gpt-6.1-sol");
        assert.equal(args.includes("--acp"), false);
        assert.ok(args[args.indexOf("--prompt") + 1].includes("Fix reconnect"));
        console.log('{"title":"Repair reconnect state"}');
      `);
      expect(yield* generation.generateThreadTitle(titleInput).pipe(Effect.result)).toEqual(
        Result.succeed({ title: "Repair reconnect state" }),
      );
      expect(yield* fs.readDirectory(helperDirectory)).toEqual([]);
    }),
  );

  it.effect("keeps title generation tool-free and outside the original home and checkout", () =>
    Effect.gen(function* () {
      const test = yield* makeFixture(
        `
        import { readFileSync } from "node:fs";
        import path from "node:path";
        const args = process.argv.slice(2);
        const config = JSON.parse(readFileSync(path.join(process.env.COPILOT_HOME, "config.json")));
        const required = [
          "--available-tools=t3-text-generation-no-tools",
          "--deny-tool=shell", "--deny-tool=write", "--deny-tool=read",
          "--disable-builtin-mcps", "--no-custom-instructions", "--no-bash-env",
        ];
        const isolated =
          required.every(flag => args.includes(flag)) &&
          config.lastLoggedInUser === "fixture-user" &&
          config.disableAllHooks === true &&
          Object.keys(config.hooks).length === 0 &&
          config.mcpServers === undefined &&
          config.trustedFolders === undefined &&
          process.env.T3CODE_HOME === undefined &&
          process.env.COPILOT_ALLOW_ALL === undefined &&
          process.cwd().endsWith("/workspace") &&
          process.env.HOME.endsWith("/home") &&
          !process.env.COPILOT_HOME.endsWith("/native-copilot");
        console.log(JSON.stringify({ title: isolated ? "Isolated title" : "Unsafe helper access" }));
      `,
        { environment: { T3CODE_HOME: "/live-t3", COPILOT_ALLOW_ALL: "1" } },
      );
      yield* test.fs.makeDirectory(test.nativeHome);
      const configuration = JSON.stringify({
        lastLoggedInUser: "fixture-user",
        trustedFolders: ["/untrusted-workspace"],
        mcpServers: { forbidden: {} },
        hooks: { forbidden: [] },
      });
      const source = test.path.join(test.nativeHome, "config.json");
      yield* test.fs.writeFileString(source, configuration);
      expect(yield* test.generation.generateThreadTitle(titleInput).pipe(Effect.result)).toEqual(
        Result.succeed({ title: "Isolated title" }),
      );
      expect(yield* test.fs.readFileString(source)).toBe(configuration);
      expect(yield* test.fs.readDirectory(test.helperDirectory)).toEqual([]);
    }),
  );

  it.effect("uses the configured provider default without forcing another model", () =>
    Effect.gen(function* () {
      const { generation } = yield* makeFixture(`
        const correct = !process.argv.includes("--model");
        console.log(JSON.stringify({ title: correct ? "Default model title" : "Wrong forced model" }));
      `);
      expect(
        yield* generation
          .generateThreadTitle({
            ...titleInput,
            modelSelection: { ...modelSelection, model: "default" },
          })
          .pipe(Effect.result),
      ).toEqual(Result.succeed({ title: "Default model title" }));
    }),
  );

  it.effect("rejects unsafe registry launch arguments before spawning a title helper", () =>
    Effect.gen(function* () {
      const { generation } = yield* makeFixture(
        `console.log('{"title":"Unsafe helper was spawned"}');`,
        { args: ["--acp", "--additional-mcp-config", '{"mcpServers":{}}'] },
      );
      const result = yield* generation.generateThreadTitle(titleInput).pipe(Effect.result);
      expect(result).toMatchObject({
        _tag: "Failure",
        failure: {
          operation: "generateThreadTitle",
          detail: expect.stringContaining("only the registry --acp"),
        },
      });
    }),
  );

  it.effect("does not accept a failed helper's valid-looking title or expose stderr", () =>
    Effect.gen(function* () {
      const { generation } = yield* makeFixture(`
        console.log('{"title":"Not a successful title"}');
        console.error("fixture-private-stderr");
        process.exitCode = 7;
      `);
      const result = yield* generation.generateThreadTitle(titleInput).pipe(Effect.result);
      expect(result).toMatchObject({
        _tag: "Failure",
        failure: { detail: expect.stringContaining("code 7") },
      });
      if (result._tag !== "Failure") throw new Error("Expected a failed helper");
      expect(result.failure.message).not.toContain("fixture-private-stderr");
      expect(result.failure.cause).toBeUndefined();
    }),
  );
});
