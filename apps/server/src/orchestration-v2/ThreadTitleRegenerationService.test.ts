import { assert, describe, it, vi } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AcpRegistrySettings,
  type ChatAttachment,
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TextGenerationError,
  type ServerSettingsError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { ProjectionProjectRepository } from "../persistence/Services/ProjectionProjects.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import { makeCopilotTextGeneration } from "../textGeneration/CopilotTextGeneration.ts";
import { AcpRegistryCatalog } from "../provider/acp/AcpRegistrySupport.ts";
import { writeFakeCli } from "../testUtils/fakeCli.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as ThreadTitleRegeneration from "./ThreadTitleRegenerationService.ts";
import { formatThreadTitleContext } from "./ThreadTitleRegenerationService.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";

const projectId = ProjectId.make("project:title-regeneration");
const decodeCopilotConfig = Schema.decodeEffect(AcpRegistrySettings);
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.1-codex",
} as const;

const adapter = {
  instanceId: modelSelection.instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("provider execution is disabled in title regeneration tests"),
} as ProviderAdapterV2Shape;

function makeHarness(
  options: {
    readonly generateTitle?: TextGeneration.TextGeneration["Service"]["generateThreadTitle"];
    readonly settings?: Layer.Layer<ServerSettings.ServerSettingsService, ServerSettingsError>;
  } = {},
) {
  const database = SqlitePersistenceMemory;
  const registry = ProviderAdapterRegistry.makeLayer([adapter]);
  const orchestrator = makeOrchestratorV2ReplayLayerWithRegistry(
    { name: "thread-title-regeneration" },
    registry,
    { databaseLayer: database, runEffectWorker: false },
  );
  const threadManagement = ThreadManagement.layer.pipe(Layer.provide(orchestrator));
  const outbox = EffectOutbox.layer.pipe(Layer.provide(database));
  const generateThreadTitle = vi.fn(
    options.generateTitle ?? (() => Effect.succeed({ title: "Generated title" })),
  );
  const projectedProjects = Layer.mock(ProjectionProjectRepository)({
    getById: ({ projectId: requestedProjectId }) =>
      Effect.succeed(
        requestedProjectId === projectId
          ? Option.some({
              projectId,
              title: "Project",
              workspaceRoot: "/repo",
              defaultModelSelection: modelSelection,
              defaultThreadEnvMode: null,
              autoPull: false,
              scripts: [],
              createdAt: "2026-06-20T00:00:00.000Z",
              updatedAt: "2026-06-20T00:00:00.000Z",
              deletedAt: null,
            })
          : Option.none(),
      ),
  });
  const titleRegeneration = ThreadTitleRegeneration.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        threadManagement,
        projectedProjects,
        Layer.mock(TextGeneration.TextGeneration)({ generateThreadTitle }),
        options.settings ?? ServerSettings.layerTest({}),
      ),
    ),
  );
  return {
    layer: Layer.mergeAll(threadManagement, titleRegeneration, outbox, database),
    generateThreadTitle,
  };
}

function createThread(input: { readonly command: string; readonly thread: string }) {
  return Effect.gen(function* () {
    const threads = yield* ThreadManagement.ThreadManagementService;
    const threadId = ThreadId.make(input.thread);
    yield* threads.dispatch({
      type: "thread.create",
      commandId: CommandId.make(input.command),
      threadId,
      projectId,
      title: "Seed title",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    return threadId;
  });
}

function dispatchUserMessage(input: {
  readonly command: string;
  readonly threadId: ThreadId;
  readonly text: string;
}) {
  return Effect.gen(function* () {
    const threads = yield* ThreadManagement.ThreadManagementService;
    yield* threads.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(input.command),
      threadId: input.threadId,
      messageId: MessageId.make(`${input.command}:message`),
      text: input.text,
      attachments: [],
      modelSelection,
      dispatchMode: { type: "defer_start" },
      createdBy: "user",
      creationSource: "web",
    });
  });
}

function armRegeneration(input: { readonly command: string; readonly threadId: ThreadId }) {
  return Effect.gen(function* () {
    const threads = yield* ThreadManagement.ThreadManagementService;
    const requestId = CommandId.make(input.command);
    yield* threads.dispatch({
      type: "thread.metadata.update",
      commandId: requestId,
      threadId: input.threadId,
      regenerateTitle: true,
    });
    return requestId;
  });
}

describe("formatThreadTitleContext", () => {
  const attachment = (id: string): ChatAttachment => ({
    type: "image",
    id,
    name: `${id}.png`,
    mimeType: "image/png",
    sizeBytes: 1,
  });

  it("builds a newest-first digest, skipping system messages and empty sections", () => {
    const context = formatThreadTitleContext([
      { role: "user", text: "First question" },
      { role: "system", text: "Hidden instructions" },
      { role: "assistant", text: "" },
      { role: "assistant", text: "Second answer", attachments: [attachment("shot")] },
    ]);
    assert.equal(
      context.message,
      "USER:\nFirst question\n\nASSISTANT:\nSecond answer\n[Attachments: shot.png]",
    );
    assert.deepEqual(
      context.attachments.map((entry) => entry.name),
      ["shot.png"],
    );
  });

  it("pins the first user message ahead of the retained tail once content stops fitting", () => {
    const context = formatThreadTitleContext([
      { role: "user", text: `Ancient context that anchors the topic ${"x".repeat(600)}` },
      { role: "assistant", text: "y".repeat(6_000) },
      { role: "user", text: "z".repeat(1_500) },
    ]);
    assert.isTrue(context.message.startsWith("USER:\nAncient context that anchors the topic"));
    assert.isTrue(context.message.includes("[Earlier content truncated]\n\n"));
    assert.isTrue(context.message.includes("y".repeat(100)));
  });

  it("truncates an oversized pinned first user message", () => {
    const context = formatThreadTitleContext([
      { role: "user", text: `Topic anchor ${"a".repeat(4_000)}` },
      { role: "assistant", text: "y".repeat(9_000) },
      { role: "user", text: "z".repeat(1_500) },
    ]);
    assert.isTrue(context.message.startsWith("USER:\nTopic anchor"));
    assert.isTrue(context.message.includes("[First user message truncated]"));
    assert.isTrue(context.message.includes("[Earlier content truncated]\n\n"));
  });

  it("retains at most four attachments from the newest messages", () => {
    const context = formatThreadTitleContext([
      { role: "user", text: "older", attachments: [attachment("a"), attachment("b")] },
      {
        role: "user",
        text: "newer",
        attachments: [attachment("c"), attachment("d"), attachment("e")],
      },
    ]);
    assert.deepEqual(
      context.attachments.map((entry) => entry.name),
      ["b.png", "c.png", "d.png", "e.png"],
    );
  });
});

describe("ThreadTitleRegenerationService", () => {
  for (const selection of ["explicit", "unsupported"] as const) {
    it.effect(
      `lands an initial Copilot title with ${selection} host text generation selection`,
      () =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const directory = yield* fs.makeTempDirectoryScoped({
            directory: process.cwd(),
            prefix: ".copilot-title-service-test-",
          });
          const command = writeFakeCli({
            directory,
            name: "copilot",
            source: `
            import assert from "node:assert/strict";
            const args = process.argv.slice(2);
            ${
              selection === "explicit"
                ? 'assert.equal(args[args.indexOf("--model") + 1], "gpt-4.1");'
                : 'assert.equal(args.includes("--model"), false);'
            }
            assert.ok(args[args.indexOf("--prompt") + 1].includes("synthetic reconnect failure"));
            console.log('Experimental features enabled!\\n{"title":"Repair synthetic reconnect"}');
          `,
          });
          const copilotConfig = yield* decodeCopilotConfig({
            agentId: "github-copilot-cli",
            enabled: true,
          });
          const copilot = yield* makeCopilotTextGeneration({
            settings: copilotConfig,
            environment: { PATH: process.env.PATH },
            helperDirectory: path.join(directory, "helpers"),
          }).pipe(
            Effect.provideService(
              AcpRegistryCatalog,
              AcpRegistryCatalog.of({
                search: () => Effect.die("unused"),
                prepare: () => Effect.die("unused"),
                inspect: () => Effect.die("unused"),
                uninstallManagedBinary: () => Effect.die("unused"),
                resolve: (_settings, cwd, environment) =>
                  Effect.succeed({
                    agent: {
                      id: "github-copilot-cli",
                      name: "Copilot",
                      version: "1.0.83",
                      description: "",
                      distribution: {},
                    },
                    distribution: "binary" as const,
                    spawn: { command, args: ["--acp"], cwd, env: environment ?? {} },
                  }),
              }),
            ),
          );
          const copilotId = ProviderInstanceId.make("copilot_personal");
          const unsupportedId = ProviderInstanceId.make("unrelated_acp");
          const harness = makeHarness({
            generateTitle: copilot.generateThreadTitle,
            settings: ServerSettings.layerTest({
              providers: {
                codex: { enabled: false },
                claudeAgent: { enabled: false },
                cursor: { enabled: false },
                grok: { enabled: false },
                opencode: { enabled: false },
                pi: { enabled: false },
              },
              providerInstances: {
                [unsupportedId]: {
                  driver: ProviderDriverKind.make("acpRegistry"),
                  enabled: true,
                  config: { agentId: "gemini-cli" },
                },
                [copilotId]: {
                  driver: ProviderDriverKind.make("acpRegistry"),
                  enabled: true,
                  config: copilotConfig,
                },
              },
              textGenerationModelSelection: {
                instanceId: selection === "explicit" ? copilotId : unsupportedId,
                model: "gpt-4.1",
                options: [{ id: "reasoning_effort", value: "medium" }],
              },
            }),
          });
          yield* Effect.gen(function* () {
            const threads = yield* ThreadManagement.ThreadManagementService;
            const titles = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
            const threadId = yield* createThread({
              command: "command:copilot:create",
              thread: "thread:copilot-title",
            });
            const messageCommand = "command:copilot:message";
            yield* dispatchUserMessage({
              command: messageCommand,
              threadId,
              text: "Investigate synthetic reconnect failure",
            });
            const requestId = yield* armRegeneration({
              command: "command:copilot:title",
              threadId,
            });
            yield* titles.execute({
              threadId,
              requestId,
              kind: { type: "initial", messageId: MessageId.make(`${messageCommand}:message`) },
            });
            const projection = yield* threads.getThreadProjection(threadId);
            assert.equal(projection.thread.title, "Repair synthetic reconnect");
            assert.isNotOk(projection.thread.titleRegeneration);
            assert.equal(harness.generateThreadTitle.mock.calls.length, 1);
            assert.deepEqual(harness.generateThreadTitle.mock.calls[0]?.[0].modelSelection, {
              instanceId: copilotId,
              model: selection === "explicit" ? "gpt-4.1" : "default",
              ...(selection === "explicit"
                ? { options: [{ id: "reasoning_effort", value: "medium" }] }
                : {}),
            });
          }).pipe(Effect.provide(harness.layer));
          assert.deepEqual(yield* fs.readDirectory(path.join(directory, "helpers")), []);
        }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );
  }

  it.effect("arms and clears the regeneration marker through metadata commands", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const threadId = yield* createThread({
          command: "command:title:arm:create",
          thread: "thread:title:arm",
        });

        const firstRequest = yield* armRegeneration({ command: "command:title:arm:1", threadId });
        const armed = yield* threads.getThreadProjection(threadId);
        assert.equal(armed.thread.titleRegeneration?.requestId, firstRequest);
        assert.deepEqual(
          (yield* outbox.listByCommandId(firstRequest)).map((effect) => effect.request),
          [{ type: "thread-title.generate", kind: { type: "regenerate" } }],
        );

        yield* threads.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("command:title:arm:abandon"),
          threadId,
          regenerateTitle: false,
        });
        const abandoned = yield* threads.getThreadProjection(threadId);
        assert.isNotOk(abandoned.thread.titleRegeneration);

        yield* armRegeneration({ command: "command:title:arm:2", threadId });
        yield* threads.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("command:title:arm:rename"),
          threadId,
          title: "Manual title",
        });
        const renamed = yield* threads.getThreadProjection(threadId);
        assert.equal(renamed.thread.title, "Manual title");
        assert.isNotOk(renamed.thread.titleRegeneration);
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("skips execution when the marker was superseded by a newer request", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const threadId = yield* createThread({
          command: "command:title:stale:create",
          thread: "thread:title:stale",
        });
        const staleRequest = yield* armRegeneration({ command: "command:title:stale:1", threadId });
        const currentRequest = yield* armRegeneration({
          command: "command:title:stale:2",
          threadId,
        });

        yield* titleRegeneration.execute({
          threadId,
          requestId: staleRequest,
          kind: { type: "regenerate" },
        });

        assert.equal(harness.generateThreadTitle.mock.calls.length, 0);
        const projection = yield* threads.getThreadProjection(threadId);
        assert.equal(projection.thread.title, "Seed title");
        assert.equal(projection.thread.titleRegeneration?.requestId, currentRequest);
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("lands the regenerated title from the conversation digest", () =>
    Effect.gen(function* () {
      const harness = makeHarness({
        generateTitle: () => Effect.succeed({ title: "Fresh title" }),
      });
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const threadId = yield* createThread({
          command: "command:title:landing:create",
          thread: "thread:title:landing",
        });
        yield* dispatchUserMessage({
          command: "command:title:landing:message",
          threadId,
          text: "Investigate the flaky login test",
        });
        const requestId = yield* armRegeneration({ command: "command:title:landing:1", threadId });

        yield* titleRegeneration.execute({ threadId, requestId, kind: { type: "regenerate" } });

        const call = harness.generateThreadTitle.mock.calls[0]?.[0];
        assert.equal(call?.previousTitle, "Seed title");
        assert.equal(call?.cwd, "/repo");
        assert.include(call?.message, "USER:\nInvestigate the flaky login test");
        const projection = yield* threads.getThreadProjection(threadId);
        assert.equal(projection.thread.title, "Fresh title");
        assert.isNotOk(projection.thread.titleRegeneration);
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("keeps the current title when generation returns the fallback", () =>
    Effect.gen(function* () {
      const harness = makeHarness({
        generateTitle: () => Effect.succeed({ title: "New thread" }),
      });
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const threadId = yield* createThread({
          command: "command:title:fallback:create",
          thread: "thread:title:fallback",
        });
        yield* dispatchUserMessage({
          command: "command:title:fallback:message",
          threadId,
          text: "Some conversation",
        });
        const requestId = yield* armRegeneration({ command: "command:title:fallback:1", threadId });

        yield* titleRegeneration.execute({ threadId, requestId, kind: { type: "regenerate" } });

        const projection = yield* threads.getThreadProjection(threadId);
        assert.equal(projection.thread.title, "Seed title");
        assert.isNotOk(projection.thread.titleRegeneration);
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("keeps the current title when regeneration reproduces it", () =>
    Effect.gen(function* () {
      const harness = makeHarness({
        generateTitle: () => Effect.succeed({ title: "Seed title" }),
      });
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const threadId = yield* createThread({
          command: "command:title:unchanged:create",
          thread: "thread:title:unchanged",
        });
        yield* dispatchUserMessage({
          command: "command:title:unchanged:message",
          threadId,
          text: "Some conversation",
        });
        const requestId = yield* armRegeneration({
          command: "command:title:unchanged:1",
          threadId,
        });

        yield* titleRegeneration.execute({ threadId, requestId, kind: { type: "regenerate" } });

        const projection = yield* threads.getThreadProjection(threadId);
        assert.equal(projection.thread.title, "Seed title");
        assert.isNotOk(projection.thread.titleRegeneration);
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("clears the marker and keeps the title when generation fails", () =>
    Effect.gen(function* () {
      const harness = makeHarness({
        generateTitle: () => Effect.die(new Error("model unavailable")),
      });
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const threadId = yield* createThread({
          command: "command:title:failure:create",
          thread: "thread:title:failure",
        });
        yield* dispatchUserMessage({
          command: "command:title:failure:message",
          threadId,
          text: "Some conversation",
        });
        const requestId = yield* armRegeneration({ command: "command:title:failure:1", threadId });

        yield* titleRegeneration.execute({ threadId, requestId, kind: { type: "regenerate" } });

        assert.equal(harness.generateThreadTitle.mock.calls.length, 1);
        const projection = yield* threads.getThreadProjection(threadId);
        assert.equal(projection.thread.title, "Seed title");
        assert.isNotOk(projection.thread.titleRegeneration);
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("completes without generating when the initial message is unavailable", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const threadId = yield* createThread({
          command: "command:title:missing:create",
          thread: "thread:title:missing",
        });
        const requestId = yield* armRegeneration({ command: "command:title:missing:1", threadId });

        yield* titleRegeneration.execute({
          threadId,
          requestId,
          kind: { type: "initial", messageId: MessageId.make("message:title:missing") },
        });

        assert.equal(harness.generateThreadTitle.mock.calls.length, 0);
        const projection = yield* threads.getThreadProjection(threadId);
        assert.equal(projection.thread.title, "Seed title");
        assert.isNotOk(projection.thread.titleRegeneration);
      }).pipe(Effect.provide(harness.layer));
    }),
  );
});

for (const outcome of ["success", "exhausted", "stale", "interrupted"] as const) {
  it.effect(`initial title retry: ${outcome}`, () =>
    Effect.gen(function* () {
      const attempted = yield* Deferred.make<void>();
      let attempts = 0;
      const harness = makeHarness({
        generateTitle: () =>
          Effect.gen(function* () {
            attempts += 1;
            yield* Deferred.succeed(attempted, undefined);
            if (outcome === "success" && attempts === 3) return { title: "Recovered title" };
            return yield* new TextGenerationError({
              operation: "generateThreadTitle",
              detail: "Temporary failure",
            });
          }),
      });
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const service = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const threadId = yield* createThread({
          command: `create:${outcome}`,
          thread: `thread:${outcome}`,
        });
        const messageCommand = `message:${outcome}`;
        yield* dispatchUserMessage({ command: messageCommand, threadId, text: "Fix the title" });
        const requestId = yield* armRegeneration({ command: `title:${outcome}`, threadId });
        const fiber = yield* service
          .execute({
            threadId,
            requestId,
            kind: { type: "initial", messageId: MessageId.make(`${messageCommand}:message`) },
          })
          .pipe(Effect.forkChild);
        yield* Deferred.await(attempted);
        if (outcome === "interrupted") {
          yield* Fiber.interrupt(fiber);
        } else {
          if (outcome === "stale")
            yield* threads.dispatch({
              type: "thread.metadata.update",
              commandId: CommandId.make("manual-title"),
              threadId,
              title: "Manual title",
            });
          yield* TestClock.adjust("2 seconds");
          if (outcome !== "stale") yield* TestClock.adjust("4 seconds");
          yield* Fiber.join(fiber);
        }
        assert.equal(attempts, outcome === "success" || outcome === "exhausted" ? 3 : 1);
        const projection = yield* threads.getThreadProjection(threadId);
        assert.equal(
          projection.thread.title,
          outcome === "success"
            ? "Recovered title"
            : outcome === "stale"
              ? "Manual title"
              : "Seed title",
        );
        if (outcome === "interrupted")
          assert.equal(projection.thread.titleRegeneration?.requestId, requestId);
        else assert.isNotOk(projection.thread.titleRegeneration);
      }).pipe(Effect.provide(harness.layer));
    }),
  );
}
