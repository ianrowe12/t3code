import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ServerConfig } from "../../config.ts";
import { IdAllocatorV2, layer as idAllocatorLayer } from "../IdAllocator.ts";
import {
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2TurnInput,
} from "../ProviderAdapter.ts";
import { AcpRegistryAdapterV2Driver, makeAcpRegistryAdapterV2 } from "./AcpRegistryAdapterV2.ts";

const decodeSettings = Schema.decodeUnknownEffect(AcpRegistryAdapterV2Driver.configSchema);
const Request = Schema.Struct({
  id: Schema.optionalKey(Schema.Union([Schema.Finite, Schema.String])),
  method: Schema.String,
  params: Schema.optionalKey(
    Schema.Struct({
      sessionId: Schema.optionalKey(Schema.String),
      clientCapabilities: Schema.optionalKey(Schema.Unknown),
    }),
  ),
});
const decodeRequest = Schema.decodeUnknownEffect(Schema.fromJsonString(Request));
type Request = typeof Request.Type;

const testLayer = Layer.mergeAll(
  NodeServices.layer,
  idAllocatorLayer,
  ServerConfig.layerTest(process.cwd(), { prefix: "t3-copilot-settlement-" }).pipe(
    Layer.provide(NodeServices.layer),
  ),
);

// Real ACP framing, runtime, and adapter; only the native process is replaced.
const makeFixture = Effect.fnUntraced(function* (agentId = "github-copilot-cli") {
  const prompts = yield* Queue.unbounded<Request>();
  const rpcSettled = yield* Queue.unbounded<void>();
  const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
  const observed: ProviderAdapterV2Event[] = [];
  const requests: Request[] = [];
  const processLifecycle: string[] = [];
  let spawns = 0;
  let newSessions = 0;
  let incoming = yield* Queue.unbounded<string>();
  const notify = (method: string, params: unknown) =>
    Queue.offer(incoming, `${JSON.stringify({ jsonrpc: "2.0", method, params })}\n`);
  const respond = (request: Request, result: unknown, error?: unknown) =>
    Queue.offer(
      incoming,
      `${JSON.stringify({
        jsonrpc: "2.0",
        id: request.id,
        ...(error === undefined ? { result } : { error }),
      })}\n`,
    );
  const spawner = ChildProcessSpawner.make(() =>
    Effect.gen(function* () {
      spawns += 1;
      const runtimeOrdinal = spawns;
      processLifecycle.push(`spawn:${runtimeOrdinal}`);
      const output = yield* Queue.unbounded<string>();
      incoming = output;
      const exited = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
      const kill = Effect.sync(() => processLifecycle.push(`kill:${runtimeOrdinal}`)).pipe(
        Effect.andThen(Deferred.succeed(exited, ChildProcessSpawner.ExitCode(0))),
        Effect.asVoid,
      );
      yield* Effect.addFinalizer(() => kill);
      let buffered = "";
      const decoder = new TextDecoder();
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1000 + spawns),
        exitCode: Deferred.await(exited),
        isRunning: Deferred.isDone(exited).pipe(Effect.map((done) => !done)),
        kill: () => kill,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((chunk: Uint8Array) =>
          Effect.gen(function* () {
            buffered += decoder.decode(chunk, { stream: true });
            while (buffered.includes("\n")) {
              const end = buffered.indexOf("\n");
              const line = buffered.slice(0, end);
              buffered = buffered.slice(end + 1);
              if (!line.trim()) continue;
              const request = yield* decodeRequest(line).pipe(Effect.orDie);
              requests.push(request);
              if (request.method === "session/prompt") {
                processLifecycle.push(`prompt:${runtimeOrdinal}`);
                yield* Queue.offer(prompts, request);
              } else if (request.id !== undefined) {
                yield* respond(
                  request,
                  request.method === "initialize"
                    ? {
                        protocolVersion: 1,
                        agentCapabilities: {
                          loadSession: true,
                          sessionCapabilities: { close: {} },
                        },
                      }
                    : request.method === "session/new"
                      ? {
                          sessionId:
                            ++newSessions === 1 ? "native-root" : `bootstrap-${newSessions}`,
                        }
                      : {},
                );
              }
            }
          }),
        ),
        stdout: Stream.fromQueue(output).pipe(Stream.encodeText),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );
  const instanceId = ProviderInstanceId.make("copilot-completion");
  const threadId = ThreadId.make("copilot-completion-thread");
  const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
    runtimeMode: "full-access",
    interactionMode: "default",
    cwd: process.cwd(),
  });
  const modelSelection = { instanceId, model: "default" } as const;
  const settings = yield* decodeSettings({ agentId });
  const adapter = makeAcpRegistryAdapterV2({
    instanceId,
    settings,
    environment: {},
    childProcessSpawner: spawner,
    crypto: yield* Crypto.Crypto,
    fileSystem: yield* FileSystem.FileSystem,
    idAllocator: yield* IdAllocatorV2,
    serverConfig: yield* ServerConfig,
    resolver: {
      resolve: () =>
        Effect.succeed({
          agent: {
            id: agentId,
            name: "Fixture",
            version: "1",
            description: "Copilot completion regression",
            distribution: {},
          },
          spawn: { command: "fixture", args: [], cwd: process.cwd() },
          distribution: "binary" as const,
        }),
    },
    nativeLogging: () => ({
      requestLogger: (event) =>
        event.method === "session/prompt" && event.status !== "started"
          ? Queue.offer(rpcSettled, undefined).pipe(Effect.asVoid)
          : Effect.void,
    }),
  });
  const runtime = yield* adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make("copilot-completion-session"),
    modelSelection,
    runtimePolicy,
  });
  yield* runtime.events.pipe(
    Stream.runForEach((event) => {
      observed.push(event);
      return Queue.offer(events, event);
    }),
    Effect.forkScoped,
  );
  const providerThread = yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
  const now = yield* DateTime.now;
  const turn = (ordinal: number): ProviderAdapterV2TurnInput => ({
    appThread: {
      createdBy: "user",
      creationSource: "web",
      id: threadId,
      projectId: ProjectId.make("copilot-completion-project"),
      title: "Completion regression",
      providerInstanceId: instanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: providerThread.id,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
    threadId,
    providerThread,
    runId: RunId.make(`completion-run-${ordinal}`),
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`completion-attempt-${ordinal}`),
    rootNodeId: NodeId.make(`completion-node-${ordinal}`),
    message: {
      createdBy: "user",
      creationSource: "web",
      messageId: MessageId.make(`completion-message-${ordinal}`),
      text: "Perform the requested work.",
      attachments: [],
    },
    modelSelection,
    runtimePolicy,
  });
  const lifecycle = (type: string, data: unknown = {}, agentId?: string) =>
    notify("github.com/copilot/sessionEvent", {
      sessionId: "native-root",
      type,
      data,
      ...(agentId === undefined ? {} : { agentId }),
    });
  const update = (update: unknown) =>
    notify("session/update", { sessionId: "native-root", update });
  const terminal = Stream.fromQueue(events).pipe(
    Stream.filter((event) => event.type === "turn.terminal"),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
  );
  return {
    runtime,
    turn,
    prompts,
    rpcSettled,
    lifecycle,
    update,
    respond,
    terminal,
    observed,
    requests,
    processLifecycle,
    spawns: () => spawns,
  };
});

describe("Copilot ACP settlement", () => {
  for (const outcome of [
    "completed",
    "no semantic event",
    "failed",
    "blocked",
    "later reply",
    "child",
    "tagged tool",
    "unmatched result",
    "cancelled",
  ] as const) {
    it.effect(`preserves a large task_complete answer with omitted native data: ${outcome}`, () =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture();
        yield* fixture.runtime.startTurn(fixture.turn(1));
        const prompt = yield* Queue.take(fixture.prompts);
        yield* fixture.lifecycle("user.message", { interactionId: "current" });
        yield* fixture.lifecycle("assistant.turn_start", { interactionId: "current" });
        const summary = `# Complete report\n${"A full line of the answer.\n".repeat(3_000)}END`;
        assert.isAbove(Buffer.byteLength(summary), 65_536);
        const agentId = outcome === "child" ? "child" : undefined;
        yield* fixture.lifecycle("tool.execution_start", { omitted: "too-large" }, agentId);
        yield* fixture.update({
          sessionUpdate: "tool_call",
          toolCallId: "large-completion",
          title: "task_complete",
          kind: "other",
          status: "pending",
          rawInput: { summary },
          ...(outcome === "tagged tool" ? { _meta: { agentId: "child" } } : {}),
        });
        yield* fixture.lifecycle("tool.execution_complete", { omitted: "too-large" }, agentId);
        yield* fixture.update({
          sessionUpdate: "tool_call_update",
          toolCallId: "large-completion",
          status: outcome === "failed" ? "failed" : "completed",
          rawOutput: {
            content: outcome === "unmatched result" ? "Completion was declined." : summary,
            detailedContent: `Task completed: ${summary}`,
          },
        });
        if (outcome === "blocked") {
          yield* fixture.lifecycle("session.task_complete", { success: false, outcome: "blocked" });
        } else if (outcome !== "no semantic event") {
          yield* fixture.lifecycle("session.task_complete", { omitted: "too-large" }, agentId);
        }
        if (outcome === "later reply") {
          yield* fixture.update({
            sessionUpdate: "agent_message_chunk",
            content: { type: "text", text: "A later answer in chat." },
          });
        }
        yield* fixture.lifecycle("assistant.idle");
        yield* fixture.respond(prompt, {
          stopReason: outcome === "cancelled" ? "cancelled" : "end_turn",
        });
        assert.equal(
          (yield* fixture.terminal).status,
          outcome === "cancelled" ? "cancelled" : "completed",
        );
        const replies = fixture.observed.flatMap((event) =>
          event.type === "message.updated" &&
          event.message.role === "assistant" &&
          !event.message.streaming
            ? [event.message]
            : [],
        );
        if (outcome === "completed" || outcome === "no semantic event") {
          assert.isTrue(
            replies.some((message) => message.text === summary),
            "the full answer must reach chat even when Copilot omits oversized native event data",
          );
          assert.equal(new Set(replies.map((message) => message.id)).size, 1);
        } else {
          assert.isFalse(replies.some((message) => message.text === summary));
        }
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
    );
  }

  for (const nativeCompletionEvent of [false, true]) {
    it.effect(
      `publishes root task_complete as chat ${nativeCompletionEvent ? "with" : "without"} the semantic completion event`,
      () =>
        Effect.gen(function* () {
          const fixture = yield* makeFixture();
          yield* fixture.runtime.startTurn(fixture.turn(1));
          const prompt = yield* Queue.take(fixture.prompts);
          yield* fixture.lifecycle("user.message", { interactionId: "current" });
          yield* fixture.lifecycle("assistant.turn_start", { interactionId: "current" });
          const summary = nativeCompletionEvent
            ? "T3_ACP_FINAL_ANSWER_PROBE_20260918"
            : "[Download ZIP](/workspace/deliverable.zip)\n\nThe requested files are ready.";
          yield* fixture.lifecycle("tool.execution_start", {
            toolCallId: "task-complete",
            toolName: "task_complete",
            toolTitle: "Task complete",
            arguments: { summary },
          });
          yield* fixture.update({
            sessionUpdate: "tool_call",
            toolCallId: "task-complete",
            title: "task_complete",
            kind: "other",
            status: "pending",
            rawInput: { summary },
          });
          yield* fixture.lifecycle("tool.execution_complete", {
            toolCallId: "task-complete",
            success: true,
            result: { content: summary, detailedContent: `✓ Task completed: ${summary}` },
          });
          yield* fixture.update({
            sessionUpdate: "tool_call_update",
            toolCallId: "task-complete",
            status: "completed",
            content: [{ type: "content", content: { type: "text", text: summary } }],
            rawOutput: { content: summary, detailedContent: `✓ Task completed: ${summary}` },
          });
          if (nativeCompletionEvent) {
            yield* fixture.lifecycle("session.task_complete", { summary, success: true });
          }
          yield* fixture.lifecycle("assistant.idle");
          yield* fixture.respond(prompt, { stopReason: "end_turn" });
          assert.equal((yield* fixture.terminal).status, "completed");
          const replies = fixture.observed.flatMap((event) =>
            event.type === "message.updated" && event.message.role === "assistant"
              ? [event.message]
              : [],
          );
          assert.isTrue(
            replies.some((message) => message.text === summary && !message.streaming),
            "a native task_complete answer must not remain hidden in the tool card",
          );
          assert.equal(new Set(replies.map((message) => message.id)).size, 1);
        }).pipe(Effect.scoped, Effect.provide(testLayer)),
    );
  }

  for (const scenario of [
    "child agent",
    "unidentified tool",
    "nested tool",
    "MCP tool",
    "failed tool",
    "unmatched completion",
    "failed completion",
    "blocked completion",
    "blocked completion before tool success",
    "rejected completion",
    "cancelled turn",
    "later assistant answer",
    "assistant answer before completion event",
  ]) {
    it.effect(`does not promote task_complete text from ${scenario}`, () =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture();
        yield* fixture.runtime.startTurn(fixture.turn(1));
        const prompt = yield* Queue.take(fixture.prompts);
        yield* fixture.lifecycle("user.message", { interactionId: "current" });
        yield* fixture.lifecycle("assistant.turn_start", { interactionId: "current" });
        const summary = "A tool's proposed summary.";
        const agentId = scenario === "child agent" ? "child" : undefined;
        yield* fixture.lifecycle(
          "tool.execution_start",
          {
            toolCallId: "completion",
            toolName: scenario === "unidentified tool" ? "other_tool" : "task_complete",
            arguments: { summary },
            ...(scenario === "nested tool" ? { parentToolCallId: "parent" } : {}),
            ...(scenario === "MCP tool" ? { mcpServerName: "external" } : {}),
          },
          agentId,
        );
        if (scenario === "blocked completion before tool success") {
          yield* fixture.lifecycle("session.task_complete", {
            summary,
            success: false,
            outcome: "blocked",
          });
        }
        yield* fixture.lifecycle(
          "tool.execution_complete",
          {
            toolCallId: scenario === "unmatched completion" ? "other-completion" : "completion",
            success: scenario !== "failed tool",
          },
          agentId,
        );
        yield* fixture.update({
          sessionUpdate: "tool_call",
          toolCallId: "completion",
          title: "Tool",
          status: scenario === "failed tool" ? "failed" : "completed",
          rawInput: { summary },
          rawOutput: { content: summary, detailedContent: `✓ Task completed: ${summary}` },
        });
        if (scenario === "assistant answer before completion event") {
          yield* fixture.update({
            sessionUpdate: "agent_message_chunk",
            content: { type: "text", text: "The actual assistant answer." },
          });
        }
        if (
          [
            "child agent",
            "failed completion",
            "blocked completion",
            "rejected completion",
            "later assistant answer",
            "assistant answer before completion event",
          ].includes(scenario)
        ) {
          yield* fixture.lifecycle(
            "session.task_complete",
            {
              summary,
              success: scenario !== "failed completion",
              outcome:
                scenario === "blocked completion"
                  ? "blocked"
                  : scenario === "rejected completion"
                    ? "continue"
                    : "completed",
            },
            agentId,
          );
        }
        if (scenario === "later assistant answer") {
          yield* fixture.update({
            sessionUpdate: "agent_message_chunk",
            content: { type: "text", text: "The actual assistant answer." },
          });
        }
        yield* fixture.lifecycle("assistant.idle");
        yield* fixture.respond(prompt, {
          stopReason: scenario === "cancelled turn" ? "cancelled" : "end_turn",
        });
        assert.equal(
          (yield* fixture.terminal).status,
          scenario === "cancelled turn" ? "cancelled" : "completed",
        );
        const replies = fixture.observed.flatMap((event) =>
          event.type === "message.updated" && event.message.role === "assistant"
            ? [event.message.text]
            : [],
        );
        assert.notInclude(replies, summary);
        if (
          scenario === "later assistant answer" ||
          scenario === "assistant answer before completion event"
        ) {
          assert.include(replies, "The actual assistant answer.");
        } else {
          assert.isEmpty(replies);
        }
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
    );
  }

  it.effect("fails a lost native session delivered after a premature RPC success", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      yield* fixture.runtime.startTurn(fixture.turn(1));
      const prompt = yield* Queue.take(fixture.prompts);
      yield* fixture.lifecycle("user.message", { interactionId: "current" });
      yield* fixture.lifecycle("assistant.turn_start", { interactionId: "current" });
      yield* fixture.respond(prompt, { stopReason: "end_turn" });
      yield* Queue.take(fixture.rpcSettled);
      yield* fixture.lifecycle("session.error", {
        errorType: "session_error",
        message: "session event delivery failed: session not found: missing-child",
      });
      yield* TestClock.adjust("1 second");
      const failed = fixture.observed.find((event) => event.type === "turn.terminal");
      assert.equal(failed?.status, "failed");
      assert.include(failed?.failure?.message ?? "", "session event delivery failed");
      yield* fixture.runtime.startTurn(fixture.turn(2));
      const next = yield* Queue.take(fixture.prompts);
      assert.equal(fixture.spawns(), 2);
      yield* fixture.lifecycle("user.message", { interactionId: "next" });
      yield* fixture.lifecycle("assistant.turn_start", { interactionId: "next" });
      yield* fixture.lifecycle("assistant.idle");
      yield* fixture.respond(next, { stopReason: "end_turn" });
      yield* fixture.terminal;
      assert.equal((yield* fixture.terminal).status, "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect(
    "keeps the root turn active after an early RPC result and preserves its final reply",
    () =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture();
        assert.deepInclude(
          fixture.requests.find((request) => request.method === "initialize")?.params
            ?.clientCapabilities,
          {
            _meta: {
              "github.com/copilot": {
                events: [
                  "user.message",
                  "assistant.turn_start",
                  "assistant.idle",
                  "session.error",
                  "session.task_complete",
                  "tool.execution_start",
                  "tool.execution_complete",
                  "subagent.started",
                  "subagent.completed",
                  "subagent.failed",
                ],
              },
            },
          },
        );
        yield* fixture.runtime.startTurn(fixture.turn(1));
        const prompt = yield* Queue.take(fixture.prompts);
        yield* fixture.lifecycle("user.message", { interactionId: "current" });
        yield* fixture.lifecycle("assistant.turn_start", { interactionId: "current" });
        yield* fixture.update({
          sessionUpdate: "tool_call",
          toolCallId: "child-result",
          title: "task",
          status: "completed",
          content: [{ type: "content", content: { type: "text", text: "Child-only findings." } }],
        });
        yield* fixture.lifecycle("assistant.idle", {}, "child");
        yield* fixture.respond(prompt, { stopReason: "end_turn" });
        yield* Queue.take(fixture.rpcSettled);
        yield* TestClock.adjust("1 second");
        assert.isFalse(fixture.observed.some((event) => event.type === "turn.terminal"));
        const overlap = yield* fixture.runtime.startTurn(fixture.turn(2)).pipe(Effect.result);
        assert.equal(overlap._tag, "Failure", "a genuine active root turn must reject overlap");
        yield* fixture.update({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "The root final answer." },
        });
        yield* fixture.lifecycle("assistant.idle");
        assert.equal((yield* fixture.terminal).status, "completed");
        const messages = fixture.observed.flatMap((event) =>
          event.type === "message.updated" && event.message.role === "assistant"
            ? [event.message.text]
            : [],
        );
        assert.include(messages, "The root final answer.");
        assert.isFalse(messages.some((text) => text.includes("Child-only findings.")));
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("recovers a native session-host failure after a legitimately completed turn", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      yield* fixture.runtime.startTurn(fixture.turn(1));
      const first = yield* Queue.take(fixture.prompts);
      yield* fixture.lifecycle("user.message", { interactionId: "first" });
      yield* fixture.lifecycle("assistant.turn_start", { interactionId: "first" });
      yield* fixture.lifecycle("assistant.idle");
      yield* fixture.respond(first, { stopReason: "end_turn" });
      assert.equal((yield* fixture.terminal).status, "completed");

      // A later native wake is not evidence that the preceding RPC settled early.
      yield* fixture.lifecycle("assistant.turn_start", { interactionId: "background-wake" });
      yield* fixture.runtime.startTurn(fixture.turn(2));
      const prompt = yield* Queue.take(fixture.prompts);
      yield* fixture.respond(prompt, undefined, {
        code: -32603,
        message: "Internal error",
        data: {
          details:
            "Request session.send failed with message: session event delivery failed: session not found: missing-child",
        },
      });
      const failed = yield* fixture.terminal;
      assert.equal(failed.status, "failed");
      assert.include(failed.failure?.message ?? "", "saved conversation");
      assert.include(
        failed.failure?.message ?? "",
        "Request session.send failed with message: session event delivery failed: session not found: missing-child",
      );
      assert.equal(fixture.spawns(), 1);
      assert.equal(
        fixture.requests.filter((request) => request.method === "session/prompt").length,
        2,
      );
      yield* fixture.runtime.startTurn(fixture.turn(3));
      const next = yield* Queue.take(fixture.prompts);
      assert.equal(fixture.spawns(), 2);
      assert.deepEqual(fixture.processLifecycle, [
        "spawn:1",
        "prompt:1",
        "prompt:1",
        "kill:1",
        "spawn:2",
        "prompt:2",
      ]);
      assert.equal(next.params?.sessionId, "native-root");
      assert.isTrue(fixture.requests.some((request) => request.method === "session/load"));
      const latestActivation = fixture.requests.findLast((request) =>
        ["session/new", "session/load"].includes(request.method),
      );
      assert.equal(latestActivation?.method, "session/load");
      assert.equal(latestActivation?.params?.sessionId, "native-root");
      yield* fixture.lifecycle("user.message", { interactionId: "next" });
      yield* fixture.lifecycle("assistant.turn_start", { interactionId: "next" });
      yield* fixture.lifecycle("assistant.idle");
      yield* fixture.respond(next, { stopReason: "end_turn" });
      assert.equal((yield* fixture.terminal).status, "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("recovers a Copilot session-lock timeout only on explicit retry", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      yield* fixture.runtime.startTurn(fixture.turn(1));
      const prompt = yield* Queue.take(fixture.prompts);
      yield* fixture.respond(prompt, undefined, {
        code: -32603,
        message: "Internal error",
        data: {
          details:
            "session lock unavailable during is_session_generation_current: acquisition timed out",
        },
      });
      const failed = yield* fixture.terminal;
      assert.equal(failed.status, "failed");
      assert.include(
        failed.failure?.message ?? "",
        "session lock unavailable during is_session_generation_current: acquisition timed out",
      );
      assert.include(failed.failure?.message ?? "", "Send your message again");
      assert.deepEqual(fixture.processLifecycle, ["spawn:1", "prompt:1"]);
      assert.equal(
        fixture.requests.filter((request) => request.method === "session/prompt").length,
        1,
        "a failed request must not be automatically replayed",
      );

      yield* fixture.runtime.startTurn(fixture.turn(2));
      const next = yield* Queue.take(fixture.prompts);
      assert.equal(fixture.spawns(), 2);
      assert.deepEqual(fixture.processLifecycle, [
        "spawn:1",
        "prompt:1",
        "kill:1",
        "spawn:2",
        "prompt:2",
      ]);
      assert.equal(next.params?.sessionId, "native-root");
      const latestActivation = fixture.requests.findLast((request) =>
        ["session/new", "session/load"].includes(request.method),
      );
      assert.equal(latestActivation?.method, "session/load");
      assert.equal(latestActivation?.params?.sessionId, "native-root");
      yield* fixture.lifecycle("user.message", { interactionId: "retry" });
      yield* fixture.lifecycle("assistant.turn_start", { interactionId: "retry" });
      yield* fixture.lifecycle("assistant.idle");
      yield* fixture.respond(next, { stopReason: "end_turn" });
      assert.equal((yield* fixture.terminal).status, "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  for (const scenario of [
    {
      name: "another Registry agent",
      agentId: "fixture-agent",
      code: -32603,
      details:
        "session lock unavailable during is_session_generation_current: acquisition timed out",
    },
    {
      name: "a different error code",
      agentId: "github-copilot-cli",
      code: -32602,
      details:
        "session lock unavailable during is_session_generation_current: acquisition timed out",
    },
    {
      name: "a tool error quoting a lock timeout",
      agentId: "github-copilot-cli",
      code: -32603,
      details:
        "Tool failed: session lock unavailable during is_session_generation_current: acquisition timed out",
    },
  ]) {
    it.effect(`does not replace the runtime for a lock error from ${scenario.name}`, () =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture(scenario.agentId);
        yield* fixture.runtime.startTurn(fixture.turn(1));
        yield* fixture.respond(yield* Queue.take(fixture.prompts), undefined, {
          code: scenario.code,
          message: "Internal error",
          data: { details: scenario.details },
        });
        const failed = yield* fixture.terminal;
        assert.equal(failed.status, "failed");
        assert.equal(failed.failure?.message, "Internal error");
        yield* fixture.runtime.startTurn(fixture.turn(2));
        const next = yield* Queue.take(fixture.prompts);
        assert.equal(fixture.spawns(), 1);
        assert.deepEqual(fixture.processLifecycle, ["spawn:1", "prompt:1", "prompt:1"]);
        if (scenario.agentId === "github-copilot-cli") {
          yield* fixture.lifecycle("user.message", { interactionId: "retry" });
          yield* fixture.lifecycle("assistant.turn_start", { interactionId: "retry" });
          yield* fixture.lifecycle("assistant.idle");
        }
        yield* fixture.respond(next, { stopReason: "end_turn" });
        assert.equal((yield* fixture.terminal).status, "completed");
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
    );
  }

  for (const agentId of ["github-copilot-cli", "fixture-agent"]) {
    it.effect(`preserves ordinary prompt failures without replacing ${agentId}`, () =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture(agentId);
        yield* fixture.runtime.startTurn(fixture.turn(1));
        yield* fixture.respond(yield* Queue.take(fixture.prompts), undefined, {
          code: -32603,
          message: "Internal error",
          data: {
            details:
              agentId === "fixture-agent"
                ? "Request session.send failed with message: session event delivery failed: session not found: other-provider"
                : "An unrelated provider failure",
          },
        });
        assert.equal((yield* fixture.terminal).failure?.message, "Internal error");
        yield* fixture.runtime.startTurn(fixture.turn(2));
        const next = yield* Queue.take(fixture.prompts);
        assert.equal(fixture.spawns(), 1);
        if (agentId === "github-copilot-cli") {
          yield* fixture.lifecycle("user.message", { interactionId: "next" });
          yield* fixture.lifecycle("assistant.turn_start", { interactionId: "next" });
          yield* fixture.lifecycle("assistant.idle");
        }
        yield* fixture.respond(next, { stopReason: "end_turn" });
        assert.equal((yield* fixture.terminal).status, "completed");
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
    );
  }

  it.effect("does not turn genuine tool-only output into a fabricated root answer", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      yield* fixture.runtime.startTurn(fixture.turn(1));
      const prompt = yield* Queue.take(fixture.prompts);
      yield* fixture.lifecycle("user.message", { interactionId: "current" });
      yield* fixture.lifecycle("assistant.turn_start", { interactionId: "current" });
      yield* fixture.update({
        sessionUpdate: "tool_call",
        toolCallId: "data",
        title: "bash",
        status: "completed",
        content: [{ type: "content", content: { type: "text", text: "Actual tool data." } }],
      });
      yield* fixture.lifecycle("assistant.idle");
      yield* fixture.respond(prompt, { stopReason: "end_turn" });
      assert.equal((yield* fixture.terminal).status, "completed");
      assert.isFalse(
        fixture.observed.some(
          (event) => event.type === "message.updated" && event.message.role === "assistant",
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});
