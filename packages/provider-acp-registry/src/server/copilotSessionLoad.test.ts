import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";

import * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import { makeCopilotPromptCompletionRuntime } from "./copilotPromptCompletion.ts";

const Request = Schema.Struct({
  id: Schema.Union([Schema.String, Schema.Number]),
  method: Schema.String,
  params: Schema.Unknown,
});
const decodeRequestLine = Schema.decodeEffect(Schema.fromJsonString(Request));

const makeFixture = Effect.fnUntraced(function* (allowSessionLoadReplayIdle: boolean) {
  const input = yield* Queue.unbounded<Uint8Array>();
  const output = yield* Queue.unbounded<Uint8Array>();
  const requests = yield* Queue.unbounded<typeof Request.Type>();
  const exited = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
  const replayed = yield* Deferred.make<void>();
  const encoder = new TextEncoder();
  const stdout = Stream.fromQueue(input);
  const handle = ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(123),
    stdin: Sink.forEach((chunk: Uint8Array) => Queue.offer(output, chunk)),
    stdout,
    stderr: Stream.empty,
    all: stdout,
    exitCode: Deferred.await(exited),
    isRunning: Effect.succeed(true),
    kill: () => Deferred.succeed(exited, ChildProcessSpawner.ExitCode(0)).pipe(Effect.asVoid),
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
    unref: Effect.succeed(Effect.void),
  });
  const peer = yield* Stream.fromQueue(output).pipe(
    Stream.decodeText,
    Stream.splitLines,
    Stream.filter((line) => line.length > 0),
    Stream.mapEffect((line) => decodeRequestLine(line)),
    Stream.runForEach((request) => Queue.offer(requests, request)),
    Effect.forkChild,
  );
  const takeRequest = Queue.take(requests).pipe(
    Effect.raceFirst(
      Fiber.join(peer).pipe(Effect.andThen(Effect.die("Mock transport ended before its request"))),
    ),
  );
  const original = yield* AcpSessionRuntime.make({
    cwd: "/workspace",
    spawn: { command: "copilot", args: ["--acp"] },
    resumeSessionId: "saved-native-session",
    allowSessionLoadReplayIdle,
    clientInfo: { name: "t3-code", version: "0.0.0" },
  }).pipe(
    Effect.provideService(
      ChildProcessSpawner.ChildProcessSpawner,
      ChildProcessSpawner.make(() => Effect.succeed(handle)),
    ),
  );
  const runtime = yield* makeCopilotPromptCompletionRuntime(original, Effect.void);
  yield* runtime.handleExtNotification("test/replay-drained", Schema.Unknown, () =>
    Deferred.succeed(replayed, undefined).pipe(Effect.asVoid),
  );
  const send = (message: Record<string, unknown>) =>
    Queue.offer(input, encoder.encode(`${JSON.stringify({ jsonrpc: "2.0", ...message })}\n`));
  const respond = (request: typeof Request.Type, result: unknown) =>
    send({ id: request.id, result });
  const replay = Effect.gen(function* () {
    const initialize = yield* takeRequest;
    assert.equal(initialize.method, "initialize");
    yield* respond(initialize, {
      protocolVersion: 1,
      agentInfo: { name: "Copilot", version: "1.0.95" },
      agentCapabilities: { loadSession: true },
    });
    const load = yield* takeRequest;
    assert.equal(load.method, "session/load");
    yield* send({
      method: "session/update",
      params: {
        sessionId: "saved-native-session",
        update: {
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "History" },
        },
      },
    });
    yield* send({ method: "test/replay-drained", params: {} });
    yield* Deferred.await(replayed);
    yield* TestClock.adjust("3 seconds");
    yield* Effect.yieldNow.pipe(Effect.repeat({ times: 20 }));
    return load;
  });
  return { runtime, requests, takeRequest, send, respond, replay };
});

it.layer(NodeServices.layer)("Copilot session load readiness", (it) => {
  it.effect(
    "quarantines historical replay and keeps current thought and answer channels separate",
    () =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture(false);
        const deltas = yield* fixture.runtime.getEvents().pipe(
          Stream.filter((event) => event._tag === "ThoughtDelta" || event._tag === "ContentDelta"),
          Stream.map((event) => ({ kind: event._tag, text: event.text })),
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild,
        );
        const prompt = yield* fixture.runtime
          .prompt({ prompt: [{ type: "text", text: "Continue" }] })
          .pipe(Effect.forkChild);
        const load = yield* fixture.replay;
        const update = (sessionUpdate: string, text: string, extra = {}) =>
          fixture.send({
            method: "session/update",
            params: {
              sessionId: "saved-native-session",
              update: { sessionUpdate, content: { type: "text", text } },
              ...extra,
            },
          });
        yield* update("agent_thought_chunk", "Historical thought");
        yield* fixture.respond(load, {});
        const request = yield* fixture.takeRequest;
        assert.equal(request.method, "session/prompt");
        yield* update("agent_message_chunk", "Late historical answer", {
          _meta: { isReplay: true },
        });
        yield* update("agent_thought_chunk", "Current thought");
        yield* update("agent_message_chunk", "Foreign child answer", {
          sessionId: "child-session",
        });
        yield* update("agent_message_chunk", "Current answer");
        for (const type of ["user.message", "assistant.turn_start", "assistant.idle"]) {
          yield* fixture.send({
            method: "github.com/copilot/sessionEvent",
            params: { sessionId: "saved-native-session", type, data: { interactionId: "current" } },
          });
        }
        yield* fixture.respond(request, { stopReason: "end_turn" });
        assert.deepEqual(yield* Fiber.join(deltas), [
          { kind: "ThoughtDelta", text: "Current thought" },
          { kind: "ContentDelta", text: "Current answer" },
        ]);
        assert.equal((yield* Fiber.join(prompt)).stopReason, "end_turn");
      }),
  );

  it.effect("does not dispatch a prompt until the saved native session acknowledges its load", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture(false);
      const prompt = yield* fixture.runtime
        .prompt({ prompt: [{ type: "text", text: "Continue" }] })
        .pipe(Effect.forkChild);
      const load = yield* fixture.replay;
      assert.equal(
        yield* Queue.size(fixture.requests),
        0,
        "history idleness is not load readiness",
      );
      assert.isUndefined(prompt.pollUnsafe());

      yield* fixture.respond(load, {
        modes: {
          currentModeId: "agent",
          availableModes: [{ id: "agent", name: "Agent" }],
        },
      });
      const request = yield* fixture.takeRequest;
      assert.equal(request.method, "session/prompt");
      assert.deepEqual(request.params, {
        sessionId: "saved-native-session",
        prompt: [{ type: "text", text: "Continue" }],
      });
      for (const type of ["user.message", "assistant.turn_start", "assistant.idle"]) {
        yield* fixture.send({
          method: "github.com/copilot/sessionEvent",
          params: { sessionId: "saved-native-session", type, data: { interactionId: "new" } },
        });
      }
      yield* fixture.respond(request, { stopReason: "end_turn" });
      assert.equal((yield* Fiber.join(prompt)).stopReason, "end_turn");
      assert.equal((yield* fixture.runtime.getModeState)?.currentModeId, "agent");
    }),
  );

  it.effect("retains replay-idle readiness for agents that do not acknowledge session/load", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture(true);
      const start = yield* fixture.runtime.start().pipe(Effect.forkChild);
      yield* fixture.replay;
      assert.equal(
        (yield* Fiber.join(start)).sessionSetupResult._meta?.t3SessionLoadReady,
        "replay_idle",
      );
    }),
  );

  it.effect("surfaces a lost native session after replay instead of sending a new prompt", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture(false);
      const prompt = yield* fixture.runtime
        .prompt({ prompt: [{ type: "text", text: "Continue" }] })
        .pipe(Effect.forkChild);
      const load = yield* fixture.replay;
      yield* fixture.send({
        id: load.id,
        error: { code: -32603, message: "Native session not found" },
      });
      const error = yield* Fiber.join(prompt).pipe(Effect.flip);
      assert.include(error.message, "Native session not found");
      assert.equal(yield* Queue.size(fixture.requests), 0);
    }),
  );

  it.effect("fails a load timeout without treating the replayed history as success", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture(false);
      const start = yield* fixture.runtime.start().pipe(Effect.forkChild);
      yield* fixture.replay;
      yield* TestClock.adjust("90 seconds");
      const error = yield* Fiber.join(start).pipe(Effect.flip);
      if (error._tag !== "AcpTransportError") return assert.fail("Expected a native load timeout");
      assert.include(error.detail, "native load acknowledgement");
    }),
  );
});
