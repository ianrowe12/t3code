import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scheduler from "effect/Scheduler";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as AcpErrors from "effect-acp/errors";
import type * as AcpSchema from "effect-acp/compat";

import type { AcpSessionRuntime, AcpSessionRuntimeEvent } from "./AcpSessionRuntime.ts";
import {
  copilotPromptFinalAnswer,
  makeCopilotPromptCompletionRuntime,
} from "./CopilotPromptCompletion.ts";

const makeFixture = Effect.fnUntraced(function* (
  options: {
    readonly cancel?: Effect.Effect<void, AcpErrors.AcpError>;
    readonly load?: Effect.Effect<void, AcpErrors.AcpError>;
  } = {},
) {
  const rpc = yield* Deferred.make<AcpSchema.PromptResponse, AcpErrors.AcpError>();
  const dispatched = yield* Queue.unbounded<void>();
  const loading = yield* Deferred.make<void>();
  const cancelling = yield* Deferred.make<void>();
  const events = yield* Queue.unbounded<AcpSessionRuntimeEvent>();
  let notify: (event: unknown) => Effect.Effect<void> = () =>
    Effect.die("Native event handler not registered");
  const calls: string[] = [];
  const setup = {
    sessionId: "root",
    initializeResult: { protocolVersion: 1 },
    sessionSetupResult: {},
    modelConfigId: undefined,
  };
  const handleExtNotification: AcpSessionRuntime["Service"]["handleExtNotification"] = (
    method,
    schema,
    handler,
  ) =>
    Effect.sync(() => {
      assert.equal(method, "github.com/copilot/sessionEvent");
      notify = (event) => handler(Schema.decodeUnknownSync(schema)(event)).pipe(Effect.orDie);
    });
  const runtime = yield* makeCopilotPromptCompletionRuntime(
    {
      start: () => Effect.succeed(setup),
      prompt: (_payload: Omit<AcpSchema.PromptRequest, "sessionId">) =>
        Queue.offer(dispatched, undefined).pipe(Effect.andThen(Deferred.await(rpc))),
      cancel: Effect.gen(function* () {
        calls.push("cancel");
        yield* Deferred.succeed(cancelling, undefined);
        yield* options.cancel ?? Effect.void;
      }),
      closeSession: (sessionId?: string) =>
        Effect.sync(() => {
          calls.push(`close:${sessionId}`);
          return {};
        }),
      loadSession: (sessionId: string) =>
        Effect.gen(function* () {
          calls.push(`load:${sessionId}`);
          yield* Deferred.succeed(loading, undefined);
          yield* options.load ?? Effect.void;
          return setup;
        }),
      getConfigOptions: Effect.succeed([]),
      getModeState: Effect.succeed(undefined),
      setConfigOption: () => Effect.succeed({ configOptions: [] }),
      setMode: () => Effect.succeed({}),
      getEvents: () => Stream.fromQueue(events),
      handleExtNotification,
      handleSessionUpdate: () => Effect.void,
    },
    Effect.sync(() => {
      calls.push("invalidate");
    }),
  );
  const event = (
    type: string,
    data: Record<string, unknown> = {},
    envelope: Record<string, unknown> = {},
  ) => notify({ sessionId: "root", type, data, ...envelope });
  const start = (text = "Continue?") =>
    Effect.gen(function* () {
      const fiber = yield* runtime
        .prompt({ prompt: [{ type: "text", text }] })
        .pipe(Effect.forkChild);
      yield* Queue.take(dispatched);
      return fiber;
    });
  const accept = Effect.gen(function* () {
    yield* event("user.message", { interactionId: "new" });
    yield* event("assistant.turn_start", { interactionId: "new" });
  });
  // Envelope recorded from Copilot CLI 1.0.88 events.jsonl, trimmed to identifying fields.
  const agentEvent = (
    type: "subagent.started" | "subagent.completed" | "subagent.failed",
    agentId: string,
  ) =>
    event(
      type,
      {
        toolCallId: `toolu_${agentId}`,
        agentName: "general-purpose",
        agentDisplayName: "sleeper",
        ...(type === "subagent.started" ? { executionMode: "background" } : {}),
      },
      { agentId, id: `${type}:${agentId}`, parentId: "parent-event" },
    );
  // A run that settled while its background agents keep running.
  const settleWithAgents = (...agentIds: ReadonlyArray<string>) =>
    Effect.gen(function* () {
      const first = yield* start();
      yield* accept;
      for (const agentId of agentIds) yield* agentEvent("subagent.started", agentId);
      yield* event("assistant.idle");
      yield* Deferred.succeed(rpc, { stopReason: "end_turn" });
      yield* Fiber.join(first);
    });
  const promptNext = runtime
    .prompt({ prompt: [{ type: "text", text: "Steer" }] })
    .pipe(Effect.forkChild);
  return {
    runtime,
    rpc,
    events,
    event,
    agentEvent,
    settleWithAgents,
    promptNext,
    start,
    accept,
    calls,
    loading,
    cancelling,
    dispatched,
  };
});

describe("Copilot prompt completion", () => {
  it.effect(
    "accepts legacy root completion summaries without carrying them into later prompts",
    () =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture();
        const first = yield* fixture.start();
        yield* fixture.accept;
        yield* fixture.event("session.task_complete", { summary: "The root answer." });
        yield* fixture.event("assistant.idle");
        yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
        assert.equal(copilotPromptFinalAnswer(yield* Fiber.join(first)), "The root answer.");
        const next = yield* fixture.start();
        yield* fixture.accept;
        yield* fixture.event("assistant.idle");
        assert.isUndefined(copilotPromptFinalAnswer(yield* Fiber.join(next)));
      }),
  );

  it.effect("does not finish on the old loop's abort before the new prompt starts", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const fiber = yield* fixture.start();
      yield* fixture.event("assistant.turn_start", { interactionId: "old" });
      yield* fixture.event("assistant.idle", { aborted: true });
      yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
      yield* Effect.yieldNow;
      assert.isUndefined(fiber.pollUnsafe());
      yield* fixture.accept;
      yield* fixture.event("assistant.idle", { aborted: true });
      yield* Effect.yieldNow;
      assert.isUndefined(fiber.pollUnsafe());
      yield* fixture.event("assistant.idle");
      assert.equal((yield* Fiber.join(fiber)).stopReason, "end_turn");
    }),
  );

  it.effect("ignores foreign sessions, child agents, and unrelated interaction starts", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const fiber = yield* fixture.start();
      yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
      for (const envelope of [{ sessionId: "foreign" }, { agentId: "child" }]) {
        yield* fixture.event("user.message", {}, envelope);
        yield* fixture.event("assistant.turn_start", {}, envelope);
        yield* fixture.event("assistant.idle", {}, envelope);
        yield* fixture.event("session.task_complete", { summary: "A different agent." }, envelope);
        yield* fixture.event(
          "session.error",
          { message: "session event delivery failed: session not found: missing-child" },
          envelope,
        );
      }
      yield* fixture.event("user.message", { interactionId: "new" });
      yield* fixture.event("assistant.turn_start", { interactionId: "old" });
      yield* fixture.event("assistant.idle");
      yield* Effect.yieldNow;
      assert.isUndefined(fiber.pollUnsafe());
      yield* fixture.event("assistant.turn_start", { interactionId: "new" });
      yield* fixture.event("session.error", { message: "An unrelated provider notice" });
      yield* fixture.event("assistant.idle");
      yield* Fiber.join(fiber);
      assert.notInclude(fixture.calls, "invalidate");
    }),
  );

  it.effect("accepts a large user-message envelope whose data was omitted by Copilot", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const fiber = yield* fixture.start();
      yield* fixture.event("user.message", { omitted: "too-large" }, { dataOmitted: "too-large" });
      yield* fixture.event("assistant.turn_start", { interactionId: "new" });
      yield* fixture.event("assistant.idle");
      const response = { stopReason: "end_turn", _meta: { retained: true } } as const;
      yield* Deferred.succeed(fixture.rpc, response);
      assert.deepEqual(yield* Fiber.join(fiber), response);
    }),
  );

  it.effect("preserves native commands that do not run a model turn", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const fiber = yield* fixture.start("/help");
      yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
      assert.equal((yield* Fiber.join(fiber)).stopReason, "end_turn");
    }),
  );

  it.effect("still waits for a model-backed command once its message has been accepted", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const fiber = yield* fixture.start("/skill");
      yield* fixture.accept;
      yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
      yield* Effect.yieldNow;
      assert.isUndefined(fiber.pollUnsafe());
      yield* fixture.event("assistant.idle");
      yield* Fiber.join(fiber);
    }),
  );

  it.effect("holds a new prompt until Copilot's running background agents finish", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      yield* fixture.settleWithAgents("bg-1", "bg-2");

      const next = yield* fixture.promptNext;
      yield* Effect.yieldNow;
      assert.equal(yield* Queue.size(fixture.dispatched), 0);
      yield* fixture.agentEvent("subagent.completed", "bg-2");
      yield* Effect.yieldNow;
      assert.equal(yield* Queue.size(fixture.dispatched), 0);
      yield* fixture.agentEvent("subagent.failed", "bg-1");
      yield* Queue.take(fixture.dispatched);
      yield* fixture.accept;
      yield* fixture.event("assistant.idle");
      assert.equal((yield* Fiber.join(next)).stopReason, "end_turn");
    }),
  );

  it.effect("cancels a held prompt without sending it into the busy session", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      yield* fixture.settleWithAgents("bg-1");

      const held = yield* fixture.promptNext;
      yield* Effect.yieldNow;
      yield* fixture.runtime.cancel;
      yield* Effect.yieldNow;
      assert.equal(yield* Queue.size(fixture.dispatched), 0);
      assert.equal((yield* Fiber.join(held)).stopReason, "cancelled");
      assert.deepEqual(fixture.calls, ["cancel"]);
    }),
  );

  it.effect("drops background-agent tracking when stopped with no prompt waiting", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      yield* fixture.settleWithAgents("bg-1");
      yield* fixture.runtime.cancel;
      assert.deepEqual(fixture.calls, ["cancel"]);

      yield* fixture.promptNext;
      yield* Effect.yieldNow;
      assert.equal(yield* Queue.size(fixture.dispatched), 1);
    }),
  );

  it.effect("drops background-agent tracking when a stop closes and reloads the session", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const first = yield* fixture.start();
      yield* fixture.agentEvent("subagent.started", "bg-1");
      yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
      yield* fixture.runtime.cancel;
      assert.deepEqual(fixture.calls, ["cancel", "close:root", "load:root"]);
      assert.equal((yield* Fiber.join(first)).stopReason, "cancelled");

      yield* fixture.promptNext;
      yield* Effect.yieldNow;
      assert.equal(yield* Queue.size(fixture.dispatched), 1);
    }),
  );

  it.effect("drops background-agent tracking when a missed start closes and reloads", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const first = yield* fixture.start();
      yield* fixture.agentEvent("subagent.started", "bg-1");
      yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
      yield* TestClock.adjust("15 seconds");
      const error = yield* Fiber.join(first).pipe(Effect.flip);
      assert.include(error.message, "before acknowledging the new turn");
      assert.deepEqual(fixture.calls, ["close:root", "load:root"]);

      yield* fixture.promptNext;
      yield* Effect.yieldNow;
      assert.equal(yield* Queue.size(fixture.dispatched), 1);
    }),
  );

  it.effect("keeps the next prompt out until a stop has dropped agent tracking", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const fixture = yield* makeFixture({ cancel: Deferred.await(release) });
      yield* fixture.settleWithAgents("bg-1");
      const cancel = yield* fixture.runtime.cancel.pipe(Effect.forkChild);
      yield* Deferred.await(fixture.cancelling);
      yield* fixture.agentEvent("subagent.completed", "bg-1");

      const second = yield* fixture.promptNext;
      yield* Effect.yieldNow;
      assert.equal(yield* Queue.size(fixture.dispatched), 0);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(cancel);
      yield* Queue.take(fixture.dispatched);
      yield* fixture.accept;
      yield* fixture.agentEvent("subagent.started", "bg-2");
      yield* fixture.event("assistant.idle");
      yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
      yield* Fiber.join(second);

      yield* fixture.promptNext;
      yield* Effect.yieldNow;
      assert.equal(yield* Queue.size(fixture.dispatched), 0, "held behind running bg-2");
    }),
  );

  it.effect("releases a prompt whose last background agent finishes as it begins waiting", () =>
    Effect.gen(function* () {
      // Preempt the prompt fiber after every operation and land the completion
      // at each point in its path to the wait.
      for (let delay = 0; delay < 40; delay++) {
        const fixture = yield* makeFixture();
        yield* fixture.settleWithAgents("bg-1");
        yield* fixture.promptNext.pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 3));
        yield* Effect.yieldNow.pipe(
          Effect.repeat({ times: delay }),
          Effect.andThen(fixture.agentEvent("subagent.completed", "bg-1")),
        );
        yield* Effect.yieldNow.pipe(Effect.repeat({ times: 100 }));
        assert.equal(yield* Queue.size(fixture.dispatched), 1, `completion after ${delay} yields`);
      }
    }),
  );

  it.effect("surfaces a missing start instead of claiming success or waiting forever", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const fiber = yield* fixture.start();
      yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
      yield* TestClock.adjust("15 seconds");
      const error = yield* Fiber.join(fiber).pipe(Effect.flip);
      assert.equal(error._tag, "AcpRequestError");
      assert.include(error.message, "before acknowledging the new turn");
      assert.deepEqual(fixture.calls, ["close:root", "load:root"]);
    }),
  );

  it.effect("propagates RPC failures without waiting for lifecycle events", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const fiber = yield* fixture.start();
      const error = new AcpErrors.AcpRequestError({ code: -32000, errorMessage: "Quota exceeded" });
      yield* Deferred.fail(fixture.rpc, error);
      assert.equal(yield* Fiber.join(fiber).pipe(Effect.flip), error);
    }),
  );

  it.effect("propagates connection loss after the premature RPC success", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      yield* fixture.runtime.getEvents().pipe(Stream.runDrain, Effect.forkChild);
      const fiber = yield* fixture.start();
      yield* fixture.accept;
      yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
      const error = new AcpErrors.AcpRequestError({ code: -32000, errorMessage: "Disconnected" });
      yield* Queue.offer(fixture.events, { _tag: "ConnectionTerminated", error });
      assert.equal(yield* Fiber.join(fiber).pipe(Effect.flip), error);
    }),
  );

  it.effect("cancels the native work even after Copilot has stopped honoring session/cancel", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const fiber = yield* fixture.start();
      yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
      yield* fixture.accept;
      yield* fixture.runtime.cancel;
      assert.equal((yield* Fiber.join(fiber)).stopReason, "cancelled");
      assert.deepEqual(fixture.calls, ["cancel", "close:root", "load:root"]);
    }),
  );

  for (const cancelFirst of [true, false]) {
    it.effect(
      `shares one recovery when ${cancelFirst ? "cancel races timeout" : "timeout races cancel"}`,
      () =>
        Effect.gen(function* () {
          const release = yield* Deferred.make<void>();
          const fixture = yield* makeFixture({ load: Deferred.await(release) });
          const fiber = yield* fixture.start();
          yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
          yield* TestClock.adjust(cancelFirst ? "14 seconds" : "15 seconds");
          const cancel = yield* fixture.runtime.cancel.pipe(Effect.forkChild);
          yield* Deferred.await(fixture.cancelling);
          yield* Deferred.await(fixture.loading);
          if (cancelFirst) yield* TestClock.adjust("1 second");
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(cancel);
          assert.equal((yield* Fiber.join(fiber)).stopReason, "cancelled");
          assert.equal(fixture.calls.filter((call) => call === "close:root").length, 1);
          assert.equal(fixture.calls.filter((call) => call === "load:root").length, 1);
        }),
    );
  }

  it.effect(
    "does not replay a failed cancel into later prompts and requests runtime replacement",
    () =>
      Effect.gen(function* () {
        const error = new AcpErrors.AcpRequestError({
          code: -32000,
          errorMessage: "Cancel failed",
        });
        const fixture = yield* makeFixture({ cancel: Effect.fail(error) });
        const first = yield* fixture.start();
        yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
        assert.equal(yield* fixture.runtime.cancel.pipe(Effect.flip), error);
        assert.equal(yield* Fiber.join(first).pipe(Effect.flip), error);
        const next = yield* fixture.start();
        yield* fixture.accept;
        yield* fixture.event("assistant.idle");
        assert.equal((yield* Fiber.join(next)).stopReason, "end_turn");
        assert.include(fixture.calls, "invalidate");
      }),
  );

  it.effect("invalidates a failed reload rather than poisoning subsequent prompts", () =>
    Effect.gen(function* () {
      const error = new AcpErrors.AcpRequestError({ code: -32000, errorMessage: "Load failed" });
      const fixture = yield* makeFixture({ load: Effect.fail(error) });
      const first = yield* fixture.start();
      yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
      assert.equal(yield* fixture.runtime.cancel.pipe(Effect.flip), error);
      assert.equal(yield* Fiber.join(first).pipe(Effect.flip), error);
      assert.deepEqual(fixture.calls, ["cancel", "close:root", "load:root", "invalidate"]);
      const next = yield* fixture.start();
      yield* fixture.accept;
      yield* fixture.event("assistant.idle");
      assert.equal((yield* Fiber.join(next)).stopReason, "end_turn");
    }),
  );

  it.effect("does not replay an interrupted cancellation into the next turn", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const fixture = yield* makeFixture({ load: Deferred.await(release) });
      const first = yield* fixture.start();
      yield* Deferred.succeed(fixture.rpc, { stopReason: "end_turn" });
      const cancel = yield* fixture.runtime.cancel.pipe(Effect.forkChild);
      yield* Deferred.await(fixture.loading);
      yield* Fiber.interrupt(cancel);
      yield* Fiber.await(first);
      const next = yield* fixture.start();
      yield* fixture.accept;
      yield* fixture.event("assistant.idle");
      assert.equal((yield* Fiber.join(next)).stopReason, "end_turn");
      assert.include(fixture.calls, "invalidate");
    }),
  );
});
