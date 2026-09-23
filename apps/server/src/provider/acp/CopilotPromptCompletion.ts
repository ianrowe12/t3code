import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as AcpErrors from "effect-acp/errors";
import type * as AcpSchema from "effect-acp/compat";

import type { AcpSessionRuntime } from "./AcpSessionRuntime.ts";

export const copilotCompletionCapabilities = {
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
};

const SessionEvent = Schema.Struct({
  sessionId: Schema.String,
  type: Schema.String,
  agentId: Schema.optionalKey(Schema.String),
  data: Schema.Struct({
    interactionId: Schema.optionalKey(Schema.String),
    source: Schema.optionalKey(Schema.String),
    isAutopilotContinuation: Schema.optionalKey(Schema.Boolean),
    aborted: Schema.optionalKey(Schema.Boolean),
    message: Schema.optionalKey(Schema.String),
    summary: Schema.optionalKey(Schema.String),
    success: Schema.optionalKey(Schema.Boolean),
    outcome: Schema.optionalKey(Schema.String),
    toolCallId: Schema.optionalKey(Schema.String),
    toolName: Schema.optionalKey(Schema.String),
    parentToolCallId: Schema.optionalKey(Schema.String),
    mcpServerName: Schema.optionalKey(Schema.String),
    arguments: Schema.optionalKey(Schema.Unknown),
    omitted: Schema.optionalKey(Schema.String),
  }),
});
const isTaskCompleteInput = Schema.is(Schema.Struct({ summary: Schema.String }));
const isTaskCompleteOutput = Schema.is(
  Schema.Struct({ content: Schema.String, isError: Schema.optionalKey(Schema.Boolean) }),
);
const isTaskCompleteMeta = Schema.is(
  Schema.Struct({ "github.com/copilot/task_complete": Schema.String }),
);

export function copilotPromptFinalAnswer(response: AcpSchema.PromptResponse): string | undefined {
  return isTaskCompleteMeta(response._meta)
    ? response._meta["github.com/copilot/task_complete"]
    : undefined;
}

type Runtime = Pick<
  AcpSessionRuntime["Service"],
  | "start"
  | "prompt"
  | "cancel"
  | "closeSession"
  | "loadSession"
  | "getConfigOptions"
  | "getModeState"
  | "setConfigOption"
  | "setMode"
  | "getEvents"
  | "handleExtNotification"
  | "handleSessionUpdate"
>;

interface PendingPrompt {
  readonly sessionId: string;
  readonly started: Deferred.Deferred<void, AcpErrors.AcpError>;
  readonly idle: Deferred.Deferred<void, AcpErrors.AcpError>;
  readonly rpcFinished: Deferred.Deferred<void>;
  accepted: boolean;
  interactionId: string | undefined;
  running: boolean;
  cancelled: boolean;
  rpcSucceeded: boolean;
  recovery: Deferred.Deferred<void, AcpErrors.AcpError> | undefined;
  invalidated: boolean;
  omittedToolStart: boolean;
  taskComplete:
    | {
        readonly toolCallId?: string;
        readonly summary: string;
        status: "pending" | "accepted" | "rejected";
        hasAssistantReply: boolean;
        readonly useAcpResult?: boolean;
      }
    | undefined;
}

function completionError(detail: string) {
  return new AcpErrors.AcpRequestError({
    code: -32603,
    errorMessage: detail,
  });
}

/**
 * Copilot 1.0.83 can resolve send(wait: true) on the preceding loop's abort.
 * Its root user-message/start/idle events still delimit the new loop correctly.
 */
export const makeCopilotPromptCompletionRuntime = Effect.fn("makeCopilotPromptCompletionRuntime")(
  function* <R extends Runtime>(runtime: R, onRecoveryFailure: Effect.Effect<void>) {
    const permit = yield* Semaphore.make(1);
    let pending: PendingPrompt | undefined;
    let cancellation: Deferred.Deferred<void> | undefined;
    // Copilot's ACP prompt and cancel both call session.abort(), which cancels
    // every running background agent. Track those agents outside T3 prompts so
    // a new prompt waits for them instead of killing them.
    let agentSessionId: string | undefined;
    const runningAgents = new Set<string>();
    let quietWaiter: Deferred.Deferred<boolean> | undefined;
    const releaseQuietWaiter = Effect.suspend(() =>
      quietWaiter !== undefined && runningAgents.size === 0
        ? Deferred.succeed(quietWaiter, true)
        : Effect.void,
    );
    const resetRunningAgents = Effect.suspend(() => {
      runningAgents.clear();
      return releaseQuietWaiter;
    });
    // Check and register synchronously so a completion processed on the
    // transport fiber cannot drain the set between the two.
    const awaitRunningAgents = Effect.suspend(() => {
      if (runningAgents.size === 0) return Effect.succeed(true);
      const waiter = Deferred.makeUnsafe<boolean>();
      quietWaiter = waiter;
      return Deferred.await(waiter).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            if (quietWaiter === waiter) quietWaiter = undefined;
          }),
        ),
      );
    });
    const invalidate = Effect.fnUntraced(function* (current: PendingPrompt) {
      if (current.invalidated) return;
      current.invalidated = true;
      yield* onRecoveryFailure;
    });
    const closeAndReload = Effect.fnUntraced(function* (current: PendingPrompt) {
      if (current.recovery !== undefined) return yield* Deferred.await(current.recovery);
      const recovery = yield* Deferred.make<void, AcpErrors.AcpError>();
      current.recovery = recovery;
      yield* Effect.gen(function* () {
        const config = yield* runtime.getConfigOptions;
        const mode = yield* runtime.getModeState;
        yield* runtime.closeSession(current.sessionId);
        // Closing aborts every background agent, and the reload keeps the same
        // session id, so nothing else would drop their tracking.
        yield* resetRunningAgents;
        yield* runtime.loadSession(current.sessionId);
        for (const option of config) {
          yield* runtime.setConfigOption(option.id, option.currentValue);
        }
        if (mode !== undefined) yield* runtime.setMode(mode.currentModeId);
      }).pipe(
        Effect.onExit((exit) =>
          Effect.gen(function* () {
            if (Exit.isFailure(exit)) yield* invalidate(current);
            yield* Deferred.done(recovery, exit);
          }),
        ),
      );
    });
    yield* runtime.handleExtNotification(
      "github.com/copilot/sessionEvent",
      SessionEvent,
      Effect.fnUntraced(function* (event) {
        // Native subagent lifecycle events carry the child's agentId on both
        // start and completion; keying on it alone keeps add/delete symmetric.
        const agent = event.agentId;
        if (event.sessionId === agentSessionId && agent !== undefined) {
          if (event.type === "subagent.started") runningAgents.add(agent);
          if (event.type === "subagent.completed" || event.type === "subagent.failed") {
            runningAgents.delete(agent);
            yield* releaseQuietWaiter;
          }
        }
        const current = pending;
        if (
          current === undefined ||
          event.sessionId !== current.sessionId ||
          event.agentId !== undefined
        ) {
          return;
        }
        if (event.type === "tool.execution_start" && current.running) {
          current.omittedToolStart = event.data.omitted === "too-large";
        }
        if (
          event.type === "tool.execution_start" &&
          current.running &&
          event.data.toolName === "task_complete" &&
          event.data.toolCallId !== undefined &&
          event.data.parentToolCallId === undefined &&
          event.data.mcpServerName === undefined &&
          isTaskCompleteInput(event.data.arguments) &&
          event.data.arguments.summary.trim().length > 0
        ) {
          // Native task_complete summaries arrive as tool output, not ACP chat text.
          current.taskComplete = {
            toolCallId: event.data.toolCallId,
            summary: event.data.arguments.summary,
            status: "pending",
            hasAssistantReply: false,
          };
        } else if (
          event.type === "tool.execution_complete" &&
          event.data.parentToolCallId === undefined &&
          current.taskComplete?.toolCallId !== undefined &&
          current.taskComplete.toolCallId === event.data.toolCallId &&
          current.taskComplete.status !== "rejected"
        ) {
          current.taskComplete.status = event.data.success === true ? "accepted" : "rejected";
        } else if (
          event.type === "session.task_complete" &&
          current.running &&
          event.data.omitted === undefined
        ) {
          // The native CLI also accepts legacy completion events without a success field.
          const accepted =
            event.data.success !== false &&
            (event.data.outcome === undefined || event.data.outcome === "completed");
          current.taskComplete = {
            ...(current.taskComplete?.toolCallId === undefined
              ? {}
              : { toolCallId: current.taskComplete.toolCallId }),
            summary: event.data.summary ?? current.taskComplete?.summary ?? "",
            status: accepted ? "accepted" : "rejected",
            hasAssistantReply: current.taskComplete?.hasAssistantReply ?? false,
          };
        } else if (
          event.type === "session.error" &&
          event.data.message?.startsWith("session event delivery failed: session not found: ")
        ) {
          const error = completionError(
            "Copilot session event delivery failed because its native session was lost. Send your message again to reload the saved conversation.",
          );
          yield* invalidate(current);
          yield* Deferred.fail(current.started, error);
          yield* Deferred.fail(current.idle, error);
        } else if (
          event.type === "user.message" &&
          !current.accepted &&
          event.data.source === undefined &&
          event.data.isAutopilotContinuation !== true
        ) {
          current.accepted = true;
          current.interactionId = event.data.interactionId;
        } else if (
          event.type === "assistant.turn_start" &&
          current.accepted &&
          (current.interactionId === undefined ||
            current.interactionId === event.data.interactionId)
        ) {
          current.running = true;
          yield* Deferred.succeed(current.started, undefined);
        } else if (
          event.type === "assistant.idle" &&
          current.running &&
          (event.data.aborted !== true || current.cancelled)
        ) {
          yield* Deferred.succeed(current.idle, undefined);
        }
      }),
    );
    yield* runtime.handleSessionUpdate((notification) =>
      Effect.sync(() => {
        const current = pending;
        const update = notification.update;
        if (
          current === undefined ||
          notification.sessionId !== current.sessionId ||
          !current.running
        ) {
          return;
        }
        if (update.sessionUpdate === "tool_call") {
          const omittedStart = current.omittedToolStart;
          current.omittedToolStart = false;
          if (
            omittedStart &&
            update.title === "task_complete" &&
            update.kind === "other" &&
            update._meta === undefined &&
            isTaskCompleteInput(update.rawInput) &&
            update.rawInput.summary.trim().length > 0
          ) {
            // Copilot caps native event data; ACP still carries the complete tool input.
            current.taskComplete = {
              toolCallId: update.toolCallId,
              summary: update.rawInput.summary,
              status: update.status === "failed" ? "rejected" : "pending",
              hasAssistantReply: false,
              useAcpResult: true,
            };
          }
        }
        if (
          (update.sessionUpdate === "tool_call" || update.sessionUpdate === "tool_call_update") &&
          current.taskComplete?.useAcpResult === true &&
          current.taskComplete.toolCallId === update.toolCallId &&
          current.taskComplete.status === "pending"
        ) {
          if (update.status === "failed") {
            current.taskComplete.status = "rejected";
          } else if (update.status === "completed") {
            current.taskComplete.status =
              isTaskCompleteOutput(update.rawOutput) &&
              update.rawOutput.isError !== true &&
              update.rawOutput.content === current.taskComplete.summary
                ? "accepted"
                : "rejected";
          }
        }
        if (
          current?.taskComplete !== undefined &&
          update.sessionUpdate === "agent_message_chunk" &&
          update.content.type === "text" &&
          update.content.text.trim().length > 0
        ) {
          // Prefer the provider's actual reply when it follows task_complete.
          current.taskComplete.hasAssistantReply = true;
        }
      }),
    );

    return {
      ...runtime,
      getEvents: () =>
        runtime.getEvents().pipe(
          Stream.tap((event) => {
            if (event._tag !== "ConnectionTerminated") return Effect.void;
            // A held prompt is released on purpose: it proceeds into the dead
            // transport and surfaces that failure rather than silently vanishing.
            if (pending === undefined) return resetRunningAgents;
            return Effect.all([
              resetRunningAgents,
              Deferred.fail(pending.started, event.error),
              Deferred.fail(pending.idle, event.error),
            ]);
          }),
        ),
      prompt: (payload, options) =>
        permit.withPermit(
          Effect.gen(function* () {
            if (cancellation !== undefined) yield* Deferred.await(cancellation);
            const session = yield* runtime.start();
            if (agentSessionId !== session.sessionId) {
              agentSessionId = session.sessionId;
              yield* resetRunningAgents;
            }
            if (!(yield* awaitRunningAgents)) return { stopReason: "cancelled" as const };
            const current: PendingPrompt = {
              sessionId: session.sessionId,
              started: yield* Deferred.make<void, AcpErrors.AcpError>(),
              idle: yield* Deferred.make<void, AcpErrors.AcpError>(),
              rpcFinished: yield* Deferred.make<void>(),
              accepted: false,
              interactionId: undefined,
              running: false,
              cancelled: false,
              rpcSucceeded: false,
              recovery: undefined,
              invalidated: false,
              omittedToolStart: false,
              taskComplete: undefined,
            };
            pending = current;
            const result = yield* runtime.prompt(payload, options).pipe(
              Effect.tap((response) =>
                Effect.sync(() => {
                  current.rpcSucceeded = response.stopReason === "end_turn";
                }),
              ),
              Effect.ensuring(Deferred.succeed(current.rpcFinished, undefined)),
              Effect.flatMap((response) =>
                Effect.gen(function* () {
                  // Native slash commands can return text/configuration without a model loop.
                  const nativeCommand =
                    payload.prompt.length === 1 &&
                    payload.prompt[0]?.type === "text" &&
                    payload.prompt[0].text.trimStart().startsWith("/");
                  if (response.stopReason !== "end_turn" || (nativeCommand && !current.accepted)) {
                    return response;
                  }
                  yield* Deferred.await(current.started).pipe(
                    Effect.timeoutOrElse({
                      duration: "15 seconds",
                      orElse: () =>
                        Effect.gen(function* () {
                          yield* closeAndReload(current);
                          if (current.cancelled) return;
                          return yield* completionError(
                            "Copilot returned before acknowledging the new turn. The native session was stopped and reloaded; retry your message.",
                          );
                        }),
                    }),
                  );
                  yield* Deferred.await(current.idle);
                  if (current.cancelled) return { ...response, stopReason: "cancelled" as const };
                  return current.taskComplete?.status === "accepted" &&
                    !current.taskComplete.hasAssistantReply &&
                    current.taskComplete.summary.trim().length > 0
                    ? {
                        ...response,
                        _meta: {
                          ...response._meta,
                          "github.com/copilot/task_complete": current.taskComplete.summary,
                        },
                      }
                    : response;
                }),
              ),
              Effect.onExit(() =>
                Effect.sync(() => {
                  if (pending === current) pending = undefined;
                }),
              ),
            );
            return result;
          }),
        ),
      // Copilot's cancel (and the close fallback below) abort the whole native
      // session, background agents included, so every explicit cancel drops
      // agent tracking. Otherwise a stale entry holds the next prompt forever.
      // The barrier keeps the next prompt out until that reset lands, so it
      // cannot wipe agents the next turn has already started.
      cancel: Effect.gen(function* () {
        const current = pending;
        const barrier = yield* Deferred.make<void>();
        cancellation = barrier;
        if (current === undefined) {
          if (quietWaiter !== undefined) yield* Deferred.succeed(quietWaiter, false);
          return yield* runtime.cancel.pipe(
            Effect.ensuring(
              resetRunningAgents.pipe(Effect.andThen(Deferred.succeed(barrier, undefined))),
            ),
          );
        }
        current.cancelled = true;
        yield* Effect.gen(function* () {
          yield* runtime.cancel;
          yield* Deferred.await(current.rpcFinished);
          if (current.rpcSucceeded && !(yield* Deferred.isDone(current.idle))) {
            // After the early RPC return, Copilot's session/cancel becomes a no-op.
            // Closing aborts unconditionally; reload the saved session for the next turn.
            yield* closeAndReload(current);
          }
          yield* Deferred.succeed(current.started, undefined);
          yield* Deferred.succeed(current.idle, undefined);
        }).pipe(
          Effect.onExit((exit) =>
            Effect.gen(function* () {
              yield* resetRunningAgents;
              if (Exit.isFailure(exit)) {
                yield* invalidate(current);
                yield* Deferred.done(current.started, exit);
                yield* Deferred.done(current.idle, exit);
              }
              yield* Deferred.succeed(barrier, undefined);
            }),
          ),
        );
      }),
    } satisfies R & Pick<AcpSessionRuntime["Service"], "prompt" | "cancel" | "getEvents">;
  },
);
