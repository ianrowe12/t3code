import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { AcpSessionRuntime } from "./AcpSessionRuntime.ts";
import {
  copilotCompletionCapabilities,
  makeCopilotPromptCompletionRuntime,
} from "./CopilotPromptCompletion.ts";

const command = process.env.T3_COPILOT_COMPLETION_CLI;
const Lifecycle = Schema.Struct({
  type: Schema.String,
  agentId: Schema.optionalKey(Schema.String),
  data: Schema.Struct({ aborted: Schema.optionalKey(Schema.Boolean) }),
});
const decodeLifecycle = Schema.decodeUnknownEffect(Lifecycle);

// Opt-in only: starts a disposable native session and makes two real model requests.
describe.skipIf(command === undefined)("Copilot native completion regression", () => {
  for (const cancelReplacement of [false, true]) {
    it.live(
      cancelReplacement
        ? "cancels an early-returned native turn and resumes the same session"
        : "keeps the replacement turn open after an autonomous background wake",
      () =>
        Effect.gen(function* () {
          if (command === undefined) return yield* Effect.die("Copilot executable is required");
          const fs = yield* FileSystem.FileSystem;
          const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-copilot-completion-" });
          const wake = yield* Deferred.make<void>();
          const replacementStarted = yield* Deferred.make<void>();
          const observations: string[] = [];
          let firstCompleted = false;
          let secondStarted = false;
          let reply = "";
          const context = yield* Layer.build(
            AcpSessionRuntime.layer({
              cwd,
              spawn: { command, args: ["--acp", "--model", "gpt-6-astra"], cwd },
              clientInfo: { name: "t3-copilot-completion-regression", version: "1" },
              clientCapabilities: { _meta: copilotCompletionCapabilities },
              cancelBehavior: "wait-for-prompt",
              requestLogger: (event) =>
                Effect.sync(() => {
                  if (
                    secondStarted &&
                    event.method === "session/prompt" &&
                    event.status === "succeeded"
                  ) {
                    observations.push("rpc-completed");
                  }
                }).pipe(
                  Effect.andThen(
                    Effect.log("Copilot probe request", {
                      method: event.method,
                      status: event.status,
                    }),
                  ),
                ),
            }),
          );
          const base = yield* Effect.service(AcpSessionRuntime).pipe(Effect.provide(context));
          yield* base.handleRequestPermission((request) => {
            const allow = request.options.find((option) => option.kind === "allow_once");
            return Effect.succeed({
              outcome:
                allow === undefined
                  ? { outcome: "cancelled" as const }
                  : { outcome: "selected" as const, optionId: allow.optionId },
            });
          });
          const handleExtNotification: AcpSessionRuntime["Service"]["handleExtNotification"] = (
            method,
            schema,
            handler,
          ) =>
            base.handleExtNotification(method, schema, (notification) =>
              Effect.gen(function* () {
                const event = yield* decodeLifecycle(notification).pipe(Effect.orDie);
                if (event.agentId === undefined) {
                  if (secondStarted) observations.push(event.type);
                  if (secondStarted && event.type === "assistant.turn_start") {
                    yield* Deferred.succeed(replacementStarted, undefined);
                  }
                  if (firstCompleted && !secondStarted && event.type === "assistant.turn_start") {
                    yield* Deferred.succeed(wake, undefined);
                  }
                }
                yield* handler(notification);
              }),
            );
          const runtime = yield* makeCopilotPromptCompletionRuntime(
            { ...base, handleExtNotification },
            Effect.logError("Copilot probe recovery failed"),
          );
          yield* runtime.getEvents().pipe(
            Stream.runForEach((event) => {
              if (event._tag === "EventStreamBarrier") {
                return Deferred.succeed(event.acknowledge, undefined);
              }
              if (secondStarted && event._tag === "ContentDelta") reply += event.text;
              return Effect.void;
            }),
            Effect.forkChild,
          );
          yield* runtime.prompt({
            prompt: [
              {
                type: "text",
                text: "This is a controlled integration probe. Do not inspect or modify files, use the network, or delegate. Run exactly one bash command `sleep 8` with mode async and shellId `completion-regression-sleep`. Then immediately reply FIRST_DONE without waiting for the command. When notified it finishes, reply WAKE_DONE with no tools.",
              },
            ],
          });
          firstCompleted = true;
          yield* Deferred.await(wake);
          secondStarted = true;
          const replacement = yield* runtime
            .prompt({
              prompt: [{ type: "text", text: "Reply exactly SECOND_DONE. Do not run any tools." }],
            })
            .pipe(Effect.forkChild);
          if (cancelReplacement) {
            yield* Deferred.await(replacementStarted);
            yield* runtime.cancel;
            assert.equal((yield* Fiber.join(replacement)).stopReason, "cancelled");
            yield* runtime.prompt({
              prompt: [{ type: "text", text: "Reply exactly RESUMED_DONE. Do not run any tools." }],
            });
            yield* runtime.drainEvents;
            assert.include(reply, "RESUMED_DONE");
            return;
          }
          yield* Fiber.join(replacement);
          observations.push("wrapped-completed");
          yield* runtime.drainEvents;
          assert.include(reply, "SECOND_DONE");
          assert.isAbove(
            observations.indexOf("wrapped-completed"),
            observations.indexOf("user.message"),
          );
          assert.isAbove(
            observations.indexOf("wrapped-completed"),
            observations.indexOf("assistant.idle"),
          );
          assert.include(observations, "rpc-completed");
          yield* Effect.log("Copilot completion ordering", observations.join(" -> "));
        }).pipe(Effect.timeout("45 seconds"), Effect.scoped, Effect.provide(NodeServices.layer)),
    );
  }
});
