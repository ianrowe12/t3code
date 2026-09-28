import { describe, expect, it } from "@effect/vitest";
import {
  ChatAttachmentId,
  MessageId,
  PlanId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { queuedMessageInTurnBlocker } from "./Orchestrator.ts";

const threadId = ThreadId.make("thread:in-turn");
const instanceId = ProviderInstanceId.make("copilot");

function run(overrides: Partial<OrchestrationV2Run> = {}): OrchestrationV2Run {
  return {
    id: RunId.make("run:in-turn"),
    threadId,
    ordinal: 1,
    providerInstanceId: instanceId,
    modelSelection: { instanceId, model: "gpt-5.4" },
    providerThreadId: null,
    userMessageId: MessageId.make("message:in-turn"),
    rootNodeId: null,
    activeAttemptId: null,
    status: "queued",
    requestedAt: DateTime.makeUnsafe(0),
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
    ...overrides,
  };
}

const plain = { text: "Also check the logs.", attachments: [] };
const target = run({ id: RunId.make("run:target"), status: "running" });
const targetMessage = { text: "Run the long job.", attachments: [] };

const blockerFor = (input: Partial<Parameters<typeof queuedMessageInTurnBlocker>[0]>) =>
  queuedMessageInTurnBlocker({
    queuedRun: run(),
    message: plain,
    targetRun: target,
    targetMessage,
    ...input,
  });

describe("queuedMessageInTurnBlocker", () => {
  it("lets a plain message for the same model into the running turn", () => {
    expect(blockerFor({})).toBeNull();
  });

  it("keeps a message with attachments as its own turn", () => {
    expect(
      blockerFor({
        message: {
          text: "See the screenshot.",
          attachments: [
            {
              type: "image",
              id: ChatAttachmentId.make("attachment-1"),
              name: "shot.png",
              mimeType: "image/png",
              sizeBytes: 4,
            },
          ],
        },
      }),
    ).toBe("Messages with attachments must run as their own turn.");
  });

  it("keeps a message sent for another model as its own turn", () => {
    expect(
      blockerFor({ queuedRun: run({ modelSelection: { instanceId, model: "gpt-5.5" } }) }),
    ).toBe("The queued message asked for a different model selection.");
  });

  it("keeps plan and restart continuations as their own turn", () => {
    const reason = "Plan and restart continuations must run as their own turn.";
    expect(
      blockerFor({
        queuedRun: run({ sourcePlanRef: { threadId, planId: PlanId.make("plan-1") } }),
      }),
    ).toBe(reason);
    expect(
      blockerFor({ queuedRun: run({ restartContinuationOfRunId: RunId.make("run:earlier") }) }),
    ).toBe(reason);
  });

  it("keeps maintenance commands out of a turn and out of a maintenance turn", () => {
    expect(blockerFor({ message: { text: " /compact ", attachments: [] } })).toBe(
      "Native maintenance commands must run as their own turn.",
    );
    expect(blockerFor({ targetMessage: { text: "/logout", attachments: [] } })).toBe(
      "The target run is a maintenance command.",
    );
  });

  it("leaves completion notices to their own delivery path", () => {
    const reason = "Automatic completion deliveries keep their own delivery path.";
    expect(blockerFor({ message: { ...plain, delegatedCompletion: { taskIds: ["t"] } } })).toBe(
      reason,
    );
    expect(blockerFor({ message: { ...plain, notification: { summary: "done" } } })).toBe(reason);
  });
});
