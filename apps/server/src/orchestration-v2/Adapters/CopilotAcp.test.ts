import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import type { AcpToolCallState } from "../../provider/acp/AcpRuntimeModel.ts";
import {
  extractCopilotSubagentEndNotice,
  extractCopilotSubagentUpdate,
  extractCopilotSubagentUpdates,
} from "./CopilotAcp.ts";

const agentId = "c5dc746c-ebda-449a-99dd-180f6ed7a63e";
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
// The text is observed CLI 1.0.83 output. The ACP wrappers below are adapter fixtures.
const idle = `Agent is idle (waiting for messages). agent_id: ${agentId}, agent_type: general-purpose, status: idle, description: Prepare native Mac build, elapsed: 425s, total_turns: 3, model: gpt-6-astra`;
const delivered = `Message delivered to agent ${agentId}. Use read_agent to check the agent's response.`;
const unavailable = `No agent found with agent_id: ${agentId}. The agent may have been cleared or never existed.`;
const notice = `Agent "t3-desktop" (general-purpose) has finished processing and is now idle. Use read_agent with agent_id "${agentId}" to read the results, or write_agent to send follow-up messages.`;
const taskInput = {
  prompt: "Inspect the fixture.",
  agent_type: "general-purpose",
  description: "Inspect fixture",
  name: "fixture",
  model: "gpt-6-astra",
};

function tool(rawInput: unknown, output?: unknown, title = "read_agent"): AcpToolCallState {
  return {
    toolCallId: "tool-1",
    title,
    kind: "other",
    status: "completed",
    data: { rawInput, rawOutput: output },
  };
}

describe("Copilot native subagents", () => {
  it("keeps a background agent running when the launch tool completes", () => {
    expect(
      extractCopilotSubagentUpdate({
        toolCallId: "launch-1",
        title: "Inspect the fixture",
        kind: "other",
        status: "completed",
        data: {
          rawInput: {
            description: "Inspect the fixture",
            prompt: "Reply CHILD_DONE.",
            agent_type: "general-purpose",
            name: "fixture-agent",
            mode: "background",
            model: "gpt-6-astra",
          },
          rawOutput: {
            content:
              "Agent started in background with agent_id: 3135539c-7e0d-4027-9505-a1df9f746260. You'll be notified when it completes.",
            detailedContent: "Prompt to general-purpose agent: Reply CHILD_DONE.",
          },
        },
      }),
    ).toEqual({
      nativeTaskId: "3135539c-7e0d-4027-9505-a1df9f746260",
      launchToolCallId: "launch-1",
      childSessionId: null,
      prompt: "Reply CHILD_DONE.",
      title: "fixture-agent",
      model: "gpt-6-astra",
      status: "running",
      result: null,
    });
  });

  it("uses the launch tool identity until the native agent id is returned", () => {
    expect(
      extractCopilotSubagentUpdate({
        ...tool(taskInput, undefined, "Inspect fixture"),
        status: "inProgress",
      }),
    ).toMatchObject({
      nativeTaskId: "tool-1",
      launchToolCallId: "tool-1",
      status: "running",
      result: null,
    });
  });

  it("distinguishes synthetic sync success, launch failure and background ACKs", () => {
    expect(
      extractCopilotSubagentUpdate(tool(taskInput, { content: "DONE" }, "task")),
    ).toMatchObject({ status: "completed", result: "DONE", childSessionId: null });
    expect(
      extractCopilotSubagentUpdate({
        ...tool({ ...taskInput, mode: "background" }, { content: "Launch failed" }, "task"),
        status: "failed",
      }),
    ).toMatchObject({ status: "failed", result: "Launch failed" });
    expect(
      extractCopilotSubagentUpdate(
        tool({ ...taskInput, mode: "background" }, { content: "Acknowledged" }, "task"),
      ),
    ).toMatchObject({ status: "running", result: null });
  });

  it("reads observed idle and cancelled status headers without inventing telemetry", () => {
    expect(extractCopilotSubagentUpdate(tool({ agent_id: agentId }, { content: idle }))).toEqual({
      nativeTaskId: agentId,
      childSessionId: null,
      prompt: "",
      title: null,
      model: null,
      status: "idle",
      result: null,
      suppressNormalTool: false,
    });
    const cancelledId = "c825803d-b7bf-4e45-9124-2e6f203bf3df";
    const cancelled = `Agent cancelled. agent_id: ${cancelledId}, agent_type: general-purpose, status: cancelled, description: Expose Copilot background agents, elapsed: 952s, total_turns: 0, model: gpt-6-astra\n\nError: Cancelled`;
    expect(extractCopilotSubagentUpdate(tool({ agent_id: cancelledId }, cancelled))).toMatchObject({
      nativeTaskId: cancelledId,
      status: "cancelled",
      result: "Cancelled",
    });
  });

  it.each(["running", "idle", "completed", "failed", "cancelled"] as const)(
    "accepts schema-supported %s in a synthetic read envelope",
    (status) => {
      expect(
        extractCopilotSubagentUpdate(
          tool(
            { agent_id: agentId },
            { content: encodeJson({ agent_id: agentId, status, result: "RESULT" }) },
          ),
        ),
      ).toMatchObject({ nativeTaskId: agentId, status, result: "RESULT" });
    },
  );

  it("hydrates only the latest response with stable native turn identity", () => {
    const history = `${idle}\n\n[Turn 0]\nFIRST\n\n[Turn 1]\n[Message]\nFollow up.\n[Response]\nSECOND`;
    const update = extractCopilotSubagentUpdate(tool({ agent_id: agentId }, { content: history }));
    expect(update).toMatchObject({
      status: "idle",
      result: "SECOND",
      resultMessageId: "copilot:turn:1",
    });
    expect(
      extractCopilotSubagentUpdate(
        tool(
          { agent_id: agentId, since_turn: 1 },
          {
            content: `${idle}\n\n[Turn 1]\n[Message]\nFollow up.\n[Response]\nSECOND`,
          },
        ),
      ),
    ).toEqual(update);
  });

  it("hydrates the read-kind envelope emitted by Copilot read_agent", () => {
    expect(
      extractCopilotSubagentUpdate({
        ...tool(
          { agent_id: agentId, wait: true, timeout: 120 },
          {
            content: `${idle}\n\n[Turn 0]\nNATIVE_CHILD_OK`,
            detailedContent: `<agent ${agentId} idle>`,
          },
          "Tool",
        ),
        kind: "read",
      }),
    ).toMatchObject({
      nativeTaskId: agentId,
      status: "idle",
      result: "NATIVE_CHILD_OK",
      resultMessageId: "copilot:turn:0",
      suppressNormalTool: false,
    });
  });

  it("does not treat read-kind file tools, task launches or writes as agent reads", () => {
    for (const rawInput of [
      { file_path: "/workspace/agent.txt" },
      taskInput,
      { agent_id: agentId, message: "Continue." },
    ]) {
      expect(
        extractCopilotSubagentUpdate({ ...tool(rawInput, idle, "Tool"), kind: "read" }),
      ).toBeUndefined();
    }
  });

  it("projects confirmed delivery as pending, not started, and supports explicit batch recipients", () => {
    expect(
      extractCopilotSubagentUpdate(
        tool({ agent_id: agentId, message: "Continue." }, { content: delivered }, "write_agent"),
      ),
    ).toMatchObject({
      nativeTaskId: agentId,
      status: "pending",
      resumeToolCallId: "tool-1",
      followupPrompt: "Continue.",
      result: null,
      suppressNormalTool: false,
    });
    const otherId = "agent-2";
    const ack = `${delivered}\nMessage delivered to agent ${otherId}. Use read_agent to check the agent's response.`;
    expect(
      extractCopilotSubagentUpdates(
        tool({ agent_ids: [agentId, otherId, agentId], message: "Continue." }, ack, "write_agent"),
      ),
    ).toMatchObject([{ nativeTaskId: agentId }, { nativeTaskId: otherId }]);
  });

  it("does not mistake missing agents or failed delivery for child state", () => {
    for (const rawInput of [{ agent_id: agentId }, { agent_id: agentId, message: "Continue." }]) {
      expect(extractCopilotSubagentUpdate(tool(rawInput, unavailable))).toBeUndefined();
      expect(
        extractCopilotSubagentUpdate({ ...tool(rawInput, "Transport error"), status: "failed" }),
      ).toBeUndefined();
    }
    expect(
      extractCopilotSubagentUpdate(
        tool({ agent_id: agentId, message: "Continue." }, { content: delivered, isError: true }),
      ),
    ).toBeUndefined();
    expect(extractCopilotSubagentUpdate(tool({ agent_id: "other-agent" }, idle))).toBeUndefined();
  });

  it("ignores ordinary shell output, invalid identities and unrelated tool text", () => {
    expect(
      extractCopilotSubagentUpdate({
        ...tool({ command: "echo agent" }, idle, "bash"),
        kind: "execute",
      }),
    ).toBeUndefined();
    expect(
      extractCopilotSubagentUpdate(tool({ url: "https://example.com" }, idle, "fetch")),
    ).toBeUndefined();
    expect(
      extractCopilotSubagentUpdate(
        tool(
          { agent_id: "../bad" },
          {
            agent_id: "../bad",
            status: "completed",
          },
        ),
      ),
    ).toBeUndefined();
    expect(
      extractCopilotSubagentUpdate(tool({ agent_id: agentId }, "Work completed")),
    ).toBeUndefined();
  });

  it("recognizes only the observed idle notification body, not assistant commentary", () => {
    expect(extractCopilotSubagentEndNotice(notice)).toEqual({
      childSessionId: agentId,
      status: "idle",
    });
    expect(
      extractCopilotSubagentEndNotice(`<system_notification>\n${notice}\n</system_notification>`),
    ).toEqual({ childSessionId: agentId, status: "idle" });
    expect(extractCopilotSubagentEndNotice(`I saw this notification: ${notice}`)).toBeUndefined();
    expect(extractCopilotSubagentEndNotice(`Agent ${agentId} completed.`)).toBeUndefined();
  });
});
