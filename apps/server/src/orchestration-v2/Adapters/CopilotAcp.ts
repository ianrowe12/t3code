import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { AcpToolCallState } from "../../provider/acp/AcpRuntimeModel.ts";
import type { AcpAdapterV2SubagentUpdate } from "./AcpAdapterV2.ts";

const NonEmptyText = Schema.String.check(Schema.isMinLength(1));
const AgentId = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,199}$/));
const TaskInput = Schema.Struct({
  prompt: NonEmptyText,
  agent_type: NonEmptyText,
  description: Schema.optional(Schema.String),
  name: Schema.optional(Schema.String),
  model: Schema.optional(Schema.String),
  mode: Schema.optional(Schema.Literals(["sync", "background"])),
});
const AgentInput = Schema.Struct({
  agent_id: AgentId,
  message: Schema.optional(Schema.String),
  wait: Schema.optional(Schema.Boolean),
  since_turn: Schema.optional(Schema.Finite),
});
const AgentBatchInput = Schema.Struct({
  agent_ids: Schema.Array(AgentId),
  message: Schema.String,
});
const AgentStatus = Schema.Literals(["running", "idle", "completed", "failed", "cancelled"]);
const AgentOutput = Schema.Struct({
  agent_id: Schema.optional(AgentId),
  status: Schema.optional(AgentStatus),
  result: Schema.optional(Schema.String),
  summary: Schema.optional(Schema.String),
  output: Schema.optional(Schema.String),
});
const TextOutput = Schema.Struct({
  content: Schema.String,
});
const ContentOutput = Schema.Array(
  Schema.Struct({
    type: Schema.String,
    text: Schema.optional(Schema.String),
    content: Schema.optional(Schema.Struct({ type: Schema.String, text: Schema.String })),
  }),
);
const ErrorOutput = Schema.Struct({ isError: Schema.Literal(true) });
const decodeJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));
const decodeTask = Schema.decodeUnknownOption(TaskInput);
const decodeAgent = Schema.decodeUnknownOption(AgentInput);
const decodeBatch = Schema.decodeUnknownOption(AgentBatchInput);
const decodeOutput = Schema.decodeUnknownOption(AgentOutput);
const decodeText = Schema.decodeUnknownOption(TextOutput);
const decodeContent = Schema.decodeUnknownOption(ContentOutput);
const decodeStatus = Schema.decodeUnknownOption(AgentStatus);
const decodeId = Schema.decodeUnknownOption(AgentId);
const decodeError = Schema.decodeUnknownOption(ErrorOutput);

function outputText(tool: AcpToolCallState): string {
  const raw = tool.data.rawOutput;
  if (typeof raw === "string") return raw;
  const text = decodeText(raw);
  if (Option.isSome(text)) return text.value.content;
  const content = decodeContent(tool.data.content);
  return Option.isSome(content)
    ? content.value
        .flatMap((block) =>
          block.type === "text" && block.text !== undefined
            ? [block.text]
            : block.type === "content" && block.content?.type === "text"
              ? [block.content.text]
              : [],
        )
        .join("\n")
    : "";
}

function reportedAgent(tool: AcpToolCallState, text: string) {
  const json = Option.getOrUndefined(decodeJson(text));
  const structured = Option.getOrUndefined(decodeOutput(json ?? tool.data.rawOutput));
  const header = text
    .split("\n", 1)[0]
    ?.match(
      /^Agent [^\r\n]*?\. agent_id: ([a-zA-Z0-9_-]+), agent_type: [^,\r\n]+, status: (running|idle|completed|failed|cancelled),/,
    );
  const id = Option.getOrUndefined(
    decodeId(
      structured?.agent_id ??
        header?.[1] ??
        text.match(/^(?:Agent (?:ID|id)|agent_id):\s*([a-zA-Z0-9_-]+)\s*$/m)?.[1],
    ),
  );
  const status = Option.getOrUndefined(
    decodeStatus(
      structured?.status ??
        header?.[2] ??
        text.match(/^(?:Status|status):\s*(running|idle|completed|failed|cancelled)\s*$/m)?.[1],
    ),
  );
  const turns = [...text.matchAll(/^\[Turn (\d+)\][ \t]*\r?$/gm)];
  let historyResult: { readonly text: string; readonly messageId: string } | undefined;
  for (let index = 0; index < turns.length; index++) {
    const turn = turns[index]!;
    const body = text.slice(turn.index + turn[0].length, turns[index + 1]?.index).trim();
    const response =
      body.match(/^\[Response\][ \t]*\r?\n([\s\S]*)$/m)?.[1] ??
      (body.startsWith("[Message]") ? undefined : body);
    if (response?.trim()) {
      historyResult = { text: response.trim(), messageId: `copilot:turn:${turn[1]}` };
    }
  }
  const result =
    historyResult?.text ??
    structured?.result ??
    structured?.summary ??
    structured?.output ??
    text.match(/^(?:Result|Output|Summary|Error):[ \t]*\n?([\s\S]*)$/m)?.[1] ??
    null;
  return { id, status, result, resultMessageId: historyResult?.messageId };
}

/** Copilot task tools have descriptive ACP titles, so their native input is the discriminator. */
function copilotSubagentUpdate(
  tool: AcpToolCallState,
): AcpAdapterV2SubagentUpdate | ReadonlyArray<AcpAdapterV2SubagentUpdate> | undefined {
  // Never mine command/file output for agent-looking text.
  if (tool.command !== undefined) return undefined;
  const task = decodeTask(tool.data.rawInput);
  const agent = decodeAgent(tool.data.rawInput);
  const batch = decodeBatch(tool.data.rawInput);
  // Copilot classifies read_agent as "read", not "other".
  const isAgentRead =
    tool.kind === "read" && Option.isSome(agent) && agent.value.message === undefined;
  if (tool.kind !== undefined && tool.kind !== "other" && !isAgentRead) return undefined;
  if (Option.isNone(task) && Option.isNone(agent) && Option.isNone(batch)) return undefined;
  const text = outputText(tool);
  const reported = reportedAgent(tool, text);
  if (Option.isSome(task)) {
    const input = task.value;
    const backgroundId = text.match(
      /^Agent started in background with agent_id: ([a-zA-Z0-9_-]+)\./,
    )?.[1];
    const nativeTaskId =
      reported.id ?? Option.getOrUndefined(decodeId(backgroundId)) ?? tool.toolCallId;
    const background = input.mode === "background" || backgroundId !== undefined;
    const failed = tool.status === "failed" || Option.isSome(decodeError(tool.data.rawOutput));
    return {
      nativeTaskId,
      launchToolCallId: tool.toolCallId,
      childSessionId: null,
      prompt: input.prompt,
      title: input.name || input.description || null,
      model: input.model ?? null,
      status: failed
        ? "failed"
        : (reported.status ??
          (tool.status === "pending"
            ? "pending"
            : background || tool.status !== "completed"
              ? "running"
              : "completed")),
      result:
        reported.result ??
        (failed || (!background && tool.status === "completed") ? text || null : null),
    };
  }

  const input = Option.getOrUndefined(agent);
  const isWrite =
    input?.message !== undefined || (Option.isSome(batch) && batch.value.agent_ids.length > 0);
  if (isWrite) {
    // Delivery failure is not child failure. Keep the ordinary tool/error visible.
    if (tool.status !== "completed" || Option.isSome(decodeError(tool.data.rawOutput))) {
      return undefined;
    }
    const ids =
      input !== undefined ? [input.agent_id] : Option.isSome(batch) ? batch.value.agent_ids : [];
    const delivered = new Set(
      [
        ...text.matchAll(
          /(?:^|\n)Message delivered to agent ([a-zA-Z0-9_-]+)\. Use read_agent to check the agent's response\./g,
        ),
      ].map((match) => match[1]),
    );
    const updates = [...new Set(ids)]
      .filter((id) => delivered.has(id))
      .map((nativeTaskId): AcpAdapterV2SubagentUpdate => ({
        nativeTaskId,
        childSessionId: null,
        prompt: "",
        title: null,
        model: null,
        status: "pending",
        result: null,
        resumeToolCallId: tool.toolCallId,
        followupPrompt: input?.message ?? (Option.isSome(batch) ? batch.value.message : ""),
        suppressNormalTool: false,
      }));
    return updates.length === 0 ? undefined : updates.length === 1 ? updates[0] : updates;
  }

  if (
    input !== undefined &&
    reported.status !== undefined &&
    (reported.id === undefined || reported.id === input.agent_id)
  ) {
    return {
      nativeTaskId: input.agent_id,
      childSessionId: null,
      prompt: "",
      title: null,
      model: null,
      status: reported.status,
      result: reported.result,
      ...(reported.resultMessageId === undefined
        ? {}
        : { resultMessageId: reported.resultMessageId }),
      ...(reported.status === "running" ? { resumeToolCallId: tool.toolCallId } : {}),
      suppressNormalTool: false,
    };
  }
  return undefined;
}

export function extractCopilotSubagentUpdates(
  tool: AcpToolCallState,
): ReadonlyArray<AcpAdapterV2SubagentUpdate> {
  const update = copilotSubagentUpdate(tool);
  if (update === undefined) return [];
  return "nativeTaskId" in update ? [update] : update;
}

export function extractCopilotSubagentUpdate(
  tool: AcpToolCallState,
): AcpAdapterV2SubagentUpdate | undefined {
  const updates = extractCopilotSubagentUpdates(tool);
  return updates.length === 1 ? updates[0] : undefined;
}

/** Only the runtime notification envelope can announce completion, not assistant prose. */
export function extractCopilotSubagentEndNotice(text: string) {
  const body = text
    .trim()
    .replace(/^<system_notification>\s*([\s\S]*?)\s*<\/system_notification>$/, "$1");
  const notification = body.match(
    /^Agent "[^"\r\n]+" \([^()\r\n]+\) has finished processing and is now idle\. Use read_agent with agent_id "([a-zA-Z0-9_-]+)" to read the results, or write_agent to send follow-up messages\.$/,
  );
  if (notification?.[1] === undefined) return undefined;
  const id = Option.getOrUndefined(decodeId(notification[1]));
  return id === undefined ? undefined : { childSessionId: id, status: "idle" as const };
}
