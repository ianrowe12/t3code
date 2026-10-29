import { describe, expect, it } from "@effect/vitest";

import { copilotUsageFormat } from "./copilotUsage.ts";

// Shapes and figures from Copilot CLI 1.0.105 events.jsonl, without workspace paths.
const SESSION = "2bc5779c-7239-402e-ab8c-ded91a7024cd";
const SUBAGENT_SOURCE = "6c911624-d3c2-47cc-9403-285f81f2dd8c";
const SUBAGENT_USAGE_ID = `PL6k5gkBQ9DZmXy8eu8rBtjI76t4f3pqFoxIsD261a3+${"a".repeat(456)}`;

function receipt(input: {
  readonly eventId: string;
  readonly timestamp: string;
  readonly agentId?: string;
  readonly model: string;
  readonly tokens: readonly [input: number, output: number, read: number, write: number];
  readonly reasoningTokens: number;
  readonly accounting: { readonly sourceSessionId: string; readonly sequence: number };
  readonly usageId: string;
  readonly totalNanoAiu: number;
  readonly overrides?: Record<string, unknown>;
}) {
  const [inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens] = input.tokens;
  return {
    type: "session.usage_record",
    data: {
      usage: {
        model: input.model,
        provider: { id: "copilot", kind: "copilot", label: "GitHub Copilot" },
        inputTokens,
        outputTokens,
        cacheReadTokens,
        cacheWriteTokens,
        reasoningTokens: input.reasoningTokens,
        // The premium request multiplier, not a price.
        cost: input.agentId === undefined ? 15.0 : 1.0,
        initiator: input.agentId === undefined ? "user" : "sub-agent",
        isByok: false,
        accounting: { ...input.accounting, usageId: input.usageId },
        copilotUsage: {
          tokenDetails: [
            {
              batchSize: 1_000_000,
              costPerBatch: 400_000_000_000,
              tokenCount: inputTokens - cacheReadTokens - cacheWriteTokens,
              tokenType: "input",
              model: input.model,
            },
          ],
          totalNanoAiu: input.totalNanoAiu,
        },
        aiCreditsStatus: "complete",
        ...input.overrides,
      },
    },
    id: input.eventId,
    timestamp: input.timestamp,
    ...(input.agentId === undefined ? {} : { agentId: input.agentId }),
  };
}

const sessionStart = {
  type: "session.start",
  data: { sessionId: SESSION, version: 1, producer: "copilot-agent", copilotVersion: "1.0.105" },
  id: "bb13fcb6-47a0-4877-a32c-ea15edb40b75",
  timestamp: "2026-10-29T12:22:58.891Z",
  parentId: null,
};
const mainReceipt = receipt({
  eventId: "6045ff8d-c1c8-4b07-9c99-91c072bacb43",
  timestamp: "2026-10-29T12:23:06.671Z",
  model: "claude-opus-5.5",
  tokens: [31_204, 382, 0, 31_200],
  reasoningTokens: 98,
  accounting: { sourceSessionId: SESSION, sequence: 1 },
  usageId: "msg_011CgWXCE3L2SyTmgTTRixKp",
  totalNanoAiu: 16_365_600_000,
});
const subagentReceipt = (sequence: number) =>
  receipt({
    eventId: `subagent-event-${sequence}`,
    timestamp: `2026-10-29T12:42:${19 + sequence}.826Z`,
    agentId: "32b1a7ed-76b2-45bc-b8bd-9cfe22862e25",
    model: "gpt-6.1-sol",
    tokens: sequence === 1 ? [14_865, 108, 0, 14_862] : [15_217, 287, 14_862, 352],
    reasoningTokens: sequence === 1 ? 0 : 57,
    accounting: { sourceSessionId: SUBAGENT_SOURCE, sequence },
    usageId: SUBAGENT_USAGE_ID + sequence,
    totalNanoAiu: sequence === 1 ? 3_824_100_000 : 524_220_000,
  });
const sessionResume = {
  type: "session.resume",
  data: { resumeTime: "2026-10-29T14:02:50.245Z", eventCount: 553, alreadyInUse: false },
  id: "1b9bd65c-2a9f-4425-bc5b-bf767ebbdd0b",
  timestamp: "2026-10-29T14:02:50.246Z",
};
const sessionShutdown = {
  type: "session.shutdown",
  data: {
    totalNanoAiu: 376_927_720_000,
    modelMetrics: {
      "claude-opus-5.5": {
        usage: { inputTokens: 3_524_284, outputTokens: 40_474, cacheReadTokens: 3_164_251 },
        totalNanoAiu: 324_240_720_000,
      },
    },
    usageAccountingWatermarks: { [SESSION]: 41, [SUBAGENT_SOURCE]: 21 },
  },
  timestamp: "2026-10-29T14:33:01.194Z",
};

function parse(lines: readonly unknown[]) {
  const state = copilotUsageFormat.state!.initial();
  const records = lines.flatMap((line) =>
    copilotUsageFormat.parseLine(JSON.stringify(line), state),
  );
  return { records, malformedRecords: copilotUsageFormat.readMalformedRecords!(state) };
}

describe("copilotUsageFormat", () => {
  it("reads main and subagent receipts across a resume and skips shutdown totals", () => {
    const { records, malformedRecords } = parse([
      sessionStart,
      mainReceipt,
      subagentReceipt(1),
      sessionResume,
      // A later process appending the same model call writes a new event.
      { ...mainReceipt, id: "copied-event" },
      subagentReceipt(2),
      sessionShutdown,
    ]);

    expect(malformedRecords).toBe(0);
    expect(records.map((record) => record.model)).toEqual([
      "claude-opus-5.5",
      "gpt-6.1-sol",
      "claude-opus-5.5",
      "gpt-6.1-sol",
    ]);
    expect(records.every((record) => record.sessionId === SESSION)).toBe(true);
    const [main, firstSubagent, copiedMain, secondSubagent] = records;
    expect(main).toMatchObject({
      provider: "copilot",
      rateModel: "github_copilot/claude-opus-5.5",
      timestampMs: Date.parse("2026-10-29T12:23:06.671Z"),
      totals: {
        uncachedInputTokens: 4,
        cachedInputTokens: 0,
        cacheCreationTokens: 31_200,
        outputTokens: 382,
        reasoningTokens: 98,
      },
    });
    expect(main?.reportedCostUsd).toBeCloseTo(0.163_656, 12);
    expect(secondSubagent?.totals).toEqual({
      uncachedInputTokens: 3,
      cachedInputTokens: 14_862,
      cacheCreationTokens: 352,
      outputTokens: 287,
      reasoningTokens: 57,
    });
    expect(secondSubagent?.reportedCostUsd).toBeCloseTo(0.005_242_2, 12);
    expect(copiedMain?.dedupeKey).toBe(main?.dedupeKey);
    expect(new Set(records.map((record) => record.dedupeKey)).size).toBe(3);
    expect(firstSubagent?.dedupeKey?.length).toBeLessThan(64);
  });

  it.each([
    ["partial credits", { aiCreditsStatus: "partial" }, "price"],
    ["unavailable credits", { aiCreditsStatus: "unavailable" }, "price"],
    ["zero credits", { copilotUsage: { totalNanoAiu: 0 } }, "zero"],
    ["missing credits", { copilotUsage: undefined }, "price"],
    ["a BYOK model", { isByok: true, copilotUsage: undefined }, "byok"],
    ["cache tokens above input", { cacheReadTokens: 1 }, "malformed"],
    ["a non-integer counter", { outputTokens: 1.5 }, "malformed"],
  ] as const)("handles %s", (_name, overrides, expected) => {
    const { records, malformedRecords } = parse([
      sessionStart,
      receipt({
        eventId: "event-1",
        timestamp: "2026-10-29T12:23:06.671Z",
        model: "claude-opus-5.5",
        tokens: [31_204, 382, 0, 31_204],
        reasoningTokens: 98,
        accounting: { sourceSessionId: SESSION, sequence: 1 },
        usageId: "msg_1",
        totalNanoAiu: 16_365_600_000,
        overrides,
      }),
    ]);

    if (expected === "malformed") {
      expect(records).toEqual([]);
      expect(malformedRecords).toBe(1);
      return;
    }
    expect(malformedRecords).toBe(0);
    expect(records).toHaveLength(1);
    expect(records[0]?.reportedCostUsd).toBe(expected === "zero" ? 0 : null);
    expect(records[0]?.rateModel).toBe(
      expected === "byok" ? undefined : "github_copilot/claude-opus-5.5",
    );
  });
});
