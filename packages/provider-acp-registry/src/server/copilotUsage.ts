import { sha256 } from "@noble/hashes/sha2";
import { bytesToHex, utf8ToBytes } from "@noble/hashes/utils";
import { NonNegativeInt, ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import {
  parseTimestampMs,
  totalTokens,
  type ProviderUsageReader,
  type TranscriptUsageFormat,
  type UsageRecord,
} from "@t3tools/provider-core/server/usage";
import * as HostProcess from "@t3tools/shared/HostProcess";

import type { AcpRegistrySettings } from "../settings.ts";
import { ACP_REGISTRY_PROVIDER } from "./adapter.ts";
import { isCopilotAgent } from "./copilotAcp.ts";

/** GitHub bills one AI credit at $0.01, and Copilot records credits in nano-AIU (1e-9 credits). */
const COPILOT_NANO_AIU_PER_USD = 100_000_000_000;

/** Usage adds an unconfigured entry under this ID for the host's Copilot CLI when no instance claims it. */
const HOST_INSTANCE_ID = ProviderInstanceId.make(ACP_REGISTRY_PROVIDER);

const CopilotScanState = Schema.Struct({
  sessionId: Schema.mutableKey(Schema.String),
  malformedRecords: Schema.mutableKey(NonNegativeInt),
});
type CopilotScanState = typeof CopilotScanState.Type;

const isSessionStart = Schema.is(
  Schema.Struct({
    type: Schema.Literal("session.start"),
    data: Schema.Struct({ sessionId: Schema.NonEmptyString }),
  }),
);
const isUsageReceipt = Schema.is(Schema.Struct({ type: Schema.Literal("session.usage_record") }));

const Tokens = Schema.optional(NonNegativeInt);
// Persisted receipts, not live `assistant.usage` events; shutdown totals overlap these and are skipped.
const UsageReceipt = Schema.Struct({
  id: Schema.optional(Schema.NonEmptyString),
  timestamp: Schema.String,
  data: Schema.Struct({
    usage: Schema.Struct({
      model: Schema.NonEmptyString,
      inputTokens: Tokens,
      outputTokens: Tokens,
      cacheReadTokens: Tokens,
      cacheWriteTokens: Tokens,
      reasoningTokens: Tokens,
      isByok: Schema.optional(Schema.Unknown),
      aiCreditsStatus: Schema.optional(Schema.Unknown),
      accounting: Schema.optional(
        Schema.Struct({
          sourceSessionId: Schema.optional(Schema.String),
          usageId: Schema.NonEmptyString,
        }),
      ),
      copilotUsage: Schema.optional(
        Schema.Struct({ totalNanoAiu: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)) }),
      ),
    }),
  }),
});
const decodeUsageReceipt = Schema.decodeUnknownOption(UsageReceipt);

// OpenAI usage ids run to 500 characters; a 128-bit digest keeps the scan cache small.
const usageKey = (id: string) => `copilot:${bytesToHex(sha256(utf8ToBytes(id))).slice(0, 32)}`;

function parseEvent(parsed: unknown, state: CopilotScanState): readonly UsageRecord[] {
  if (isSessionStart(parsed)) {
    state.sessionId = parsed.data.sessionId;
    return [];
  }
  if (!isUsageReceipt(parsed)) return [];
  const decoded = decodeUsageReceipt(parsed);
  const timestampMs = Option.isSome(decoded) ? parseTimestampMs(decoded.value.timestamp) : null;
  if (Option.isNone(decoded) || timestampMs === null) {
    state.malformedRecords += 1;
    return [];
  }
  const receipt = decoded.value;
  const usage = receipt.data.usage;
  const inputTokens = usage.inputTokens ?? 0;
  const cachedInputTokens = usage.cacheReadTokens ?? 0;
  const cacheCreationTokens = usage.cacheWriteTokens ?? 0;
  const outputTokens = usage.outputTokens ?? 0;
  // inputTokens includes cache reads and writes; a smaller total means a changed format.
  if (cachedInputTokens + cacheCreationTokens > inputTokens) {
    state.malformedRecords += 1;
    return [];
  }
  const totals = {
    uncachedInputTokens: inputTokens - cachedInputTokens - cacheCreationTokens,
    cachedInputTokens,
    cacheCreationTokens,
    outputTokens,
    reasoningTokens: Math.min(outputTokens, usage.reasoningTokens ?? 0),
  };
  const nanoAiu = usage.copilotUsage?.totalNanoAiu;
  // Partial and unavailable credits are not the full charge; a complete zero is a real zero.
  const reportedCostUsd =
    usage.aiCreditsStatus === "complete" && nanoAiu !== undefined
      ? nanoAiu / COPILOT_NANO_AIU_PER_USD
      : null;
  if (totalTokens(totals) === 0 && reportedCostUsd === null) return [];
  const identity = usage.accounting?.usageId ?? receipt.id;
  return [
    {
      provider: "copilot",
      timestampMs,
      model: usage.model,
      // Copilot-served models bill at the github_copilot rates; BYOK models bill elsewhere.
      ...(usage.isByok === true ? {} : { rateModel: `github_copilot/${usage.model}` }),
      // Subagent receipts sit in their parent's file and belong to its session.
      sessionId: state.sessionId || (usage.accounting?.sourceSessionId ?? ""),
      totals,
      reportedCostUsd,
      speed: "standard",
      dedupeKey: identity === undefined ? null : usageKey(identity),
    },
  ];
}

export const copilotUsageFormat: TranscriptUsageFormat<CopilotScanState> = {
  selectFields: {
    type: true,
    id: true,
    timestamp: true,
    data: {
      sessionId: true,
      usage: {
        model: true,
        inputTokens: true,
        outputTokens: true,
        cacheReadTokens: true,
        cacheWriteTokens: true,
        reasoningTokens: true,
        isByok: true,
        aiCreditsStatus: true,
        accounting: true,
        copilotUsage: { totalNanoAiu: true },
      },
    },
  },
  usageMarkers: ['"session.usage_record"', '"session.start"'],
  parseLine: (line, state) => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      return [];
    }
    return parseEvent(parsed, state);
  },
  parseProjected: parseEvent,
  readMalformedRecords: (state) => state.malformedRecords,
  state: { initial: () => ({ sessionId: "", malformedRecords: 0 }), schema: CopilotScanState },
};

export const copilotUsageReader: ProviderUsageReader<AcpRegistrySettings, Path.Path> = {
  kind: "transcripts",
  provider: "copilot",
  format: copilotUsageFormat,
  directories: Effect.fn("copilotUsageReader.directories")(function* ({
    instanceId,
    config,
    environment,
  }) {
    const runsCopilot = config !== undefined && isCopilotAgent(config);
    // An instance holding the host ID still owns the host's Copilot CLI history, whichever agent it runs.
    if (!runsCopilot && instanceId !== HOST_INSTANCE_ID) return [];
    const path = yield* Path.Path;
    const homeDirectory = yield* HostProcess.HomeDirectory;
    const copilotEnvironment = runsCopilot ? environment : yield* HostProcess.Environment;
    const copilotHome = copilotEnvironment.COPILOT_HOME?.trim();
    const home = copilotHome
      ? expandHomePath(copilotHome, homeDirectory)
      : path.join(homeDirectory, ".copilot");
    return [{ dir: path.resolve(home, "session-state"), fileName: "events.jsonl" }];
  }),
};
