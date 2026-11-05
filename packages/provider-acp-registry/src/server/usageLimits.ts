import type { ServerProviderUsageWindow } from "@t3tools/contracts";
import {
  clampPercent,
  makeUnavailableUsageLimits,
  makeUsageLimits,
} from "@t3tools/provider-core/server/usageLimits";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import type { AcpRegistrySettings } from "../settings.ts";
import * as AcpRegistrySupport from "./AcpRegistrySupport.ts";

const FiniteNumber = Schema.Number.check(Schema.isFinite());
const QuotaSnapshot = Schema.Struct({
  isUnlimitedEntitlement: Schema.Boolean,
  entitlementRequests: FiniteNumber.check(Schema.isGreaterThanOrEqualTo(0)),
  usedRequests: FiniteNumber.check(Schema.isGreaterThanOrEqualTo(0)),
  remainingPercentage: FiniteNumber,
  resetDate: Schema.optionalKey(Schema.String),
  tokenBasedBilling: Schema.optionalKey(Schema.Boolean),
});
const QuotaResponse = Schema.Struct({
  quotaSnapshots: Schema.Record(Schema.String, QuotaSnapshot),
});
const decodeQuotaResponse = Schema.decodeUnknownOption(QuotaResponse);
const Reply = Schema.Struct({
  jsonrpc: Schema.Literal("2.0"),
  id: Schema.Literal(1),
  result: Schema.Unknown,
});
const isReply = Schema.is(Schema.Struct({ id: Schema.Literal(1) }));
const decodeReply = Schema.decodeUnknownEffect(Reply);
const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const numberFormat = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const MAX_FRAME_BYTES = 1_048_576;
const MAX_HEADER_BYTES = 8_192;
// Copilot's enterprise resetDate can be the request time, not a billing-cycle boundary.
const MIN_RESET_DELAY_MS = 60_000;

class CopilotQuotaReadError extends Schema.TaggedError<CopilotQuotaReadError>()(
  "CopilotQuotaReadError",
  { cause: Schema.optionalKey(Schema.Defect()) },
) {}

const probeFailed = (checkedAt: string) =>
  makeUnavailableUsageLimits({
    checkedAt,
    reason: "probeFailed",
    message: "GitHub Copilot could not read usage limits.",
  });

export function copilotQuotaResponseToLimits(response: unknown, checkedAt: string) {
  const decoded = decodeQuotaResponse(response);
  if (Option.isNone(decoded)) return probeFailed(checkedAt);
  const windows: ServerProviderUsageWindow[] = [];
  for (const [id, quota] of Object.entries(decoded.value.quotaSnapshots)) {
    if (quota.isUnlimitedEntitlement || quota.entitlementRequests === 0) continue;
    const name =
      id === "premium_interactions"
        ? quota.tokenBasedBilling
          ? "AI credits"
          : "Premium requests"
        : id === "chat"
          ? "Chat"
          : id === "completions"
            ? "Completions"
            : id;
    const spend =
      id === "premium_interactions" && quota.tokenBasedBilling
        ? ` ($${numberFormat.format(quota.usedRequests * 0.01)})`
        : "";
    const reset = Date.parse(quota.resetDate ?? "");
    const resetsAt =
      Number.isFinite(reset) && reset > Date.parse(checkedAt) + MIN_RESET_DELAY_MS
        ? DateTime.formatIso(DateTime.makeUnsafe(reset))
        : undefined;
    windows.push({
      id,
      kind: "monthly",
      label: `${name}: ${numberFormat.format(quota.usedRequests)} of ${numberFormat.format(quota.entitlementRequests)}${spend}`,
      usedPercent: clampPercent(100 - quota.remainingPercentage),
      ...(resetsAt ? { resetsAt } : {}),
    });
  }
  return windows.length > 0
    ? makeUsageLimits({ checkedAt, windows })
    : makeUnavailableUsageLimits({ checkedAt, reason: "unsupported" });
}

function makeFrameReader() {
  let pending = new Uint8Array(0);
  let contentLength: number | undefined;
  const decoder = new TextDecoder("utf-8", { fatal: true });
  return (chunk: Uint8Array): string[] => {
    if (pending.length + chunk.length > MAX_FRAME_BYTES + MAX_HEADER_BYTES) {
      throw new Error("Copilot quota frame is too large.");
    }
    const bytes = new Uint8Array(pending.length + chunk.length);
    bytes.set(pending);
    bytes.set(chunk, pending.length);
    pending = bytes;
    const frames: string[] = [];
    while (pending.length > 0) {
      if (contentLength === undefined) {
        const boundary = pending.findIndex(
          (byte, index) =>
            byte === 13 &&
            pending[index + 1] === 10 &&
            pending[index + 2] === 13 &&
            pending[index + 3] === 10,
        );
        if (boundary < 0) {
          if (pending.length > MAX_HEADER_BYTES) throw new Error("Invalid Copilot quota header.");
          break;
        }
        if (boundary > MAX_HEADER_BYTES) throw new Error("Invalid Copilot quota header.");
        const header = decoder.decode(pending.subarray(0, boundary));
        const lengths = [...header.matchAll(/^Content-Length:\s*(\d+)\s*$/gim)];
        const length = Number(lengths[0]?.[1]);
        if (lengths.length !== 1 || !Number.isSafeInteger(length) || length > MAX_FRAME_BYTES) {
          throw new Error("Invalid Copilot quota content length.");
        }
        contentLength = length;
        pending = pending.subarray(boundary + 4);
      }
      if (pending.length < contentLength) break;
      frames.push(decoder.decode(pending.subarray(0, contentLength)));
      pending = pending.subarray(contentLength);
      contentLength = undefined;
    }
    return frames;
  };
}

export const readCopilotUsageLimits = Effect.fn("readCopilotUsageLimits")(function* (
  settings: AcpRegistrySettings,
  cwd: string,
  environment: NodeJS.ProcessEnv,
) {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  return yield* Effect.gen(function* () {
    const catalog = yield* AcpRegistrySupport.AcpRegistryCatalog;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const { spawn } = yield* catalog.resolve(settings, cwd, environment);
    // Keep launchers and their arguments, replacing only ACP's transport selector.
    const args = [
      ...spawn.args.filter((arg) => arg !== "--acp"),
      "--headless",
      "--stdio",
      "--no-auto-update",
    ];
    const command =
      spawn.shell === false
        ? { command: spawn.command, args, shell: false }
        : yield* resolveSpawnCommand(spawn.command, args, {
            ...(spawn.env ? { env: spawn.env } : {}),
            extendEnv: spawn.extendEnv ?? true,
          });
    const child = yield* spawner.spawn(
      ChildProcess.make(command.command, command.args, {
        cwd: spawn.cwd,
        env: spawn.env,
        extendEnv: spawn.extendEnv ?? true,
        shell: command.shell,
        stdin: { stream: "pipe", endOnDone: false },
        stdout: "pipe",
        stderr: "ignore",
        killSignal: "SIGKILL",
      }),
    );
    const request = '{"jsonrpc":"2.0","id":1,"method":"account.getQuota","params":{}}';
    const body = new TextEncoder().encode(request);
    const frame = new TextEncoder().encode(`Content-Length: ${body.length}\r\n\r\n${request}`);
    const readFrame = makeFrameReader();
    const response = child.stdout.pipe(
      Stream.mapEffect((chunk) =>
        Effect.try({
          try: () => readFrame(chunk),
          catch: (cause) => new CopilotQuotaReadError({ cause }),
        }),
      ),
      Stream.flatMap(Stream.fromIterable),
      Stream.mapEffect((json) => decodeJson(json)),
      Stream.filter(isReply),
      Stream.mapEffect((reply) => decodeReply(reply)),
      Stream.runHead,
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.fail(new CopilotQuotaReadError({})),
          onSome: (reply) => Effect.succeed(reply.result),
        }),
      ),
    );
    const [, result] = yield* Effect.all(
      [Stream.run(Stream.succeed(frame), child.stdin), response],
      { concurrency: "unbounded" },
    );
    return copilotQuotaResponseToLimits(result, checkedAt);
  }).pipe(
    Effect.scoped,
    Effect.timeout("10 seconds"),
    Effect.orElseSucceed(() => probeFailed(checkedAt)),
  );
});
