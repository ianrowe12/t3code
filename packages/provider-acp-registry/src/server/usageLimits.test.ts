import { describe, expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { hashTelemetryId } from "@t3tools/shared/telemetryId";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { AcpRegistrySettings } from "../settings.ts";
import * as AcpRegistrySupport from "./AcpRegistrySupport.ts";
import { copilotQuotaResponseToLimits, readCopilotUsageLimits } from "./usageLimits.ts";

const checkedAt = "2026-11-05T00:00:00.000Z";
const futureReset = "2026-12-01T00:00:00.000Z";
const quota = {
  isUnlimitedEntitlement: false,
  entitlementRequests: 1_000_000,
  usedRequests: 234_000,
  remainingPercentage: 76.6,
  tokenBasedBilling: true,
  resetDate: checkedAt,
};
const settings = Schema.decodeSync(AcpRegistrySettings)({
  agentId: "github-copilot-cli",
  commandPath: "/custom/copilot-wrapper",
});
const encoder = new TextEncoder();
const frame = (value: unknown) => {
  const body = JSON.stringify(value);
  return encoder.encode(`Content-Length: ${encoder.encode(body).length}\r\n\r\n${body}`);
};

describe("Copilot quota mapping", () => {
  it.each([
    {
      name: "enterprise AI credits without a synthetic reset",
      snapshots: { premium_interactions: quota },
      expected: [
        {
          id: "premium_interactions",
          label: "AI credits",
          detail: "234,000 of 1,000,000 ($2,340)",
          usedPercent: 23.400000000000006,
        },
      ],
    },
    {
      name: "individual premium requests with a real reset",
      snapshots: {
        premium_interactions: {
          ...quota,
          tokenBasedBilling: false,
          entitlementRequests: 300,
          usedRequests: 234,
          remainingPercentage: 22,
          resetDate: futureReset,
        },
      },
      expected: [
        {
          id: "premium_interactions",
          label: "Premium requests",
          detail: "234 of 300",
          usedPercent: 78,
          resetsAt: futureReset,
        },
      ],
    },
    {
      name: "legacy premium requests without a billing flag",
      snapshots: {
        premium_interactions: {
          isUnlimitedEntitlement: false,
          entitlementRequests: 300,
          usedRequests: 234,
          remainingPercentage: 22,
        },
      },
      expected: [
        {
          id: "premium_interactions",
          label: "Premium requests",
          detail: "234 of 300",
          usedPercent: 78,
        },
      ],
    },
    {
      name: "unlimited allowances",
      snapshots: { premium_interactions: { ...quota, isUnlimitedEntitlement: true } },
      expected: [],
    },
    {
      name: "zero entitlement",
      snapshots: { premium_interactions: { ...quota, entitlementRequests: 0 } },
      expected: [],
    },
    {
      name: "completions alone do not report chat limits",
      snapshots: { completions: { ...quota, entitlementRequests: -1 } },
      expected: [],
    },
    {
      name: "exhaustion with overage still uses the advertised percentage",
      snapshots: {
        premium_interactions: {
          ...quota,
          usedRequests: 1_100_000,
          remainingPercentage: -10,
          overage: 100_000,
          overageAllowedWithExhaustedQuota: true,
        },
      },
      expected: [
        {
          id: "premium_interactions",
          label: "AI credits",
          detail: "1,100,000 of 1,000,000 ($11,000)",
          usedPercent: 100,
          overageAllowed: true,
        },
      ],
    },
    {
      name: "capped chat ignores exhausted completions and unrelated quotas",
      snapshots: {
        chat: { ...quota, entitlementRequests: 200, usedRequests: 4, remainingPercentage: 98 },
        completions: {
          ...quota,
          entitlementRequests: 2_000,
          usedRequests: 2_000,
          remainingPercentage: 0,
        },
        unrelated: quota,
      },
      expected: [{ id: "chat", label: "Chat", detail: "4 of 200", usedPercent: 2 }],
    },
    {
      name: "near-present or invalid reset dates and percentages above 100",
      snapshots: {
        chat: { ...quota, remainingPercentage: 110, resetDate: "2026-11-05T00:00:30.000Z" },
        premium_interactions: { ...quota, remainingPercentage: 100, resetDate: "not-a-date" },
      },
      expected: [
        { id: "chat", label: "Chat", detail: "234,000 of 1,000,000", usedPercent: 0 },
        {
          id: "premium_interactions",
          label: "AI credits",
          detail: "234,000 of 1,000,000 ($2,340)",
          usedPercent: 0,
        },
      ],
    },
  ])("$name", ({ snapshots, expected }) => {
    const limits = copilotQuotaResponseToLimits({ quotaSnapshots: snapshots }, checkedAt);
    expect(limits.checkedAt).toBe(checkedAt);
    expect(limits.windows).toEqual(expected.map((window) => ({ kind: "monthly", ...window })));
    expect(limits.unavailable?.reason).toBe(expected.length === 0 ? "unsupported" : undefined);
  });

  it.each([
    null,
    {},
    { quotaSnapshots: [] },
    { quotaSnapshots: { premium_interactions: {} } },
    ...["NaN", NaN, Infinity, undefined].map((remainingPercentage) => ({
      quotaSnapshots: { premium_interactions: { ...quota, remainingPercentage } },
    })),
    { quotaSnapshots: { premium_interactions: { ...quota, usedRequests: -1 } } },
    { quotaSnapshots: { premium_interactions: { ...quota, entitlementRequests: Infinity } } },
  ])("reports malformed quota data as a failed probe: %j", (response) => {
    expect(copilotQuotaResponseToLimits(response, checkedAt)).toMatchObject({
      windows: [],
      unavailable: { reason: "probeFailed" },
    });
  });
});

const makePeer = Effect.fn("test.makeCopilotQuotaPeer")(function* (
  output: readonly Uint8Array[] | "never" | "closed",
  auth: unknown = { authInfo: { login: "Example-Login", token: "never-publish" } },
) {
  const requested = yield* Deferred.make<void>();
  const stopped = yield* Deferred.make<void>();
  const stdout = yield* Queue.unbounded<Uint8Array>();
  const launches: ChildProcess.StandardCommand[] = [];
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.acquireRelease(
      Effect.sync(() => {
        if (!ChildProcess.isStandardCommand(command)) throw new Error("Unexpected pipeline.");
        launches.push(command);
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(999_999_999),
          exitCode: Effect.never,
          isRunning: Effect.succeed(true),
          kill: () => Deferred.succeed(stopped, undefined).pipe(Effect.asVoid),
          unref: Effect.succeed(Effect.void),
          stdin: Sink.forEach((bytes: Uint8Array) =>
            Effect.gen(function* () {
              const text = new TextDecoder().decode(bytes);
              const [header, body] = text.split("\r\n\r\n");
              expect(header).toBe(`Content-Length: ${encoder.encode(body!).length}`);
              const request = JSON.parse(body!);
              expect(request).toEqual({
                jsonrpc: "2.0",
                id: request.id,
                method: request.id === 1 ? "account.getQuota" : "account.getCurrentAuth",
                params: {},
              });
              if (request.id === 2) {
                if (auth === "deferred") return;
                yield* Queue.offer(
                  stdout,
                  frame({
                    jsonrpc: "2.0",
                    id: 2,
                    ...(auth === "error"
                      ? { error: { code: -32601, message: "unsupported" } }
                      : { result: auth }),
                  }),
                );
                return;
              }
              yield* Deferred.succeed(requested, undefined);
              if (Array.isArray(output)) {
                for (const chunk of output) yield* Queue.offer(stdout, chunk);
              }
            }),
          ),
          stdout: output === "closed" ? Stream.empty : Stream.fromQueue(stdout),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        });
      }),
      (child) => child.kill().pipe(Effect.orDie),
    ),
  );
  const catalog: AcpRegistrySupport.AcpRegistryCatalog["Service"] = {
    search: () => Effect.die("unused search"),
    prepare: () => Effect.die("unused prepare"),
    inspect: () => Effect.die("unused inspect"),
    uninstallManagedBinary: () => Effect.die("unused uninstall"),
    resolve: (actualSettings, cwd, environment) => {
      expect(actualSettings).toBe(settings);
      expect(cwd).toBe("/workspace");
      expect(environment).toEqual({ INSTANCE_ENV: "configured" });
      return Effect.succeed({
        distribution: "binary",
        spawn: {
          command: "/custom/copilot-wrapper",
          args: ["--acp", "--log-level", "error"],
          cwd,
          env: { ...environment, DISTRIBUTION_ENV: "configured" },
        },
      });
    },
  };
  const read = readCopilotUsageLimits(settings, "/workspace", { INSTANCE_ENV: "configured" }).pipe(
    Effect.provideService(AcpRegistrySupport.AcpRegistryCatalog, catalog),
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.provideService(HostProcess.Platform, "linux"),
    Effect.provideService(HostProcess.Environment, {}),
  );
  return { read, launches, requested, stopped };
});

describe("Copilot quota transport", () => {
  it.effect.each(["fragmented", "coalesced"] as const)(
    "reads %s byte-framed replies and ignores notifications and other request ids",
    (delivery) =>
      Effect.gen(function* () {
        const notice = frame({ jsonrpc: "2.0", method: "status", params: { text: "✓" } });
        const other = frame({ jsonrpc: "2.0", id: 99, result: {} });
        const reply = frame({
          jsonrpc: "2.0",
          id: 1,
          result: { quotaSnapshots: { premium_interactions: quota } },
        });
        const authReply = frame({
          jsonrpc: "2.0",
          id: 2,
          result: { authInfo: { login: "Example-Login", token: "never-publish" } },
        });
        const bytes = new Uint8Array([...notice, ...other, ...reply, ...authReply]);
        const peer = yield* makePeer(
          delivery === "fragmented" ? Array.from(bytes, (byte) => Uint8Array.of(byte)) : [bytes],
          "deferred",
        );
        const limits = yield* peer.read;
        expect(limits.windows[0]).toMatchObject({
          label: "AI credits",
          detail: "234,000 of 1,000,000 ($2,340)",
        });
        expect(limits.credentialFingerprint).toBe(hashTelemetryId("github-copilot\0example-login"));
        expect(limits.providerLabel).toBe("GitHub Copilot");
        expect(JSON.stringify(limits)).not.toMatch(/Example-Login|never-publish/);
        expect(yield* Deferred.isDone(peer.stopped)).toBe(true);
        expect(peer.launches).toHaveLength(1);
        expect(peer.launches[0]).toMatchObject({
          command: "/custom/copilot-wrapper",
          args: ["--log-level", "error", "--headless", "--stdio", "--no-auto-update"],
          options: {
            cwd: "/workspace",
            env: { INSTANCE_ENV: "configured", DISTRIBUTION_ENV: "configured" },
            killSignal: "SIGKILL",
          },
        });
      }),
  );

  it.effect.each(["error", null, {}, { authInfo: { login: "" } }, { authInfo: { login: "  " } }])(
    "keeps quota windows when current auth has no usable login: %j",
    (auth) =>
      Effect.gen(function* () {
        const peer = yield* makePeer(
          [
            frame({
              jsonrpc: "2.0",
              id: 1,
              result: { quotaSnapshots: { premium_interactions: quota } },
            }),
          ],
          auth,
        );
        const limits = yield* peer.read;
        expect(limits.windows).toHaveLength(1);
        expect(limits.credentialFingerprint).toBeUndefined();
      }),
  );

  it.effect.each([
    [frame({ jsonrpc: "2.0", id: 1, error: { code: -32601, message: "private details" } })],
    [encoder.encode("Content-Length: invalid\r\n\r\n{}")],
    [encoder.encode("Content-Length: 1048577\r\n\r\n")],
    [encoder.encode("Content-Length: 2\r\n\r\nxx")],
    [frame({ jsonrpc: "2.0", id: 1, result: { quotaSnapshots: "invalid" } })],
    "closed",
  ] as const)("returns probeFailed and releases the child on failure: %j", (output) =>
    Effect.gen(function* () {
      const peer = yield* makePeer(output);
      const limits = yield* peer.read;
      expect(limits.unavailable).toEqual({
        reason: "probeFailed",
        message: "GitHub Copilot could not read usage limits.",
      });
      if (Array.isArray(output)) {
        expect(limits.credentialFingerprint).toBe(hashTelemetryId("github-copilot\0example-login"));
      }
      expect(yield* Deferred.isDone(peer.stopped)).toBe(true);
    }),
  );

  it.effect("kills a child that never answers at the probe deadline", () =>
    Effect.gen(function* () {
      const peer = yield* makePeer("never");
      const fiber = yield* peer.read.pipe(Effect.forkChild);
      yield* Deferred.await(peer.requested);
      yield* TestClock.adjust("10 seconds");
      expect((yield* Fiber.join(fiber)).unavailable?.reason).toBe("probeFailed");
      expect(yield* Deferred.isDone(peer.stopped)).toBe(true);
    }),
  );

  it.effect("releases the child when a refresh is interrupted", () =>
    Effect.gen(function* () {
      const peer = yield* makePeer("never");
      const fiber = yield* peer.read.pipe(Effect.forkChild);
      yield* Deferred.await(peer.requested);
      yield* Fiber.interrupt(fiber);
      expect(yield* Deferred.isDone(peer.stopped)).toBe(true);
    }),
  );
});
