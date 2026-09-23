import {
  EnvironmentId,
  ProviderThreadId,
  RunId,
  RuntimeRequestId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import { presentThreadShell } from "./models.ts";
import { v2Now, v2ThreadShell } from "./orchestrationV2TestFixtures.ts";
import { resolveThreadCardStatus, threadHasUnseenCompletion } from "./threadCardStatus.ts";

const environmentId = EnvironmentId.make("environment-v2");
const runId = RunId.make("run-1");
const completedAt = DateTime.makeUnsafe("2026-06-20T00:05:00.000Z");
const backgroundAgent = { taskId: "agent-1", description: "Review", taskType: "subagent" };

function cardStatus(overrides: Partial<OrchestrationV2ThreadShell>) {
  return resolveThreadCardStatus(
    presentThreadShell(environmentId, { ...v2ThreadShell, ...overrides }),
  );
}

describe("resolveThreadCardStatus", () => {
  it.each([
    ["preparing", "preparing"],
    ["queued behind nothing", "queued"],
    ["starting", "starting"],
    ["running", "running"],
    ["checkpointing after the turn", "waiting"],
  ] as const)("reports working while the latest run is %s", (_label, status) => {
    expect(cardStatus({ latestRunId: runId, status })).toBe("working");
  });

  it("reports working for the active run when a follow-up is queued behind it", () => {
    expect(
      cardStatus({
        latestRunId: RunId.make("run-2"),
        status: "queued",
        activeRunId: runId,
        activityRunStatus: "running",
      }),
    ).toBe("working");
  });

  it("reports background while agents the settled run will report back on still run", () => {
    expect(
      cardStatus({
        latestRunId: runId,
        status: "completed",
        latestRunCompletedAt: completedAt,
        pendingBackgroundTasks: [backgroundAgent],
      }),
    ).toBe("background");
  });

  it("puts approval and input requests ahead of any activity", () => {
    const running = { latestRunId: runId, status: "running" as const };
    expect(
      cardStatus({
        ...running,
        pendingRuntimeRequest: {
          id: RuntimeRequestId.make("request-1"),
          kind: "command",
          createdAt: v2Now,
        },
      }),
    ).toBe("approval");
    expect(
      cardStatus({
        ...running,
        pendingRuntimeRequest: {
          id: RuntimeRequestId.make("request-2"),
          kind: "user_input",
          createdAt: v2Now,
        },
      }),
    ).toBe("input");
  });

  it("reports failed only while the latest run failed", () => {
    expect(cardStatus({ latestRunId: runId, status: "failed", lastError: "boom" })).toBe("failed");
  });

  it.each(["completed", "interrupted", "cancelled"] as const)(
    "rests at ready once the latest run is %s",
    (status) => {
      expect(cardStatus({ latestRunId: runId, status, latestRunCompletedAt: completedAt })).toBe(
        "ready",
      );
    },
  );

  it("rests at ready for a thread whose provider thread exists but has never run", () => {
    // The shell reports status "idle" here; that is not background work.
    expect(cardStatus({ activeProviderThreadId: ProviderThreadId.make("provider-thread-1") })).toBe(
      "ready",
    );
    expect(cardStatus({})).toBe("ready");
  });
});

describe("threadHasUnseenCompletion", () => {
  const settled = presentThreadShell(environmentId, {
    ...v2ThreadShell,
    latestRunId: runId,
    status: "completed",
    latestRunCompletedAt: completedAt,
  });

  it("marks a completion newer than the last visit", () => {
    expect(
      threadHasUnseenCompletion({ ...settled, lastVisitedAt: "2026-06-20T00:04:00.000Z" }),
    ).toBe(true);
    expect(
      threadHasUnseenCompletion({ ...settled, lastVisitedAt: "2026-06-20T00:05:00.000Z" }),
    ).toBe(false);
  });

  it("treats never-visited threads as read and unparseable visits as unread", () => {
    expect(threadHasUnseenCompletion({ ...settled, lastVisitedAt: null })).toBe(false);
    expect(threadHasUnseenCompletion({ ...settled, lastVisitedAt: "not a date" })).toBe(true);
  });

  it("never marks a run that has not finished", () => {
    const checkpointing = presentThreadShell(environmentId, {
      ...v2ThreadShell,
      latestRunId: runId,
      status: "waiting",
      latestRunCompletedAt: null,
    });
    expect(
      threadHasUnseenCompletion({ ...checkpointing, lastVisitedAt: "2026-06-20T00:00:00.000Z" }),
    ).toBe(false);
  });
});
