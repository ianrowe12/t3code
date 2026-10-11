import type { HostResourcesSnapshot } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { chooseLoadBalancedEnvironment } from "./load-balancing.ts";

const now = 100_000;
const resources: HostResourcesSnapshot = {
  sampledAt: now,
  cpuUtilization: 0.1,
  cpuCount: 48,
  availableMemoryBytes: 60,
  totalMemoryBytes: 100,
};
const candidate = { environmentId: "available", resources, weight: 1 };

describe("chooseLoadBalancedEnvironment", () => {
  it.each([
    { cpuUtilization: 0.95 },
    { cpuUtilization: 1 },
    { cpuUtilization: null },
    { availableMemoryBytes: 5 },
    { availableMemoryBytes: 0 },
    { totalMemoryBytes: 0 },
    { cpuCount: 0 },
    { sampledAt: now - 15_001 },
    { sampledAt: now + 5_001 },
  ])("excludes resources outside the capacity or freshness limits: %j", (patch) => {
    expect(
      chooseLoadBalancedEnvironment([{ ...candidate, resources: { ...resources, ...patch } }], now),
    ).toBeNull();
  });

  it.each([
    { cpuUtilization: 0.949 },
    { availableMemoryBytes: 6 },
    { sampledAt: now - 15_000 },
    { sampledAt: now + 5_000 },
  ])("includes resources just within the capacity or freshness limits: %j", (patch) => {
    expect(
      chooseLoadBalancedEnvironment([{ ...candidate, resources: { ...resources, ...patch } }], now),
    ).toBe(candidate.environmentId);
  });

  it.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY])("rejects invalid weight %s", (weight) => {
    expect(chooseLoadBalancedEnvironment([{ ...candidate, weight }], now)).toBeNull();
  });

  it("uses client receipt time instead of an unsynchronized remote clock", () => {
    expect(
      chooseLoadBalancedEnvironment(
        [{ ...candidate, receivedAt: now, resources: { ...resources, sampledAt: 0 } }],
        now,
      ),
    ).toBe(candidate.environmentId);
    expect(
      chooseLoadBalancedEnvironment([{ ...candidate, receivedAt: now - 15_001 }], now),
    ).toBeNull();
  });

  it("ranks eligible capacity by weight and keeps the first candidate on a tie", () => {
    const second = { ...candidate, environmentId: "second" };
    expect(chooseLoadBalancedEnvironment([candidate, { ...second, weight: 2 }], now)).toBe(
      second.environmentId,
    );
    expect(chooseLoadBalancedEnvironment([candidate, second], now)).toBe(candidate.environmentId);
    expect(
      chooseLoadBalancedEnvironment(
        [candidate, { ...second, resources: { ...resources, cpuCount: 96 } }],
        now,
      ),
    ).toBe(second.environmentId);
  });

  it("returns null when no resource samples are available", () => {
    expect(chooseLoadBalancedEnvironment([], now)).toBeNull();
    expect(chooseLoadBalancedEnvironment([{ ...candidate, resources: null }], now)).toBeNull();
  });
});
