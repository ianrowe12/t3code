import type { EnvironmentId } from "@t3tools/contracts";

/**
 * A complete timeline paint. Generic fields keep entries and their rendering
 * metadata together instead of pairing held entries with live destination data.
 */
export interface ThreadTimelineDisplaySnapshot {
  readonly threadKey: string;
  readonly environmentId: EnvironmentId;
  readonly entries: readonly unknown[];
}

let lastPaintedThreadTimeline: ThreadTimelineDisplaySnapshot | null = null;

export function timelineHasEphemeralPreviewUrls(value: unknown): boolean {
  const seen = new Set<object>();
  const visit = (candidate: unknown): boolean => {
    if (typeof candidate === "string") {
      return candidate.startsWith("blob:");
    }
    if (candidate === null || typeof candidate !== "object" || seen.has(candidate)) {
      return false;
    }
    seen.add(candidate);
    return Array.isArray(candidate) ? candidate.some(visit) : Object.values(candidate).some(visit);
  };
  return visit(value);
}

export function rememberReadyThreadTimeline<T extends ThreadTimelineDisplaySnapshot>(
  snapshot: T,
): boolean {
  if (snapshot.entries.length === 0 || timelineHasEphemeralPreviewUrls(snapshot)) {
    return false;
  }
  lastPaintedThreadTimeline = snapshot;
  return true;
}

export function peekHeldThreadTimeline<T extends ThreadTimelineDisplaySnapshot>(): T | null {
  return lastPaintedThreadTimeline as T | null;
}

export function resetHeldThreadTimeline(): void {
  lastPaintedThreadTimeline = null;
}

export function clearHeldThreadTimelineForEnvironment(environmentId: EnvironmentId): void {
  if (lastPaintedThreadTimeline?.environmentId === environmentId) {
    resetHeldThreadTimeline();
  }
}

export function resolveThreadSwitchTimeline<T extends ThreadTimelineDisplaySnapshot>(input: {
  readonly loading: boolean;
  readonly activeThreadKey: string | null;
  readonly activeEnvironmentId: EnvironmentId | null;
  readonly current: T | null;
  readonly held?: T | null;
}): { readonly snapshot: T | null; readonly paintOnly: boolean } {
  if (input.current === null || !input.loading || input.current.entries.length > 0) {
    return { snapshot: input.current, paintOnly: false };
  }

  const held = input.held ?? peekHeldThreadTimeline<T>();
  if (
    held === null ||
    input.activeThreadKey === null ||
    input.activeEnvironmentId === null ||
    held.threadKey === input.activeThreadKey ||
    held.environmentId !== input.activeEnvironmentId ||
    held.entries.length === 0
  ) {
    return { snapshot: input.current, paintOnly: false };
  }
  return { snapshot: held, paintOnly: true };
}
