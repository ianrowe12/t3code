import type { EnvironmentId } from "@t3tools/contracts";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { useCallback, useSyncExternalStore } from "react";

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
const invalidationListeners = new Set<() => void>();

function subscribeInvalidation(listener: () => void): () => void {
  invalidationListeners.add(listener);
  return () => {
    invalidationListeners.delete(listener);
  };
}

export function useHeldThreadTimeline<T extends ThreadTimelineDisplaySnapshot>(
  loading: boolean,
): T | null {
  // A live view writes each streaming paint to the cache. Only read it while
  // loading, so those writes cannot cause an extra render of their own view.
  const getSnapshot = useCallback(() => (loading ? peekHeldThreadTimeline<T>() : null), [loading]);
  return useSyncExternalStore(subscribeInvalidation, getSnapshot, getSnapshot);
}

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
  if (lastPaintedThreadTimeline === null) return;
  lastPaintedThreadTimeline = null;
  for (const listener of invalidationListeners) listener();
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
  readonly heldThread: Pick<EnvironmentThreadShell, "archivedAt" | "deletedAt"> | null;
  readonly environmentReady: boolean;
}): { readonly snapshot: T | null; readonly paintOnly: boolean } {
  if (input.current === null || !input.loading || input.current.entries.length > 0) {
    return { snapshot: input.current, paintOnly: false };
  }

  const held = input.held === undefined ? peekHeldThreadTimeline<T>() : input.held;
  if (
    !input.environmentReady ||
    input.heldThread === null ||
    input.heldThread.archivedAt !== null ||
    input.heldThread.deletedAt !== null ||
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
