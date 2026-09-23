import { threadRuntimeIsActive, type EnvironmentThreadShell } from "./models.ts";

/**
 * What a thread card's status chip reports. Web and mobile thread lists both
 * resolve through here so a thread reads the same everywhere.
 *
 * - `approval` / `input`: the agent is blocked on the user.
 * - `working`: a run is in flight, from preparing through the post-turn
 *   checkpoint.
 * - `background`: the run settled, but background agents or tasks it will
 *   report back on are still running. Cards present this as Working too.
 * - `failed`: the latest run failed.
 * - `ready`: nothing in flight. A Done chip marks a completion the user has
 *   not seen yet (see `threadHasUnseenCompletion`).
 */
export type ThreadCardStatus = "approval" | "input" | "working" | "background" | "failed" | "ready";

export type ThreadCardStatusInput = Pick<
  EnvironmentThreadShell,
  "hasPendingApprovals" | "hasPendingUserInput" | "runtime"
> & {
  readonly pendingBackgroundTasks?: EnvironmentThreadShell["pendingBackgroundTasks"] | undefined;
};

export function resolveThreadCardStatus(thread: ThreadCardStatusInput): ThreadCardStatus {
  if (thread.hasPendingApprovals) return "approval";
  if (thread.hasPendingUserInput) return "input";
  if (threadRuntimeIsActive(thread.runtime)) return "working";
  // Read the roster itself: runtime "idle" also covers a thread whose provider
  // thread exists but has never run, which has nothing in flight.
  if ((thread.pendingBackgroundTasks?.length ?? 0) > 0) return "background";
  if (thread.runtime?.status === "failed") return "failed";
  return "ready";
}

export function threadCardStatusIsInFlight(status: ThreadCardStatus): boolean {
  return status === "working" || status === "background";
}

/**
 * True when the latest run finished after the user last looked at the thread.
 * A thread that was never visited counts as read, so historical threads do
 * not all light up as unread.
 */
export function threadHasUnseenCompletion(thread: {
  readonly latestRun: { readonly completedAt: string | null } | null;
  readonly lastVisitedAt?: string | null | undefined;
}): boolean {
  const completedAt = thread.latestRun?.completedAt;
  if (!completedAt) return false;
  const completedAtMs = Date.parse(completedAt);
  if (Number.isNaN(completedAtMs)) return false;
  if (!thread.lastVisitedAt) return false;
  const lastVisitedAtMs = Date.parse(thread.lastVisitedAt);
  if (Number.isNaN(lastVisitedAtMs)) return true;
  return completedAtMs > lastVisitedAtMs;
}
