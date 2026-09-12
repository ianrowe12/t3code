import { EnvironmentId } from "@t3tools/contracts";
import { act, useLayoutEffect } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import {
  clearHeldThreadTimelineForEnvironment,
  rememberReadyThreadTimeline,
  resetHeldThreadTimeline,
  resolveThreadSwitchTimeline,
  useHeldThreadTimeline,
} from "./heldThreadTimeline";

const environmentId = EnvironmentId.make("env");
const held = { environmentId, threadKey: "env:A", entries: ["message A"] };
const destination = { environmentId, threadKey: "env:B", entries: [] as string[] };
let renderer: ReactTestRenderer | undefined;
let displayed: ReturnType<typeof resolveThreadSwitchTimeline<typeof held>>;
let renders = 0;

function TimelineProbe({
  loading = true,
  current = destination,
}: {
  loading?: boolean;
  current?: typeof held;
}) {
  const snapshot = useHeldThreadTimeline<typeof held>(loading);
  const display = resolveThreadSwitchTimeline({
    loading,
    activeThreadKey: current.threadKey,
    activeEnvironmentId: environmentId,
    current,
    held: snapshot,
    heldThread: { archivedAt: null, deletedAt: null },
    environmentReady: true,
  });
  useLayoutEffect(() => {
    displayed = display;
    renders += 1;
  });
  useLayoutEffect(() => {
    if (!loading) rememberReadyThreadTimeline(current);
  }, [current, loading]);
  return null;
}

beforeEach(async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  resetHeldThreadTimeline();
  rememberReadyThreadTimeline(held);
  renders = 0;
  await act(() => {
    renderer = create(<TimelineProbe />);
  });
});

afterEach(async () => {
  await act(() => renderer?.unmount());
  renderer = undefined;
  resetHeldThreadTimeline();
  vi.unstubAllGlobals();
});

describe("held timeline invalidation", () => {
  it("removes a painted source on auth/environment cleanup without a parent render", async () => {
    expect(displayed).toEqual({ snapshot: held, paintOnly: true });
    await act(() => clearHeldThreadTimelineForEnvironment(environmentId));
    expect(displayed).toEqual({ snapshot: destination, paintOnly: false });
  });

  it("removes the held display on an explicit reset", async () => {
    await act(() => resetHeldThreadTimeline());
    expect(displayed).toEqual({ snapshot: destination, paintOnly: false });
  });

  it("does not schedule extra renders for streaming paints or unrelated environment cleanup", async () => {
    const initialRenders = renders;
    await act(() => {
      rememberReadyThreadTimeline({ ...held, entries: ["message A", "streaming delta"] });
      clearHeldThreadTimelineForEnvironment(EnvironmentId.make("other"));
    });
    expect(renders).toBe(initialRenders);
    expect(displayed).toEqual({ snapshot: held, paintOnly: true });
  });

  it("does not echo live streaming paints and holds the latest one on the next switch", async () => {
    const initialRenders = renders;
    await act(() => renderer?.update(<TimelineProbe loading={false} current={held} />));
    expect(renders).toBe(initialRenders + 1);
    const streamed = { ...held, entries: ["message A", "streaming delta"] };
    await act(() => renderer?.update(<TimelineProbe loading={false} current={streamed} />));
    expect(renders).toBe(initialRenders + 2);
    expect(displayed).toEqual({ snapshot: streamed, paintOnly: false });

    await act(() => renderer?.update(<TimelineProbe />));
    expect(displayed).toEqual({ snapshot: streamed, paintOnly: true });
  });

  it("does not resurrect the previous thread when the current thread reloads", async () => {
    const loadedDestination = { ...destination, entries: ["message B"] };
    await act(() =>
      renderer?.update(<TimelineProbe loading={false} current={loadedDestination} />),
    );
    expect(displayed).toEqual({ snapshot: loadedDestination, paintOnly: false });

    await act(() => renderer?.update(<TimelineProbe />));
    expect(displayed).toEqual({ snapshot: destination, paintOnly: false });
  });
});
