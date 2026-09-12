import { useLexicalComposerContext } from "@lexical/react/LexicalComposerContext";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import { DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts/settings";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/unstable/reactivity";
import {
  $getRoot,
  $getSelection,
  $isRangeSelection,
  $isTextNode,
  type LexicalEditor,
} from "lexical";
import { act, createRef, useLayoutEffect, useRef, useState } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import {
  type ComposerFileAttachment,
  type ComposerImageAttachment,
  useComposerDraftStore,
  useComposerThreadDraft,
} from "../../composerDraftStore";
import { makeThreadFixture } from "../../test-fixtures";
import * as attachmentUploads from "../../lib/attachmentUploadQueue";
import type { ChatMessage } from "../../types";
import { ComposerPromptEditor, type ComposerPromptEditorHandle } from "../ComposerPromptEditor";
import { compactThreadContext, type ContextCompactionInput } from "./contextCompaction";
import { ChatComposer, type ChatComposerHandle } from "./ChatComposer";

let lexicalEditor: LexicalEditor;
// Use the real editor and selection model without its DOM view.
vi.mock("@lexical/react/LexicalPlainTextPlugin", () => ({
  PlainTextPlugin: function HeadlessEditor() {
    [lexicalEditor] = useLexicalComposerContext();
    return null;
  },
}));
vi.mock("~/hooks/useSettings", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useSettings")>()),
  useEnvironmentIdentificationMode: () => "none",
}));

const thread = makeThreadFixture();
const threadRef = scopeThreadRef(thread.environmentId, thread.id);
const editorRef = createRef<ComposerPromptEditorHandle>();
const composerRef = createRef<ChatComposerHandle>();
const prompt = "Keep this unfinished draft";
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex-secondary"),
  model: "gpt-5.4",
  options: [{ id: "effort", value: "high" }],
};
const image: ComposerImageAttachment = {
  type: "image",
  id: "draft-image",
  name: "draft.png",
  mimeType: "image/png",
  sizeBytes: 5,
  file: new File(["image"], "draft.png", { type: "image/png" }),
  previewUrl: "blob:draft-image",
};
const file: ComposerFileAttachment = {
  type: "file",
  id: "draft-file",
  name: "notes.txt",
  mimeType: "text/plain",
  sizeBytes: 5,
  file: new File(["notes"], "notes.txt", { type: "text/plain" }),
};

let renderer: ReactTestRenderer | undefined;
let compact: () => Promise<void>;
let messages: ChatMessage[];
let busy: boolean;
let error: string | null;
let input: ContextCompactionInput;
let pendingCompact: Promise<void> | undefined;
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

let startResult: ReturnType<typeof deferred<AtomCommandResult<unknown, unknown>>>;
const normalSend = vi.fn();
const upload = vi.fn();
const releasePreview = vi.fn();
const persistSettings = vi.fn<ContextCompactionInput["persistSettings"]>();
const startTurn = vi.fn<ContextCompactionInput["startTurn"]>();
const clearUsageLimits = vi.fn();
const acknowledgeThreadWoke = vi.fn();

function ChatSurface({ fullComposer = false }: { fullComposer?: boolean }) {
  const draft = useComposerThreadDraft(threadRef);
  const [optimisticMessages, setOptimisticMessages] = useState<ChatMessage[]>([]);
  const [isBusy, setBusy] = useState(false);
  const [threadError, setThreadError] = useState<string | null>(null);
  const sendInFlightRef = useRef(false);
  const promptRef = useRef(draft.prompt);
  const imagesRef = useRef(draft.images);
  const filesRef = useRef(draft.files);
  const terminalContextsRef = useRef(draft.terminalContexts);
  const elementContextsRef = useRef(draft.elementContexts);
  const request: ContextCompactionInput = {
    environmentId: thread.environmentId,
    threadId: thread.id,
    disabled: isBusy,
    sendInFlightRef,
    getRunContext: fullComposer
      ? () => composerRef.current?.getRunContext()
      : () => ({
          providerAvailable: true,
          selectedModelSelection: modelSelection,
          interactionMode: "plan",
        }),
    runtimeMode: "approval-required",
    branch: "ianrowe12-current-checkout",
    persistSettings,
    startTurn,
    setOptimisticMessages,
    beginLocalDispatch: () => setBusy(true),
    resetLocalDispatch: () => setBusy(false),
    setThreadError: (_threadId, nextError) => setThreadError(nextError),
    scrollToEnd: () => {},
    clearUsageLimits,
    acknowledgeThreadWoke,
  };
  const compactContext = () => compactThreadContext(request);
  useLayoutEffect(() => {
    messages = optimisticMessages;
    busy = isBusy;
    error = threadError;
    input = request;
    compact = compactContext;
  });
  if (fullComposer) {
    return (
      <ChatComposer
        composerDraftTarget={threadRef}
        environmentId={thread.environmentId}
        attachmentUploadsCapabilityKnown
        supportsAttachmentUploads
        supportsQuestionAttachments={false}
        maxFileAttachmentBytes={10_000_000}
        routeKind="server"
        routeThreadRef={threadRef}
        draftId={null}
        activeThreadId={thread.id}
        activeThreadEnvironmentId={thread.environmentId}
        activeThread={thread}
        activeThreadShell={thread}
        promptHistoryMessages={[]}
        isServerThread
        isLocalDraftThread={false}
        forceExpandedOnMobile={false}
        projectSelectionRequired={false}
        phase="ready"
        isConnecting={false}
        isSendBusy={isBusy}
        sendDisabledReason={null}
        isPreparingWorktree={false}
        bannerItems={[]}
        environmentUnavailable={null}
        activePendingApproval={null}
        pendingApprovals={[]}
        pendingUserInputs={[]}
        activePendingProgress={null}
        activePendingResolvedAnswers={null}
        activePendingIsResponding={false}
        activePendingDraftAnswers={{}}
        activePendingQuestionIndex={0}
        respondingRequestIds={[]}
        showPlanFollowUpPrompt={false}
        activeProposedPlan={null}
        activeTasksProgress={null}
        activeTaskSteps={null}
        threadSyncPhase={null}
        runtimeMode="approval-required"
        interactionMode="default"
        lockedProvider={null}
        providerStatuses={[
          {
            instanceId: thread.modelSelection.instanceId,
            driver: ProviderDriverKind.make("codex"),
            enabled: true,
            installed: true,
            version: null,
            status: "ready",
            auth: { status: "authenticated" },
            checkedAt: "2026-09-12T00:00:00.000Z",
            models: [
              {
                slug: thread.modelSelection.model,
                name: "GPT",
                isCustom: false,
                capabilities: {},
              },
            ],
            slashCommands: [{ name: "compact", description: "Compact context" }],
            skills: [],
          },
        ]}
        providerCatalogKnown
        activeProjectDefaultModelSelection={thread.modelSelection}
        activeThreadModelSelection={thread.modelSelection}
        activeContextWindow={null}
        compactThreadUnavailable={isBusy}
        compactDisabled={isBusy}
        compactDisabledReason={isBusy ? "Compacting is unavailable right now" : null}
        resolvedTheme="dark"
        settings={DEFAULT_UNIFIED_SETTINGS}
        keybindings={[]}
        terminalOpen={false}
        gitCwd={null}
        restingControlsHost={null}
        restingControlsHaveLeadingContext={false}
        onRestingControlsVisibilityChange={() => {}}
        getTimelineScrollableNode={() => null}
        isTimelineAtLogicalEnd={() => true}
        timelineOverflows={false}
        onComposerOverlayHeightChange={() => {}}
        onRestingChange={() => {}}
        promptRef={promptRef}
        composerImagesRef={imagesRef}
        composerFilesRef={filesRef}
        composerTerminalContextsRef={terminalContextsRef}
        composerElementContextsRef={elementContextsRef}
        composerRef={composerRef}
        onPageScrollKeyDown={() => {}}
        onPageScrollKeyUp={() => {}}
        onPageScrollRelease={() => {}}
        editingQueuedAttachments={null}
        onRemoveEditingQueuedAttachment={() => {}}
        onCompactContext={() => {
          const pending = compactContext();
          pendingCompact ??= pending;
        }}
        onSend={normalSend}
        onInterrupt={() => {}}
        onImplementPlanInNewThread={() => {}}
        onRespondToApproval={async () => {}}
        onSelectActivePendingUserInputOption={() => {}}
        onAdvanceActivePendingUserInput={() => {}}
        onDismissActivePendingUserInput={() => {}}
        onPreviousActivePendingUserInputQuestion={() => {}}
        onChangeActivePendingUserInputCustomAnswer={() => {}}
        onProviderModelSelect={() => {}}
        onOpenProviderSetup={() => {}}
        getModelDisabledReason={() => null}
        toggleInteractionMode={() => {}}
        handleRuntimeModeChange={() => {}}
        handleInteractionModeChange={() => {}}
        focusComposer={() => {}}
        scheduleComposerFocus={() => {}}
        setThreadError={(_threadId, nextError) => setThreadError(nextError)}
        onExpandImage={() => {}}
        onFileOpen={() => {}}
      />
    );
  }
  return (
    <form onSubmit={normalSend}>
      <ComposerPromptEditor
        value={draft.prompt}
        cursor={draft.prompt.length}
        terminalContexts={draft.terminalContexts}
        skills={[]}
        disabled={false}
        placeholder="Write a prompt"
        onRemoveTerminalContext={() => {}}
        onChange={() => {}}
        onPaste={() => {}}
        editorRef={editorRef}
      />
    </form>
  );
}

function readSelection() {
  return lexicalEditor.getEditorState().read(() => {
    const selection = $getSelection();
    if (!$isRangeSelection(selection)) throw new Error("Expected an editor selection");
    return {
      anchor: { key: selection.anchor.key, offset: selection.anchor.offset },
      focus: { key: selection.focus.key, offset: selection.focus.offset },
    };
  });
}

async function selectDraftText() {
  await act(() => {
    lexicalEditor.update(
      () => {
        const text = $getRoot().getFirstDescendant();
        if (!$isTextNode(text)) throw new Error("Expected draft text");
        text.select(5, 14);
      },
      { discrete: true },
    );
  });
}

beforeEach(async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("document", { activeElement: null });
  vi.stubGlobal("fetch", upload);
  vi.spyOn(URL, "revokeObjectURL").mockImplementation(releasePreview);
  useComposerDraftStore.setState({ draftsByThreadKey: {} });
  const store = useComposerDraftStore.getState();
  store.setPrompt(threadRef, prompt);
  store.addImages(threadRef, [image]);
  store.addFiles(threadRef, [file]);
  startResult = deferred<AtomCommandResult<unknown, unknown>>();
  pendingCompact = undefined;
  persistSettings.mockResolvedValue(AsyncResult.success(undefined));
  startTurn.mockImplementation(() => startResult.promise);
  await act(() => {
    renderer = create(<ChatSurface />);
  });
  await selectDraftText();
});

afterEach(async () => {
  await act(() => renderer?.unmount());
  renderer = undefined;
  useComposerDraftStore.setState({ draftsByThreadKey: {} });
  vi.restoreAllMocks();
  vi.clearAllMocks();
  vi.unstubAllGlobals();
});

describe("standalone context compaction", () => {
  it.each(["success", "failure"] as const)(
    "keeps the real composer draft intact through compact %s without normal submission",
    async (outcome) => {
      const windowEvents = new EventTarget();
      const documentEvents = new EventTarget();
      const stored = new Map<string, string>();
      vi.stubGlobal("Element", EventTarget);
      vi.stubGlobal("window", {
        Element: EventTarget,
        addEventListener: windowEvents.addEventListener.bind(windowEvents),
        removeEventListener: windowEvents.removeEventListener.bind(windowEvents),
        matchMedia: () => ({
          matches: false,
          addEventListener() {},
          removeEventListener() {},
        }),
        requestAnimationFrame: () => 1,
        cancelAnimationFrame() {},
        setTimeout,
        clearTimeout,
        performance,
        localStorage: {
          getItem: (key: string) => stored.get(key) ?? null,
          setItem: (key: string, value: string) => stored.set(key, value),
          removeItem: (key: string) => stored.delete(key),
        },
      });
      vi.stubGlobal("document", {
        activeElement: null,
        getElementById: () => ({}),
        addEventListener: documentEvents.addEventListener.bind(documentEvents),
        removeEventListener: documentEvents.removeEventListener.bind(documentEvents),
      });
      await act(() => renderer!.update(<ChatSurface fullComposer />));
      await selectDraftText();
      const draftBefore = useComposerDraftStore.getState().getComposerDraft(threadRef);
      const selectionBefore = readSelection();
      const readDraftForSend = vi.spyOn(composerRef.current!, "getSendContext");
      const startAttachmentUpload = vi.spyOn(attachmentUploads, "startAttachmentUpload");
      await act(() => {
        composerRef.current!.compactContext();
        composerRef.current!.compactContext();
      });
      expect(startTurn).toHaveBeenCalledOnce();
      expect(startTurn.mock.calls[0]?.[0].input.message).toMatchObject({
        text: "/compact",
        attachments: [],
      });
      expect(normalSend).not.toHaveBeenCalled();
      expect(readDraftForSend).not.toHaveBeenCalled();
      expect(useComposerDraftStore.getState().getComposerDraft(threadRef)).toBe(draftBefore);
      expect(readSelection()).toEqual(selectionBefore);

      await act(async () => {
        startResult.resolve(
          outcome === "success"
            ? AsyncResult.success(undefined)
            : AsyncResult.failure(Cause.fail(new Error("Compaction is unsupported."))),
        );
        await pendingCompact;
      });
      expect(composerRef.current!.readSnapshot().value).toBe(prompt);
      expect(readSelection()).toEqual(selectionBefore);
      expect(useComposerDraftStore.getState().getComposerDraft(threadRef)).toBe(draftBefore);
      expect(normalSend).not.toHaveBeenCalled();
      expect(upload).not.toHaveBeenCalled();
      expect(startAttachmentUpload).not.toHaveBeenCalled();
      expect(releasePreview).not.toHaveBeenCalled();
      if (outcome === "failure") {
        expect(messages).toEqual([]);
        expect(busy).toBe(false);
        expect(error).toBe("Compaction is unsupported.");
      }
    },
  );

  it("sends an attachment-free command without touching the draft or editor selection", async () => {
    const draftBefore = useComposerDraftStore.getState().getComposerDraft(threadRef);
    const selectionBefore = readSelection();
    let pending!: Promise<void>;
    await act(() => {
      pending = compact();
    });

    expect(busy).toBe(true);
    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatchObject({ role: "user", text: "/compact", runId: null });
    expect(messages[0]?.attachments).toBeUndefined();
    expect(persistSettings).toHaveBeenCalledExactlyOnceWith({
      threadId: thread.id,
      createdAt: messages[0]?.createdAt,
      modelSelection,
      runtimeMode: "approval-required",
      interactionMode: "plan",
      branch: "ianrowe12-current-checkout",
    });
    expect(startTurn).toHaveBeenCalledExactlyOnceWith({
      environmentId: thread.environmentId,
      input: {
        threadId: thread.id,
        message: {
          messageId: messages[0]?.id,
          role: "user",
          text: "/compact",
          attachments: [],
        },
        modelSelection,
        runtimeMode: "approval-required",
        interactionMode: "plan",
        dispatchMode: "auto",
        createdAt: messages[0]?.createdAt,
      },
    });
    expect(useComposerDraftStore.getState().getComposerDraft(threadRef)).toBe(draftBefore);
    expect(readSelection()).toEqual(selectionBefore);

    await act(async () => {
      startResult.resolve(AsyncResult.success(undefined));
      await pending;
    });
    expect(editorRef.current?.readSnapshot().value).toBe(prompt);
    expect(readSelection()).toEqual(selectionBefore);
    expect(useComposerDraftStore.getState().getComposerDraft(threadRef)).toBe(draftBefore);
    expect(normalSend).not.toHaveBeenCalled();
    expect(upload).not.toHaveBeenCalled();
    expect(releasePreview).not.toHaveBeenCalled();
    expect(input.sendInFlightRef.current).toBe(false);
    expect(error).toBeNull();
    expect(clearUsageLimits).toHaveBeenCalledOnce();
    expect(acknowledgeThreadWoke).toHaveBeenCalledOnce();
  });

  it("surfaces a rejected compact request and removes its optimistic row without changing the draft", async () => {
    const draftBefore = useComposerDraftStore.getState().getComposerDraft(threadRef);
    const selectionBefore = readSelection();
    startTurn.mockRejectedValueOnce(new Error("Compaction is unsupported for this account."));

    await act(async () => {
      await compact();
    });

    expect(error).toBe("Compaction is unsupported for this account.");
    expect(messages).toEqual([]);
    expect(busy).toBe(false);
    expect(input.sendInFlightRef.current).toBe(false);
    expect(editorRef.current?.readSnapshot().value).toBe(prompt);
    expect(readSelection()).toEqual(selectionBefore);
    expect(useComposerDraftStore.getState().getComposerDraft(threadRef)).toBe(draftBefore);
    expect(normalSend).not.toHaveBeenCalled();
    expect(upload).not.toHaveBeenCalled();
    expect(releasePreview).not.toHaveBeenCalled();
    expect(clearUsageLimits).not.toHaveBeenCalled();
    expect(acknowledgeThreadWoke).not.toHaveBeenCalled();
  });

  it("rejects duplicate activations immediately, then allows a fresh command after failure", async () => {
    const settingsResult = deferred<AtomCommandResult<void, unknown>>();
    persistSettings.mockReturnValueOnce(settingsResult.promise);
    let pending!: Promise<void>;
    let duplicate!: Promise<void>;
    await act(() => {
      pending = compact();
      duplicate = compact();
    });
    await duplicate;
    const firstMessageId = messages[0]?.id;
    expect(input.sendInFlightRef.current).toBe(true);
    expect(busy).toBe(true);
    expect(messages).toHaveLength(1);
    expect(persistSettings).toHaveBeenCalledOnce();
    expect(startTurn).not.toHaveBeenCalled();

    await act(async () => {
      settingsResult.resolve(AsyncResult.success(undefined));
      startResult.resolve(
        AsyncResult.failure(Cause.fail(new Error("Compaction is unsupported for this account."))),
      );
      await pending;
    });
    expect(error).toBe("Compaction is unsupported for this account.");
    expect(messages).toEqual([]);
    expect(busy).toBe(false);
    expect(input.sendInFlightRef.current).toBe(false);

    startTurn.mockResolvedValueOnce(AsyncResult.success(undefined));
    await act(async () => {
      await compact();
    });
    expect(messages).toHaveLength(1);
    expect(messages[0]?.id).not.toBe(firstMessageId);
    expect(startTurn).toHaveBeenCalledTimes(2);
    expect(error).toBeNull();
    expect(useComposerDraftStore.getState().getComposerDraft(threadRef)?.prompt).toBe(prompt);
  });

  it.each(["failure", "rejection"] as const)(
    "does not start a run after a settings %s and leaves the draft ready to retry",
    async (kind) => {
      const draftBefore = useComposerDraftStore.getState().getComposerDraft(threadRef);
      const selectionBefore = readSelection();
      const settingsError = new Error("Could not persist the selected runtime mode.");
      if (kind === "failure") {
        persistSettings.mockResolvedValueOnce(AsyncResult.failure(Cause.fail(settingsError)));
      } else {
        persistSettings.mockRejectedValueOnce(settingsError);
      }

      await act(async () => {
        await compact();
      });

      expect(startTurn).not.toHaveBeenCalled();
      expect(error).toBe(settingsError.message);
      expect(messages).toEqual([]);
      expect(busy).toBe(false);
      expect(input.sendInFlightRef.current).toBe(false);
      expect(useComposerDraftStore.getState().getComposerDraft(threadRef)).toBe(draftBefore);
      expect(readSelection()).toEqual(selectionBefore);
      expect(clearUsageLimits).not.toHaveBeenCalled();
    },
  );

  it("settles an interrupted compact request without a spurious error", async () => {
    const draftBefore = useComposerDraftStore.getState().getComposerDraft(threadRef);
    const selectionBefore = readSelection();
    startTurn.mockResolvedValueOnce(AsyncResult.failure(Cause.interrupt()));
    await act(async () => {
      await compact();
    });

    expect(error).toBeNull();
    expect(messages).toEqual([]);
    expect(busy).toBe(false);
    expect(input.sendInFlightRef.current).toBe(false);
    expect(useComposerDraftStore.getState().getComposerDraft(threadRef)).toBe(draftBefore);
    expect(readSelection()).toEqual(selectionBefore);
    expect(clearUsageLimits).not.toHaveBeenCalled();
  });

  it.each(["success", "failure"] as const)(
    "preserves edits and attachments arriving while compacting through %s",
    async (outcome) => {
      let pending!: Promise<void>;
      await act(() => {
        pending = compact();
      });
      // A paste can finish compressing while the independent command is in flight.
      const nextImage = {
        ...image,
        id: "new-paste",
        name: "new-paste.png",
        file: new File(["new image"], "new-paste.png", { type: "image/png" }),
        previewUrl: "blob:new-paste",
      };
      await act(() => {
        const store = useComposerDraftStore.getState();
        store.setPrompt(threadRef, "Draft edited while compacting");
        store.addImages(threadRef, [nextImage]);
      });
      const nextDraft = useComposerDraftStore.getState().getComposerDraft(threadRef);
      const nextSelection = readSelection();

      await act(async () => {
        startResult.resolve(
          outcome === "success"
            ? AsyncResult.success(undefined)
            : AsyncResult.failure(Cause.fail(new Error("Provider rejected compaction."))),
        );
        await pending;
      });

      expect(useComposerDraftStore.getState().getComposerDraft(threadRef)).toBe(nextDraft);
      expect(nextDraft?.images).toEqual([image, nextImage]);
      expect(nextDraft?.files).toEqual([file]);
      expect(editorRef.current?.readSnapshot().value).toBe("Draft edited while compacting");
      expect(readSelection()).toEqual(nextSelection);
      expect(normalSend).not.toHaveBeenCalled();
      expect(upload).not.toHaveBeenCalled();
      expect(releasePreview).not.toHaveBeenCalled();
    },
  );

  it.each([
    { name: "unavailable thread", overrides: { disabled: true } },
    { name: "missing thread", overrides: { threadId: null } },
    { name: "send in flight", overrides: { sendInFlightRef: { current: true } } },
    { name: "missing composer", overrides: { getRunContext: () => undefined } },
    {
      name: "unavailable provider",
      overrides: {
        getRunContext: () => ({
          providerAvailable: false,
          selectedModelSelection: modelSelection,
          interactionMode: "default" as const,
        }),
      },
    },
  ])("keeps the $name guard without touching the draft", async ({ overrides }) => {
    const draftBefore = useComposerDraftStore.getState().getComposerDraft(threadRef);
    const selectionBefore = readSelection();
    await act(async () => {
      await compactThreadContext({ ...input, ...overrides });
    });

    expect(messages).toEqual([]);
    expect(busy).toBe(false);
    expect(error).toBeNull();
    expect(persistSettings).not.toHaveBeenCalled();
    expect(startTurn).not.toHaveBeenCalled();
    expect(useComposerDraftStore.getState().getComposerDraft(threadRef)).toBe(draftBefore);
    expect(readSelection()).toEqual(selectionBefore);
  });
});
