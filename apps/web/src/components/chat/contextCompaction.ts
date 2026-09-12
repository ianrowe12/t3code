import type { StartThreadTurnInput } from "@t3tools/client-runtime/state/threads";
import {
  isAtomCommandInterrupted,
  settlePromise,
  squashAtomCommandFailure,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";
import type {
  EnvironmentId,
  ModelSelection,
  ProviderInteractionMode,
  RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import type { Dispatch, RefObject, SetStateAction } from "react";

import { newMessageId } from "../../lib/utils";
import type { ChatMessage } from "../../types";

export interface ComposerRunContext {
  readonly providerAvailable: boolean;
  readonly selectedModelSelection: ModelSelection;
  readonly interactionMode: ProviderInteractionMode;
}

export interface ThreadTurnSettings {
  threadId: ThreadId;
  createdAt: string;
  modelSelection?: ModelSelection;
  branch?: string;
  runtimeMode: RuntimeMode;
  interactionMode: ProviderInteractionMode;
}

export interface ContextCompactionInput {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId | null;
  readonly disabled: boolean;
  readonly sendInFlightRef: RefObject<boolean>;
  readonly getRunContext: () => ComposerRunContext | undefined;
  readonly runtimeMode: RuntimeMode;
  readonly branch?: string;
  readonly persistSettings: (
    settings: ThreadTurnSettings,
  ) => Promise<AtomCommandResult<void, unknown>>;
  readonly startTurn: (request: {
    environmentId: EnvironmentId;
    input: StartThreadTurnInput;
  }) => Promise<AtomCommandResult<unknown, unknown>>;
  readonly setOptimisticMessages: Dispatch<SetStateAction<ChatMessage[]>>;
  readonly beginLocalDispatch: () => void;
  readonly resetLocalDispatch: () => void;
  readonly setThreadError: (threadId: ThreadId, error: string | null) => void;
  readonly scrollToEnd: () => void;
  readonly clearUsageLimits: () => void;
  readonly acknowledgeThreadWoke: () => void;
}

export async function compactThreadContext(input: ContextCompactionInput): Promise<void> {
  if (input.disabled || input.threadId === null || input.sendInFlightRef.current) return;
  const context = input.getRunContext();
  if (!context?.providerAvailable) return;

  // This command has no access to the composer draft or its attachment lifecycle.
  input.sendInFlightRef.current = true;
  const threadId = input.threadId;
  const messageId = newMessageId();
  const createdAt = new Date().toISOString();
  let started = false;
  try {
    input.beginLocalDispatch();
    input.setThreadError(threadId, null);
    input.setOptimisticMessages((messages) => [
      ...messages,
      {
        id: messageId,
        role: "user",
        text: "/compact",
        runId: null,
        createdAt,
        updatedAt: createdAt,
        streaming: false,
      },
    ]);
    input.scrollToEnd();
    const dispatchResult = await settlePromise(async () => {
      const settingsResult = await input.persistSettings({
        threadId,
        createdAt,
        ...(context.selectedModelSelection.model
          ? { modelSelection: context.selectedModelSelection }
          : {}),
        ...(input.branch ? { branch: input.branch } : {}),
        runtimeMode: input.runtimeMode,
        interactionMode: context.interactionMode,
      });
      return settingsResult._tag === "Failure"
        ? settingsResult
        : await input.startTurn({
            environmentId: input.environmentId,
            input: {
              threadId,
              message: { messageId, role: "user", text: "/compact", attachments: [] },
              modelSelection: context.selectedModelSelection,
              runtimeMode: input.runtimeMode,
              interactionMode: context.interactionMode,
              dispatchMode: "auto",
              createdAt,
            },
          });
    });
    const result = dispatchResult._tag === "Failure" ? dispatchResult : dispatchResult.value;
    if (result._tag === "Failure") {
      if (!isAtomCommandInterrupted(result)) {
        const error = squashAtomCommandFailure(result);
        input.setThreadError(
          threadId,
          error instanceof Error ? error.message : "Failed to compact context.",
        );
      }
    } else {
      started = true;
      input.clearUsageLimits();
      input.acknowledgeThreadWoke();
    }
  } finally {
    input.sendInFlightRef.current = false;
    if (!started) {
      input.setOptimisticMessages((messages) =>
        messages.filter((message) => message.id !== messageId),
      );
      input.resetLocalDispatch();
    }
  }
}
