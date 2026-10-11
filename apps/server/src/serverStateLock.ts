import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";

import { acquireRuntimeStateOwnership, acquireStateLaunchGate } from "./serverStateOwnership.ts";

export class ServerStateLockError extends Schema.TaggedError<ServerStateLockError>()(
  "ServerStateLockError",
  { stateDir: Schema.String, cause: Schema.Defect() },
) {
  override get message(): string {
    return `Could not exclusively lock T3 state at ${this.stateDir}. Reuse its running server or choose a different home.`;
  }
}

export const acquireServerStateLock = Effect.fn("acquireServerStateLock")(function* (
  stateDir: string,
  launcherStateDir?: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const lockError = (cause: unknown) => new ServerStateLockError({ stateDir, cause });
  yield* fs.makeDirectory(stateDir, { recursive: true }).pipe(Effect.mapError(lockError));
  const canonical = yield* fs.realPath(stateDir).pipe(Effect.mapError(lockError));
  if (launcherStateDir === undefined) {
    yield* Effect.acquireRelease(
      Effect.tryPromise(() => acquireStateLaunchGate(canonical)).pipe(Effect.mapError(lockError)),
      (release) => Effect.sync(release),
    );
  } else {
    const launcherDir = yield* fs.realPath(launcherStateDir).pipe(Effect.mapError(lockError));
    if (launcherDir !== canonical) {
      return yield* lockError(new Error("The launcher does not own this state directory."));
    }
  }
  yield* Effect.acquireRelease(
    Effect.tryPromise(() => acquireRuntimeStateOwnership(canonical)).pipe(
      Effect.mapError(lockError),
    ),
    (release) => Effect.sync(release),
  );
});
