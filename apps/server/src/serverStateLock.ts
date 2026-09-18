import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";

import type * as ServerConfig from "./config.ts";
import { resolveServiceLauncherMode } from "./cloud/serviceLauncherClient.ts";
import {
  acquireDatabaseAccess,
  acquireRuntimeStateOwnership,
  acquireStateLaunchGate,
  hasRuntimeStateOwner,
} from "./serverStateOwnership.ts";

export class ServerStateLockError extends Schema.TaggedError<ServerStateLockError>()(
  "ServerStateLockError",
  {
    stateDir: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Could not exclusively lock T3 server state at ${this.stateDir}. Another server may already be using it. Reuse that server or choose a different --base-dir.`;
  }
}

export const acquireServerStateLock = Effect.fn("acquireServerStateLock")(function* (
  config: Pick<ServerConfig.ServerConfig["Service"], "stateDir" | "serverRuntimeStatePath">,
  options?: { readonly launcherStateDir: string },
) {
  const fs = yield* FileSystem.FileSystem;
  const lockError = (cause: unknown) =>
    new ServerStateLockError({ stateDir: config.stateDir, cause });
  yield* fs.makeDirectory(config.stateDir, { recursive: true }).pipe(Effect.mapError(lockError));
  const stateDir = yield* fs.realPath(config.stateDir).pipe(Effect.mapError(lockError));
  if (options === undefined) {
    yield* Effect.acquireRelease(
      Effect.tryPromise(() => acquireStateLaunchGate(stateDir)).pipe(Effect.mapError(lockError)),
      (release) => Effect.sync(release),
    );
  } else {
    const launcherDir = yield* fs
      .realPath(options.launcherStateDir)
      .pipe(Effect.mapError(lockError));
    if (launcherDir !== stateDir) {
      return yield* lockError(new Error("The service launcher does not own this state directory."));
    }
  }
  yield* Effect.acquireRelease(
    Effect.tryPromise(() => acquireRuntimeStateOwnership(stateDir)).pipe(
      Effect.mapError(lockError),
    ),
    (release) => Effect.sync(release),
  );
  if (options !== undefined) {
    // A parent can die while we acquire the lock. Recheck before any app state opens.
    const launcher = yield* resolveServiceLauncherMode();
    if (!launcher.managed || launcher.stateDir !== options.launcherStateDir) {
      return yield* lockError(new Error("The service launcher no longer owns this startup."));
    }
  }
});

export const acquireServerDatabaseAccess = Effect.fn("acquireServerDatabaseAccess")(function* (
  stateDir: string,
) {
  yield* Effect.acquireRelease(
    Effect.tryPromise(() => acquireDatabaseAccess(stateDir)).pipe(
      Effect.mapError((cause) => new ServerStateLockError({ stateDir, cause })),
    ),
    (release) => Effect.sync(release),
  );
});

export const serverStateHasOwner = Effect.fn("serverStateHasOwner")(function* (stateDir: string) {
  return yield* Effect.tryPromise(() => hasRuntimeStateOwner(stateDir)).pipe(
    Effect.mapError((cause) => new ServerStateLockError({ stateDir, cause })),
  );
});
