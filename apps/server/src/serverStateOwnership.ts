// @effect-diagnostics nodeBuiltinImport:off
// Shared with the standalone launcher; keep runtime dependencies limited to built-ins.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

export const SERVER_LAUNCH_GATE_FILE = "server-launcher-lock.sqlite";
export const SERVER_RUNTIME_LOCK_FILE = "server-lock.sqlite";
export const SERVER_DATABASE_ACCESS_LOCK_FILE = "server-database-access.sqlite";

async function acquireFile(
  stateDir: string,
  filename: string,
  mode: "exclusive" | "access" = "exclusive",
) {
  await NodeFSP.mkdir(stateDir, { recursive: true, mode: 0o700 });
  const canonicalDir = await NodeFSP.realpath(stateDir);
  const lockPath = NodePath.join(canonicalDir, filename);
  const database =
    process.versions.bun === undefined
      ? new (await import("node:sqlite")).DatabaseSync(lockPath)
      : new (await import("bun:sqlite")).Database(lockPath);
  try {
    database.exec("PRAGMA busy_timeout = 0");
    const journal = database.prepare("PRAGMA journal_mode").get() as { journal_mode?: unknown };
    if (journal.journal_mode !== "delete") {
      throw new Error("State ownership locks require rollback journal mode.");
    }
    database.exec(
      mode === "exclusive" ? "BEGIN EXCLUSIVE" : "BEGIN; SELECT name FROM sqlite_schema",
    );
  } catch (cause) {
    database.close();
    throw new Error(
      `Cannot acquire ${mode} lease on ${lockPath}; another T3 process may be active.`,
      {
        cause,
      },
    );
  }
  return { canonicalDir, release: () => database.close() };
}

/** Held by a standalone owner or its launcher, including child handoffs and rollback. */
export async function acquireStateLaunchGate(stateDir: string): Promise<() => void> {
  return (await acquireFile(stateDir, SERVER_LAUNCH_GATE_FILE)).release;
}

/** Kept until the application SQLite connection closes, including auth cleanup. */
export async function acquireDatabaseAccess(
  stateDir: string,
  options?: { readonly exclusive: true },
): Promise<() => void> {
  return (
    await acquireFile(
      stateDir,
      SERVER_DATABASE_ACCESS_LOCK_FILE,
      options?.exclusive ? "exclusive" : "access",
    )
  ).release;
}

function isLeaseBusy(error: unknown): boolean {
  const cause = error instanceof Error ? error.cause : undefined;
  return (
    typeof cause === "object" &&
    cause !== null &&
    (("errcode" in cause && cause.errcode === 5) ||
      ("code" in cause && cause.code === "SQLITE_BUSY"))
  );
}

/** Database access does not itself count as runtime ownership. */
export async function hasRuntimeStateOwner(stateDir: string): Promise<boolean> {
  try {
    (await acquireFile(stateDir, SERVER_RUNTIME_LOCK_FILE)).release();
    return false;
  } catch (error) {
    if (isLeaseBusy(error)) return true;
    throw error;
  }
}

/** Every runtime holds this independently, so an orphan child still excludes new owners. */
export async function acquireRuntimeStateOwnership(stateDir: string): Promise<() => void> {
  const lock = await acquireFile(stateDir, SERVER_RUNTIME_LOCK_FILE);
  try {
    let raw: string;
    try {
      raw = await NodeFSP.readFile(NodePath.join(lock.canonicalDir, "server-runtime.json"), "utf8");
    } catch (cause) {
      if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return lock.release;
      throw cause;
    }

    const owner: unknown = JSON.parse(raw);
    if (
      typeof owner !== "object" ||
      owner === null ||
      !("version" in owner) ||
      owner.version !== 1 ||
      !("pid" in owner) ||
      typeof owner.pid !== "number" ||
      !Number.isInteger(owner.pid) ||
      owner.pid <= 0
    )
      throw new Error("Server runtime ownership metadata is invalid.");
    if ("stateLockVersion" in owner) {
      if (owner.stateLockVersion === 1) return lock.release;
      throw new Error("Server runtime ownership protocol is unsupported.");
    }
    try {
      process.kill(owner.pid, 0);
    } catch (cause) {
      if (cause instanceof Error && "code" in cause) {
        if (cause.code === "ESRCH") return lock.release;
        if (cause.code !== "EPERM") throw cause;
      } else throw cause;
    }
    throw new Error(`Legacy T3 server ${owner.pid} is still using ${lock.canonicalDir}.`);
  } catch (cause) {
    lock.release();
    throw cause;
  }
}

/** Auth can initialize only as a full owner, or share an already-initialized guarded runtime. */
export async function acquireAuthDatabaseAccess(stateDir: string) {
  await NodeFSP.mkdir(stateDir, { recursive: true, mode: 0o700 });
  const canonicalDir = await NodeFSP.realpath(stateDir);
  const releases: Array<() => void> = [];
  let releaseDatabase: (() => void) | undefined;
  const completeInitialization = () => {
    while (releases.length > 0) releases.pop()!();
  };
  const release = () => {
    releaseDatabase?.();
    releaseDatabase = undefined;
    completeInitialization();
  };
  try {
    try {
      releases.push(await acquireStateLaunchGate(canonicalDir));
      releases.push(await acquireRuntimeStateOwnership(canonicalDir));
    } catch (error) {
      release();
      if (!isLeaseBusy(error)) throw error;
      releaseDatabase = await acquireDatabaseAccess(canonicalDir);
      const raw = await NodeFSP.readFile(
        NodePath.join(canonicalDir, "server-runtime.json"),
        "utf8",
      );
      const owner: unknown = JSON.parse(raw);
      if (
        typeof owner !== "object" ||
        owner === null ||
        !("version" in owner) ||
        owner.version !== 1 ||
        !("stateLockVersion" in owner) ||
        owner.stateLockVersion !== 1 ||
        !("pid" in owner) ||
        typeof owner.pid !== "number" ||
        !Number.isInteger(owner.pid) ||
        owner.pid <= 0 ||
        !("port" in owner) ||
        typeof owner.port !== "number" ||
        !Number.isInteger(owner.port) ||
        owner.port <= 0 ||
        owner.port > 65535 ||
        !("origin" in owner) ||
        typeof owner.origin !== "string" ||
        owner.origin.trim() === "" ||
        !("startedAt" in owner) ||
        typeof owner.startedAt !== "string" ||
        owner.startedAt.trim() === ""
      ) {
        throw new Error("Auth requires a guarded live runtime or exclusive state ownership.", {
          cause: error,
        });
      }
      try {
        process.kill(owner.pid, 0);
      } catch (cause) {
        if (!(cause instanceof Error && "code" in cause && cause.code === "EPERM")) throw cause;
      }
      if (!(await hasRuntimeStateOwner(canonicalDir))) {
        throw new Error("The guarded runtime no longer owns this state.", { cause: error });
      }
      return { allowMigrations: false, completeInitialization, release };
    }
    releaseDatabase = await acquireDatabaseAccess(canonicalDir);
    return { allowMigrations: true, completeInitialization, release };
  } catch (error) {
    release();
    throw error;
  }
}
