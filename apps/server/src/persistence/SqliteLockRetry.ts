import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import type * as SqlClient from "effect/unstable/sql/SqlClient";
import { isSqlError } from "effect/unstable/sql/SqlError";

const MAX_CAUSE_DEPTH = 16;
const LOCK_RETRY_TIMES = 6;

/**
 * True when an error, or any error it wraps through `cause`, is a SQLite
 * SQLITE_BUSY/SQLITE_LOCKED failure. Store errors wrap the SqlError, so the
 * chain has to be walked.
 */
export function isSqliteLockContention(error: unknown): boolean {
  let current = error;
  for (let depth = 0; depth < MAX_CAUSE_DEPTH; depth += 1) {
    if (isSqlError(current)) {
      return current.reason._tag === "LockTimeoutError";
    }
    if (typeof current !== "object" || current === null || !("cause" in current)) {
      return false;
    }
    current = current.cause;
  }
  return false;
}

/**
 * Retries a whole `sql.withTransaction(...)` when SQLite reports lock
 * contention. Another process can hold the write lock past `busy_timeout`, and
 * a deferred transaction that read before another writer committed fails with
 * SQLITE_BUSY_SNAPSHOT without waiting at all. Either way the transaction was
 * rolled back, so rerunning it is safe as long as its body only touches SQL.
 *
 * Nested calls run once: the outermost transaction owns the retry, because a
 * savepoint cannot recover a snapshot the outer transaction already holds.
 * The backoff sleeps are async, unlike busy_timeout, which blocks the event
 * loop on the synchronous SQLite driver.
 */
export const retryTransactionOnSqliteLock =
  (sql: SqlClient.SqlClient, operation: string) =>
  <A, E, R>(transaction: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.serviceOption(sql.transactionService).pipe(
      Effect.flatMap((outer) =>
        Option.isSome(outer)
          ? transaction
          : transaction.pipe(
              Effect.tapError((error) =>
                isSqliteLockContention(error)
                  ? Effect.logWarning("SQLite transaction hit lock contention; retrying", {
                      operation,
                    })
                  : Effect.void,
              ),
              Effect.retry({
                while: isSqliteLockContention,
                times: LOCK_RETRY_TIMES,
                schedule: Schedule.exponential("50 millis").pipe(Schedule.jittered),
              }),
            ),
      ),
    );
