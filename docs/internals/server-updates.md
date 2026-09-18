# Server updates

A [stable launcher](../../apps/server/src/serviceLauncher.ts) owns the runtime
selected by systemd or launchd. It is the only runtime writer of durable service
state. Server children request updates over inherited IPC; they never rewrite
their service definition or select their own replacement. Local service commands
may replace the launcher and state while the service is stopped. Foreground CLI
processes do not self-update.

Exact-version installs keep restarts independent of npm cache eviction or a moving
release tag. Installation and preflight happen in staging before publishing an
immutable runtime. Preflight checks the launcher protocol because a target that
needs new rollback guarantees cannot safely run under an older launcher. Upgrading
that launcher requires a local service update.

## Commit boundary

The launcher durably records the pending update before acknowledging it, then
stops the old child and starts the target as a trial. Service-state writes use
same-directory replacement with file and directory fsync. Invalid state stops
startup rather than guessing which runtime to boot.

The trial must finish migrations, acquire dependencies, bind HTTP, and park every
long-running root at the activation gate before reporting `prepared`. The launcher
then commits the target version durably and replies `committed`. Only then may the
child release its gates, accept commands, and publish ready. Keep fallible startup
acquisitions before this boundary. A listener alone does not prove the runtime is
ready to commit.

A failed or timed-out trial returns to the old version. After commit, the target
is authoritative and the service manager's ordinary restart policy applies.

## State ownership

Only one server may own a T3 state directory, regardless of port or launcher.
[State ownership](../../apps/server/src/serverStateLock.ts) must be acquired
before migrations or startup recovery and held through shutdown recovery. A
second process cannot reconcile runs whose provider processes are still alive
in the first. SSH reconnects reuse a live owner; a readiness timeout is not
permission to replace it or start another server against its state. Project CLI
commands must not discard a live owner's discovery or silently switch to offline
mutations when that owner cannot be reached.

Ownership uses two SQLite lock files, separate from the application database.
The launcher holds an exclusive transaction in `server-launcher-lock.sqlite` for
its whole lifetime, including gaps between children and database rollback. Every
child holds an exclusive transaction in `server-lock.sqlite` through shutdown.
A standalone server or offline project mutation acquires both, launch gate first.
One lock file is insufficient: a child-only
lock leaves handoffs open to independent starts, while a launcher-only lock
disappears on parent death even if its child is still writing.

A managed child checks its launcher's state directory and live parent/IPC again
after acquiring the runtime lock, before opening application state. A restarted
launcher must acquire the runtime lock before snapshotting or restoring, so an
orphan child blocks recovery.

Database access is a separate role. Every file-backed application SQLite
connection holds a shared transaction in `server-database-access.sqlite` from
before opening the database through its final close and WAL checkpoint, including
failed layer builds. Snapshot and restore require exclusive database access after
runtime ownership. Auth cleanup therefore cannot race a raw restore, even after
the server exits. Merely starting a child does not require exclusive database
access. All three coordination files require rollback journal mode, not WAL,
because WAL readers do not exclude an exclusive writer. Contention fails
immediately; the launcher never copies through a busy access lease.

Offline project commands acquire ownership before constructing persistence and
do not initialize authentication. All CLI auth entry points use the same admission
layer: fresh/offline initialization takes both ownership locks before opening
SQLite or migrating; live access requires guarded discovery and native runtime
ownership and skips migrations. Shared access alone never authorizes migrations.
Initialization releases ownership only after the auth layer is ready, retaining
database access through session cleanup and connection close. This lets a cloud
command start its managed server without retaining the launch gate. Live project
commands also confirm native runtime ownership before selecting HTTP execution.
Merely wrapping a mutation body would leave auth-layer migrations and cleanup
outside coordination.
Updates may only use `state.sqlite` in the
canonical owned directory; database and sidecar symlinks are rejected. Otherwise
holding a lock would say nothing about the database being replaced.

The OS releases transactions on process exit. Never delete or replace any
coordination file while a participant might exist. Discovery metadata is not ownership.
Guarded discovery can outlive its PID, so SSH checks both locks rather than
trusting a recycled PID. SSH retains guarded provenance alongside its cached
PID and port; otherwise a clean shutdown that removes discovery would make a
recycled cached PID indistinguishable from a legacy server. Busy locks without usable discovery, unreadable metadata,
and readiness failures stop the connection attempt instead of launching a rival.
Legacy servers and CLI processes do not participate in these leases; stop them
and upgrade every participating binary before relying on the guarantee. Launcher
protocol 3 requires a local `t3 service update` to replace the stable launcher;
updating only its child cannot add the missing launch gate or access coordinator.

## Database rollback

After the old child exits, the launcher snapshots SQLite's main file, WAL, and
shared-memory file. This makes trial migrations reversible without down
migrations. The snapshot is made once per update and survives launcher restarts;
replacing it during a retry could capture changes from the failed trial.

Rollback stops the trial before restoring. A durable restore marker makes an
interrupted restore finish before either version boots. Keep the snapshot until
commit, or until both restoration and the terminal rollback state are durable.
Attachments and other files outside SQLite are outside this rollback boundary.

## Client acknowledgement

An accepted update is still pending. Clients correlate the launcher's update ID
with the ready event after reconnecting, then check the outcome and target version.
A reconnect alone cannot distinguish successful replacement from rollback. Older
servers without an update ID retain version-only correlation.

Desktop updates have a separate two-phase handoff because installing the app stops
its bundled backend. Preparation returns a token while the connection is alive;
the client commits that token only after receiving it. Otherwise backend shutdown
could lose the only successful RPC result. The client must then observe the
prepared version after reconnecting. If installation fails, desktop restarts the
stopped backends and replays the failure for the same token.

## Recovering interrupted threads

Restart continuation is an environment-owned preference, off by default. The
[v2 recovery service](../../apps/server/src/orchestration-v2/ProviderRuntimeRecoveryService.ts)
requires matching durable run, provider thread, session, and native resume identity.
Ordinary queued work, finished runs, and background-only work do not qualify.

Recovery retires effects tied to the lost process and records continuation intent
in the durable outbox. That intent survives another restart before provider startup.
Continuation effects wait for activation; a slow provider must not delay the server's
readiness or the launcher's commit boundary. Graceful shutdown captures intent before
closing providers, then reconciles after ingestion has stopped so a late completion
cannot be overwritten by a stale cancellation.

The [continuation handler](../../apps/server/src/orchestration-v2/RestartContinuation.ts)
rechecks the preference, archive state, provider selection, and newer user work before
dispatching. Stable command and message IDs prevent duplicate submissions after an
outbox retry. Codex resumes without adding provider prompt text; other adapters receive
the continuation message through their normal turn path.
