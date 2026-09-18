// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import { assert, expect, it } from "@effect/vitest";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Context from "effect/Context";

import { remoteStateKey } from "./command.ts";
import { buildRemoteLaunchScript, buildRemoteStopScript } from "./tunnel.ts";

const isWindows = Context.get(Context.empty(), HostProcessPlatform) === "win32";

const fixtureServer = `
const http = require("node:http");
const server = http.createServer((_request, response) => {
  if (process.argv[1] === "ready") response.end("ready");
});
server.listen(0, "127.0.0.1", () => {
  process.stdout.write(JSON.stringify({ port: server.address().port }) + "\\n");
});
`;

async function withRemoteRuntime(
  ready: boolean,
  use: (fixture: {
    home: string;
    stateDir: string;
    pid: number;
    port: number;
    launchesPath: string;
    launch: (script?: string) => Promise<{ code: number; stdout: string; stderr: string }>;
  }) => Promise<void>,
) {
  const home = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-ssh-owner-"));
  const server = NodeChildProcess.spawn(
    process.execPath,
    ["-e", fixtureServer, ready ? "ready" : "pending"],
    {
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const exited = NodeEvents.EventEmitter.once(server, "exit");
  try {
    const [output] = await NodeEvents.EventEmitter.once(server.stdout, "data");
    const { port } = JSON.parse(String(output)) as { port: number };
    const pid = server.pid;
    if (pid === undefined) throw new Error("Fixture server did not start.");
    const stateDir = NodePath.join(home, ".t3", "ssh-launch", "test");
    const userdata = NodePath.join(home, ".t3", "userdata");
    await NodeFSP.mkdir(stateDir, { recursive: true });
    await NodeFSP.mkdir(userdata, { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(userdata, "server-runtime.json"),
      JSON.stringify({ version: 1, pid, port, origin: `http://127.0.0.1:${port}` }),
    );
    const runner = NodePath.join(home, "fixture-cli.cjs");
    const launchesPath = NodePath.join(home, "unexpected-launch");
    await NodeFSP.writeFile(
      runner,
      `require("node:fs").writeFileSync(${JSON.stringify(launchesPath)}, String(process.pid));
require("node:http").createServer((_req, res) => res.end("ready")).listen(Number(process.argv[process.argv.indexOf("--port") + 1]), "127.0.0.1");`,
    );
    const script = buildRemoteLaunchScript({ nodeScriptPath: runner })
      .replaceAll('wait_ready "2000"', 'wait_ready "100"')
      .replaceAll('wait_ready "60000"', 'wait_ready "1000"');
    await use({
      home,
      stateDir,
      pid,
      port,
      launchesPath,
      launch: (overrideScript = script) =>
        new Promise((resolve, reject) => {
          const command = NodeChildProcess.execFile(
            "sh",
            ["-s", "--", "test"],
            {
              env: { HOME: home, PATH: `${NodePath.dirname(process.execPath)}:/usr/bin:/bin` },
              timeout: 10_000,
            },
            (error, stdout, stderr) => {
              if (error?.killed) reject(error);
              else resolve({ code: error ? Number(error.code) : 0, stdout, stderr });
            },
          );
          command.stdin?.end(overrideScript);
        }),
    });
  } finally {
    const launchedPid = Number(
      await NodeFSP.readFile(NodePath.join(home, "unexpected-launch"), "utf8").catch(() => ""),
    );
    if (launchedPid > 0) {
      try {
        process.kill(launchedPid, "SIGTERM");
      } catch {}
    }
    if (server.exitCode === null && server.signalCode === null) server.kill("SIGTERM");
    await exited;
    await NodeFSP.rm(home, { recursive: true, force: true });
  }
}

it.skipIf(isWindows)(
  "ignores a recycled PID in guarded discovery when both ownership locks are free",
  async () =>
    withRemoteRuntime(true, async ({ home, stateDir, pid, port, launch, launchesPath }) => {
      for (const filename of ["server-launcher-lock.sqlite", "server-lock.sqlite"]) {
        new NodeSqlite.DatabaseSync(NodePath.join(home, ".t3", "userdata", filename)).close();
      }
      await NodeFSP.writeFile(
        NodePath.join(home, ".t3", "userdata", "server-runtime.json"),
        JSON.stringify({
          version: 1,
          stateLockVersion: 1,
          pid,
          port,
          origin: `http://127.0.0.1:${port}`,
        }),
      );
      await NodeFSP.writeFile(NodePath.join(stateDir, "pid"), String(pid));
      await NodeFSP.writeFile(NodePath.join(stateDir, "port"), String(port));
      const result = await launch();
      assert.equal(result.code, 0, result.stderr);
      assert.isAbove(Number(await NodeFSP.readFile(launchesPath, "utf8")), 0);
      assert.doesNotThrow(() => process.kill(pid, 0));
    }),
);

for (const clearCache of [false, true]) {
  it.skipIf(isWindows)(
    `does not reuse a guarded cached PID after clean shutdown with cache ${clearCache ? "cleared" : "retained"}`,
    async () =>
      withRemoteRuntime(true, async ({ home, pid, port, launch, launchesPath }) => {
        const userdata = NodePath.join(home, ".t3", "userdata");
        const runtimePath = NodePath.join(userdata, "server-runtime.json");
        await NodeFSP.writeFile(
          runtimePath,
          JSON.stringify({
            version: 1,
            stateLockVersion: 1,
            pid,
            port,
            origin: `http://127.0.0.1:${port}`,
          }),
        );
        const launchGate = new NodeSqlite.DatabaseSync(
          NodePath.join(userdata, "server-launcher-lock.sqlite"),
        );
        const runtimeLock = new NodeSqlite.DatabaseSync(
          NodePath.join(userdata, "server-lock.sqlite"),
        );
        try {
          launchGate.exec("BEGIN EXCLUSIVE");
          runtimeLock.exec("BEGIN EXCLUSIVE");
          const reused = await launch();
          assert.equal(reused.code, 0, reused.stderr);
          if (clearCache) {
            const target = { alias: "fixture", hostname: "fixture", username: "fixture", port: 22 };
            const stopped = await launch(
              buildRemoteStopScript(target).replace(remoteStateKey(target), "test"),
            );
            assert.equal(stopped.code, 0, stopped.stderr);
          }
        } finally {
          runtimeLock.close();
          launchGate.close();
        }
        await NodeFSP.rm(runtimePath);

        const restarted = await launch();

        assert.equal(restarted.code, 0, restarted.stderr);
        assert.isAbove(Number(await NodeFSP.readFile(launchesPath, "utf8")), 0);
        assert.doesNotThrow(() => process.kill(pid, 0));
      }),
  );
}

for (const lockFile of ["server-launcher-lock.sqlite", "server-lock.sqlite"]) {
  for (const discovery of ["present", "missing", "stale"] as const) {
    it.skipIf(isWindows)(`respects busy ${lockFile} with ${discovery} discovery`, async () =>
      withRemoteRuntime(true, async ({ home, pid, port, launch, launchesPath }) => {
        const userdata = NodePath.join(home, ".t3", "userdata");
        const runtimeFile = NodePath.join(userdata, "server-runtime.json");
        if (discovery === "missing") await NodeFSP.rm(runtimeFile);
        else
          await NodeFSP.writeFile(
            runtimeFile,
            JSON.stringify({
              version: 1,
              stateLockVersion: 1,
              pid: discovery === "stale" ? 2147483647 : pid,
              port,
              origin: `http://127.0.0.1:${port}`,
            }),
          );
        const lock = new NodeSqlite.DatabaseSync(NodePath.join(userdata, lockFile));
        try {
          lock.exec("BEGIN EXCLUSIVE");
          const result = await launch();
          if (discovery === "present") {
            assert.equal(result.code, 0, result.stderr);
            assert.equal(JSON.parse(result.stdout).remotePort, port);
          } else {
            assert.notEqual(result.code, 0);
            assert.include(result.stderr, "ownership is busy");
          }
          await expect(NodeFSP.readFile(launchesPath)).rejects.toMatchObject({ code: "ENOENT" });
          assert.doesNotThrow(() => process.kill(pid, 0));
        } finally {
          lock.close();
        }
      }),
    );
  }
}

for (const invalid of [
  "malformed",
  "empty",
  "unreadable",
  "unsupported",
  "corrupt-lock",
  "malformed-guarded-cache",
  "unreadable-guarded-cache",
  "mismatched-guarded-cache",
] as const) {
  it.skipIf(isWindows)(`fails closed with ${invalid} ownership discovery`, async () =>
    withRemoteRuntime(true, async ({ home, launch, launchesPath }) => {
      const userdata = NodePath.join(home, ".t3", "userdata");
      const runtimeFile = NodePath.join(userdata, "server-runtime.json");
      const guardedCache = NodePath.join(home, ".t3", "ssh-launch", "test", "guarded-runtime.json");
      if (invalid === "malformed-guarded-cache") {
        await NodeFSP.writeFile(guardedCache, "{");
      } else if (invalid === "unreadable-guarded-cache") {
        await NodeFSP.mkdir(guardedCache);
      } else if (invalid === "mismatched-guarded-cache") {
        await NodeFSP.rm(runtimeFile);
        await NodeFSP.writeFile(guardedCache, '{"version":1,"pid":1,"port":1}');
      } else if (invalid === "unreadable") {
        await NodeFSP.rm(runtimeFile);
        await NodeFSP.mkdir(runtimeFile);
      } else if (invalid === "corrupt-lock") {
        await NodeFSP.writeFile(NodePath.join(userdata, "server-lock.sqlite"), "not sqlite");
      } else {
        await NodeFSP.writeFile(
          runtimeFile,
          invalid === "empty" ? "" : invalid === "malformed" ? "{" : '{"version":2}',
        );
      }
      const result = await launch();
      assert.notEqual(result.code, 0);
      assert.include(result.stderr, "Refusing to start another T3 server");
      await expect(NodeFSP.readFile(launchesPath)).rejects.toMatchObject({ code: "ENOENT" });
    }),
  );
}

it.skipIf(isWindows)(
  "does not launch a second server when the live state owner is slow to respond",
  async () =>
    withRemoteRuntime(false, async ({ home, pid, launch }) => {
      const result = await launch();
      await expect(
        NodeFSP.readFile(NodePath.join(home, "unexpected-launch")),
      ).rejects.toMatchObject({
        code: "ENOENT",
      });
      assert.notEqual(result.code, 0);
      assert.include(result.stderr, "already running");
      assert.doesNotThrow(() => process.kill(pid, 0));
    }),
);

it.skipIf(isWindows)(
  "fails closed for an unready legacy cached PID without discovery or guarded provenance",
  async () =>
    withRemoteRuntime(false, async ({ home, stateDir, pid, port, launch, launchesPath }) => {
      await NodeFSP.rm(NodePath.join(home, ".t3", "userdata", "server-runtime.json"));
      await NodeFSP.writeFile(NodePath.join(stateDir, "pid"), String(pid));
      await NodeFSP.writeFile(NodePath.join(stateDir, "port"), String(port));
      await NodeFSP.writeFile(NodePath.join(stateDir, "managed"), "managed");
      const result = await launch();
      assert.notEqual(result.code, 0);
      assert.include(result.stderr, "already running");
      await expect(NodeFSP.readFile(launchesPath)).rejects.toMatchObject({ code: "ENOENT" });
      assert.doesNotThrow(() => process.kill(pid, 0));
    }),
);

for (const discoveryExists of [true, false]) {
  it.skipIf(isWindows)(
    `reuses a healthy managed server without stopping it, with discovery ${discoveryExists ? "present" : "absent"}`,
    async () =>
      withRemoteRuntime(true, async ({ home, stateDir, pid, port, launch }) => {
        await NodeFSP.writeFile(NodePath.join(stateDir, "pid"), String(pid));
        await NodeFSP.writeFile(NodePath.join(stateDir, "port"), String(port));
        await NodeFSP.writeFile(NodePath.join(stateDir, "managed"), "managed");
        if (!discoveryExists) {
          await NodeFSP.rm(NodePath.join(home, ".t3", "userdata", "server-runtime.json"));
        }

        const result = await launch();

        assert.equal(result.code, 0, result.stderr);
        assert.deepEqual(JSON.parse(result.stdout.trim()), {
          remotePort: port,
          serverKind: discoveryExists ? "external" : "managed",
        });
        assert.doesNotThrow(() => process.kill(pid, 0));
        await expect(
          NodeFSP.readFile(NodePath.join(home, "unexpected-launch")),
        ).rejects.toMatchObject({
          code: "ENOENT",
        });
        assert.equal(
          (await NodeFSP.readFile(NodePath.join(stateDir, "pid"), "utf8")).trim(),
          String(pid),
        );
      }),
  );
}
