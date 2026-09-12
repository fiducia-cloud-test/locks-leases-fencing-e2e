#!/usr/bin/env node

// DEN-1391 / RAFT-003 synthetic test-fleet proof.
//
// Boots three real fiducia-node processes with durable Raft state, transfers a
// fenced lock after TTL expiry, verifies a reference downstream rejects the old
// holder, kills the current leader, and restarts the whole cluster without
// allowing fencing-token regression. This is test-org automation, not deployed
// production evidence.

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer as createHttpServer } from "node:http";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

const NODE_BIN = process.env.FIDUCIA_NODE_BIN;
const EVIDENCE_PATH = resolve(
  process.env.FIDUCIA_FENCING_EVIDENCE_PATH ??
    join(process.cwd(), "test-results", "stale-fencing-evidence.json"),
);
const INTERNAL_SECRET =
  "test-fleet-internal-secret-000000000000000000000000000000000000";
const ORG_ID = "test-fleet-org";
const SHARD_COUNT = 1;
const FIRST_TTL_MS = 1_500;
const ACTIVE_TTL_MS = 30_000;
const REQUEST_TIMEOUT_MS = 4_000;
const STARTUP_TIMEOUT_MS = 90_000;

if (!NODE_BIN) throw new Error("FIDUCIA_NODE_BIN is required");

const evidence = {
  schema_version: 1,
  evidence_type: "fiducia_test_fleet_stale_fencing",
  production_evidence: false,
  source_commit: process.env.FIDUCIA_NODE_SOURCE_COMMIT ?? "unknown",
  started_at: new Date().toISOString(),
  assertions: [],
  tokens: {},
  leader_events: [],
  downstream_history: [],
  limitations: [
    "All three Raft members and the reference downstream run on one disposable GitHub Actions host.",
    "This proves automated process behavior, not physical failure-domain independence or production networking.",
    "Exact production images, TLS, NetworkPolicy, load balancer behavior, and independent review remain required.",
  ],
};

function record(name, details = {}) {
  evidence.assertions.push({ name, passed: true, ...details });
}

async function pickPorts(count) {
  const servers = [];
  const ports = [];
  for (let index = 0; index < count; index += 1) {
    const server = createNetServer();
    // eslint-disable-next-line no-await-in-loop
    await new Promise((resolvePromise, rejectPromise) => {
      server.once("error", rejectPromise);
      server.listen(0, "127.0.0.1", resolvePromise);
    });
    ports.push(server.address().port);
    servers.push(server);
  }
  await Promise.all(
    servers.map(
      (server) =>
        new Promise((resolvePromise) => server.close(resolvePromise)),
    ),
  );
  return ports;
}

function appendBounded(current, chunk) {
  return (current + String(chunk)).slice(-64 * 1024);
}

async function startNode({ index, clientPort, peerPort, peerPorts, dataDir }) {
  let logs = "";
  let spawnError;
  const env = {
    ...process.env,
    PORT: String(clientPort),
    FIDUCIA_PEER_PORT: String(peerPort),
    FIDUCIA_NODE_ID: `127.0.0.1:${clientPort}`,
    FIDUCIA_PEERS: peerPorts
      .filter((port) => port !== peerPort)
      .map((port) => `127.0.0.1:${port}`)
      .join(","),
    FIDUCIA_SHARD_COUNT: String(SHARD_COUNT),
    FIDUCIA_DATA_DIR: dataDir,
    FIDUCIA_RAFT_SNAPSHOT_THRESHOLD: "8",
    FIDUCIA_INTERNAL_SECRET: INTERNAL_SECRET,
    RUST_LOG: "fiducia_node=info",
  };
  const child = spawn(NODE_BIN, [], {
    env,
    stdio: ["ignore", "pipe", "pipe"],
    detached: process.platform !== "win32",
  });
  child.stdout.on("data", (chunk) => {
    logs = appendBounded(logs, chunk);
  });
  child.stderr.on("data", (chunk) => {
    logs = appendBounded(logs, chunk);
  });
  child.on("error", (error) => {
    spawnError = error;
  });

  const killTree = (signal) => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    try {
      if (process.platform !== "win32") process.kill(-child.pid, signal);
      else child.kill(signal);
    } catch {
      // Process already exited.
    }
  };

  const handle = {
    index,
    url: `http://127.0.0.1:${clientPort}`,
    clientPort,
    peerPort,
    dataDir,
    child,
    logs: () => logs,
    crash: () => killTree("SIGKILL"),
    stop: async () => {
      if (child.exitCode !== null || child.signalCode !== null) return;
      killTree("SIGTERM");
      const stopped = await Promise.race([
        new Promise((resolvePromise) =>
          child.once("exit", () => resolvePromise(true)),
        ),
        delay(4_000).then(() => false),
      ]);
      if (!stopped) {
        killTree("SIGKILL");
        await new Promise((resolvePromise) => child.once("exit", resolvePromise));
      }
    },
  };

  const deadline = Date.now() + STARTUP_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (spawnError) {
      throw new Error(`node ${index} failed to spawn: ${spawnError.message}`);
    }
    if (child.exitCode !== null) {
      throw new Error(
        `node ${index} exited ${child.exitCode} before health:\n${logs}`,
      );
    }
    try {
      // eslint-disable-next-line no-await-in-loop
      const response = await fetch(`${handle.url}/healthz`, {
        signal: AbortSignal.timeout(1_000),
      });
      if (response.ok) return handle;
    } catch {
      // Startup in progress.
    }
    // eslint-disable-next-line no-await-in-loop
    await delay(150);
  }
  await handle.stop();
  throw new Error(`node ${index} did not become healthy:\n${logs}`);
}

function headers() {
  return {
    "content-type": "application/json",
    "x-fiducia-internal-auth": INTERNAL_SECRET,
    "x-fiducia-org-id": ORG_ID,
  };
}

function normalizeRedirect(location, currentUrl) {
  if (!location) return null;
  try {
    return new URL(location, currentUrl).toString();
  } catch {
    const candidate = location.startsWith("//")
      ? `http:${location}`
      : `http://${location.replace(/^\/+/, "")}`;
    return new URL(candidate).toString();
  }
}

async function responseBody(response) {
  const text = await response.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text.slice(0, 2_048) };
  }
}

async function directRequest(baseUrl, path, { method = "GET", body } = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: headers(),
    body: body === undefined ? undefined : JSON.stringify(body),
    redirect: "manual",
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  return { response, body: await responseBody(response) };
}

async function clusterRequest(nodes, path, options = {}, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    for (const node of nodes) {
      if (!node || node.child.exitCode !== null || node.child.signalCode !== null) {
        continue;
      }
      try {
        // eslint-disable-next-line no-await-in-loop
        let result = await directRequest(node.url, path, options);
        if ([307, 308].includes(result.response.status)) {
          const redirected = normalizeRedirect(
            result.response.headers.get("location"),
            `${node.url}${path}`,
          );
          if (redirected) {
            const target = new URL(redirected);
            // eslint-disable-next-line no-await-in-loop
            result = await directRequest(
              `${target.protocol}//${target.host}`,
              `${target.pathname}${target.search}`,
              options,
            );
          }
        }
        if (result.response.ok) return result;
        last = new Error(
          `${options.method ?? "GET"} ${path} returned ${result.response.status}: ${JSON.stringify(result.body)}`,
        );
      } catch (error) {
        last = error;
      }
    }
    // eslint-disable-next-line no-await-in-loop
    await delay(150);
  }
  throw new Error(
    `cluster request timed out for ${options.method ?? "GET"} ${path}: ${last?.message ?? last}`,
  );
}

function visit(value, predicate) {
  if (predicate(value)) return value;
  if (Array.isArray(value)) {
    for (const child of value) {
      const found = visit(child, predicate);
      if (found !== undefined) return found;
    }
  } else if (value && typeof value === "object") {
    for (const child of Object.values(value)) {
      const found = visit(child, predicate);
      if (found !== undefined) return found;
    }
  }
  return undefined;
}

function output(body) {
  return body?.result?.output ?? body?.output ?? body?.result ?? body;
}

function booleanField(body, name) {
  const found = visit(output(body), (value) =>
    Boolean(
      value &&
        typeof value === "object" &&
        Object.hasOwn(value, name) &&
        typeof value[name] === "boolean",
    ),
  );
  return found?.[name];
}

function fencingToken(body) {
  const found = visit(output(body), (value) =>
    Boolean(
      value &&
        typeof value === "object" &&
        Number.isSafeInteger(value.fencing_token),
    ),
  );
  return found?.fencing_token;
}

function lockRecord(body) {
  return visit(output(body), (value) =>
    Boolean(
      value &&
        typeof value === "object" &&
        typeof value.holder === "string" &&
        Number.isSafeInteger(value.fencing_token),
    ),
  );
}

async function acquire(nodes, keys, holder, ttlMs) {
  const result = await clusterRequest(nodes, "/v1/locks/acquire", {
    method: "POST",
    body: { keys, holder, ttl_ms: ttlMs, wait: false },
  });
  return {
    acquired: booleanField(result.body, "acquired"),
    token: fencingToken(result.body),
    body: result.body,
  };
}

async function renew(nodes, keys, holder, token, ttlMs = ACTIVE_TTL_MS) {
  const result = await clusterRequest(nodes, "/v1/locks/renew", {
    method: "POST",
    body: {
      keys,
      holder,
      fencing_token: token,
      ttl_ms: ttlMs,
    },
  });
  return {
    renewed: booleanField(result.body, "renewed"),
    body: result.body,
  };
}

async function release(nodes, keys, holder, token) {
  const result = await clusterRequest(nodes, "/v1/locks/release", {
    method: "POST",
    body: { keys, holder, fencing_token: token },
  });
  return {
    released: booleanField(result.body, "released"),
    body: result.body,
  };
}

async function inspectLock(nodes, key) {
  const result = await clusterRequest(
    nodes,
    `/v1/locks?key=${encodeURIComponent(key)}`,
  );
  return lockRecord(result.body);
}

async function leaderIndex(nodes, key) {
  const path = `/v1/locks?key=${encodeURIComponent(key)}`;
  for (const node of nodes) {
    if (!node || node.child.exitCode !== null || node.child.signalCode !== null) {
      continue;
    }
    try {
      // eslint-disable-next-line no-await-in-loop
      const result = await directRequest(node.url, path);
      if (result.response.ok) return node.index;
    } catch {
      // Try next member.
    }
  }
  return null;
}

async function startDownstreamReference() {
  const state = { highestToken: 0, history: [] };
  const server = createHttpServer((request, response) => {
    if (request.method !== "POST" || request.url !== "/mutate") {
      response.writeHead(404).end();
      return;
    }
    const chunks = [];
    request.on("data", (chunk) => chunks.push(chunk));
    request.on("end", () => {
      try {
        const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8"));
        const token = Number(parsed.fencing_token);
        if (!Number.isSafeInteger(token) || token <= 0) {
          response.writeHead(400, { "content-type": "application/json" });
          response.end(JSON.stringify({ accepted: false, error: "invalid_token" }));
          return;
        }
        const accepted = token >= state.highestToken;
        if (accepted) state.highestToken = token;
        const event = {
          writer: String(parsed.writer ?? "unknown").slice(0, 64),
          fencing_token: token,
          accepted,
          highest_token: state.highestToken,
        };
        state.history.push(event);
        response.writeHead(accepted ? 200 : 409, {
          "content-type": "application/json",
        });
        response.end(JSON.stringify(event));
      } catch {
        response.writeHead(400, { "content-type": "application/json" });
        response.end(JSON.stringify({ accepted: false, error: "invalid_json" }));
      }
    });
  });
  await new Promise((resolvePromise, rejectPromise) => {
    server.once("error", rejectPromise);
    server.listen(0, "127.0.0.1", resolvePromise);
  });
  const url = `http://127.0.0.1:${server.address().port}`;
  return {
    state,
    mutate: async (writer, token) => {
      const response = await fetch(`${url}/mutate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ writer, fencing_token: token }),
        signal: AbortSignal.timeout(2_000),
      });
      return { status: response.status, body: await response.json() };
    },
    stop: () =>
      new Promise((resolvePromise, rejectPromise) =>
        server.close((error) =>
          error ? rejectPromise(error) : resolvePromise(),
        ),
      ),
  };
}

async function main() {
  const scratch = await mkdtemp(join(tmpdir(), "fiducia-stale-fencing-"));
  const ports = await pickPorts(6);
  const clientPorts = ports.slice(0, 3);
  const peerPorts = ports.slice(3, 6);
  const dataDirs = [0, 1, 2].map((index) => join(scratch, `node-${index}`));
  await Promise.all(dataDirs.map((path) => mkdir(path, { recursive: true })));
  /** @type {Array<Awaited<ReturnType<typeof startNode>> | null>} */
  let nodes = [null, null, null];
  const downstream = await startDownstreamReference();

  const spawnIndex = async (index) => {
    nodes[index] = await startNode({
      index,
      clientPort: clientPorts[index],
      peerPort: peerPorts[index],
      peerPorts,
      dataDir: dataDirs[index],
    });
    return nodes[index];
  };
  const stopAll = async () => {
    await Promise.all(nodes.map((node) => node?.stop()));
  };
  const restartAll = async () => {
    await stopAll();
    nodes = [null, null, null];
    for (let index = 0; index < 3; index += 1) {
      // Sequential startup keeps logs deterministic; health does not require quorum.
      // eslint-disable-next-line no-await-in-loop
      await spawnIndex(index);
    }
  };

  try {
    for (let index = 0; index < 3; index += 1) {
      // eslint-disable-next-line no-await-in-loop
      await spawnIndex(index);
    }

    const key = "reference/orders-writer";
    const keys = [key];
    const first = await acquire(nodes, keys, "holder-a", FIRST_TTL_MS);
    assert.equal(first.acquired, true, JSON.stringify(first.body));
    assert.ok(Number.isSafeInteger(first.token) && first.token > 0);
    evidence.tokens.holder_a = first.token;
    record("initial holder acquired a positive fencing token", {
      fencing_token: first.token,
    });

    const firstWrite = await downstream.mutate("holder-a", first.token);
    assert.equal(firstWrite.status, 200);
    assert.equal(firstWrite.body.accepted, true);
    record("initial fencing token accepted by downstream reference");

    await delay(FIRST_TTL_MS + 900);
    const second = await acquire(nodes, keys, "holder-b", ACTIVE_TTL_MS);
    assert.equal(second.acquired, true, JSON.stringify(second.body));
    assert.ok(Number.isSafeInteger(second.token));
    assert.ok(second.token > first.token);
    evidence.tokens.holder_b = second.token;
    record("expired holder was superseded by a strictly newer token", {
      previous: first.token,
      current: second.token,
    });

    const secondWrite = await downstream.mutate("holder-b", second.token);
    assert.equal(secondWrite.status, 200);
    const staleFirstWrite = await downstream.mutate("holder-a-stale", first.token);
    assert.equal(staleFirstWrite.status, 409);
    assert.equal(staleFirstWrite.body.accepted, false);
    record("downstream rejected the paused old holder after transfer");

    const oldRenew = await renew(nodes, keys, "holder-a", first.token);
    assert.notEqual(oldRenew.renewed, true, JSON.stringify(oldRenew.body));
    const oldRelease = await release(nodes, keys, "holder-a", first.token);
    assert.notEqual(oldRelease.released, true, JSON.stringify(oldRelease.body));
    const active = await inspectLock(nodes, key);
    assert.equal(active?.holder, "holder-b");
    assert.equal(active?.fencing_token, second.token);
    record("old token could neither renew nor release the new holder");

    let currentLeader = null;
    const leaderDeadline = Date.now() + 15_000;
    while (Date.now() < leaderDeadline && currentLeader === null) {
      // eslint-disable-next-line no-await-in-loop
      currentLeader = await leaderIndex(nodes, key);
      if (currentLeader === null) {
        // eslint-disable-next-line no-await-in-loop
        await delay(150);
      }
    }
    assert.notEqual(currentLeader, null, "unable to identify current lock leader");
    evidence.leader_events.push({ event: "crash", node_index: currentLeader });
    nodes[currentLeader].crash();
    await delay(500);

    const afterLeaderCrash = await inspectLock(nodes, key);
    assert.equal(afterLeaderCrash?.holder, "holder-b");
    assert.equal(afterLeaderCrash?.fencing_token, second.token);
    const renewAfterCrash = await renew(
      nodes,
      keys,
      "holder-b",
      second.token,
      ACTIVE_TTL_MS,
    );
    assert.equal(renewAfterCrash.renewed, true, JSON.stringify(renewAfterCrash.body));
    record("new quorum preserved holder and fencing token after leader crash", {
      crashed_node_index: currentLeader,
    });

    nodes[currentLeader] = await startNode({
      index: currentLeader,
      clientPort: clientPorts[currentLeader],
      peerPort: peerPorts[currentLeader],
      peerPorts,
      dataDir: dataDirs[currentLeader],
    });
    evidence.leader_events.push({ event: "restart", node_index: currentLeader });
    const afterMemberRestart = await inspectLock(nodes, key);
    assert.equal(afterMemberRestart?.holder, "holder-b");
    assert.equal(afterMemberRestart?.fencing_token, second.token);
    record("restarted member caught up without token regression");

    const releaseSecond = await release(nodes, keys, "holder-b", second.token);
    assert.equal(releaseSecond.released, true, JSON.stringify(releaseSecond.body));
    const third = await acquire(nodes, keys, "holder-c", ACTIVE_TTL_MS);
    assert.equal(third.acquired, true, JSON.stringify(third.body));
    assert.ok(third.token > second.token);
    evidence.tokens.holder_c = third.token;
    assert.equal((await downstream.mutate("holder-c", third.token)).status, 200);
    assert.equal((await downstream.mutate("holder-b-stale", second.token)).status, 409);
    record("post-failover holder received a newer downstream-enforced token", {
      previous: second.token,
      current: third.token,
    });

    await restartAll();
    const afterFullRestart = await inspectLock(nodes, key);
    assert.equal(afterFullRestart?.holder, "holder-c");
    assert.equal(afterFullRestart?.fencing_token, third.token);
    const renewAfterFullRestart = await renew(
      nodes,
      keys,
      "holder-c",
      third.token,
      ACTIVE_TTL_MS,
    );
    assert.equal(
      renewAfterFullRestart.renewed,
      true,
      JSON.stringify(renewAfterFullRestart.body),
    );
    record("full cluster restart preserved committed holder and token");

    assert.equal((await release(nodes, keys, "holder-c", third.token)).released, true);
    const fourth = await acquire(nodes, keys, "holder-d", ACTIVE_TTL_MS);
    assert.equal(fourth.acquired, true, JSON.stringify(fourth.body));
    assert.ok(fourth.token > third.token);
    evidence.tokens.holder_d = fourth.token;
    assert.equal((await downstream.mutate("holder-d", fourth.token)).status, 200);
    assert.equal((await downstream.mutate("holder-c-stale", third.token)).status, 409);
    record("token monotonicity survived complete cluster restart", {
      previous: third.token,
      current: fourth.token,
    });

    const unionKeys = ["union/a", "union/b"];
    const union = await acquire(nodes, unionKeys, "holder-union", ACTIVE_TTL_MS);
    assert.equal(union.acquired, true, JSON.stringify(union.body));
    const conflict = await acquire(
      nodes,
      ["union/b"],
      "holder-conflict",
      ACTIVE_TTL_MS,
    );
    assert.equal(conflict.acquired, false, JSON.stringify(conflict.body));
    assert.equal(
      (await release(nodes, unionKeys, "holder-union", union.token)).released,
      true,
    );
    const afterUnionRelease = await acquire(
      nodes,
      ["union/b"],
      "holder-conflict",
      ACTIVE_TTL_MS,
    );
    assert.equal(afterUnionRelease.acquired, true, JSON.stringify(afterUnionRelease.body));
    assert.ok(afterUnionRelease.token > union.token);
    record("multi-key union remained atomic and advanced fencing on transfer");

    evidence.downstream_history = downstream.state.history;
    evidence.completed_at = new Date().toISOString();
    evidence.passed = true;
    await mkdir(dirname(EVIDENCE_PATH), { recursive: true });
    await writeFile(EVIDENCE_PATH, `${JSON.stringify(evidence, null, 2)}\n`, {
      mode: 0o600,
    });
    process.stdout.write(
      `stale-fencing test passed; evidence=${EVIDENCE_PATH}; tokens=${JSON.stringify(evidence.tokens)}\n`,
    );
  } catch (error) {
    evidence.completed_at = new Date().toISOString();
    evidence.passed = false;
    evidence.failure = String(error?.stack ?? error).slice(0, 8_192);
    evidence.node_logs = nodes.map((node) => node?.logs()?.slice(-8_192) ?? null);
    await mkdir(dirname(EVIDENCE_PATH), { recursive: true });
    await writeFile(EVIDENCE_PATH, `${JSON.stringify(evidence, null, 2)}\n`, {
      mode: 0o600,
    });
    throw error;
  } finally {
    evidence.downstream_history = downstream.state.history;
    await Promise.allSettled(nodes.map((node) => node?.stop()));
    await downstream.stop().catch(() => {});
    await rm(scratch, { recursive: true, force: true });
  }
}

main().catch((error) => {
  process.stderr.write(`${error?.stack ?? error}\n`);
  process.exitCode = 1;
});
