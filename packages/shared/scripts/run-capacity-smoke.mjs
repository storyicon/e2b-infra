import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";

import { Sandbox } from "e2b";

import {
  assertNoAWSCredentialEnvironment,
  buildBenchmarkSummary,
  createConcurrencyGate,
  parseBenchmarkIdentity,
  parseObserverJSONL,
  validateApiTransportCapacity,
  validateBenchmarkConfig,
  waitUntilEpochMs,
} from "./capacity-smoke-model.mjs";

const target = Number(process.env.TARGET_SANDBOXES ?? "500");
const concurrency = Number(process.env.CREATE_CONCURRENCY ?? String(target));
const perNode = Number(process.env.SANDBOXES_PER_NODE ?? "20");
const requestTimeoutMs = Number(process.env.REQUEST_TIMEOUT_MS ?? "600000");
const template = process.env.E2B_TEMPLATE_ALIAS;
const apiKey = process.env.E2B_API_KEY;
const apiUrl = process.env.E2B_API_URL ?? "http://127.0.0.1:3000";
const apiConnectionLimit = Number(process.env.E2B_API_CONNECTIONS ?? "100");
const apiInflightLimit = Number(process.env.E2B_API_INFLIGHT_REQUESTS ?? "1000");
const sandboxUrl = process.env.E2B_SANDBOX_URL ?? "http://127.0.0.1:3002";
const resultRoot = process.env.RESULT_ROOT ?? "./capacity-smoke-results";
// A separate read-only observer owns cloud and scheduler access. Each JSONL
// record must carry this runner's runId and elapsedMs plus the latest
// serverAdmitted/controller/asgDesired/ec2InService/nomadReady observations.
// serverAdmitted is the run-scoped API intent-persisted delta, never a raw
// process-global counter or the number of client promises.
// The legacy nodes.jsonl desired/asg[]/nomad[] fields remain accepted.
const observerEventsPath = process.env.CAPACITY_OBSERVER_EVENTS_PATH;
const benchmarkIdentity = parseBenchmarkIdentity({
  runId: process.env.BENCHMARK_RUN_ID,
  startEpochMs: process.env.BENCHMARK_START_EPOCH_MS,
});

const contract = validateBenchmarkConfig({ target, concurrency, perNode, templateAlias: template });
const transport = validateApiTransportCapacity({
  apiUrl,
  concurrency,
  connectionLimit: apiConnectionLimit,
  inflightLimit: apiInflightLimit,
});
assertNoAWSCredentialEnvironment(process.env);
if (!apiKey) throw new Error("E2B_API_KEY is required");
if (!observerEventsPath) throw new Error("CAPACITY_OBSERVER_EVENTS_PATH is required");
if (!Number.isSafeInteger(requestTimeoutMs) || requestTimeoutMs <= 0) {
  throw new Error("REQUEST_TIMEOUT_MS must be a positive integer");
}

const { runId, startEpochMs: benchmarkStartEpochMs } = benchmarkIdentity;
// Require the observer output to exist before creating any paid workload. It
// may be empty until the observer sees run_started and begins sampling.
const preexistingObserverEvents = parseObserverJSONL(readFileSync(observerEventsPath, "utf8"));
if (preexistingObserverEvents.length > 0) {
  throw new Error("capacity observer events file must be empty before the benchmark starts");
}
const resultDir = `${resultRoot}/${runId}`;
const eventsPath = `${resultDir}/events.jsonl`;
const summaryPath = `${resultDir}/summary.json`;
mkdirSync(resultDir, { recursive: true, mode: 0o700 });

let t0 = performance.now();
let activeCreates = 0;
let maximumClientConcurrency = 0;
const created = new Map();
const results = [];
const clientEvents = [];

function elapsedMs() {
  return Number((performance.now() - t0).toFixed(3));
}

function event(value) {
  const row = { at: new Date().toISOString(), elapsedMs: elapsedMs(), ...value };
  clientEvents.push(row);
  appendFileSync(eventsPath, `${JSON.stringify(row)}\n`, {
    mode: 0o600,
  });
}

function percentile(sorted, quantile) {
  if (sorted.length === 0) return null;
  const index = Math.min(sorted.length - 1, Math.ceil(sorted.length * quantile) - 1);
  return Number(sorted[index].toFixed(3));
}

async function assertMetadataBlocked() {
  try {
    const response = await fetch("http://169.254.169.254/latest/meta-data/", {
      redirect: "manual",
      signal: AbortSignal.timeout(250),
    });
    await response.body?.cancel();
    throw new Error(`instance metadata endpoint is reachable with status ${response.status}`);
  } catch (error) {
    if (String(error).includes("instance metadata endpoint is reachable")) throw error;
  }
}

const connection = {
  apiKey,
  apiUrl,
  sandboxUrl,
  requestTimeoutMs,
};

const withCreatePermit = createConcurrencyGate(concurrency, (active) => {
  activeCreates = active;
  maximumClientConcurrency = Math.max(maximumClientConcurrency, active);
});

async function createOne(index) {
  const clientQueuedAt = elapsedMs();
  event({ type: "client_queued", index, clientQueuedAt });

  return withCreatePermit(async () => {
    const requestStartedAt = elapsedMs();
    event({ type: "request_started", index, activeCreates, requestStartedAt });

    let sandbox;
    try {
      sandbox = await Sandbox.create(template, {
        ...connection,
        timeoutMs: requestTimeoutMs,
        metadata: { benchmarkRunId: runId, benchmarkIndex: String(index) },
      });
      created.set(sandbox.sandboxId, sandbox);
      const createResponseAt = elapsedMs();
      event({
        type: "create_response",
        index,
        sandboxId: sandbox.sandboxId,
        createResponseAt,
        requestMs: Number((createResponseAt - requestStartedAt).toFixed(3)),
      });

      const nonce = `${runId}-${index}`;
      const command = await sandbox.commands.run(`printf '%s\\n' '${nonce}'`, {
        user: "root",
        timeoutMs: 60_000,
      });
      if (command.exitCode !== 0 || command.stdout.trim() !== nonce) {
        throw new Error(`guest readiness failed with exit code ${command.exitCode}`);
      }

      const guestReadyAt = elapsedMs();
      const result = {
        index,
        sandboxId: sandbox.sandboxId,
        clientQueuedAt,
        requestStartedAt,
        createResponseAt,
        guestReadyAt,
        readyMs: guestReadyAt,
      };
      results.push(result);
      event({ type: "guest_ready", ...result });

      return result;
    } catch (error) {
      event({ type: "create_or_readiness_error", index, message: String(error).slice(0, 500) });
      throw error;
    }
  });
}

async function cleanup() {
  const gate = createConcurrencyGate(50);
  const outcomes = await Promise.allSettled(
    [...created.values()].map((sandbox) => gate(async () => {
      await sandbox.kill();
      event({ type: "sandbox_killed", sandboxId: sandbox.sandboxId });
    })),
  );
  const failed = outcomes.filter((outcome) => outcome.status === "rejected");
  if (failed.length > 0) throw new Error(`cleanup failed for ${failed.length} sandboxes`);
}

let runError;
try {
  await assertMetadataBlocked();
  const delayUntilStartMs = benchmarkStartEpochMs - Date.now();
  if (delayUntilStartMs <= 0) {
    throw new Error("BENCHMARK_START_EPOCH_MS must still be in the future after runner preflight");
  }
  t0 = performance.now() + delayUntilStartMs;
  await waitUntilEpochMs(benchmarkStartEpochMs);
  event({
    type: "run_started",
    runId,
    startEpochMs: benchmarkStartEpochMs,
    target,
    concurrency,
    perNode,
    expectedNodes: contract.expectedNodes,
    apiConnectionLimit: transport.connectionLimit,
    apiInflightLimit: transport.inflightLimit,
  });

  const outcomes = await Promise.allSettled(
    Array.from({ length: target }, (_, index) => createOne(index + 1)),
  );
  const failures = outcomes.filter((outcome) => outcome.status === "rejected");
  const ready = results.map((result) => result.readyMs).sort((left, right) => left - right);
  const summary = buildBenchmarkSummary({
    runId,
    target,
    concurrency,
    maximumClientConcurrency,
    expectedNodes: contract.expectedNodes,
    completed: results.length,
    failed: failures.length,
    clientEvents,
    observerEvents: parseObserverJSONL(readFileSync(observerEventsPath, "utf8")),
    readyMs: {
      p50: percentile(ready, 0.50),
      p90: percentile(ready, 0.90),
      p95: percentile(ready, 0.95),
      p99: percentile(ready, 0.99),
      max: ready.at(-1) ?? null,
    },
  });
  writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, { mode: 0o600 });
  event({ type: "run_completed", completed: results.length, failed: failures.length });
  if (!summary.observerEvidenceComplete) {
    throw new Error(`observer evidence is incomplete: ${summary.missingObserverEvidence.join(", ")}`);
  }
  if (!summary.allDemandObservedByServer) {
    throw new Error(`server admitted ${summary.serverAdmitted ?? "unknown"} of ${target} logical requests`);
  }
  if (failures.length > 0) throw new Error(`${failures.length} sandbox starts failed`);
} catch (error) {
  runError = error;
  event({ type: "run_error", message: String(error).slice(0, 500) });
} finally {
  try {
    await cleanup();
  } catch (error) {
    runError ??= error;
    event({ type: "cleanup_error", message: String(error).slice(0, 500) });
  }
}

if (runError) throw runError;
