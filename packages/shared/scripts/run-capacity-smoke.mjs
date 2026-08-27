import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";

import { Sandbox } from "e2b";

import {
  assertNoAWSCredentialEnvironment,
  buildBenchmarkSummary,
  collectBenchmarkSummary,
  createConcurrencyGate,
  parseBenchmarkIdentity,
  parseObserverJSONL,
  parseObserverJSONLSnapshot,
  validateApiTransportCapacity,
  validateBenchmarkConfig,
  waitUntilEpochMs,
  waitForColdObserverPreflight,
} from "./capacity-smoke-model.mjs";

const target = Number(process.env.TARGET_SANDBOXES ?? "500");
const concurrency = Number(process.env.CREATE_CONCURRENCY ?? String(target));
const perNode = Number(process.env.SANDBOXES_PER_NODE ?? "20");
const requestTimeoutMs = Number(process.env.REQUEST_TIMEOUT_MS ?? "600000");
const observerSettleTimeoutMs = Number(process.env.CAPACITY_OBSERVER_SETTLE_TIMEOUT_MS ?? "30000");
const observerBaselineTimeoutMs = Number(process.env.CAPACITY_OBSERVER_BASELINE_TIMEOUT_MS ?? "30000");
const batchMaxDurationMs = Number(process.env.CAPACITY_BATCH_MAX_DURATION_MS ?? "10000");
const reconcileLagBudgetMs = Number(process.env.CAPACITY_RECONCILE_LAG_BUDGET_MS ?? "2000");
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
// Any record with serverAdmitted must also carry benchmarkRunHash, the SHA-256
// of runId, and count only API admission markers with that same hash. It is
// never a raw process-global counter or the number of client promises.
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
if (!Number.isSafeInteger(observerSettleTimeoutMs) || observerSettleTimeoutMs <= 0) {
  throw new Error("CAPACITY_OBSERVER_SETTLE_TIMEOUT_MS must be a positive integer");
}

const { runId, startEpochMs: benchmarkStartEpochMs } = benchmarkIdentity;
// The observer owns an initially empty file. The configured future timestamp
// opens its evidence window; the workload clock starts only after the observer
// publishes a trusted cold baseline.
const preexistingObserverEvents = parseObserverJSONL(readFileSync(observerEventsPath, "utf8"));
if (preexistingObserverEvents.length > 0) {
  throw new Error("capacity observer events file must be empty before T0");
}
const resultDir = `${resultRoot}/${runId}`;
const eventsPath = `${resultDir}/events.jsonl`;
const summaryPath = `${resultDir}/summary.json`;
if (existsSync(resultDir)) throw new Error("benchmark result directory already exists; use a new run ID");

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
  const runSandboxIDs = new Set(created.keys());
  let discoveryError;
  try {
    for (const sandboxID of await listRunSandboxIDs()) runSandboxIDs.add(sandboxID);
  } catch (error) {
    discoveryError = error;
  }

  const gate = createConcurrencyGate(50);
  const outcomes = await Promise.allSettled(
    [...runSandboxIDs].map((sandboxID) => gate(async () => {
      const sandbox = created.get(sandboxID) ?? await Sandbox.connect(sandboxID, connection);
      await sandbox.kill();
      event({ type: "sandbox_killed", sandboxId: sandbox.sandboxId });
    })),
  );
  const failed = outcomes.filter((outcome) => outcome.status === "rejected");
  if (discoveryError !== undefined || failed.length > 0) {
    const issues = [];
    if (discoveryError !== undefined) issues.push(`run discovery failed: ${String(discoveryError)}`);
    if (failed.length > 0) issues.push(`kill failed for ${failed.length} sandboxes`);
    throw new Error(`cleanup incomplete: ${issues.join("; ")}`);
  }
}

async function listRunSandboxIDs() {
  const paginator = Sandbox.list({
    ...connection,
    query: { metadata: { benchmarkRunId: runId } },
    limit: 100,
  });
  const runSandboxIDs = [];
  while (paginator.hasNext) {
    for (const info of await paginator.nextItems()) runSandboxIDs.push(info.sandboxId);
  }

  return runSandboxIDs;
}

const preexistingRunSandboxIDs = await listRunSandboxIDs();
if (preexistingRunSandboxIDs.length > 0) {
  throw new Error(`benchmark run ID is already used by ${preexistingRunSandboxIDs.length} sandboxes`);
}
mkdirSync(resultRoot, { recursive: true, mode: 0o700 });
mkdirSync(resultDir, { mode: 0o700 });

let runError;
try {
  await assertMetadataBlocked();
  const delayUntilStartMs = benchmarkStartEpochMs - Date.now();
  if (delayUntilStartMs <= 0) {
    throw new Error("BENCHMARK_START_EPOCH_MS must still be in the future after runner preflight");
  }
  await waitUntilEpochMs(benchmarkStartEpochMs);
  const coldBaseline = await waitForColdObserverPreflight({
    runId,
    readEvents: () => parseObserverJSONLSnapshot(readFileSync(observerEventsPath, "utf8")),
    timeoutMs: observerBaselineTimeoutMs,
  });
  const workloadStartEpochMs = Date.now();
  t0 = performance.now();
  const observerWorkloadStartOffsetMs = workloadStartEpochMs - benchmarkStartEpochMs;
  event({
    type: "run_started",
    runId,
    observerWindowStartEpochMs: benchmarkStartEpochMs,
    workloadStartEpochMs,
    coldBaselineElapsedMs: coldBaseline.elapsedMs,
    target,
    concurrency,
    perNode,
    batchMaxDurationMs,
    reconcileLagBudgetMs,
    expectedNodes: contract.expectedNodes,
    apiConnectionLimit: transport.connectionLimit,
    apiInflightLimit: transport.inflightLimit,
  });

  const outcomes = await Promise.allSettled(
    Array.from({ length: target }, (_, index) => createOne(index + 1)),
  );
  const failures = outcomes.filter((outcome) => outcome.status === "rejected");
  const ready = results.map((result) => result.readyMs).sort((left, right) => left - right);
  const buildSummary = (observerEvents) => buildBenchmarkSummary({
    runId,
    target,
    concurrency,
    maximumClientConcurrency,
    expectedNodes: contract.expectedNodes,
    perNode,
    completed: results.length,
    failed: failures.length,
    clientEvents,
    observerEvents,
    observerWorkloadStartOffsetMs,
    batchMaxDurationMs,
    reconcileLagBudgetMs,
    readyMs: {
      p50: percentile(ready, 0.50),
      p90: percentile(ready, 0.90),
      p95: percentile(ready, 0.95),
      p99: percentile(ready, 0.99),
      max: ready.at(-1) ?? null,
    },
  });
  const summary = await collectBenchmarkSummary({
    failureCount: failures.length,
    buildSummary,
    readObserverEvents: () => parseObserverJSONLSnapshot(readFileSync(observerEventsPath, "utf8")),
    observerSettleTimeoutMs,
  });
  writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, { mode: 0o600 });
  event({ type: "run_completed", completed: results.length, failed: failures.length });
  if (failures.length > 0) throw new Error(`${failures.length} sandbox starts failed`);
  if (!summary.observerEvidenceComplete) {
    throw new Error(`observer evidence is incomplete: ${summary.missingObserverEvidence.join(", ")}`);
  }
  if (!summary.coldBaselineObserved) {
    throw new Error("observer did not prove the cold baseline: desired=1, InService=1, Nomad ready=1, running=0");
  }
  if (!summary.allDemandObservedByServer) {
    throw new Error(`server admitted ${summary.serverAdmitted ?? "unknown"} of ${target} logical requests`);
  }
  if (!summary.targetCapacityObserved) {
    throw new Error(`observer did not prove the controller and ASG reached ${contract.expectedNodes} nodes`);
  }
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
