import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  admissionReport,
  assertNoAWSCredentialEnvironment,
  buildBenchmarkSummary,
  createConcurrencyGate,
  parseObserverJSONL,
  parseBenchmarkIdentity,
  summarizeClientMilestones,
  summarizeObserverEvidence,
  validateApiTransportCapacity,
  validateBenchmarkConfig,
  waitUntilEpochMs,
} from "./capacity-smoke-model.mjs";

test("a concurrency-100 gate admits only 100 of 500 queued tasks", async () => {
  let releaseAll;
  const blocker = new Promise((resolve) => {
    releaseAll = resolve;
  });
  let started = 0;
  let maximumActive = 0;
  const gate = createConcurrencyGate(100, (active) => {
    maximumActive = Math.max(maximumActive, active);
  });

  const tasks = Array.from({ length: 500 }, () => gate(async () => {
    started += 1;
    await blocker;
  }));

  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(started, 100);
  assert.equal(maximumActive, 100);

  releaseAll();
  await Promise.all(tasks);
});

test("the report never equates client promises with server-admitted intents", () => {
  assert.deepEqual(admissionReport({
    logicalTarget: 500,
    requestsStarted: 500,
    serverAdmitted: 100,
    maximumClientConcurrency: 500,
  }), {
    logicalTarget: 500,
    requestsStarted: 500,
    serverAdmitted: 100,
    maximumClientConcurrency: 500,
    allDemandObservedByServer: false,
  });
});

test("the formal 1-to-500 contract requires real concurrency 500", () => {
  assert.throws(
    () => validateBenchmarkConfig({ target: 500, concurrency: 100, perNode: 20, templateAlias: "benchmark-template" }),
    /concurrency must equal target/,
  );
  assert.deepEqual(validateBenchmarkConfig({
    target: 500,
    concurrency: 500,
    perNode: 20,
    templateAlias: "benchmark-template",
  }), {
    target: 500,
    concurrency: 500,
    perNode: 20,
    expectedNodes: 25,
    templateAlias: "benchmark-template",
  });
});

test("the formal benchmark requires an explicit template alias", () => {
  assert.throws(
    () => validateBenchmarkConfig({ target: 500, concurrency: 500, perNode: 20 }),
    /E2B_TEMPLATE_ALIAS is required/,
  );
  assert.throws(
    () => validateBenchmarkConfig({ target: 500, concurrency: 500, perNode: 20, templateAlias: "   " }),
    /E2B_TEMPLATE_ALIAS is required/,
  );
});

test("the formal benchmark rejects SDK transport limits below client concurrency", () => {
  assert.throws(
    () => validateApiTransportCapacity({
      apiUrl: "http://127.0.0.1:3000",
      concurrency: 500,
      connectionLimit: 100,
      inflightLimit: 1000,
    }),
    /E2B_API_CONNECTIONS.*500/,
  );
  assert.throws(
    () => validateApiTransportCapacity({
      apiUrl: "http://127.0.0.1:3000",
      concurrency: 500,
      connectionLimit: 500,
      inflightLimit: 100,
    }),
    /E2B_API_INFLIGHT_REQUESTS.*500/,
  );
  assert.deepEqual(validateApiTransportCapacity({
    apiUrl: "http://127.0.0.1:3000",
    concurrency: 500,
    connectionLimit: 500,
    inflightLimit: 500,
  }), {
    connectionLimit: 500,
    inflightLimit: 500,
  });
});

test("benchmark identity and future T0 are explicit and strictly validated", async () => {
  assert.deepEqual(parseBenchmarkIdentity({
    runId: "capacity-smoke-2026-08-26T10-00-00Z",
    startEpochMs: "1787738405000",
  }), {
    runId: "capacity-smoke-2026-08-26T10-00-00Z",
    startEpochMs: 1787738405000,
  });
  assert.throws(() => parseBenchmarkIdentity({ runId: "", startEpochMs: "1787738405000" }), /RUN_ID/);
  assert.throws(
    () => parseBenchmarkIdentity({ runId: "run/other", startEpochMs: "1787738405000" }),
    /RUN_ID/,
  );
  assert.throws(
    () => parseBenchmarkIdentity({ runId: "run-1", startEpochMs: "1787738405" }),
    /EPOCH_MS/,
  );

  const delays = [];
  const times = [1787738400000, 1787738405000];
  await waitUntilEpochMs(1787738405000, {
    nowEpochMs: () => times.shift(),
    sleep: async (delayMs) => delays.push(delayMs),
  });
  assert.deepEqual(delays, [5000]);
  await assert.rejects(
    () => waitUntilEpochMs(1787738405000, { nowEpochMs: () => 1787738405000 }),
    /future/,
  );
});

test("trusted observer evidence produces distinct infrastructure timelines", () => {
  const report = summarizeObserverEvidence({
    runId: "run-1",
    logicalTarget: 500,
    events: [
      {
        runId: "run-1",
        elapsedMs: 10,
        serverAdmitted: 100,
        controller: {
          mode: "start-intent-v1",
          workloadCount: 100,
          currentDesired: 1,
          targetNodes: 5,
          readyNodes: 1,
          capped: false,
          outcome: "success",
        },
        asgDesired: 5,
        ec2InService: 1,
        nomadReady: 1,
      },
      {
        runId: "run-1",
        elapsedMs: 20,
        serverAdmitted: 500,
        controller: {
          mode: "start-intent-v1",
          workloadCount: 500,
          currentDesired: 5,
          targetNodes: 25,
          readyNodes: 1,
          capped: false,
          outcome: "success",
        },
        asgDesired: 25,
        ec2InService: 1,
        nomadReady: 1,
      },
      {
        runId: "run-1",
        elapsedMs: 40,
        serverAdmitted: 500,
        asgDesired: 25,
        ec2InService: 25,
        nomadReady: 8,
      },
      {
        runId: "run-1",
        elapsedMs: 60,
        serverAdmitted: 500,
        asgDesired: 25,
        ec2InService: 25,
        nomadReady: 25,
      },
    ],
  });

  assert.equal(report.serverAdmitted, 500);
  assert.equal(report.allDemandObservedByServer, true);
  assert.equal(report.serverAdmittedAt, 20);
  assert.equal(report.firstScaleDecisionAt, 10);
  assert.equal(report.firstScaleDecision.targetNodes, 5);
  assert.deepEqual(report.asgDesiredTimeline, [
    { elapsedMs: 10, value: 5 },
    { elapsedMs: 20, value: 25 },
  ]);
  assert.deepEqual(report.ec2InServiceTimeline, [
    { elapsedMs: 10, value: 1 },
    { elapsedMs: 40, value: 25 },
  ]);
  assert.deepEqual(report.nomadReadyTimeline, [
    { elapsedMs: 10, value: 1 },
    { elapsedMs: 40, value: 8 },
    { elapsedMs: 60, value: 25 },
  ]);
  assert.equal(report.complete, true);
  assert.deepEqual(report.missingEvidence, []);
});

test("observer evidence reuses the existing nodes.jsonl snapshot shape", () => {
  const report = summarizeObserverEvidence({
    runId: "run-1",
    logicalTarget: 500,
    events: [{
      runId: "run-1",
      elapsedMs: 5,
      serverAdmitted: 100,
      desired: 6,
      asg: [
        { lifecycle: "InService" },
        { lifecycle: "Pending" },
      ],
      nomad: [
        { status: "ready", drain: false },
        { status: "down", drain: false },
      ],
    }],
  });

  assert.equal(report.serverAdmitted, 100);
  assert.equal(report.allDemandObservedByServer, false);
  assert.equal(report.serverAdmittedAt, null);
  assert.deepEqual(report.asgDesiredTimeline, [{ elapsedMs: 5, value: 6 }]);
  assert.deepEqual(report.ec2InServiceTimeline, [{ elapsedMs: 5, value: 1 }]);
  assert.deepEqual(report.nomadReadyTimeline, [{ elapsedMs: 5, value: 1 }]);
  assert.deepEqual(report.missingEvidence, ["firstScaleDecision"]);
});

test("observer evidence rejects mixed runs and regressing admission counts", () => {
  assert.throws(
    () => summarizeObserverEvidence({
      runId: "run-1",
      logicalTarget: 500,
      events: [{ runId: "run-2", elapsedMs: 1, serverAdmitted: 1 }],
    }),
    /runId/,
  );
  assert.throws(
    () => summarizeObserverEvidence({
      runId: "run-1",
      logicalTarget: 500,
      events: [
        { runId: "run-1", elapsedMs: 1, serverAdmitted: 100 },
        { runId: "run-1", elapsedMs: 2, serverAdmitted: 99 },
      ],
    }),
    /regressed/,
  );
});

test("observer JSONL parsing rejects malformed or oversized evidence", () => {
  assert.deepEqual(parseObserverJSONL(
    '{"runId":"run-1","elapsedMs":1}\n\n{"runId":"run-1","elapsedMs":2}\n',
  ), [
    { runId: "run-1", elapsedMs: 1 },
    { runId: "run-1", elapsedMs: 2 },
  ]);
  assert.throws(() => parseObserverJSONL("{not-json}\n"), /line 1/);
  assert.throws(() => parseObserverJSONL(`${"x".repeat(1024 * 1024 + 1)}\n`), /too large/);
});

test("client milestones preserve queue, request, response, and guest-ready boundaries", () => {
  assert.deepEqual(summarizeClientMilestones([
    { type: "client_queued", elapsedMs: 1 },
    { type: "client_queued", elapsedMs: 2 },
    { type: "request_started", elapsedMs: 3 },
    { type: "create_response", elapsedMs: 8 },
    { type: "guest_ready", elapsedMs: 10 },
  ]), {
    clientQueued: { count: 2, firstAt: 1, lastAt: 2 },
    requestStarted: { count: 1, firstAt: 3, lastAt: 3 },
    createResponse: { count: 1, firstAt: 8, lastAt: 8 },
    guestReady: { count: 1, firstAt: 10, lastAt: 10 },
  });
});

test("the final benchmark summary keeps client and server admission counts separate", () => {
  const clientEvents = Array.from({ length: 500 }, (_, index) => ({
    type: "client_queued",
    elapsedMs: index,
  }));
  clientEvents.push(...Array.from({ length: 100 }, (_, index) => ({
    type: "request_started",
    elapsedMs: 500 + index,
  })));

  const summary = buildBenchmarkSummary({
    runId: "run-1",
    target: 500,
    concurrency: 100,
    maximumClientConcurrency: 100,
    expectedNodes: 25,
    completed: 0,
    failed: 0,
    clientEvents,
    observerEvents: [{
      runId: "run-1",
      elapsedMs: 600,
      serverAdmitted: 100,
      controller: { currentDesired: 1, targetNodes: 5 },
      asgDesired: 5,
      ec2InService: 1,
      nomadReady: 1,
    }],
    readyMs: { p50: null, p90: null, p95: null, p99: null, max: null },
  });

  assert.equal(summary.target, 500);
  assert.equal(summary.maximumClientConcurrency, 100);
  assert.equal(summary.requestsStarted, 100);
  assert.equal(summary.serverAdmitted, 100);
  assert.equal(summary.allDemandObservedByServer, false);
  assert.equal(summary.milestones.clientQueued.count, 500);
  assert.equal(summary.milestones.requestStarted.count, 100);
  assert.equal(summary.milestones.serverAdmitted.count, 100);
});

test("the final benchmark summary preserves missing admission as unknown", () => {
  const summary = buildBenchmarkSummary({
    runId: "run-1",
    target: 500,
    concurrency: 500,
    maximumClientConcurrency: 0,
    expectedNodes: 25,
    completed: 0,
    failed: 500,
    clientEvents: [],
    observerEvents: [],
    readyMs: { p50: null, p90: null, p95: null, p99: null, max: null },
  });

  assert.equal(summary.serverAdmitted, null);
  assert.equal(summary.allDemandObservedByServer, false);
  assert(summary.missingObserverEvidence.includes("serverAdmitted"));
});

test("the runner refuses inherited AWS credential providers", () => {
  assert.doesNotThrow(() => assertNoAWSCredentialEnvironment({}));
  for (const name of [
    "AWS_ACCESS_KEY_ID",
    "AWS_PROFILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
  ]) {
    assert.throws(
      () => assertNoAWSCredentialEnvironment({ [name]: "configured" }),
      new RegExp(name),
    );
  }
});

test("the benchmark runner cannot scale infrastructure or retry create", async () => {
  const source = await readFile(new URL("./run-capacity-smoke.mjs", import.meta.url), "utf8");
  for (const forbidden of [
    "node:child_process",
    "set-desired-capacity",
    "autoscaling",
    "clientAutoscalingCalls",
    "createRetry",
  ]) {
    assert.equal(source.includes(forbidden), false, `runner contains forbidden capability: ${forbidden}`);
  }
  assert.match(source, /Sandbox\.create/);
  assert.match(source, /CAPACITY_OBSERVER_EVENTS_PATH/);
  assert.match(source, /BENCHMARK_RUN_ID/);
  assert.match(source, /BENCHMARK_START_EPOCH_MS/);
  assert.doesNotMatch(source, /`capacity-smoke-\$\{new Date/);
  assert(
    source.indexOf("preexistingObserverEvents") < source.indexOf("await waitUntilEpochMs"),
    "runner must check the observer output before waiting for T0",
  );
  assert(
    source.indexOf("await waitUntilEpochMs") < source.indexOf('type: "run_started"'),
    "runner must wait for T0 before starting workload",
  );
  assert.doesNotMatch(source, /while\s*\([^)]*deadline/);
});
