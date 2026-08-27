import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  admissionReport,
  assertColdObserverPreflight,
  assertNoAWSCredentialEnvironment,
  benchmarkRunHash,
  benchmarkObserverEvidenceAccepted,
  buildBenchmarkSummary,
  collectBenchmarkSummary,
  createConcurrencyGate,
  parseObserverJSONL,
  parseObserverJSONLSnapshot,
  parseBenchmarkIdentity,
  summarizeClientMilestones,
  summarizeObserverEvidence,
  validateApiTransportCapacity,
  validateBenchmarkConfig,
  waitForBenchmarkObserverEvidence,
  waitForColdObserverPreflight,
  waitUntilEpochMs,
} from "./capacity-smoke-model.mjs";

const run1Hash = benchmarkRunHash("run-1");

function strictEvidence({ progressive = true } = {}) {
  const writes = progressive
    ? [
      { sequence: 1, at: 10, workload: 100, current: 1, target: 5 },
      { sequence: 2, at: 20, workload: 500, current: 5, target: 25 },
    ]
    : [{ sequence: 1, at: 20, workload: 500, current: 1, target: 25 }];
  const events = [{
    runId: "run-1",
    elapsedMs: 0,
    baseline: true,
    benchmarkRunHash: run1Hash,
    serverAdmitted: 0,
    runningSandboxes: 0,
    asgDesired: 1,
    ec2InService: 1,
    nomadReady: 1,
    controllerAudit: { event: "controller_started", controllerInstanceID: "controller-1", mode: "start-intent-v1" },
  }];
  for (const write of writes) {
    events.push({
      runId: "run-1",
      elapsedMs: write.at,
      benchmarkRunHash: run1Hash,
      serverAdmitted: write.workload,
      serverAdmissionFirstElapsedMs: 1,
      serverAdmissionLastElapsedMs: write.at - 1,
      controllerAudit: {
        event: "scale_write_started",
        controllerInstanceID: "controller-1",
        scaleWriteSequence: write.sequence,
        mode: "start-intent-v1",
        workloadCount: write.workload,
        currentDesired: write.current,
        target: write.target,
        batchTrigger: "idle",
        batchAgeMs: 1_000,
        batchIdleAgeMs: 1_000,
      },
    }, {
      runId: "run-1",
      elapsedMs: write.at + 1,
      controllerAudit: {
        event: "scale_write_finished",
        controllerInstanceID: "controller-1",
        scaleWriteSequence: write.sequence,
        mode: "start-intent-v1",
        workloadCount: write.workload,
        currentDesired: write.current,
        target: write.target,
        batchTrigger: "idle",
        batchAgeMs: 1_000,
        batchIdleAgeMs: 1_000,
        outcome: "success",
        durationMs: 4,
        awsRequestId: `request-${write.sequence}`,
        error: "",
      },
      asgDesired: write.target,
      ec2InService: 1,
      nomadReady: 1,
    });
  }
  events.push({
    runId: "run-1",
    elapsedMs: 40,
    benchmarkRunHash: run1Hash,
    serverAdmitted: 500,
    serverAdmissionFirstElapsedMs: 1,
    serverAdmissionLastElapsedMs: writes.at(-1).at - 1,
    asgDesired: 25,
    ec2InService: 25,
    nomadReady: 25,
    asgActivityEvidence: {
      complete: true,
      asgName: "workers",
      finalDesired: 25,
      windowStartEpochMs: 1_000,
      windowEndEpochMs: 50_000,
      activities: Array.from({ length: 24 }, (_, index) => ({
        activityId: `activity-${index + 1}`,
        startTime: new Date(2_000 + index).toISOString(),
        statusCode: "Successful",
        action: "launch",
      })),
    },
  });
  events.push({
    runId: "run-1",
    elapsedMs: 50,
    controllerAudit: {
      event: "audit_checkpoint",
      controllerInstanceID: "controller-1",
      scaleWriteSequence: writes.at(-1).sequence,
      mode: "start-intent-v1",
      auditDroppedTotal: 0,
      checkpointGeneratedElapsedMs: 50,
    },
  });

  return events;
}

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
        elapsedMs: 0,
        baseline: true,
        benchmarkRunHash: run1Hash,
        serverAdmitted: 0,
        runningSandboxes: 0,
        asgDesired: 1,
        ec2InService: 1,
        nomadReady: 1,
      },
      {
        runId: "run-1",
        elapsedMs: 10,
        benchmarkRunHash: run1Hash,
        serverAdmitted: 100,
        controller: {
          mode: "start-intent-v1",
          workloadCount: 100,
          currentDesired: 1,
          targetNodes: 5,
          readyNodes: 1,
          capped: false,
          scaled: true,
          outcome: "success",
        },
        asgDesired: 5,
        ec2InService: 1,
        nomadReady: 1,
      },
      {
        runId: "run-1",
        elapsedMs: 20,
        benchmarkRunHash: run1Hash,
        serverAdmitted: 500,
        controller: {
          mode: "start-intent-v1",
          workloadCount: 500,
          currentDesired: 5,
          targetNodes: 25,
          readyNodes: 1,
          capped: false,
          scaled: true,
          outcome: "success",
        },
        asgDesired: 25,
        ec2InService: 1,
        nomadReady: 1,
      },
      {
        runId: "run-1",
        elapsedMs: 40,
        benchmarkRunHash: run1Hash,
        serverAdmitted: 500,
        asgDesired: 25,
        ec2InService: 25,
        nomadReady: 8,
      },
      {
        runId: "run-1",
        elapsedMs: 60,
        benchmarkRunHash: run1Hash,
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
  assert.equal(report.firstScaleDecisionAt, null);
  assert.equal(report.firstScaleDecision, null);
  assert.deepEqual(report.asgDesiredTimeline, [
    { elapsedMs: 0, value: 1 },
    { elapsedMs: 10, value: 5 },
    { elapsedMs: 20, value: 25 },
  ]);
  assert.deepEqual(report.ec2InServiceTimeline, [
    { elapsedMs: 0, value: 1 },
    { elapsedMs: 40, value: 25 },
  ]);
  assert.deepEqual(report.nomadReadyTimeline, [
    { elapsedMs: 0, value: 1 },
    { elapsedMs: 40, value: 8 },
    { elapsedMs: 60, value: 25 },
  ]);
  assert.equal(report.complete, false);
  assert.deepEqual(report.missingEvidence, [
    "serverAdmissionSourceTimes",
    "controllerAudit",
    "asgActivityEvidence",
  ]);
});

test("runner preflight accepts exactly one trusted cold baseline", () => {
  const baseline = strictEvidence()[0];
  assert.equal(assertColdObserverPreflight({ runId: "run-1", events: [baseline] }), baseline);
  for (const events of [
    [],
    [{ ...baseline, runId: "other" }],
    [{ ...baseline, runningSandboxes: 1 }],
  ]) {
    assert.throws(
      () => assertColdObserverPreflight({ runId: "run-1", events }),
      /cold baseline/,
    );
  }
  assert.equal(
    assertColdObserverPreflight({ runId: "run-1", events: [baseline, { ...baseline, baseline: false }] }),
    baseline,
  );
});

test("runner waits for the observer T0 baseline before starting workload", async () => {
  const baseline = strictEvidence()[0];
  const samples = [[], [baseline]];
  let reads = 0;
  let elapsedMs = 0;
  const result = await waitForColdObserverPreflight({
    runId: "run-1",
    readEvents: () => samples[Math.min(reads++, samples.length - 1)],
    timeoutMs: 1_000,
    now: () => elapsedMs,
    sleep: async (delayMs) => { elapsedMs += delayMs; },
  });

  assert.equal(result, baseline);
  assert.equal(reads, 2);
});

test("strict controller audit accepts progressive and single-step scale-out", () => {
  for (const [progressive, expectedSteps, singleStep] of [
    [true, [1, 5, 25], false],
    [false, [1, 25], true],
  ]) {
    const summary = buildBenchmarkSummary({
      runId: "run-1",
      target: 500,
      concurrency: 500,
      maximumClientConcurrency: 500,
      expectedNodes: 25,
      perNode: 20,
      completed: 500,
      failed: 0,
      clientEvents: [{ type: "guest_ready", elapsedMs: 50 }],
      observerEvents: strictEvidence({ progressive }),
      readyMs: { p50: 45, p90: 48, p95: 49, p99: 50, max: 50 },
    });

    assert.equal(summary.targetCapacityObserved, true);
    assert.deepEqual(summary.scaleWriteSteps, expectedSteps);
    assert.equal(summary.singleStepScaleout, singleStep);
    assert.equal(summary.timings.firstIntentToFirstScaleWrite, progressive ? 9 : 19);
    assert.equal(summary.timings.allAdmittedToDesiredTarget, 2);
    assert.equal(summary.timings.desiredTargetToInServiceTarget, 19);
    assert.equal(summary.timings.inServiceTargetToNomadReadyTarget, 0);
    assert.equal(summary.timings.nomadReadyTargetToLastGuestReady, 10);
  }
});

test("post-baseline workload clock excludes observer barrier delay", () => {
  const observerEvents = strictEvidence({ progressive: false }).map((event, index) => ({
    ...event,
    elapsedMs: event.elapsedMs + (index === 0 ? 9_000 : 10_000),
    ...(event.serverAdmissionFirstElapsedMs === undefined ? {} : {
      serverAdmissionFirstElapsedMs: event.serverAdmissionFirstElapsedMs + 10_000,
      serverAdmissionLastElapsedMs: event.serverAdmissionLastElapsedMs + 10_000,
    }),
    ...(event.controllerAudit?.checkpointGeneratedElapsedMs === undefined ? {} : {
      controllerAudit: {
        ...event.controllerAudit,
        checkpointGeneratedElapsedMs: event.controllerAudit.checkpointGeneratedElapsedMs + 10_000,
      },
    }),
  }));
  const summary = buildBenchmarkSummary({
    runId: "run-1",
    target: 500,
    concurrency: 500,
    maximumClientConcurrency: 500,
    expectedNodes: 25,
    perNode: 20,
    completed: 500,
    failed: 0,
    clientEvents: [{ type: "guest_ready", elapsedMs: 50 }],
    observerEvents,
    observerWorkloadStartOffsetMs: 10_000,
    readyMs: { p50: 45, p90: 48, p95: 49, p99: 50, max: 50 },
  });

  assert.equal(summary.readyMs.max, 50);
  assert.equal(summary.observerWorkloadStartOffsetMs, 10_000);
  assert.equal(summary.timings.nomadReadyTargetToLastGuestReady, 10);
});

test("formal acceptance requires a checkpoint newer than terminal evidence", () => {
  const summary = buildBenchmarkSummary({
    runId: "run-1",
    target: 500,
    concurrency: 500,
    maximumClientConcurrency: 500,
    expectedNodes: 25,
    perNode: 20,
    completed: 500,
    failed: 0,
    clientEvents: [{ type: "guest_ready", elapsedMs: 60 }],
    observerEvents: strictEvidence({ progressive: false }),
    readyMs: { p50: 55, p90: 58, p95: 59, p99: 60, max: 60 },
  });

  assert.equal(summary.terminalAuditCheckpointObserved, false);
  assert.equal(summary.observerEvidenceComplete, false);
  assert.equal(summary.targetCapacityObserved, false);
  assert(summary.missingObserverEvidence.includes("terminalAuditCheckpoint"));
});

test("delayed checkpoint delivery cannot impersonate a fresh terminal checkpoint", () => {
  const events = strictEvidence({ progressive: false });
  const checkpoint = events.at(-1);
  checkpoint.elapsedMs = 61;
  checkpoint.controllerAudit.checkpointGeneratedElapsedMs = 51;
  const summary = buildBenchmarkSummary({
    runId: "run-1", target: 500, concurrency: 500, maximumClientConcurrency: 500,
    expectedNodes: 25, perNode: 20, completed: 500, failed: 0,
    clientEvents: [{ type: "guest_ready", elapsedMs: 60 }], observerEvents: events,
    readyMs: { p50: 55, p90: 58, p95: 59, p99: 60, max: 60 },
  });

  assert.equal(summary.terminalAuditCheckpointObserved, false);
  assert.equal(summary.targetCapacityObserved, false);
});

test("admission source time prevents observer polling delay from producing negative stages", () => {
  const events = strictEvidence({ progressive: false });
  events[1].elapsedMs = 25;
  events[1].sourceElapsedMs = 20;
  events[2].elapsedMs = 25;
  events[2].sourceElapsedMs = 21;

  const summary = buildBenchmarkSummary({
    runId: "run-1",
    target: 500,
    concurrency: 500,
    maximumClientConcurrency: 500,
    expectedNodes: 25,
    perNode: 20,
    completed: 500,
    failed: 0,
    clientEvents: [{ type: "guest_ready", elapsedMs: 50 }],
    observerEvents: events,
    readyMs: { p50: 45, p90: 48, p95: 49, p99: 50, max: 50 },
  });

  assert.equal(summary.timings.firstIntentToFirstScaleWrite, 19);
  assert.equal(summary.timings.allAdmittedToDesiredTarget, 6);

  for (const event of events) {
    if (event.serverAdmitted > 0) {
      event.serverAdmissionFirstElapsedMs = 22;
      event.serverAdmissionLastElapsedMs = 22;
    }
  }
  assert.throws(
    () => buildBenchmarkSummary({
      runId: "run-1", target: 500, concurrency: 500, maximumClientConcurrency: 500,
      expectedNodes: 25, perNode: 20, completed: 500, failed: 0,
      clientEvents: [{ type: "guest_ready", elapsedMs: 50 }], observerEvents: events,
      readyMs: { p50: 45, p90: 48, p95: 49, p99: 50, max: 50 },
    }),
    /firstIntentToFirstScaleWrite has a negative duration/,
  );
});

test("strict controller audit rejects gaps, unfinished writes, restarts, and unknown fields", () => {
  for (const mutate of [
    (events) => { events[3].controllerAudit.scaleWriteSequence = 3; events[4].controllerAudit.scaleWriteSequence = 3; },
    (events) => { events.splice(4, 1); },
    (events) => { events[3].controllerAudit.controllerInstanceID = "controller-2"; },
  ]) {
    const events = strictEvidence();
    mutate(events);
    const report = summarizeObserverEvidence({ runId: "run-1", logicalTarget: 500, events });
    assert.equal(report.controllerAuditComplete, false);
    assert(report.missingEvidence.includes("controllerAudit"));
  }

  const unknown = strictEvidence();
  unknown[1].controllerAudit.untrusted = true;
  assert.throws(
    () => summarizeObserverEvidence({ runId: "run-1", logicalTarget: 500, events: unknown }),
    /unknown controller audit field/,
  );

  const dropped = strictEvidence();
  dropped.at(-1).controllerAudit.auditDroppedTotal = 1;
  const droppedReport = summarizeObserverEvidence({
    runId: "run-1",
    logicalTarget: 500,
    events: dropped,
  });
  assert.equal(droppedReport.controllerAuditComplete, false);
  assert.match(droppedReport.controllerAuditIssues.join("; "), /delivery lost 1 events/);
});

test("strict controller audit accepts a contiguous sequence from a benchmark cursor", () => {
  const events = strictEvidence();
  delete events[0].controllerAudit;
  for (const event of events) {
    if (event.controllerAudit?.scaleWriteSequence !== undefined) {
      event.controllerAudit.scaleWriteSequence += 40;
    }
  }

  const report = summarizeObserverEvidence({ runId: "run-1", logicalTarget: 500, events });
  assert.equal(report.controllerAuditComplete, true);
  assert.equal(report.controllerInstanceID, "controller-1");
});

test("controller audit source time remains causal when observer emission is delayed", () => {
  const events = strictEvidence({ progressive: false });
  events[1].sourceElapsedMs = 4;
  events[1].elapsedMs = 10;
  events[1].serverAdmissionFirstElapsedMs = 4;
  events[1].serverAdmissionLastElapsedMs = 4;
  events[2].sourceElapsedMs = 5;
  events[2].elapsedMs = 10;

  const report = summarizeObserverEvidence({ runId: "run-1", logicalTarget: 500, events });
  assert.equal(report.firstScaleDecisionAt, 4);
  assert.equal(report.scaleDecisionTimeline[0].finishedAt, 5);

  events[1].sourceElapsedMs = 11;
  assert.throws(
    () => summarizeObserverEvidence({ runId: "run-1", logicalTarget: 500, events }),
    /source time is later/,
  );
});

test("formal acceptance rejects a scale target below visible workload and AWS evidence conflict", () => {
  const underTarget = strictEvidence();
  underTarget[3].controllerAudit.target = 24;
  underTarget[4].controllerAudit.target = 24;
  const conflictedAWS = strictEvidence();
  conflictedAWS.at(-2).asgActivityEvidence.finalDesired = 24;

  for (const events of [underTarget, conflictedAWS]) {
    const summary = buildBenchmarkSummary({
      runId: "run-1", target: 500, concurrency: 500, maximumClientConcurrency: 500,
      expectedNodes: 25, perNode: 20, completed: 500, failed: 0, clientEvents: [], events,
      observerEvents: events, readyMs: { p50: 1, p90: 1, p95: 1, p99: 1, max: 1 },
    });
    assert.equal(summary.targetCapacityObserved, false);
  }
});

test("formal acceptance rejects controller batching beyond the configured response bound", () => {
  const events = strictEvidence({ progressive: false });
  events[1].controllerAudit.batchAgeMs = 12_001;
  events[2].controllerAudit.batchAgeMs = 12_001;
  const summary = buildBenchmarkSummary({
    runId: "run-1", target: 500, concurrency: 500, maximumClientConcurrency: 500,
    expectedNodes: 25, perNode: 20, completed: 500, failed: 0, clientEvents: [],
    observerEvents: events, readyMs: { p50: 1, p90: 1, p95: 1, p99: 1, max: 1 },
    batchMaxDurationMs: 10_000, reconcileLagBudgetMs: 2_000,
  });

  assert.equal(summary.targetCapacityObserved, false);
});

test("formal acceptance rejects regressing ASG desired evidence", () => {
  const events = strictEvidence();
  events.splice(-2, 0, {
    runId: "run-1", elapsedMs: 30, asgDesired: 4, ec2InService: 1, nomadReady: 1,
  });
  const summary = buildBenchmarkSummary({
    runId: "run-1", target: 500, concurrency: 500, maximumClientConcurrency: 500,
    expectedNodes: 25, perNode: 20, completed: 500, failed: 0, clientEvents: [],
    observerEvents: events, readyMs: { p50: 1, p90: 1, p95: 1, p99: 1, max: 1 },
  });

  assert.equal(summary.targetCapacityObserved, false);
});

test("ASG activity evidence may settle across multiple observer samples", () => {
  const events = strictEvidence();
  events.splice(-2, 0, {
    runId: "run-1",
    elapsedMs: 30,
    asgActivityEvidence: {
      complete: true,
      asgName: "workers",
      finalDesired: 5,
      windowStartEpochMs: 1_000,
      windowEndEpochMs: 30_000,
      activities: [],
    },
  });

  const report = summarizeObserverEvidence({ runId: "run-1", logicalTarget: 500, events });
  assert.equal(report.asgActivityEvidence.complete, true);
  assert.equal(report.asgActivityEvidence.finalDesired, 25);
});

test("ASG activity evidence rejects failed and out-of-window launches", () => {
  const failed = strictEvidence();
  failed.at(-2).asgActivityEvidence.activities[0].statusCode = "InProgress";
  const outside = strictEvidence();
  outside.at(-2).asgActivityEvidence.activities[0].startTime = new Date(999).toISOString();

  for (const events of [failed, outside]) {
    const report = summarizeObserverEvidence({ runId: "run-1", logicalTarget: 500, events });
    assert.equal(report.asgActivityEvidence.complete, false);
  }
});

test("ASG activity evidence rejects non-launch and extra activities", () => {
  const nonLaunch = strictEvidence();
  nonLaunch.at(-2).asgActivityEvidence.activities[0].action = "other";
  const extra = strictEvidence();
  extra.at(-2).asgActivityEvidence.activities.push({
    activityId: "activity-extra",
    startTime: new Date(2_100).toISOString(),
    statusCode: "Successful",
    action: "launch",
  });

  for (const events of [nonLaunch, extra]) {
    const summary = buildBenchmarkSummary({
      runId: "run-1", target: 500, concurrency: 500, maximumClientConcurrency: 500,
      expectedNodes: 25, perNode: 20, completed: 500, failed: 0, clientEvents: [],
      observerEvents: events, readyMs: { p50: 1, p90: 1, p95: 1, p99: 1, max: 1 },
    });
    assert.equal(summary.targetCapacityObserved, false);
  }
});

test("observer evidence reuses the existing nodes.jsonl snapshot shape", () => {
  const report = summarizeObserverEvidence({
    runId: "run-1",
    logicalTarget: 500,
    events: [{
      runId: "run-1",
      elapsedMs: 5,
      benchmarkRunHash: run1Hash,
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
  assert.deepEqual(report.missingEvidence, [
    "serverAdmissionSourceTimes",
    "controllerAudit",
    "asgActivityEvidence",
    "baseline",
  ]);
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
        { runId: "run-1", elapsedMs: 1, benchmarkRunHash: run1Hash, serverAdmitted: 100 },
        { runId: "run-1", elapsedMs: 2, benchmarkRunHash: run1Hash, serverAdmitted: 99 },
      ],
    }),
    /regressed/,
  );
});

test("observer evidence rejects uncorrelated admission counts and non-monotonic timestamps", () => {
  assert.throws(
    () => summarizeObserverEvidence({
      runId: "run-1",
      logicalTarget: 500,
      events: [{ runId: "run-1", elapsedMs: 1, serverAdmitted: 500 }],
    }),
    /not correlated/,
  );
  assert.throws(
    () => summarizeObserverEvidence({
      runId: "run-1",
      logicalTarget: 500,
      events: [
        { runId: "run-1", elapsedMs: 2 },
        { runId: "run-1", elapsedMs: 1 },
      ],
    }),
    /not monotonic/,
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

test("observer snapshots distinguish an appended partial tail from malformed completed records", () => {
  assert.throws(
    () => parseObserverJSONLSnapshot('{"runId":"run-1","elapsedMs":1}\n{"runId":'),
    /still being written/,
  );
  assert.throws(
    () => parseObserverJSONLSnapshot('{"runId":"run-1","elapsedMs":1}\nnot-json\n'),
    /invalid observer JSONL at line 2/,
  );
  assert.throws(
    () => parseObserverJSONLSnapshot('{"runId":"run-1","elapsedMs":1}'),
    /still being written/,
  );
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
    perNode: 20,
    completed: 0,
    failed: 0,
    clientEvents,
    observerEvents: [{
      runId: "run-1",
      elapsedMs: 600,
      benchmarkRunHash: run1Hash,
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
  assert.equal(summary.targetCapacityObserved, false);
});

test("the final benchmark requires both controller and ASG evidence for the full target", () => {
  const summary = buildBenchmarkSummary({
    runId: "run-1",
    target: 500,
    concurrency: 500,
    maximumClientConcurrency: 500,
    expectedNodes: 25,
    perNode: 20,
    completed: 500,
    failed: 0,
    clientEvents: [],
    observerEvents: strictEvidence({ progressive: false }),
    readyMs: { p50: 1, p90: 1, p95: 1, p99: 1, max: 1 },
  });

  assert.equal(summary.targetCapacityObserved, true);
  assert.equal(benchmarkObserverEvidenceAccepted(summary), true);
});

test("the benchmark waits one observer sample for final ready capacity", async () => {
  const incomplete = {
    observerEvidenceComplete: true,
    coldBaselineObserved: true,
    allDemandObservedByServer: true,
    targetCapacityObserved: false,
  };
  const complete = { ...incomplete, targetCapacityObserved: true };
  const samples = [incomplete, complete];
  let elapsedMs = 0;
  let reads = 0;

  const summary = await waitForBenchmarkObserverEvidence({
    readSummary: () => samples[Math.min(reads++, samples.length - 1)],
    timeoutMs: 1_000,
    pollIntervalMs: 250,
    now: () => elapsedMs,
    sleep: async (delayMs) => {
      elapsedMs += delayMs;
    },
  });

  assert.equal(summary, complete);
  assert.equal(reads, 2);
  assert.equal(elapsedMs, 250);
});

test("the benchmark waits for an observer JSONL tail to finish", async () => {
  const complete = {
    observerEvidenceComplete: true,
    coldBaselineObserved: true,
    allDemandObservedByServer: true,
    targetCapacityObserved: true,
  };
  let reads = 0;
  let elapsedMs = 0;

  const summary = await waitForBenchmarkObserverEvidence({
    readSummary: () => {
      reads += 1;
      if (reads === 1) parseObserverJSONLSnapshot('{"runId":');
      return complete;
    },
    timeoutMs: 1_000,
    pollIntervalMs: 250,
    now: () => elapsedMs,
    sleep: async (delayMs) => {
      elapsedMs += delayMs;
    },
  });

  assert.equal(summary, complete);
  assert.equal(reads, 2);
});

test("the benchmark fails if an observer JSONL tail never finishes", async () => {
  let elapsedMs = 0;

  await assert.rejects(
    waitForBenchmarkObserverEvidence({
      readSummary: () => parseObserverJSONLSnapshot('{"runId":'),
      timeoutMs: 500,
      pollIntervalMs: 250,
      now: () => elapsedMs,
      sleep: async (delayMs) => {
        elapsedMs += delayMs;
      },
    }),
    /remained incomplete until the settle timeout/,
  );
});

test("sandbox failures remain primary when observer evidence is malformed", async () => {
  let observerReads = 0;
  const summary = await collectBenchmarkSummary({
    failureCount: 1,
    buildSummary: (observerEvents) => ({ observerEvents }),
    readObserverEvents: () => {
      observerReads += 1;
      throw new Error("invalid observer JSONL");
    },
    observerSettleTimeoutMs: 500,
  });

  assert.deepEqual(summary, {
    observerEvents: [],
    observerDiagnosticError: "Error: invalid observer JSONL",
  });
  assert.equal(observerReads, 1);
});

test("sandbox failures preserve a valid observer snapshot as secondary diagnostics", async () => {
  const events = [{ runId: "run-1", elapsedMs: 10 }];
  const summary = await collectBenchmarkSummary({
    failureCount: 1,
    buildSummary: (observerEvents) => ({ observerEvents }),
    readObserverEvents: () => events,
    observerSettleTimeoutMs: 500,
  });

  assert.deepEqual(summary, { observerEvents: events, observerDiagnosticError: null });
});

test("the benchmark returns the latest incomplete observer evidence at timeout", async () => {
  const incomplete = {
    observerEvidenceComplete: true,
    coldBaselineObserved: true,
    allDemandObservedByServer: true,
    targetCapacityObserved: false,
  };
  let elapsedMs = 0;
  let reads = 0;

  const summary = await waitForBenchmarkObserverEvidence({
    readSummary: () => {
      reads += 1;
      return incomplete;
    },
    timeoutMs: 500,
    pollIntervalMs: 250,
    now: () => elapsedMs,
    sleep: async (delayMs) => {
      elapsedMs += delayMs;
    },
  });

  assert.equal(summary, incomplete);
  assert.equal(reads, 3);
  assert.equal(elapsedMs, 500);
});

test("the final benchmark rejects failed controller evidence and unready capacity", () => {
  const base = {
    runId: "run-1",
    target: 500,
    concurrency: 500,
    maximumClientConcurrency: 500,
    expectedNodes: 25,
    perNode: 20,
    completed: 500,
    failed: 0,
    clientEvents: [],
    readyMs: { p50: 1, p90: 1, p95: 1, p99: 1, max: 1 },
  };
  const baseline = {
    runId: "run-1",
    elapsedMs: 0,
    baseline: true,
    benchmarkRunHash: run1Hash,
    serverAdmitted: 0,
    runningSandboxes: 0,
    asgDesired: 1,
    ec2InService: 1,
    nomadReady: 1,
  };
  const failedDecision = {
    runId: "run-1",
    elapsedMs: 10,
    benchmarkRunHash: run1Hash,
    serverAdmitted: 500,
    controller: {
      mode: "legacy-failure-ledger",
      workloadCount: 500,
      currentDesired: 1,
      targetNodes: 25,
      capped: true,
      scaled: false,
      outcome: "error",
    },
    asgDesired: 25,
    ec2InService: 25,
    nomadReady: 25,
  };
  assert.equal(buildBenchmarkSummary({
    ...base,
    observerEvents: [baseline, failedDecision],
  }).targetCapacityObserved, false);

  const successfulButUnready = {
    ...failedDecision,
    controller: {
      ...failedDecision.controller,
      mode: "start-intent-v1",
      capped: false,
      scaled: true,
      outcome: "success",
    },
    ec2InService: 1,
    nomadReady: 1,
  };
  assert.equal(buildBenchmarkSummary({
    ...base,
    observerEvents: [baseline, successfulButUnready],
  }).targetCapacityObserved, false);

  const prematureASG = {
    ...baseline,
    elapsedMs: 5,
    baseline: false,
    serverAdmitted: 500,
    asgDesired: 25,
    ec2InService: 25,
    nomadReady: 25,
  };
  const decisionAfterASG = {
    ...successfulButUnready,
    elapsedMs: 10,
    ec2InService: 25,
    nomadReady: 25,
  };
  assert.equal(buildBenchmarkSummary({
    ...base,
    observerEvents: [baseline, prematureASG, decisionAfterASG],
  }).targetCapacityObserved, false);

  const partialDecision = {
    ...baseline,
    elapsedMs: 5,
    baseline: false,
    serverAdmitted: 100,
    controller: {
      mode: "start-intent-v1",
      workloadCount: 100,
      currentDesired: 1,
      targetNodes: 5,
      capped: false,
      scaled: true,
      outcome: "success",
    },
    asgDesired: 5,
  };
  const fullDecision = {
    ...decisionAfterASG,
    controller: {
      ...decisionAfterASG.controller,
      currentDesired: 5,
    },
  };
  assert.equal(buildBenchmarkSummary({
    ...base,
    observerEvents: [baseline, partialDecision, fullDecision],
  }).targetCapacityObserved, false);

  const hiddenPartialASG = {
    ...baseline,
    elapsedMs: 5,
    baseline: false,
    serverAdmitted: 500,
    asgDesired: 5,
  };
  const decisionAfterHiddenPartialASG = {
    ...decisionAfterASG,
    elapsedMs: 10,
  };
  assert.equal(buildBenchmarkSummary({
    ...base,
    observerEvents: [baseline, hiddenPartialASG, decisionAfterHiddenPartialASG],
  }).targetCapacityObserved, false);
});

test("the final benchmark rejects a baseline appended after another observer event", () => {
  const summary = buildBenchmarkSummary({
    runId: "run-1",
    target: 500,
    concurrency: 500,
    maximumClientConcurrency: 500,
    expectedNodes: 25,
    perNode: 20,
    completed: 500,
    failed: 0,
    clientEvents: [],
    observerEvents: [
      { runId: "run-1", elapsedMs: 0 },
      {
        runId: "run-1",
        elapsedMs: 1,
        baseline: true,
        benchmarkRunHash: run1Hash,
        serverAdmitted: 0,
        runningSandboxes: 0,
        asgDesired: 1,
        ec2InService: 1,
        nomadReady: 1,
      },
    ],
    readyMs: { p50: 1, p90: 1, p95: 1, p99: 1, max: 1 },
  });

  assert.equal(summary.coldBaselineObserved, false);
});

test("the final benchmark rejects a non-cold baseline and capacity overshoot", () => {
  const summary = buildBenchmarkSummary({
    runId: "run-1",
    target: 500,
    concurrency: 500,
    maximumClientConcurrency: 500,
    expectedNodes: 25,
    perNode: 20,
    completed: 500,
    failed: 0,
    clientEvents: [],
    observerEvents: [
      {
        runId: "run-1",
        elapsedMs: 0,
        baseline: true,
        benchmarkRunHash: run1Hash,
        serverAdmitted: 0,
        runningSandboxes: 1,
        asgDesired: 1,
        ec2InService: 1,
        nomadReady: 1,
      },
      {
        runId: "run-1",
        elapsedMs: 1,
        benchmarkRunHash: run1Hash,
        serverAdmitted: 500,
        controller: { workloadCount: 500, currentDesired: 1, targetNodes: 25 },
        asgDesired: 25,
        ec2InService: 1,
        nomadReady: 1,
      },
      {
        runId: "run-1",
        elapsedMs: 2,
        benchmarkRunHash: run1Hash,
        serverAdmitted: 500,
        controller: { workloadCount: 501, currentDesired: 25, targetNodes: 26 },
        asgDesired: 26,
        ec2InService: 1,
        nomadReady: 1,
      },
    ],
    readyMs: { p50: 1, p90: 1, p95: 1, p99: 1, max: 1 },
  });

  assert.equal(summary.coldBaselineObserved, false);
  assert.equal(summary.targetCapacityObserved, false);
});

test("the final benchmark summary preserves missing admission as unknown", () => {
  const summary = buildBenchmarkSummary({
    runId: "run-1",
    target: 500,
    concurrency: 500,
    maximumClientConcurrency: 0,
    expectedNodes: 25,
    perNode: 20,
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
  assert.match(source, /Sandbox\.list/);
  assert.match(source, /query: \{ metadata: \{ benchmarkRunId: runId \} \}/);
  assert(
    source.indexOf("new Set(created.keys())") < source.indexOf("await listRunSandboxIDs()"),
    "cleanup must preserve locally known sandboxes before remote discovery",
  );
  assert(
    source.lastIndexOf("if (failures.length > 0)")
      < source.indexOf("if (!summary.observerEvidenceComplete)"),
    "sandbox failures must take precedence over observer evidence failures",
  );
  assert.doesNotMatch(source, /`capacity-smoke-\$\{new Date/);
  assert(
    source.indexOf("preexistingObserverEvents") < source.indexOf("await waitUntilEpochMs"),
    "runner must check the observer output before waiting for T0",
  );
  assert(
    source.indexOf("await waitUntilEpochMs") < source.indexOf('type: "run_started"'),
    "runner must wait for T0 before starting workload",
  );
  assert(
    source.indexOf("await waitForColdObserverPreflight") < source.lastIndexOf("t0 = performance.now()"),
    "runner must start the workload latency clock only after the cold baseline barrier",
  );
  assert.doesNotMatch(source, /while\s*\([^)]*deadline/);
});
