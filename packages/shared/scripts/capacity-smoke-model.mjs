import { createHash } from "node:crypto";

function positiveInteger(name, value) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }

  return value;
}

function nonNegativeInteger(name, value) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }

  return value;
}

function elapsedMilliseconds(name, value) {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${name} must be a non-negative finite number`);
  }

  return value;
}

const benchmarkRunIdPattern = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

export function benchmarkRunHash(runId) {
  if (typeof runId !== "string" || !benchmarkRunIdPattern.test(runId)) {
    throw new Error("benchmark runId is invalid");
  }

  return createHash("sha256").update(runId).digest("hex");
}

export function assertColdObserverPreflight({ runId, events }) {
  if (!Array.isArray(events) || events.length === 0) {
    throw new Error("observer preflight requires a cold baseline event");
  }
  const baseline = events[0];
  const valid = baseline !== null
    && typeof baseline === "object"
    && !Array.isArray(baseline)
    && baseline.runId === runId
    && Number.isFinite(baseline.elapsedMs)
    && baseline.elapsedMs >= 0
    && baseline.baseline === true
    && baseline.benchmarkRunHash === benchmarkRunHash(runId)
    && baseline.serverAdmitted === 0
    && baseline.runningSandboxes === 0
    && baseline.asgDesired === 1
    && baseline.ec2InService === 1
    && baseline.nomadReady === 1;
  if (!valid) throw new Error("observer preflight cold baseline is invalid");

  return baseline;
}

export async function waitForColdObserverPreflight({
  runId,
  readEvents,
  timeoutMs,
  pollIntervalMs = 100,
  now = Date.now,
  sleep = (delayMs) => new Promise((resolve) => setTimeout(resolve, delayMs)),
}) {
  if (typeof readEvents !== "function") throw new Error("readEvents must be a function");
  positiveInteger("observer baseline timeoutMs", timeoutMs);
  positiveInteger("observer baseline pollIntervalMs", pollIntervalMs);
  const deadline = now() + timeoutMs;
  while (true) {
    try {
      const events = await readEvents();
      if (events.length > 0) return assertColdObserverPreflight({ runId, events });
    } catch (error) {
      if (!(error instanceof ObserverJSONLWriteInProgressError)) throw error;
    }
    const remainingMs = deadline - now();
    if (remainingMs <= 0) throw new Error("observer cold baseline was not published before timeout");
    await sleep(Math.min(pollIntervalMs, remainingMs));
  }
}

export function parseBenchmarkIdentity({ runId, startEpochMs }) {
  if (typeof runId !== "string" || !benchmarkRunIdPattern.test(runId)) {
    throw new Error("BENCHMARK_RUN_ID must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}");
  }
  if (typeof startEpochMs !== "string" || !/^\d{13}$/.test(startEpochMs)) {
    throw new Error("BENCHMARK_START_EPOCH_MS must be a 13-digit epoch millisecond timestamp");
  }
  const parsedStartEpochMs = Number(startEpochMs);
  if (!Number.isSafeInteger(parsedStartEpochMs) || parsedStartEpochMs <= 0) {
    throw new Error("BENCHMARK_START_EPOCH_MS must be a positive safe integer");
  }

  return { runId, startEpochMs: parsedStartEpochMs };
}

export async function waitUntilEpochMs(
  startEpochMs,
  {
    nowEpochMs = Date.now,
    sleep = (delayMs) => new Promise((resolve) => setTimeout(resolve, delayMs)),
  } = {},
) {
  if (!Number.isSafeInteger(startEpochMs) || startEpochMs <= 0) {
    throw new Error("benchmark start epoch must be a positive safe integer");
  }
  const delayMs = startEpochMs - nowEpochMs();
  if (!Number.isFinite(delayMs) || delayMs <= 0) {
    throw new Error("BENCHMARK_START_EPOCH_MS must still be in the future");
  }
  let remainingMs = delayMs;
  while (remainingMs > 0) {
    await sleep(remainingMs);
    remainingMs = startEpochMs - nowEpochMs();
  }
}

export function createConcurrencyGate(limit, onActive = () => {}) {
  positiveInteger("limit", limit);

  let active = 0;
  const queue = [];

  const drain = () => {
    while (active < limit && queue.length > 0) {
      const next = queue.shift();
      active += 1;
      onActive(active);
      Promise.resolve()
        .then(next.task)
        .then(next.resolve, next.reject)
        .finally(() => {
          active -= 1;
          onActive(active);
          drain();
        });
    }
  };

  return (task) => new Promise((resolve, reject) => {
    queue.push({ task, resolve, reject });
    drain();
  });
}

export function admissionReport({
  logicalTarget,
  requestsStarted,
  serverAdmitted,
  maximumClientConcurrency,
}) {
  for (const [name, value] of Object.entries({
    logicalTarget,
    requestsStarted,
    serverAdmitted,
    maximumClientConcurrency,
  })) {
    nonNegativeInteger(name, value);
  }

  return {
    logicalTarget,
    requestsStarted,
    serverAdmitted,
    maximumClientConcurrency,
    allDemandObservedByServer: serverAdmitted === logicalTarget,
  };
}

const awsCredentialEnvironmentNames = [
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "AWS_SESSION_TOKEN",
  "AWS_PROFILE",
  "AWS_SHARED_CREDENTIALS_FILE",
  "AWS_CONFIG_FILE",
  "AWS_WEB_IDENTITY_TOKEN_FILE",
  "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
  "AWS_CONTAINER_CREDENTIALS_FULL_URI",
];

const maximumObserverLineBytes = 1024 * 1024;
const maximumObserverEvents = 100_000;

export function assertNoAWSCredentialEnvironment(environment) {
  const configured = awsCredentialEnvironmentNames.filter((name) => environment[name]);
  if (configured.length > 0) {
    throw new Error(`benchmark runner must not inherit AWS credential providers: ${configured.join(", ")}`);
  }
}

export function parseObserverJSONL(text) {
  if (typeof text !== "string") throw new Error("observer JSONL must be text");
  const events = [];
  for (const [index, line] of text.split(/\r?\n/).entries()) {
    if (line.trim().length === 0) continue;
    if (Buffer.byteLength(line, "utf8") > maximumObserverLineBytes) {
      throw new Error(`observer JSONL line ${index + 1} is too large`);
    }
    if (events.length >= maximumObserverEvents) {
      throw new Error(`observer JSONL exceeds ${maximumObserverEvents} events`);
    }
    try {
      events.push(JSON.parse(line));
    } catch (error) {
      throw new Error(`invalid observer JSONL at line ${index + 1}`, { cause: error });
    }
  }

  return events;
}

export class ObserverJSONLWriteInProgressError extends Error {}

export function parseObserverJSONLSnapshot(text) {
  if (typeof text !== "string") throw new Error("observer JSONL must be text");
  if (text.length === 0 || text.endsWith("\n")) return parseObserverJSONL(text);

  const lastNewline = text.lastIndexOf("\n");
  const completePrefix = text.slice(0, lastNewline + 1);
  const unterminatedTail = text.slice(lastNewline + 1);
  const events = parseObserverJSONL(completePrefix);
  if (Buffer.byteLength(unterminatedTail, "utf8") > maximumObserverLineBytes) {
    throw new Error(`observer JSONL line ${events.length + 1} is too large`);
  }
  if (events.length >= maximumObserverEvents) {
    throw new Error(`observer JSONL exceeds ${maximumObserverEvents} events`);
  }

  throw new ObserverJSONLWriteInProgressError("observer JSONL tail is still being written");
}

function changedCountTimeline(events, name, valueOf) {
  const timeline = [];
  let previous;
  for (const event of events) {
    const value = valueOf(event);
    if (value === undefined || value === null) continue;
    nonNegativeInteger(name, value);
    if (value !== previous) {
      timeline.push({ elapsedMs: event.elapsedMs, value });
      previous = value;
    }
  }

  return timeline;
}

function ec2InService(event) {
  if (event.ec2InService !== undefined) return event.ec2InService;
  if (!Array.isArray(event.asg)) return undefined;

  return event.asg.filter((instance) => instance?.lifecycle === "InService").length;
}

function nomadReady(event) {
  if (event.nomadReady !== undefined) return event.nomadReady;
  if (!Array.isArray(event.nomad)) return undefined;

  return event.nomad.filter((node) => node?.status === "ready" && node?.drain !== true).length;
}

const controllerAuditFields = new Set([
  "event",
  "controllerInstanceID",
  "scaleWriteSequence",
  "mode",
  "workloadCount",
  "currentDesired",
  "target",
  "batchTrigger",
  "batchAgeMs",
  "batchIdleAgeMs",
  "outcome",
  "durationMs",
  "awsRequestId",
  "error",
  "auditDroppedTotal",
  "checkpointGeneratedElapsedMs",
]);

function summarizeControllerAudit(events) {
  const issues = [];
  const identities = new Set();
  const phases = new Map();
  const checkpoints = [];
  let startupCount = 0;

  for (const event of events) {
    const audit = event.controllerAudit;
    if (audit === undefined) continue;
    const sourceElapsedMs = event.sourceElapsedMs ?? event.elapsedMs;
    elapsedMilliseconds("controller audit sourceElapsedMs", sourceElapsedMs);
    if (sourceElapsedMs > event.elapsedMs) {
      throw new Error("controller audit source time is later than its observer emission time");
    }
    if (audit === null || typeof audit !== "object" || Array.isArray(audit)) {
      throw new Error("controller audit evidence must be an object");
    }
    for (const field of Object.keys(audit)) {
      if (!controllerAuditFields.has(field)) {
        throw new Error(`unknown controller audit field ${field}`);
      }
    }
    if (typeof audit.controllerInstanceID !== "string" || audit.controllerInstanceID.length === 0) {
      throw new Error("controller audit instance ID is required");
    }
    identities.add(audit.controllerInstanceID);
    if (audit.mode !== "start-intent-v1") issues.push("unknown controller mode");

    if (audit.event === "controller_started") {
      startupCount += 1;
      continue;
    }
    if (audit.event === "audit_checkpoint") {
      nonNegativeInteger("controller audit checkpoint sequence", audit.scaleWriteSequence);
      nonNegativeInteger("controller audit dropped total", audit.auditDroppedTotal);
      elapsedMilliseconds(
        "controller audit checkpoint generation time",
        audit.checkpointGeneratedElapsedMs,
      );
      if (audit.checkpointGeneratedElapsedMs > sourceElapsedMs) {
        throw new Error("controller audit checkpoint generation time is later than delivery time");
      }
      checkpoints.push({
        elapsedMs: sourceElapsedMs,
        generatedElapsedMs: audit.checkpointGeneratedElapsedMs,
        scaleWriteSequence: audit.scaleWriteSequence,
        auditDroppedTotal: audit.auditDroppedTotal,
      });
      continue;
    }
    if (audit.event !== "scale_write_started" && audit.event !== "scale_write_finished") {
      throw new Error(`unknown controller audit event ${audit.event}`);
    }
    positiveInteger("controller audit sequence", audit.scaleWriteSequence);
    const phase = audit.event === "scale_write_started" ? "started" : "finished";
    const key = `${audit.controllerInstanceID}:${audit.scaleWriteSequence}:${phase}`;
    const existing = phases.get(key);
    if (existing !== undefined) {
      if (JSON.stringify(existing.audit) !== JSON.stringify(audit)) issues.push(`conflicting duplicate ${key}`);
      continue;
    }
    phases.set(key, { audit, elapsedMs: sourceElapsedMs });
  }

  if (startupCount > 1) issues.push(`expected at most one controller startup event, observed ${startupCount}`);
  if (identities.size !== 1) issues.push(`expected one controller instance, observed ${identities.size}`);
  const instanceID = identities.size === 1 ? [...identities][0] : null;
  const sequences = [...new Set([...phases.values()].map(({ audit }) => audit.scaleWriteSequence))]
    .sort((left, right) => left - right);
  for (const [index, sequence] of sequences.entries()) {
    if (sequence !== sequences[0] + index) issues.push(`controller audit sequence gap before ${sequence}`);
  }

  const decisions = [];
  if (instanceID !== null) {
    for (const sequence of sequences) {
      const started = phases.get(`${instanceID}:${sequence}:started`);
      const finished = phases.get(`${instanceID}:${sequence}:finished`);
      if (started === undefined || finished === undefined) {
        issues.push(`controller audit sequence ${sequence} is missing a phase`);
        continue;
      }
      if (finished.elapsedMs < started.elapsedMs) {
        issues.push(`controller audit sequence ${sequence} finished before it started`);
        continue;
      }
      for (const field of ["workloadCount", "currentDesired", "target"]) {
        nonNegativeInteger(`controller audit ${field}`, started.audit[field]);
        if (started.audit[field] !== finished.audit[field]) {
          issues.push(`controller audit sequence ${sequence} changed ${field} between phases`);
        }
      }
      elapsedMilliseconds("controller audit batchAgeMs", started.audit.batchAgeMs);
      elapsedMilliseconds("controller audit batchIdleAgeMs", started.audit.batchIdleAgeMs);
      if (started.audit.batchTrigger !== "idle" && started.audit.batchTrigger !== "max") {
        issues.push(`controller audit sequence ${sequence} has unknown batch trigger`);
      }
      if (finished.audit.outcome !== "success" && finished.audit.outcome !== "error") {
        issues.push(`controller audit sequence ${sequence} has unknown outcome`);
      }
      decisions.push({
        elapsedMs: started.elapsedMs,
        finishedAt: finished.elapsedMs,
        sequence,
        mode: started.audit.mode,
        workloadCount: started.audit.workloadCount,
        currentDesired: started.audit.currentDesired,
        targetNodes: started.audit.target,
        batchTrigger: started.audit.batchTrigger,
        batchAgeMs: started.audit.batchAgeMs,
        batchIdleAgeMs: started.audit.batchIdleAgeMs,
        outcome: finished.audit.outcome,
        awsRequestId: finished.audit.awsRequestId ?? null,
      });
    }
  }

  const checkpoint = checkpoints.at(-1) ?? null;
  const highestSequence = sequences.at(-1) ?? 0;
  const lastFinishedAt = Math.max(
    ...decisions.map((decision) => decision.finishedAt),
    -1,
  );
  if (checkpoint === null) {
    issues.push("controller audit checkpoint is missing");
  } else {
    if (checkpoint.auditDroppedTotal !== 0) {
      issues.push(`controller audit delivery lost ${checkpoint.auditDroppedTotal} events`);
    }
    if (checkpoint.scaleWriteSequence !== highestSequence) {
      issues.push("controller audit checkpoint does not match the observed sequence tail");
    }
    if (checkpoint.elapsedMs < lastFinishedAt) {
      issues.push("controller audit checkpoint predates the observed sequence tail");
    }
  }

  return {
    instanceID,
    decisions,
    checkpoint,
    complete: issues.length === 0 && decisions.length > 0,
    issues,
  };
}

function summarizeASGActivityEvidence(events) {
  const evidence = events.filter((event) => event.asgActivityEvidence !== undefined);
  if (evidence.length === 0) return { complete: false, issues: ["ASG activity evidence is missing"] };
  const value = evidence.at(-1).asgActivityEvidence;
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("ASG activity evidence must be an object");
  }
  nonNegativeInteger("ASG activity finalDesired", value.finalDesired);
  if (typeof value.asgName !== "string" || value.asgName.length === 0) {
    throw new Error("ASG activity evidence requires asgName");
  }
  if (!Array.isArray(value.activities)) throw new Error("ASG activity evidence activities must be an array");
  nonNegativeInteger("ASG activity windowStartEpochMs", value.windowStartEpochMs);
  nonNegativeInteger("ASG activity windowEndEpochMs", value.windowEndEpochMs);
  const issues = [];
  if (value.complete !== true) issues.push("ASG activity pagination is incomplete");
  if (value.activities.length === 0) issues.push("ASG activity evidence is empty");
  if (value.windowEndEpochMs < value.windowStartEpochMs) issues.push("ASG activity evidence window regressed");
  const activityIDs = new Set();
  for (const activity of value.activities) {
    if (activity === null || typeof activity !== "object" || Array.isArray(activity)) {
      throw new Error("ASG scaling activity must be an object");
    }
    if (typeof activity.activityId !== "string" || activity.activityId.length === 0) {
      throw new Error("ASG scaling activity requires activityId");
    }
    if (activityIDs.has(activity.activityId)) issues.push("ASG scaling activities contain duplicate IDs");
    activityIDs.add(activity.activityId);
    const startedAt = Date.parse(activity.startTime);
    if (!Number.isFinite(startedAt)
      || startedAt < value.windowStartEpochMs
      || startedAt > value.windowEndEpochMs) {
      issues.push(`ASG scaling activity ${activity.activityId} is outside the evidence window`);
    }
    if (activity.statusCode !== "Successful") {
      issues.push(`ASG scaling activity ${activity.activityId} is not successful`);
    }
    if (activity.action !== "launch") {
      issues.push(`ASG scaling activity ${activity.activityId} is not an instance launch`);
    }
  }
  for (const prior of evidence.slice(0, -1)) {
    if (prior.asgActivityEvidence?.asgName !== value.asgName) {
      issues.push("ASG activity evidence changed ASG name within one run");
    }
  }

  return { ...value, complete: issues.length === 0, issues };
}

function baselineSnapshot(events) {
  // T0 opens the observation window. The runner does not create workload until
  // the observer publishes this first post-T0 cold-baseline event.
  const baseline = events[0]?.baseline === true ? events[0] : undefined;
  if (baseline === undefined) return null;

  const snapshot = {
    elapsedMs: baseline.elapsedMs,
    serverAdmitted: baseline.serverAdmitted,
    runningSandboxes: baseline.runningSandboxes,
    asgDesired: baseline.asgDesired ?? baseline.desired,
    ec2InService: ec2InService(baseline),
    nomadReady: nomadReady(baseline),
  };
  for (const [name, value] of Object.entries(snapshot)) {
    if (name !== "elapsedMs") nonNegativeInteger(`baseline.${name}`, value);
  }

  return snapshot;
}

export function summarizeObserverEvidence({ runId, logicalTarget, events }) {
  if (typeof runId !== "string" || runId.length === 0) {
    throw new Error("runId is required");
  }
  positiveInteger("logicalTarget", logicalTarget);
  if (!Array.isArray(events)) throw new Error("observer events must be an array");

  let previousElapsedMs = -1;
  const ordered = events.map((event, index) => {
    if (event === null || typeof event !== "object" || Array.isArray(event)) {
      throw new Error(`observer event ${index} must be an object`);
    }
    if (event.runId !== runId) {
      throw new Error(`observer event ${index} runId does not match benchmark runId`);
    }
    elapsedMilliseconds(`observer event ${index} elapsedMs`, event.elapsedMs);
    if (event.elapsedMs < previousElapsedMs) {
      throw new Error(`observer event ${index} elapsedMs is not monotonic`);
    }
    previousElapsedMs = event.elapsedMs;

    return event;
  });

  const serverAdmissionTimeline = [];
  let serverAdmitted;
  let serverFirstAdmittedAt = null;
  let serverAdmissionSourceComplete = true;
  const expectedRunHash = benchmarkRunHash(runId);
  for (const event of ordered) {
    if (event.serverAdmitted === undefined) continue;
    if (event.benchmarkRunHash !== expectedRunHash) {
      throw new Error("serverAdmitted evidence is not correlated to this benchmark run");
    }
    nonNegativeInteger("serverAdmitted", event.serverAdmitted);
    if (event.serverAdmitted > logicalTarget) {
      throw new Error("serverAdmitted exceeds logicalTarget; observer evidence is not run-scoped");
    }
    if (serverAdmitted !== undefined && event.serverAdmitted < serverAdmitted) {
      throw new Error("serverAdmitted regressed within one benchmark run");
    }
    let admissionElapsedMs = event.elapsedMs;
    if (event.serverAdmitted > 0) {
      const firstElapsedMs = event.serverAdmissionFirstElapsedMs;
      const lastElapsedMs = event.serverAdmissionLastElapsedMs;
      if (firstElapsedMs === undefined || lastElapsedMs === undefined) {
        serverAdmissionSourceComplete = false;
      } else {
        elapsedMilliseconds("server admission first source time", firstElapsedMs);
        elapsedMilliseconds("server admission last source time", lastElapsedMs);
        if (firstElapsedMs > lastElapsedMs || lastElapsedMs > event.elapsedMs) {
          throw new Error("server admission source times are inconsistent with observer emission");
        }
        serverFirstAdmittedAt = serverFirstAdmittedAt === null
          ? firstElapsedMs
          : Math.min(serverFirstAdmittedAt, firstElapsedMs);
        admissionElapsedMs = lastElapsedMs;
      }
    }
    if (event.serverAdmitted !== serverAdmitted) {
      serverAdmissionTimeline.push({ elapsedMs: admissionElapsedMs, value: event.serverAdmitted });
      serverAdmitted = event.serverAdmitted;
    }
  }

  const controllerAudit = summarizeControllerAudit(ordered);
  const decisionTimeline = controllerAudit.decisions;
  const decision = decisionTimeline[0] ?? null;
  const asgActivityEvidence = summarizeASGActivityEvidence(ordered);
  const baseline = baselineSnapshot(ordered);
  const asgDesiredTimeline = changedCountTimeline(
    ordered,
    "asg desired",
    (event) => event.asgDesired ?? event.desired,
  );
  const ec2InServiceTimeline = changedCountTimeline(ordered, "EC2 InService", ec2InService);
  const nomadReadyTimeline = changedCountTimeline(ordered, "Nomad ready", nomadReady);
  const missingEvidence = [];
  if (serverAdmitted === undefined) missingEvidence.push("serverAdmitted");
  if (!serverAdmissionSourceComplete) missingEvidence.push("serverAdmissionSourceTimes");
  if (!controllerAudit.complete) missingEvidence.push("controllerAudit");
  if (!asgActivityEvidence.complete) missingEvidence.push("asgActivityEvidence");
  if (asgDesiredTimeline.length === 0) missingEvidence.push("asgDesired");
  if (ec2InServiceTimeline.length === 0) missingEvidence.push("ec2InService");
  if (nomadReadyTimeline.length === 0) missingEvidence.push("nomadReady");
  if (baseline === null) missingEvidence.push("baseline");

  return {
    serverAdmitted: serverAdmitted ?? null,
    allDemandObservedByServer: serverAdmitted === logicalTarget,
    serverAdmittedAt: serverAdmissionTimeline.find((point) => point.value === logicalTarget)?.elapsedMs ?? null,
    serverFirstAdmittedAt,
    serverAdmissionSourceComplete,
    serverAdmissionTimeline,
    firstScaleDecisionAt: decision?.elapsedMs ?? null,
    firstScaleDecision: decision,
    scaleDecisionTimeline: decisionTimeline,
    controllerAuditComplete: controllerAudit.complete,
    controllerAuditIssues: controllerAudit.issues,
    controllerInstanceID: controllerAudit.instanceID,
    controllerAuditCheckpoint: controllerAudit.checkpoint,
    asgActivityEvidence,
    baseline,
    asgDesiredTimeline,
    ec2InServiceTimeline,
    nomadReadyTimeline,
    complete: missingEvidence.length === 0,
    missingEvidence,
  };
}

export function summarizeClientMilestones(events) {
  if (!Array.isArray(events)) throw new Error("client events must be an array");
  const definitions = {
    client_queued: "clientQueued",
    request_started: "requestStarted",
    create_response: "createResponse",
    guest_ready: "guestReady",
  };
  const summary = Object.fromEntries(
    Object.values(definitions).map((name) => [name, { count: 0, firstAt: null, lastAt: null }]),
  );

  for (const [index, event] of events.entries()) {
    const name = definitions[event?.type];
    if (name === undefined) continue;
    elapsedMilliseconds(`client event ${index} elapsedMs`, event.elapsedMs);
    const milestone = summary[name];
    milestone.count += 1;
    milestone.firstAt ??= event.elapsedMs;
    milestone.lastAt = event.elapsedMs;
  }

  return summary;
}

export function buildBenchmarkSummary({
  runId,
  target,
  concurrency,
  maximumClientConcurrency,
  expectedNodes,
  perNode,
  completed,
  failed,
  clientEvents,
  observerEvents,
  readyMs,
  observerWorkloadStartOffsetMs = 0,
  batchMaxDurationMs = 10_000,
  reconcileLagBudgetMs = 2_000,
}) {
  positiveInteger("target", target);
  positiveInteger("concurrency", concurrency);
  positiveInteger("expectedNodes", expectedNodes);
  positiveInteger("perNode", perNode);
  positiveInteger("batchMaxDurationMs", batchMaxDurationMs);
  nonNegativeInteger("reconcileLagBudgetMs", reconcileLagBudgetMs);
  elapsedMilliseconds("observerWorkloadStartOffsetMs", observerWorkloadStartOffsetMs);
  nonNegativeInteger("completed", completed);
  nonNegativeInteger("failed", failed);
  const clientMilestones = summarizeClientMilestones(clientEvents);
  const observerEvidence = summarizeObserverEvidence({
    runId,
    logicalTarget: target,
    events: observerEvents,
  });
  const admission = observerEvidence.serverAdmitted === null
    ? {
      logicalTarget: target,
      requestsStarted: clientMilestones.requestStarted.count,
      serverAdmitted: null,
      maximumClientConcurrency: nonNegativeInteger("maximumClientConcurrency", maximumClientConcurrency),
      allDemandObservedByServer: false,
    }
    : admissionReport({
      logicalTarget: target,
      requestsStarted: clientMilestones.requestStarted.count,
      serverAdmitted: observerEvidence.serverAdmitted,
      maximumClientConcurrency,
    });
  const successfulDecisions = observerEvidence.scaleDecisionTimeline.filter((decision) => decision.outcome === "success");
  const targetDecision = successfulDecisions.at(-1);
  const decisionsCorrect = successfulDecisions.length > 0
    && successfulDecisions.every((decision, index) => (
      decision.mode === "start-intent-v1"
      && decision.targetNodes === Math.ceil(decision.workloadCount / perNode)
      && decision.targetNodes > decision.currentDesired
      && decision.targetNodes <= expectedNodes
      && decision.batchAgeMs <= batchMaxDurationMs + reconcileLagBudgetMs
      && (index === 0 || (
        decision.currentDesired >= successfulDecisions[index - 1].targetNodes
        && decision.targetNodes > successfulDecisions[index - 1].targetNodes
      ))
    ))
    && targetDecision?.targetNodes === expectedNodes;
  const targetASGObservedAt = observerEvidence.asgDesiredTimeline.find(
    (point) => point.value === expectedNodes && point.elapsedMs >= (targetDecision?.elapsedMs ?? Infinity),
  )?.elapsedMs;
  const inServiceTargetAt = observerEvidence.ec2InServiceTimeline.find((point) => point.value === expectedNodes)?.elapsedMs;
  const nomadTargetAt = observerEvidence.nomadReadyTimeline.find((point) => point.value === expectedNodes)?.elapsedMs;
  const lastGuestReadyAt = clientMilestones.guestReady.lastAt;
  const lastGuestReadyAtOnObserverClock = lastGuestReadyAt === null
    ? null
    : observerWorkloadStartOffsetMs + lastGuestReadyAt;
  const terminalEvidenceAt = Math.max(
    ...[
      observerEvidence.serverAdmittedAt,
      targetASGObservedAt,
      inServiceTargetAt,
      nomadTargetAt,
      lastGuestReadyAtOnObserverClock,
    ].filter((value) => value !== null && value !== undefined),
    -1,
  );
  const terminalAuditCheckpointObserved = observerEvidence.controllerAuditCheckpoint !== null
    && observerEvidence.controllerAuditCheckpoint.generatedElapsedMs >= terminalEvidenceAt;
  const expectedLaunches = expectedNodes - (observerEvidence.baseline?.asgDesired ?? expectedNodes);
  const activityEvidenceConsistent = observerEvidence.asgActivityEvidence.complete
    && observerEvidence.asgActivityEvidence.finalDesired === expectedNodes
    && observerEvidence.asgActivityEvidence.activities.length === expectedLaunches;
  const asgDesiredMonotonic = observerEvidence.asgDesiredTimeline.every(
    (point, index, timeline) => index === 0 || point.value >= timeline[index - 1].value,
  );
  const targetCapacityObserved = observerEvidence.controllerAuditComplete
    && terminalAuditCheckpointObserved
    && decisionsCorrect
    && observerEvidence.serverAdmittedAt !== null
    && targetASGObservedAt !== undefined
    && activityEvidenceConsistent
    && asgDesiredMonotonic
    && Math.max(...observerEvidence.scaleDecisionTimeline.map((decision) => decision.targetNodes), -1) === expectedNodes
    && Math.max(...observerEvidence.asgDesiredTimeline.map((point) => point.value), -1) === expectedNodes
    && observerEvidence.asgDesiredTimeline.at(-1)?.value === expectedNodes
    && Math.max(...observerEvidence.ec2InServiceTimeline.map((point) => point.value), -1) === expectedNodes
    && observerEvidence.ec2InServiceTimeline.at(-1)?.value === expectedNodes
    && Math.max(...observerEvidence.nomadReadyTimeline.map((point) => point.value), -1) === expectedNodes
    && observerEvidence.nomadReadyTimeline.at(-1)?.value === expectedNodes;
  const coldBaselineObserved = observerEvidence.baseline?.serverAdmitted === 0
    && observerEvidence.baseline?.runningSandboxes === 0
    && observerEvidence.baseline?.asgDesired === 1
    && observerEvidence.baseline?.ec2InService === 1
    && observerEvidence.baseline?.nomadReady === 1;
  const scaleWriteSteps = observerEvidence.baseline === null
    ? successfulDecisions.map((decision) => decision.targetNodes)
    : [observerEvidence.baseline.asgDesired, ...successfulDecisions.map((decision) => decision.targetNodes)];
  const firstIntentAt = observerEvidence.serverFirstAdmittedAt;
  const durationBetween = (name, start, end) => {
    if (start === undefined || start === null || end === undefined || end === null) return null;
    if (end < start) throw new Error(`${name} has a negative duration`);
    return end - start;
  };
  const signedDifference = (start, end) => {
    if (start === undefined || start === null || end === undefined || end === null) return null;
    return end - start;
  };
  const missingObserverEvidence = [...observerEvidence.missingEvidence];
  if (!terminalAuditCheckpointObserved) missingObserverEvidence.push("terminalAuditCheckpoint");

  return {
    runId,
    target,
    concurrency,
    expectedNodes,
    ...admission,
    completed,
    failed,
    milestones: {
      ...clientMilestones,
      serverAdmitted: {
        count: observerEvidence.serverAdmitted,
        allAdmittedObservedAt: observerEvidence.serverAdmittedAt,
        timeline: observerEvidence.serverAdmissionTimeline,
      },
      firstScaleDecisionAt: observerEvidence.firstScaleDecisionAt,
      firstScaleDecision: observerEvidence.firstScaleDecision,
      scaleDecisions: observerEvidence.scaleDecisionTimeline,
      baseline: observerEvidence.baseline,
      asgDesired: observerEvidence.asgDesiredTimeline,
      ec2InService: observerEvidence.ec2InServiceTimeline,
      nomadReady: observerEvidence.nomadReadyTimeline,
    },
    observerEvidenceComplete: observerEvidence.complete && terminalAuditCheckpointObserved,
    terminalAuditCheckpointObserved,
    coldBaselineObserved,
    targetCapacityObserved,
    scaleWriteSteps,
    singleStepScaleout: scaleWriteSteps.length === 2
      && scaleWriteSteps[0] === 1
      && scaleWriteSteps[1] === expectedNodes,
    timings: {
      firstIntentToFirstScaleWrite: durationBetween("firstIntentToFirstScaleWrite", firstIntentAt, successfulDecisions[0]?.elapsedMs),
      allAdmittedToDesiredTarget: durationBetween("allAdmittedToDesiredTarget", observerEvidence.serverAdmittedAt, targetASGObservedAt),
      desiredTargetToInServiceTarget: durationBetween("desiredTargetToInServiceTarget", targetASGObservedAt, inServiceTargetAt),
      inServiceTargetToNomadReadyTarget: durationBetween("inServiceTargetToNomadReadyTarget", inServiceTargetAt, nomadTargetAt),
      // These barriers come from independent observers and have no causal order.
      nomadReadyTargetToLastGuestReady: signedDifference(nomadTargetAt, lastGuestReadyAtOnObserverClock),
    },
    missingObserverEvidence,
    readyMs,
    observerWorkloadStartOffsetMs,
    batchMaxDurationMs,
    reconcileLagBudgetMs,
  };
}

export function benchmarkObserverEvidenceAccepted(summary) {
  return summary.observerEvidenceComplete
    && summary.coldBaselineObserved
    && summary.allDemandObservedByServer
    && summary.targetCapacityObserved;
}

export async function waitForBenchmarkObserverEvidence({
  readSummary,
  timeoutMs,
  pollIntervalMs = 250,
  now = Date.now,
  sleep = (delayMs) => new Promise((resolve) => setTimeout(resolve, delayMs)),
}) {
  if (typeof readSummary !== "function") throw new Error("readSummary must be a function");
  positiveInteger("observer evidence timeoutMs", timeoutMs);
  positiveInteger("observer evidence pollIntervalMs", pollIntervalMs);

  const deadline = now() + timeoutMs;
  while (true) {
    let summary;
    let writeInProgress;
    try {
      summary = await readSummary();
      if (benchmarkObserverEvidenceAccepted(summary)) return summary;
    } catch (error) {
      if (!(error instanceof ObserverJSONLWriteInProgressError)) throw error;
      writeInProgress = error;
    }

    const remainingMs = deadline - now();
    if (remainingMs <= 0) {
      if (writeInProgress !== undefined) {
        throw new Error("observer JSONL tail remained incomplete until the settle timeout", {
          cause: writeInProgress,
        });
      }
      return summary;
    }
    await sleep(Math.min(pollIntervalMs, remainingMs));
  }
}

export async function collectBenchmarkSummary({
  failureCount,
  buildSummary,
  readObserverEvents,
  observerSettleTimeoutMs,
}) {
  nonNegativeInteger("failureCount", failureCount);
  if (typeof buildSummary !== "function") throw new Error("buildSummary must be a function");
  if (typeof readObserverEvents !== "function") throw new Error("readObserverEvents must be a function");

  // A failed Sandbox request is authoritative. Capture the observer once for
  // secondary diagnostics, but do not wait for it or let malformed evidence
  // replace the primary request failure.
  if (failureCount > 0) {
    try {
      return {
        ...buildSummary(readObserverEvents()),
        observerDiagnosticError: null,
      };
    } catch (error) {
      return {
        ...buildSummary([]),
        observerDiagnosticError: String(error),
      };
    }
  }

  return waitForBenchmarkObserverEvidence({
    readSummary: () => buildSummary(readObserverEvents()),
    timeoutMs: observerSettleTimeoutMs,
  });
}

export function validateBenchmarkConfig({ target, concurrency, perNode, templateAlias }) {
  positiveInteger("target", target);
  positiveInteger("concurrency", concurrency);
  positiveInteger("perNode", perNode);
  if (typeof templateAlias !== "string" || templateAlias.trim().length === 0) {
    throw new Error("E2B_TEMPLATE_ALIAS is required");
  }
  if (concurrency !== target) {
    throw new Error("concurrency must equal target for the formal cold-start benchmark");
  }

  return {
    target,
    concurrency,
    perNode,
    expectedNodes: Math.ceil(target / perNode),
    templateAlias,
  };
}

export function validateApiTransportCapacity({
  apiUrl,
  concurrency,
  connectionLimit,
  inflightLimit,
}) {
  positiveInteger("concurrency", concurrency);
  positiveInteger("E2B_API_CONNECTIONS", connectionLimit);
  nonNegativeInteger("E2B_API_INFLIGHT_REQUESTS", inflightLimit);

  let protocol;
  try {
    protocol = new URL(apiUrl).protocol;
  } catch (error) {
    throw new Error("E2B_API_URL must be an absolute URL", { cause: error });
  }
  if (protocol === "http:" && connectionLimit < concurrency) {
    throw new Error(
      `E2B_API_CONNECTIONS must be at least ${concurrency} for a plaintext HTTP benchmark`,
    );
  }
  if (inflightLimit !== 0 && inflightLimit < concurrency) {
    throw new Error(
      `E2B_API_INFLIGHT_REQUESTS must be 0 or at least ${concurrency}`,
    );
  }

  return { connectionLimit, inflightLimit };
}
