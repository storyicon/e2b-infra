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

function firstScaleDecision(events) {
  for (const event of events) {
    const decision = event.controller;
    if (decision === undefined) continue;
    if (decision === null || typeof decision !== "object" || Array.isArray(decision)) {
      throw new Error("controller evidence must be an object");
    }
    nonNegativeInteger("controller.currentDesired", decision.currentDesired);
    nonNegativeInteger("controller.targetNodes", decision.targetNodes);
    if (decision.targetNodes <= decision.currentDesired) continue;

    const result = {
      elapsedMs: event.elapsedMs,
      mode: decision.mode ?? null,
      workloadCount: decision.workloadCount ?? null,
      currentDesired: decision.currentDesired,
      targetNodes: decision.targetNodes,
      readyNodes: decision.readyNodes ?? null,
      capped: decision.capped ?? null,
      outcome: decision.outcome ?? null,
    };
    for (const name of ["workloadCount", "readyNodes"]) {
      if (result[name] !== null) nonNegativeInteger(`controller.${name}`, result[name]);
    }

    return result;
  }

  return null;
}

export function summarizeObserverEvidence({ runId, logicalTarget, events }) {
  if (typeof runId !== "string" || runId.length === 0) {
    throw new Error("runId is required");
  }
  positiveInteger("logicalTarget", logicalTarget);
  if (!Array.isArray(events)) throw new Error("observer events must be an array");

  const ordered = events.map((event, index) => {
    if (event === null || typeof event !== "object" || Array.isArray(event)) {
      throw new Error(`observer event ${index} must be an object`);
    }
    if (event.runId !== runId) {
      throw new Error(`observer event ${index} runId does not match benchmark runId`);
    }
    elapsedMilliseconds(`observer event ${index} elapsedMs`, event.elapsedMs);

    return event;
  }).toSorted((left, right) => left.elapsedMs - right.elapsedMs);

  const serverAdmissionTimeline = [];
  let serverAdmitted;
  for (const event of ordered) {
    if (event.serverAdmitted === undefined) continue;
    nonNegativeInteger("serverAdmitted", event.serverAdmitted);
    if (event.serverAdmitted > logicalTarget) {
      throw new Error("serverAdmitted exceeds logicalTarget; observer evidence is not run-scoped");
    }
    if (serverAdmitted !== undefined && event.serverAdmitted < serverAdmitted) {
      throw new Error("serverAdmitted regressed within one benchmark run");
    }
    if (event.serverAdmitted !== serverAdmitted) {
      serverAdmissionTimeline.push({ elapsedMs: event.elapsedMs, value: event.serverAdmitted });
      serverAdmitted = event.serverAdmitted;
    }
  }

  const decision = firstScaleDecision(ordered);
  const asgDesiredTimeline = changedCountTimeline(
    ordered,
    "asg desired",
    (event) => event.asgDesired ?? event.desired,
  );
  const ec2InServiceTimeline = changedCountTimeline(ordered, "EC2 InService", ec2InService);
  const nomadReadyTimeline = changedCountTimeline(ordered, "Nomad ready", nomadReady);
  const missingEvidence = [];
  if (serverAdmitted === undefined) missingEvidence.push("serverAdmitted");
  if (decision === null) missingEvidence.push("firstScaleDecision");
  if (asgDesiredTimeline.length === 0) missingEvidence.push("asgDesired");
  if (ec2InServiceTimeline.length === 0) missingEvidence.push("ec2InService");
  if (nomadReadyTimeline.length === 0) missingEvidence.push("nomadReady");

  return {
    serverAdmitted: serverAdmitted ?? null,
    allDemandObservedByServer: serverAdmitted === logicalTarget,
    serverAdmittedAt: serverAdmissionTimeline.find((point) => point.value === logicalTarget)?.elapsedMs ?? null,
    serverAdmissionTimeline,
    firstScaleDecisionAt: decision?.elapsedMs ?? null,
    firstScaleDecision: decision,
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
  completed,
  failed,
  clientEvents,
  observerEvents,
  readyMs,
}) {
  positiveInteger("target", target);
  positiveInteger("concurrency", concurrency);
  positiveInteger("expectedNodes", expectedNodes);
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
      asgDesired: observerEvidence.asgDesiredTimeline,
      ec2InService: observerEvidence.ec2InServiceTimeline,
      nomadReady: observerEvidence.nomadReadyTimeline,
    },
    observerEvidenceComplete: observerEvidence.complete,
    missingObserverEvidence: observerEvidence.missingEvidence,
    readyMs,
  };
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
