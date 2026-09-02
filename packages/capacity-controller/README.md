# Capacity controller

The capacity controller scales an AWS Auto Scaling group from E2B's
cluster-scoped sandbox workload. Scale-out is always available. Safe scale-in
is separately gated and disabled by default. The process accepts no public
traffic. `CAPACITY_DEMAND_MODE` selects exactly one input:

- `legacy-failure-ledger` reads pending, fulfilled, and direct-success state
  from Redis.
- `start-intent-v1` reads the API's deduplicated workload snapshot through the
  private internal gRPC listener.

An unknown mode fails configuration validation. A snapshot read failure fails
that reconciliation without writing ASG desired capacity; it never falls back
to the legacy ledger.

In `legacy-failure-ledger`, each reconciliation reads the current pending
demand from Redis, counts ready and scheduling-eligible nodes in the configured
Nomad node pool, and reads the ASG desired capacity. A pending burst is anchored
to the desired capacity seen when the burst starts:

```text
burst_demand = queued_fulfilled + added_node_direct_success + pending
target = burst_base_desired + ceil(burst_demand / slots_per_node)
```

The target is clamped to `MIN_NODES..MAX_NODES` and is only written when it is
greater than the current ASG desired capacity. The burst ends when pending
demand drains to zero. Direct successes observed while only baseline nodes are
ready are absorbed into the baseline; after added nodes are ready they remain
part of the burst. This prevents booting nodes from causing the same burst to be
added repeatedly while Nomad readiness changes, without losing requests that
consume new-node capacity before entering the pending ledger.

In `start-intent-v1`, the API snapshot is the authority for workload count:

```text
required = ceil(workload_count / slots_per_node)
target = clamp(max(current_asg_desired, required, min_nodes), max_nodes)
```

Current ASG desired includes nodes that are starting but not yet Nomad ready,
so low ready count does not request the same nodes twice. This mode has no
process-local burst baseline and recomputes the same target after restart.

## Safe scale-in

Safe scale-in is available only with `start-intent-v1` and has three explicit
modes:

- `off` performs no scale-in reads or writes and is the default.
- `observe` calculates candidates and budgets without changing Nomad, workers,
  or AWS.
- `enforce` runs a bounded, recoverable drain transaction.

The controller retains workload headroom and requires excess accepting capacity
to remain stable before opening a drain. Candidate summaries are only a cheap
filter. Each transaction then binds the Nomad node, EC2 instance, and worker
service-instance identity and follows this order:

```text
owned Nomad drain -> worker Draining -> live empty verification
                  -> final workload/Nomad/worker/ASG re-read
                  -> exact EC2 instance termination
```

The worker rejects new create/resume admissions once Draining wins its admission
lock. It reports `shutdown_ready` only after live sandboxes, starts/resumes,
lifecycle cleanup, and snapshot uploads all reach zero. The API additionally
requires a fresh `Sandbox.List` to be empty. The controller never migrates or
kills a sandbox to make a node empty.

At most 50 owned operations may be active. Empty workers may use that rolling
window; non-empty graceful drains are additionally limited to 10% of ready
workers. A demand increase stops new drains and restores enough uncommitted
workers in Nomad-before-worker order. Scale-out keeps its existing raw workload
formula if any scale-in observation fails.

The irreversible write is
`TerminateInstanceInAutoScalingGroup(ShouldDecrementDesiredCapacity=true)` for
the exact verified instance. The SDK does not retry it, and there is no
`SetDesiredCapacity` scale-in fallback. Explicit AWS rejection restores the
owned drain; an ambiguous transport outcome stays isolated and is reconciled by
ASG membership and scaling-activity reads.

Run one controller replica. Legacy burst state remains process-local for the
explicit rollback mode.

## Configuration

Required:

- `CAPACITY_DEMAND_MODE` (`legacy-failure-ledger` or `start-intent-v1`)
- `E2B_CLUSTER_ID`
- `NOMAD_NODE_POOL`
- `AWS_ASG_NAME`
- `AWS_REGION`

Required only for `legacy-failure-ledger`:

- exactly one of `REDIS_URL` or `REDIS_CLUSTER_URL`

Required only for `start-intent-v1`:

- `CAPACITY_SNAPSHOT_GRPC_ADDRESS`
- `CAPACITY_SNAPSHOT_SERVICE_TOKEN`

Optional:

- `REDIS_PASSWORD`
- `REDIS_TLS_ENABLED` (default `false`)
- `REDIS_TLS_CA_BASE64`
- `SLOTS_PER_NODE` (default `20`)
- `MIN_NODES` (default `1`)
- `MAX_NODES` (default `30`)
- `RECONCILE_INTERVAL` (default `1s`)
- `SCALE_IN_MODE` (`off`, `observe`, or `enforce`; default `off`)
- `SCALE_IN_HEADROOM_PERCENT` (default `10`)
- `SCALE_IN_STABILIZATION_DURATION` (default `2m`)
- `SCALE_IN_MIN_NODE_AGE` (default `10m`)
- `SCALE_IN_DRAIN_TIMEOUT` (default `15m`)

Nomad configuration uses the standard Nomad client environment variables,
including `NOMAD_ADDR`, `NOMAD_TOKEN`, `NOMAD_CACERT`, `NOMAD_CLIENT_CERT`, and
`NOMAD_CLIENT_KEY`. AWS credentials use the default AWS SDK credential chain;
no credential is accepted as a controller-specific setting.

The runtime role needs ASG/EC2 describe access plus
`autoscaling:SetDesiredCapacity` for the exact configured ASG. Terraform adds
`autoscaling:TerminateInstanceInAutoScalingGroup` for that group only when
`SCALE_IN_MODE=enforce`; off and observe deployments do not receive it.
The Nomad token needs node write access to the configured node pool. Redis authentication
must use TLS; the shared Redis factory rejects a plaintext password. The
capacity snapshot address must resolve to the API's private internal gRPC
listener. The service token is sent as bearer metadata and is never logged.

## Migration and rollback

Migration is explicit; `dual-write` is not an error fallback:

1. Deploy the API and start-intent storage code while both API and controller
   remain in `legacy-failure-ledger`.
2. Set only the API to `dual-write`. It writes both namespaces, while the
   controller continues to read only the legacy namespace.
3. Compare the legacy and start-intent observations and verify the internal
   snapshot authentication and workload union.
4. Before switching the controller, verify that every running Sandbox in the
   target cluster uses the configured pool vCPU and memory. Drain the cluster
   first if older records do not carry trustworthy resource values. CPU
   architecture, family, and model are not recoverable from existing running
   records, so the operator must also verify that the cluster is already a
   single CPU pool.
5. Set the controller to `start-intent-v1`. It then reads only the authenticated
   API snapshot. Empty data or read errors never switch its mode.

To roll back, explicitly set the controller to `legacy-failure-ledger` and
restart it before changing the API writer. Keep the API in `dual-write` until
the rollback is verified. Do not remove the legacy namespace, fulfilled/direct
success accounting, or dual-write path until the new mode has completed its
defined stable observation period.

Before enabling the Terraform job, upload its private S3 artifact for the
active AWS environment:

```sh
make build-and-upload/capacity-controller PROVIDER=aws
```

Enabling `capacity_autoscaler_enabled` also enables API-side placement waiting
with a 120-second default timeout and 500-millisecond retry interval. Both are
configurable through `capacity_api_wait_timeout` and
`capacity_api_retry_interval`. When the API writes start intents (`dual-write`
or `start-intent-v1`), `capacity_api_pool_vcpu`,
`capacity_api_pool_memory_mib`, and the pool CPU architecture/family/model are
required and define the exact single-pool resource and CPU shape accepted by
the API before an intent is persisted. Incompatible API/controller migration mode
pairs fail Terraform validation. In `start-intent-v1`, the ALB idle timeout
defaults to 240 seconds. This stays above the API's 70-second normal request
budget, the default 120-second capacity wait, and 5 seconds of server grace.
Keep `capacity_ingress_idle_timeout_seconds` at least 75 seconds above any
customized wait timeout. Legacy autoscaling retains its 180-second ALB idle
timeout and 150-second Nomad kill timeout; disabled autoscaling retains 60 and
150 seconds respectively.

`capacity_controller_slots_per_node` configures both the controller's per-node
capacity model and the orchestrator's hard running-sandbox admission. The
orchestrator's concurrent-start limit defaults to the same value and can be
lowered independently with `capacity_controller_max_starting_per_node` without
changing the controller's eventual capacity target.

In `start-intent-v1`, the controller batches visible workload until either
`capacity_controller_batch_idle_duration` (one second by default) passes
without a change or `capacity_controller_batch_max_duration` (ten seconds by
default) passes from the first unmet demand. The max boundary prevents a
continuously growing request stream from postponing scale-out indefinitely.
Every scale decision uses the latest workload snapshot; a successful write
ends that batch, and later unmet demand starts a new one without waiting for
the requested nodes to become Nomad-ready. A controller restart discards its
in-memory batch timestamps and starts from a new snapshot.

## Cold-start benchmark

`packages/shared/scripts/run-capacity-smoke.mjs` measures demand-driven scale-out
from a known baseline. It requires an explicit `E2B_TEMPLATE_ALIAS`; the repository
does not contain a deployment-specific template name. The runner also requires a
run-scoped observer JSONL file through `CAPACITY_OBSERVER_EVENTS_PATH` and fails
before creating workload if the observer contract or SDK transport limits are not
satisfied. Every observer record must end with a newline; the newline is the
commit boundary that lets the runner distinguish a published record from a
concurrent partial append.

Formal acceptance requires the first observer event to be a post-T0 cold
baseline recorded before the runner creates workload. The runner resets its
latency clock only after this barrier, so observer startup delay is not counted
as Sandbox cold-start latency. Controller audit events must contain one process
identity, contiguous write sequences, paired started/finished phases, and a
zero-loss checkpoint newer than the final admission/capacity/guest evidence. Every successful target
must equal the capacity required by the workload visible at that decision;
progressive `[1, 5, 25]` and single-step `[1, 25]` scale-out are both valid when
their decisions are correct. The observer must also archive complete ASG
instance-launch scaling activities, and ASG desired, EC2 InService, and Nomad ready must finish
at the expected node count without overshoot. Missing or conflicting evidence
invalidates the benchmark rather than being treated as proof that no hidden
scale write occurred.

Admission milestones use the first and last matching API journal timestamps,
not the observer polling timestamp. Negative cross-stage durations are rejected
instead of being published as performance evidence.

After all sandbox requests succeed, the runner waits up to 30 seconds for the
asynchronous observer to record that final capacity state. Override this bound
with `CAPACITY_OBSERVER_SETTLE_TIMEOUT_MS` when the observer samples less
frequently. The runner still fails closed if the evidence remains incomplete.

The load generator must run without AWS credential-provider environment variables
and with IMDS blocked. It never calls Auto Scaling and never retries
`Sandbox.create`; infrastructure observations come from the separate read-only
observer. The observer implementation is
`packages/shared/scripts/capacity-smoke-observer.py`; every deployment-specific
account, region, profile, ASG, systemd unit, and Nomad URL is a required command
argument rather than a repository default. Its minimal IAM policy is
`packages/shared/scripts/capacity-smoke-observer-policy.json`. AWS Auto Scaling
does not support resource-level restrictions for these Describe actions, so the
observer additionally verifies the explicit account and passes exactly one ASG
name to both APIs. This policy is not attached to the controller or load
generator by Terraform.

The observer must start before the shared future T0. After T0, it waits for a
successful zero-workload controller snapshot, records current ASG/Nomad state,
and publishes the cold baseline. The runner waits at this barrier before creating
workload. The observer then collects run-scoped admission counts, controller audit
events, current capacity, and fully paginated scaling activities. Example:

```sh
python3 packages/shared/scripts/capacity-smoke-observer.py \
  --run-id "$BENCHMARK_RUN_ID" \
  --t0 "$BENCHMARK_START_EPOCH_MS" \
  --output "$CAPACITY_OBSERVER_EVENTS_PATH" \
  --target 500 \
  --credential-source instance-role \
  --aws-region "$AWS_REGION" \
  --expected-aws-account "$EXPECTED_AWS_ACCOUNT" \
  --asg-name "$AWS_ASG_NAME" \
  --api-unit "$API_SYSTEMD_UNIT" \
  --controller-unit "$CAPACITY_CONTROLLER_SYSTEMD_UNIT" \
  --nomad-nodes-url http://127.0.0.1:4646/v1/nodes \
  --capacity-mode dual-write
```

Run the deterministic model and observer tests with:

```sh
node --test packages/shared/scripts/capacity-smoke-model.test.mjs
python3 -m unittest packages/shared/scripts/capacity-smoke-observer_test.py
```
