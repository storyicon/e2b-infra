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
- `enforce` runs bounded, recoverable drain and ASG reconciliation.

The controller retains workload headroom and requires excess accepting capacity
to remain stable before opening a drain. Candidate summaries are only a cheap
filter. Each transaction then binds the Nomad node, EC2 instance, and worker
service-instance identity and follows this order:

```text
owned Nomad drain -> worker Draining -> live empty verification
                  -> final workload/Nomad/worker/ASG re-read
                  -> remove instance scale-in protection (armed)
                  -> set absolute ASG desired capacity
```

The worker rejects new create/resume admissions once Draining wins its admission
lock. It reports `shutdown_ready` only after live sandboxes, starts/resumes,
lifecycle cleanup, and snapshot uploads all reach zero. The API additionally
requires a fresh `Sandbox.List` to be empty. The controller never migrates or
kills a sandbox to make a node empty.

New sandbox-client ASG instances start with instance scale-in protection. Normal
ASG scale-in cannot select a protected instance. Only a worker that has a
controller-owned drain, matching operation and service identity, current
`shutdown_ready=true`, a fresh empty `Sandbox.List`, and healthy `InService`
infrastructure may have protection removed. Such a worker is **armed**: ASG may
select it, but Worker and Nomad admission remain closed until it leaves the group
or is safely restored. Ordinary, busy, unknown, unhealthy, and mismatched
instances remain protected.

The ASG is **settled** when member count equals desired capacity, every member is
`InService`, and no previous termination or active instance refresh is in
progress. Only then may the controller lower desired capacity:

```text
reduction = min(armed_count, desired - safe_required, 50)
target_desired = desired - reduction
```

At most 50 owned operations may be active, matching the protection API batch
limit. Empty workers may use that rolling window; non-empty graceful drains are
additionally limited to 10% of ready workers. After an absolute desired write,
the controller waits until membership converges before starting another batch.
A lost response or controller restart is handled by reading current desired,
membership, lifecycle, and protection again; no process-local token or relative
decrement is required.

A demand increase stops new drains. If membership is above desired, the
controller first raises desired to at least current membership and safe demand,
removing the pending scale-in deficit. It then protects each surviving
`InService` armed worker, confirms that protection from a fresh ASG read,
restores the matching Worker operation, and restores Nomad eligibility last.
Workers already in `Terminating*` stay Draining and scale-out supplies any
replacement capacity. Scale-out keeps its existing raw-workload formula if any
scale-in observation fails.

The following table is the authoritative transition contract. An operation may
perform only the write shown for its observed state. Every retry first reads the
current state again; a successful write with a lost response therefore converges
through the same row or the next row instead of relying on process memory.

| Observed state | Owner | Only permitted next write | Re-entry condition | Terminal condition |
| --- | --- | --- | --- | --- |
| Protected ordinary worker | none | mark the Nomad node draining with a new operation ID | candidate identity and all safety inputs are fresh | Nomad marker is owned by the operation |
| Owned Nomad drain | operation ID | begin the matching Worker drain | Nomad node, EC2 instance, and Worker service identity still match | Worker reports the same operation as Draining |
| Owned Worker drain | operation ID | remove instance scale-in protection | a fresh empty Sandbox list and `shutdown_ready=true` still match | a fresh ASG snapshot reports the instance unprotected (`armed`) |
| Armed, settled ASG | operation ID | set absolute desired capacity | every unprotected member is an owned, freshly verified armed worker | the selected instance leaves ASG membership |
| Departed instance | operation ID | complete the Nomad termination marker | instance is absent from fresh ASG membership | operation is complete |
| Surviving operation that must recover | operation ID | restore instance scale-in protection | instance remains `InService`; demand or safety state requires recovery | a fresh ASG snapshot reports the instance protected |
| Protected owned drain | operation ID | mark the Nomad operation `restoring` | the original Nomad identity is still isolated | restoring marker is durable |
| Restoring operation | operation ID | cancel the matching Worker drain | Worker service identity still matches; replay of a completed cancel is allowed only for the same operation | Worker is Healthy while Nomad remains isolated |
| Restoring operation with a Healthy Worker | operation ID | restore Nomad eligibility | the original Nomad node is still isolated, or a prior restore is observed | Nomad is ready and eligible |
| Restored operation | operation ID | complete the restore marker | Worker is Healthy and Nomad is ready and eligible | operation is complete |
| Protected worker whose old Nomad/Worker identity was replaced | old operation ID | restore and complete only the old Nomad marker | replacement identity is independently ready and eligible | old operation is complete; replacement Worker is untouched |

The only capacity-reducing write is the absolute desired-capacity update, and it
is allowed only when every unprotected member is an owned armed operation. A
replacement Worker never inherits or cancels the previous Worker's operation.

Instance protection constrains normal ASG scale-in only. It does not prevent a
health-check replacement, Spot interruption, or manual termination. Unknown or
contradictory state fails closed: the controller preserves or restores
protection and stops scale-in, but does not block scale-out.

Run one controller replica operationally. A restart or brief rolling overlap is
safe because Worker ownership and cloud writes are replayable from observable
state. Legacy burst state remains process-local only in the explicit legacy
demand mode.

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
`autoscaling:SetInstanceProtection` for that group only when
`SCALE_IN_MODE=enforce`; off and observe deployments do not receive it. Enforce
also requires the sandbox-client ASG to protect new instances by default. The
controller establishes and re-reads the protection baseline for existing
members before lowering desired capacity.
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

Safe scale-in has an additional rollback order. Do not stop or downgrade the
controller while armed workers exist or member count is above desired. First
raise desired to at least current membership, wait until there is no pending
normal scale-in deficit, protect every remaining ASG member, cancel the matching
Worker drains, and restore Nomad eligibility last. After fresh reads confirm no
armed worker or owned drain remains, switch `SCALE_IN_MODE` to `off` or deploy
the previous controller. Restoring exact-termination permission is not required.

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
