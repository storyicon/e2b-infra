# Capacity controller

The capacity controller scales an AWS Auto Scaling group out from E2B's
cluster-scoped sandbox workload. It does not scale in and it does not accept
public traffic. `CAPACITY_DEMAND_MODE` selects exactly one input:

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

Nomad configuration uses the standard Nomad client environment variables,
including `NOMAD_ADDR`, `NOMAD_TOKEN`, `NOMAD_CACERT`, `NOMAD_CLIENT_CERT`, and
`NOMAD_CLIENT_KEY`. AWS credentials use the default AWS SDK credential chain;
no credential is accepted as a controller-specific setting.

The runtime role only needs read access to the configured ASG plus
`autoscaling:SetDesiredCapacity` for that exact group. Redis authentication
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
4. Set the controller to `start-intent-v1`. It then reads only the authenticated
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
or `start-intent-v1`), `capacity_api_pool_vcpu` and
`capacity_api_pool_memory_mib` are required and define the exact single-pool
resource shape accepted by the API. Incompatible API/controller migration mode
pairs fail Terraform validation. The ALB idle timeout defaults to 180 seconds
while autoscaling is enabled so it exceeds the API's default 120-second wait
budget; keep `capacity_ingress_idle_timeout_seconds` above any customized wait
timeout.

`capacity_controller_slots_per_node` configures both the controller's per-node
capacity model and the orchestrator's hard running-sandbox admission. The
orchestrator's concurrent-start limit defaults to the same value and can be
lowered independently with `capacity_controller_max_starting_per_node` without
changing the controller's eventual capacity target.

## Cold-start benchmark

`packages/shared/scripts/run-capacity-smoke.mjs` measures demand-driven scale-out
from a known baseline. It requires an explicit `E2B_TEMPLATE_ALIAS`; the repository
does not contain a deployment-specific template name. The runner also requires a
run-scoped observer JSONL file through `CAPACITY_OBSERVER_EVENTS_PATH` and fails
before creating workload if the observer contract or SDK transport limits are not
satisfied.

The load generator must run without AWS credential-provider environment variables
and with IMDS blocked. It never calls Auto Scaling and never retries
`Sandbox.create`; infrastructure observations come from the separate read-only
observer. Run the deterministic model tests with:

```sh
node --test packages/shared/scripts/capacity-smoke-model.test.mjs
```
