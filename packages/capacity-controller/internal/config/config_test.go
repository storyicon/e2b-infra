package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("CAPACITY_DEMAND_MODE", "legacy-failure-ledger")
	t.Setenv("E2B_CLUSTER_ID", "cluster-1")
	t.Setenv("NOMAD_NODE_POOL", "orchestrator")
	t.Setenv("AWS_ASG_NAME", "workers")
	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("REDIS_URL", "redis.internal:6379")
}

func TestLoadUsesSafeDefaults(t *testing.T) { //nolint:paralleltest // environment mutation cannot run in parallel
	setRequiredEnv(t)

	cfg, err := Load()

	require.NoError(t, err)
	require.Equal(t, "legacy-failure-ledger", string(cfg.Mode))
	require.Equal(t, 20, cfg.SlotsPerNode)
	require.Equal(t, int32(1), cfg.MinNodes)
	require.Equal(t, int32(30), cfg.MaxNodes)
	require.Equal(t, time.Second, cfg.ReconcileInterval)
	require.Equal(t, 10*time.Second, cfg.ReconcileTimeout)
}

func TestLoadSupportsStartIntentModeWithoutRedis(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("CAPACITY_DEMAND_MODE", "start-intent-v1")
	t.Setenv("REDIS_URL", "")
	t.Setenv("CAPACITY_SNAPSHOT_GRPC_ADDRESS", "api-internal-grpc.service.consul:5009")
	t.Setenv("CAPACITY_SNAPSHOT_SERVICE_TOKEN", "service-token")

	cfg, err := Load()

	require.NoError(t, err)
	require.Equal(t, "start-intent-v1", string(cfg.Mode))
	require.Equal(t, "api-internal-grpc.service.consul:5009", cfg.CapacitySnapshotGRPCAddress)
	require.Equal(t, "service-token", cfg.CapacitySnapshotServiceToken)
}

func TestLoadRejectsMissingOrUnknownCapacityDemandMode(t *testing.T) {
	tests := []struct {
		name string
		mode string
	}{
		{name: "missing"},
		{name: "unknown", mode: "automatic"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("CAPACITY_DEMAND_MODE", tt.mode)

			_, err := Load()

			require.ErrorContains(t, err, "CAPACITY_DEMAND_MODE")
		})
	}
}

func TestLoadRequiresStartIntentGRPCConfigurationOnlyInStartIntentMode(t *testing.T) {
	tests := []struct {
		name      string
		address   string
		token     string
		wantError string
	}{
		{name: "missing address", token: "service-token", wantError: "CAPACITY_SNAPSHOT_GRPC_ADDRESS"},
		{name: "missing token", address: "api-internal-grpc.service.consul:5009", wantError: "CAPACITY_SNAPSHOT_SERVICE_TOKEN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("CAPACITY_DEMAND_MODE", "start-intent-v1")
			t.Setenv("REDIS_URL", "")
			t.Setenv("CAPACITY_SNAPSHOT_GRPC_ADDRESS", tt.address)
			t.Setenv("CAPACITY_SNAPSHOT_SERVICE_TOKEN", tt.token)

			_, err := Load()

			require.ErrorContains(t, err, tt.wantError)
		})
	}

	setRequiredEnv(t)

	_, err := Load()

	require.NoError(t, err)
}

func TestLoadSupportsClusterRedisAndTLS(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("REDIS_URL", "")
	t.Setenv("REDIS_CLUSTER_URL", "redis-cluster.internal:6379")
	t.Setenv("REDIS_TLS_ENABLED", "true")
	t.Setenv("REDIS_TLS_CA_BASE64", "certificate")

	cfg, err := Load()

	require.NoError(t, err)
	require.Equal(t, "redis-cluster.internal:6379", cfg.RedisClusterURL)
	require.True(t, cfg.RedisTLSEnabled)
	require.Equal(t, "certificate", cfg.RedisTLSCABase64)
}

func TestLoadRejectsInvalidCapacityBounds(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MIN_NODES", "31")
	t.Setenv("MAX_NODES", "30")

	_, err := Load()

	require.ErrorContains(t, err, "MIN_NODES")
}

func TestLoadRequiresOneRedisEndpoint(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("REDIS_URL", "")

	_, err := Load()

	require.ErrorContains(t, err, "REDIS_URL")
}

func TestLoadRejectsAmbiguousRedisEndpoints(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("REDIS_CLUSTER_URL", "redis-cluster.internal:6379")

	_, err := Load()

	require.ErrorContains(t, err, "mutually exclusive")
}

func TestLoadRejectsMalformedValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "boolean", key: "REDIS_TLS_ENABLED", value: "sometimes"},
		{name: "integer", key: "SLOTS_PER_NODE", value: "many"},
		{name: "duration", key: "RECONCILE_INTERVAL", value: "soon"},
		{name: "reconcile timeout", key: "RECONCILE_TIMEOUT", value: "later"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(tt.key, tt.value)

			_, err := Load()

			require.ErrorContains(t, err, tt.key)
		})
	}
}

func TestLoadRejectsNonPositiveSettings(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "slots", key: "SLOTS_PER_NODE", value: "0"},
		{name: "maximum", key: "MAX_NODES", value: "0"},
		{name: "interval", key: "RECONCILE_INTERVAL", value: "0s"},
		{name: "reconcile timeout", key: "RECONCILE_TIMEOUT", value: "0s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(tt.key, tt.value)

			_, err := Load()

			require.Error(t, err)
		})
	}
}

func TestLoadRequiresDeploymentIdentity(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("AWS_ASG_NAME", "")

	_, err := Load()

	require.ErrorContains(t, err, "AWS_ASG_NAME")
}
