package cfg

import (
	"encoding/base64"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	// set base required values
	t.Setenv("POSTGRES_CONNECTION_STRING", "postgres-connection-string")
	t.Setenv("LOKI_URL", "http://loki:3100")
	t.Setenv("VOLUME_TOKEN_ISSUER", "local.e2b-dev.com")
	t.Setenv("VOLUME_TOKEN_SIGNING_METHOD", "HS256")
	t.Setenv("VOLUME_TOKEN_SIGNING_KEY", fmt.Sprintf("HMAC:%s", base64.StdEncoding.EncodeToString([]byte("secret"))))
	t.Setenv("VOLUME_TOKEN_SIGNING_KEY_NAME", "my-key-name")

	t.Run("postgres connection string is required", func(t *testing.T) { //nolint:paralleltest // cannot call t.Setenv and t.Parallel
		removeEnv(t, "POSTGRES_CONNECTION_STRING")

		_, err := Parse()
		assert.ErrorContains(t, err, `required environment variable "POSTGRES_CONNECTION_STRING" is not set`)
	})

	t.Run("postgres connection string cannot be empty", func(t *testing.T) {
		t.Setenv("POSTGRES_CONNECTION_STRING", "")

		_, err := Parse()
		assert.ErrorContains(t, err, `environment variable "POSTGRES_CONNECTION_STRING" should not be empty`)
	})

	t.Run("base64 signing key can be parsed", func(t *testing.T) {
		content := []byte{1, 2, 3, 4, 5, 6}
		encoded := base64.StdEncoding.EncodeToString(content)
		t.Setenv("VOLUME_TOKEN_SIGNING_KEY", fmt.Sprintf("HMAC:%s", encoded))

		result, err := Parse()
		require.NoError(t, err)
		assert.Equal(t, content, result.VolumesToken.SigningKey)
	})

	t.Run("default persistent volume type by region is parsed as a map", func(t *testing.T) {
		t.Setenv("DEFAULT_PERSISTENT_VOLUME_TYPE_BY_REGION", "us-west3:zonalfilestore-us-west3,other:other-type")

		result, err := Parse()
		require.NoError(t, err)
		assert.Equal(t, map[string]string{
			"us-west3": "zonalfilestore-us-west3",
			"other":    "other-type",
		}, result.DefaultPersistentVolumeTypeByRegion)
	})

	t.Run("secrets store backend address is optional and has no default", func(t *testing.T) { //nolint:paralleltest // cannot call t.Setenv and t.Parallel
		removeEnv(t, "SECRETS_STORE_BACKEND_GRPC_ADDRESS")

		result, err := Parse()
		require.NoError(t, err)
		assert.Empty(t, result.SecretsStoreBackendGrpcAddress)
	})

	t.Run("secrets store backend address is read when set", func(t *testing.T) {
		t.Setenv("SECRETS_STORE_BACKEND_GRPC_ADDRESS", "secrets-backend:5000")

		result, err := Parse()
		require.NoError(t, err)
		assert.Equal(t, "secrets-backend:5000", result.SecretsStoreBackendGrpcAddress)
	})

	t.Run("capacity waiting is disabled by default", func(t *testing.T) { //nolint:paralleltest // environment mutation cannot run in parallel
		removeEnv(t, "SANDBOX_CAPACITY_WAIT_TIMEOUT")

		result, err := Parse()
		require.NoError(t, err)
		assert.Zero(t, result.SandboxCapacityWaitTimeout)
		assert.Equal(t, 500_000_000, int(result.SandboxCapacityRetryInterval))
	})

	t.Run("capacity waiting durations are parsed", func(t *testing.T) {
		t.Setenv("SANDBOX_CAPACITY_WAIT_TIMEOUT", "2m")
		t.Setenv("SANDBOX_CAPACITY_RETRY_INTERVAL", "250ms")

		result, err := Parse()
		require.NoError(t, err)
		assert.Equal(t, 120_000_000_000, int(result.SandboxCapacityWaitTimeout))
		assert.Equal(t, 250_000_000, int(result.SandboxCapacityRetryInterval))
	})

	t.Run("capacity demand mode defaults to legacy", func(t *testing.T) { //nolint:paralleltest // environment mutation cannot run in parallel
		removeEnv(t, "SANDBOX_CAPACITY_DEMAND_MODE")
		removeEnv(t, "CAPACITY_SNAPSHOT_SERVICE_TOKEN")

		result, err := Parse()
		require.NoError(t, err)
		assert.Equal(t, SandboxCapacityDemandModeLegacy, result.SandboxCapacityDemandMode)
	})

	t.Run("start intent modes require a snapshot service token", func(t *testing.T) {
		for _, mode := range []SandboxCapacityDemandMode{
			SandboxCapacityDemandModeDualWrite,
			SandboxCapacityDemandModeStartIntentV1,
			SandboxCapacityDemandModeWorkloadV2Shadow,
			SandboxCapacityDemandModeWorkloadV2,
		} {
			t.Run(string(mode), func(t *testing.T) {
				t.Setenv("SANDBOX_CAPACITY_DEMAND_MODE", string(mode))
				removeEnv(t, "CAPACITY_SNAPSHOT_SERVICE_TOKEN")

				_, err := Parse()
				assert.ErrorContains(t, err, "CAPACITY_SNAPSHOT_SERVICE_TOKEN")
			})
		}
	})

	t.Run("workload modes are parsed explicitly", func(t *testing.T) {
		for _, mode := range []SandboxCapacityDemandMode{SandboxCapacityDemandModeWorkloadV2Shadow, SandboxCapacityDemandModeWorkloadV2} {
			t.Run(string(mode), func(t *testing.T) {
				t.Setenv("SANDBOX_CAPACITY_DEMAND_MODE", string(mode))
				t.Setenv("CAPACITY_SNAPSHOT_SERVICE_TOKEN", "capacity-service-token")
				t.Setenv("SANDBOX_CAPACITY_WAIT_TIMEOUT", "2m")
				t.Setenv("SANDBOX_CAPACITY_POOL_VCPU", "2")
				t.Setenv("SANDBOX_CAPACITY_POOL_MEMORY_MIB", "1024")
				t.Setenv("SANDBOX_CAPACITY_POOL_CPU_ARCHITECTURE", "x86_64")
				t.Setenv("SANDBOX_CAPACITY_POOL_CPU_FAMILY", "6")
				t.Setenv("SANDBOX_CAPACITY_POOL_CPU_MODEL", "207")

				result, err := Parse()
				require.NoError(t, err)
				assert.Equal(t, mode, result.SandboxCapacityDemandMode)
			})
		}
	})

	t.Run("start intent mode and snapshot service token are parsed explicitly", func(t *testing.T) {
		t.Setenv("SANDBOX_CAPACITY_DEMAND_MODE", string(SandboxCapacityDemandModeStartIntentV1))
		t.Setenv("CAPACITY_SNAPSHOT_SERVICE_TOKEN", "capacity-service-token")
		t.Setenv("SANDBOX_CAPACITY_WAIT_TIMEOUT", "2m")
		t.Setenv("SANDBOX_CAPACITY_POOL_VCPU", "2")
		t.Setenv("SANDBOX_CAPACITY_POOL_MEMORY_MIB", "1024")
		t.Setenv("SANDBOX_CAPACITY_POOL_CPU_ARCHITECTURE", "x86_64")
		t.Setenv("SANDBOX_CAPACITY_POOL_CPU_FAMILY", "6")
		t.Setenv("SANDBOX_CAPACITY_POOL_CPU_MODEL", "207")

		result, err := Parse()
		require.NoError(t, err)
		assert.Equal(t, SandboxCapacityDemandModeStartIntentV1, result.SandboxCapacityDemandMode)
		assert.Equal(t, "capacity-service-token", result.CapacitySnapshotServiceToken)
		assert.Equal(t, int64(2), result.SandboxCapacityPoolVCPU)
		assert.Equal(t, int64(1024), result.SandboxCapacityPoolMemoryMiB)
		assert.Equal(t, "x86_64", result.SandboxCapacityPoolCPUArch)
		assert.Equal(t, "6", result.SandboxCapacityPoolCPUFamily)
		assert.Equal(t, "207", result.SandboxCapacityPoolCPUModel)
	})

	t.Run("start intent modes require capacity waiting", func(t *testing.T) {
		t.Setenv("SANDBOX_CAPACITY_DEMAND_MODE", string(SandboxCapacityDemandModeStartIntentV1))
		t.Setenv("CAPACITY_SNAPSHOT_SERVICE_TOKEN", "capacity-service-token")
		t.Setenv("SANDBOX_CAPACITY_POOL_VCPU", "2")
		t.Setenv("SANDBOX_CAPACITY_POOL_MEMORY_MIB", "1024")
		t.Setenv("SANDBOX_CAPACITY_POOL_CPU_ARCHITECTURE", "x86_64")
		t.Setenv("SANDBOX_CAPACITY_POOL_CPU_FAMILY", "6")
		t.Setenv("SANDBOX_CAPACITY_POOL_CPU_MODEL", "207")
		removeEnv(t, "SANDBOX_CAPACITY_WAIT_TIMEOUT")

		_, err := Parse()
		assert.ErrorContains(t, err, "SANDBOX_CAPACITY_WAIT_TIMEOUT")
	})

	t.Run("start intent modes require complete pool CPU identity", func(t *testing.T) {
		for _, tt := range []struct {
			name      string
			arch      string
			family    string
			model     string
			wantError string
		}{
			{name: "missing architecture", family: "6", model: "207", wantError: "SANDBOX_CAPACITY_POOL_CPU_ARCHITECTURE"},
			{name: "missing family", arch: "x86_64", model: "207", wantError: "SANDBOX_CAPACITY_POOL_CPU_FAMILY"},
			{name: "missing model", arch: "x86_64", family: "6", wantError: "SANDBOX_CAPACITY_POOL_CPU_MODEL"},
		} {
			t.Run(tt.name, func(t *testing.T) {
				t.Setenv("SANDBOX_CAPACITY_DEMAND_MODE", string(SandboxCapacityDemandModeStartIntentV1))
				t.Setenv("CAPACITY_SNAPSHOT_SERVICE_TOKEN", "capacity-service-token")
				t.Setenv("SANDBOX_CAPACITY_POOL_VCPU", "2")
				t.Setenv("SANDBOX_CAPACITY_POOL_MEMORY_MIB", "1024")
				t.Setenv("SANDBOX_CAPACITY_WAIT_TIMEOUT", "2m")
				t.Setenv("SANDBOX_CAPACITY_POOL_CPU_ARCHITECTURE", tt.arch)
				t.Setenv("SANDBOX_CAPACITY_POOL_CPU_FAMILY", tt.family)
				t.Setenv("SANDBOX_CAPACITY_POOL_CPU_MODEL", tt.model)

				_, err := Parse()
				assert.ErrorContains(t, err, tt.wantError)
			})
		}
	})

	t.Run("start intent modes require positive pool resources", func(t *testing.T) {
		for _, mode := range []SandboxCapacityDemandMode{
			SandboxCapacityDemandModeDualWrite,
			SandboxCapacityDemandModeStartIntentV1,
			SandboxCapacityDemandModeWorkloadV2Shadow,
			SandboxCapacityDemandModeWorkloadV2,
		} {
			for _, tt := range []struct {
				name      string
				vcpu      string
				memoryMiB string
				wantError string
			}{
				{name: "missing vCPU", memoryMiB: "512", wantError: "SANDBOX_CAPACITY_POOL_VCPU"},
				{name: "zero vCPU", vcpu: "0", memoryMiB: "512", wantError: "SANDBOX_CAPACITY_POOL_VCPU"},
				{name: "missing memory", vcpu: "1", wantError: "SANDBOX_CAPACITY_POOL_MEMORY_MIB"},
				{name: "zero memory", vcpu: "1", memoryMiB: "0", wantError: "SANDBOX_CAPACITY_POOL_MEMORY_MIB"},
			} {
				t.Run(string(mode)+"/"+tt.name, func(t *testing.T) {
					t.Setenv("SANDBOX_CAPACITY_DEMAND_MODE", string(mode))
					t.Setenv("CAPACITY_SNAPSHOT_SERVICE_TOKEN", "capacity-service-token")
					t.Setenv("SANDBOX_CAPACITY_POOL_VCPU", tt.vcpu)
					t.Setenv("SANDBOX_CAPACITY_POOL_MEMORY_MIB", tt.memoryMiB)
					t.Setenv("SANDBOX_CAPACITY_WAIT_TIMEOUT", "2m")

					_, err := Parse()

					assert.ErrorContains(t, err, tt.wantError)
				})
			}
		}
	})

	t.Run("unknown capacity demand mode fails startup", func(t *testing.T) {
		t.Setenv("SANDBOX_CAPACITY_DEMAND_MODE", "automatic-fallback")

		_, err := Parse()
		assert.ErrorContains(t, err, "invalid SANDBOX_CAPACITY_DEMAND_MODE")
	})

	t.Run("invalid service discovery provider exposes failure condition", func(t *testing.T) {
		t.Setenv("SERVICE_DISCOVERY_PROVIDER", "invalid")

		_, err := Parse()
		require.Error(t, err)

		condition, ok := ParseFailureCondition(err)
		require.True(t, ok)
		assert.Equal(t, FailureConditionInvalidServiceDiscoveryProvider, condition)
	})
}

// removeEnv was mostly copied from the implementation of t.Setenv
func removeEnv(t *testing.T, key string) {
	t.Helper()

	prevValue, ok := os.LookupEnv(key)

	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("cannot unset environment variable: %v", err)
	}

	if ok {
		t.Cleanup(func() {
			os.Setenv(key, prevValue) //nolint:usetesting // we're doing fancy things here
		})
	} else {
		t.Cleanup(func() {
			os.Unsetenv(key)
		})
	}
}
