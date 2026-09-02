package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/e2b-dev/infra/packages/capacity-controller/internal/controller"
)

type Config struct {
	Mode                         controller.Mode
	ClusterID                    string
	NomadNodePool                string
	ASGName                      string
	AWSRegion                    string
	RedisURL                     string
	RedisClusterURL              string
	RedisPassword                string
	RedisTLSEnabled              bool
	RedisTLSCABase64             string
	CapacitySnapshotGRPCAddress  string
	CapacitySnapshotServiceToken string
	SlotsPerNode                 int
	MinNodes                     int32
	MaxNodes                     int32
	ReconcileInterval            time.Duration
	BatchIdleDuration            time.Duration
	BatchMaxDuration             time.Duration
	ReconcileTimeout             time.Duration
	ScaleInMode                  controller.ScaleInMode
	ScaleInHeadroomPercent       int
	ScaleInStabilization         time.Duration
	ScaleInMinimumNodeAge        time.Duration
	ScaleInDrainTimeout          time.Duration
}

func Load() (*Config, error) {
	mode, err := controller.ParseMode(os.Getenv("CAPACITY_DEMAND_MODE"))
	if err != nil {
		return nil, fmt.Errorf("CAPACITY_DEMAND_MODE: %w", err)
	}

	tlsEnabled := false
	if mode == controller.ModeLegacyFailureLedger {
		tlsEnabled, err = boolEnv("REDIS_TLS_ENABLED", false)
		if err != nil {
			return nil, err
		}
	}

	slotsPerNode, err := intEnv("SLOTS_PER_NODE", 20)
	if err != nil {
		return nil, err
	}
	minNodes, err := intEnv("MIN_NODES", 1)
	if err != nil {
		return nil, err
	}
	maxNodes, err := intEnv("MAX_NODES", 30)
	if err != nil {
		return nil, err
	}
	interval, err := durationEnv("RECONCILE_INTERVAL", time.Second)
	if err != nil {
		return nil, err
	}
	var batchIdleDuration time.Duration
	var batchMaxDuration time.Duration
	if mode == controller.ModeStartIntentV1 {
		batchIdleDuration, err = durationEnv("START_INTENT_BATCH_IDLE_DURATION", time.Second)
		if err != nil {
			return nil, err
		}
		batchMaxDuration, err = durationEnv("START_INTENT_BATCH_MAX_DURATION", 10*time.Second)
		if err != nil {
			return nil, err
		}
	}
	reconcileTimeout, err := durationEnv("RECONCILE_TIMEOUT", 10*time.Second)
	if err != nil {
		return nil, err
	}
	scaleInModeValue := os.Getenv("SCALE_IN_MODE")
	if scaleInModeValue == "" {
		scaleInModeValue = string(controller.ScaleInModeOff)
	}
	scaleInMode, err := controller.ParseScaleInMode(scaleInModeValue)
	if err != nil {
		return nil, fmt.Errorf("SCALE_IN_MODE: %w", err)
	}
	scaleInHeadroomPercent, err := intEnv("SCALE_IN_HEADROOM_PERCENT", 10)
	if err != nil {
		return nil, err
	}
	scaleInStabilization, err := durationEnv("SCALE_IN_STABILIZATION_DURATION", 2*time.Minute)
	if err != nil {
		return nil, err
	}
	scaleInMinimumNodeAge, err := durationEnv("SCALE_IN_MIN_NODE_AGE", 10*time.Minute)
	if err != nil {
		return nil, err
	}
	scaleInDrainTimeout, err := durationEnv("SCALE_IN_DRAIN_TIMEOUT", 15*time.Minute)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Mode:                         mode,
		ClusterID:                    os.Getenv("E2B_CLUSTER_ID"),
		NomadNodePool:                os.Getenv("NOMAD_NODE_POOL"),
		ASGName:                      os.Getenv("AWS_ASG_NAME"),
		AWSRegion:                    os.Getenv("AWS_REGION"),
		RedisURL:                     os.Getenv("REDIS_URL"),
		RedisClusterURL:              os.Getenv("REDIS_CLUSTER_URL"),
		RedisPassword:                os.Getenv("REDIS_PASSWORD"),
		RedisTLSEnabled:              tlsEnabled,
		RedisTLSCABase64:             os.Getenv("REDIS_TLS_CA_BASE64"),
		CapacitySnapshotGRPCAddress:  os.Getenv("CAPACITY_SNAPSHOT_GRPC_ADDRESS"),
		CapacitySnapshotServiceToken: os.Getenv("CAPACITY_SNAPSHOT_SERVICE_TOKEN"),
		SlotsPerNode:                 slotsPerNode,
		MinNodes:                     int32(minNodes),
		MaxNodes:                     int32(maxNodes),
		ReconcileInterval:            interval,
		BatchIdleDuration:            batchIdleDuration,
		BatchMaxDuration:             batchMaxDuration,
		ReconcileTimeout:             reconcileTimeout,
		ScaleInMode:                  scaleInMode,
		ScaleInHeadroomPercent:       scaleInHeadroomPercent,
		ScaleInStabilization:         scaleInStabilization,
		ScaleInMinimumNodeAge:        scaleInMinimumNodeAge,
		ScaleInDrainTimeout:          scaleInDrainTimeout,
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	required := map[string]string{
		"E2B_CLUSTER_ID":  c.ClusterID,
		"NOMAD_NODE_POOL": c.NomadNodePool,
		"AWS_ASG_NAME":    c.ASGName,
		"AWS_REGION":      c.AWSRegion,
	}
	for name, value := range required {
		if value == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	switch c.Mode {
	case controller.ModeLegacyFailureLedger:
		if c.RedisURL == "" && c.RedisClusterURL == "" {
			return errors.New("REDIS_URL or REDIS_CLUSTER_URL is required")
		}
		if c.RedisURL != "" && c.RedisClusterURL != "" {
			return errors.New("REDIS_URL and REDIS_CLUSTER_URL are mutually exclusive")
		}
	case controller.ModeStartIntentV1:
		if c.CapacitySnapshotGRPCAddress == "" {
			return errors.New("CAPACITY_SNAPSHOT_GRPC_ADDRESS is required")
		}
		if c.CapacitySnapshotServiceToken == "" {
			return errors.New("CAPACITY_SNAPSHOT_SERVICE_TOKEN is required")
		}
	default:
		return fmt.Errorf("CAPACITY_DEMAND_MODE %q is unsupported", c.Mode)
	}
	if c.SlotsPerNode <= 0 {
		return errors.New("SLOTS_PER_NODE must be positive")
	}
	if c.MinNodes < 0 || c.MaxNodes <= 0 || c.MinNodes > c.MaxNodes {
		return errors.New("MIN_NODES must be non-negative and no greater than MAX_NODES")
	}
	if c.ReconcileInterval <= 0 {
		return errors.New("RECONCILE_INTERVAL must be positive")
	}
	if c.Mode == controller.ModeStartIntentV1 {
		if c.BatchIdleDuration <= 0 {
			return errors.New("START_INTENT_BATCH_IDLE_DURATION must be positive in start-intent-v1 mode")
		}
		if c.BatchMaxDuration <= 0 {
			return errors.New("START_INTENT_BATCH_MAX_DURATION must be positive in start-intent-v1 mode")
		}
		if c.BatchIdleDuration > c.BatchMaxDuration {
			return errors.New("START_INTENT_BATCH_IDLE_DURATION must not exceed START_INTENT_BATCH_MAX_DURATION")
		}
	}
	if c.ReconcileTimeout <= 0 {
		return errors.New("RECONCILE_TIMEOUT must be positive")
	}
	if c.ScaleInHeadroomPercent < 0 {
		return errors.New("SCALE_IN_HEADROOM_PERCENT must be non-negative")
	}
	if c.ScaleInStabilization <= 0 {
		return errors.New("SCALE_IN_STABILIZATION_DURATION must be positive")
	}
	if c.ScaleInMinimumNodeAge <= 0 {
		return errors.New("SCALE_IN_MIN_NODE_AGE must be positive")
	}
	if c.ScaleInDrainTimeout <= 0 {
		return errors.New("SCALE_IN_DRAIN_TIMEOUT must be positive")
	}
	if c.ScaleInMode != controller.ScaleInModeOff && c.Mode != controller.ModeStartIntentV1 {
		return errors.New("SCALE_IN_MODE observe or enforce requires CAPACITY_DEMAND_MODE=start-intent-v1")
	}

	return nil
}

func intEnv(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}

	return parsed, nil
}

func boolEnv(name string, fallback bool) (bool, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", name, err)
	}

	return parsed, nil
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}

	return parsed, nil
}
