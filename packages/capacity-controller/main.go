package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	nomadapi "github.com/hashicorp/nomad/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/e2b-dev/infra/packages/capacity-controller/internal/adapters"
	appconfig "github.com/e2b-dev/infra/packages/capacity-controller/internal/config"
	"github.com/e2b-dev/infra/packages/capacity-controller/internal/controller"
	capacitydemand "github.com/e2b-dev/infra/packages/shared/pkg/capacity-demand"
	sharedfactories "github.com/e2b-dev/infra/packages/shared/pkg/factories"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

func main() {
	os.Exit(mainRun())
}

func mainRun() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		slog.Error("capacity controller stopped", "error", err)

		return 1
	}

	return 0
}

type slogAuditSink struct{}

func (slogAuditSink) Record(event controller.ScaleAuditEvent) {
	checkpointGeneratedEpochMs := int64(0)
	if !event.CheckpointGeneratedAt.IsZero() {
		checkpointGeneratedEpochMs = event.CheckpointGeneratedAt.UnixMilli()
	}
	slog.Info(event.Event,
		"controller_instance_id", event.ControllerInstanceID,
		"scale_write_sequence", event.ScaleWriteSequence,
		"mode", event.Mode,
		"workload_count", event.WorkloadCount,
		"current_desired", event.CurrentDesired,
		"target", event.Target,
		"batch_trigger", event.BatchTrigger,
		"batch_age_ms", event.BatchAge.Milliseconds(),
		"batch_idle_age_ms", event.BatchIdleAge.Milliseconds(),
		"outcome", event.Outcome,
		"duration_ms", event.Duration.Milliseconds(),
		"aws_request_id", event.AWSRequestID,
		"error", event.Error,
		"audit_dropped_total", event.AuditDroppedTotal,
		"checkpoint_generated_epoch_ms", checkpointGeneratedEpochMs,
	)
}

func run(ctx context.Context) error {
	cfg, err := appconfig.Load()
	if err != nil {
		return err
	}

	var demandReader controller.DemandReader
	var snapshotReader controller.CapacitySnapshotReader
	switch cfg.Mode {
	case controller.ModeLegacyFailureLedger:
		redisClient, err := sharedfactories.NewRedisClient(ctx, sharedfactories.RedisConfig{
			RedisURL:         cfg.RedisURL,
			RedisClusterURL:  cfg.RedisClusterURL,
			RedisPassword:    cfg.RedisPassword,
			RedisTLSEnabled:  cfg.RedisTLSEnabled,
			RedisTLSCABase64: cfg.RedisTLSCABase64,
		})
		if err != nil {
			return err
		}
		defer func() {
			if err := redisClient.Close(); err != nil {
				slog.Error("close Redis client", "error", err)
			}
		}()
		if err := redisClient.Ping(ctx).Err(); err != nil {
			return err
		}
		demandReader = capacitydemand.NewRedisStore(redisClient)
	case controller.ModeStartIntentV1:
		conn, err := grpc.NewClient(
			cfg.CapacitySnapshotGRPCAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return err
		}
		defer func() {
			if err := conn.Close(); err != nil {
				slog.Error("close capacity snapshot gRPC connection", "error", err)
			}
		}()
		snapshotReader = adapters.NewCapacitySnapshot(
			proxygrpc.NewCapacityServiceClient(conn),
			cfg.CapacitySnapshotServiceToken,
		)
	default:
		return fmt.Errorf("unsupported capacity demand mode %q", cfg.Mode)
	}

	nomadClient, err := nomadapi.NewClient(nomadapi.DefaultConfig())
	if err != nil {
		return err
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.AWSRegion))
	if err != nil {
		return err
	}
	audit := controller.NewAsyncAuditSink(slogAuditSink{}, 128)
	defer audit.Close()

	reconciler := controller.New(&controller.Config{
		Mode:              cfg.Mode,
		ClusterID:         cfg.ClusterID,
		NodePool:          cfg.NomadNodePool,
		ASGName:           cfg.ASGName,
		SlotsPerNode:      cfg.SlotsPerNode,
		MinNodes:          cfg.MinNodes,
		MaxNodes:          cfg.MaxNodes,
		BatchIdleDuration: cfg.BatchIdleDuration,
		BatchMaxDuration:  cfg.BatchMaxDuration,
		ReconcileTimeout:  cfg.ReconcileTimeout,
	}, demandReader, snapshotReader, adapters.NewNomad(nomadClient), adapters.NewASG(autoscaling.NewFromConfig(awsCfg)), audit)

	ticker := time.NewTicker(cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		result, err := reconciler.Reconcile(ctx, time.Now().UTC())
		if cfg.Mode == controller.ModeStartIntentV1 {
			reconciler.RecordAuditCheckpoint(time.Now().UTC())
		}
		if err != nil {
			slog.Error("capacity reconciliation failed",
				"mode", result.Mode,
				"workload_count", result.WorkloadCount,
				"ready_nodes", result.ReadyNodes,
				"desired_nodes", result.DesiredNodes,
				"target_nodes", result.TargetNodes,
				"capped", result.Capped,
				"outcome", "error",
				"error", err,
			)
		} else {
			slog.Info("capacity reconciled",
				"mode", result.Mode,
				"workload_count", result.WorkloadCount,
				"pending_sandboxes", result.PendingSandboxes,
				"total_fulfilled", result.TotalFulfilled,
				"total_direct_success", result.TotalDirectSuccess,
				"burst_demand", result.BurstDemand,
				"ready_nodes", result.ReadyNodes,
				"desired_nodes", result.DesiredNodes,
				"target_nodes", result.TargetNodes,
				"capped", result.Capped,
				"scaled", result.Scaled,
				"aggregating", result.Aggregating,
				"outcome", "success",
			)
			if result.ReadyNodesError != nil {
				slog.Warn("Nomad readiness diagnostic unavailable",
					"mode", result.Mode,
					"desired_nodes", result.DesiredNodes,
					"target_nodes", result.TargetNodes,
					"scaled", result.Scaled,
					"error", result.ReadyNodesError,
				)
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
