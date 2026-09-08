//go:build linux

package service

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service/machineinfo"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type Server struct {
	orchestratorinfo.UnimplementedInfoServiceServer

	info                *ServiceInfo
	sandboxes           *sandbox.Map
	hostMetrics         *metrics.HostMetrics
	createLimitProvider SandboxCreateLimitProvider
	shutdownActivity    ShutdownActivityProvider
}

type SandboxCreateLimitProvider interface {
	SandboxCreateConcurrencyLimit() uint64
}

type ShutdownActivityProvider interface {
	SnapshotUploadsInFlight() uint64
}

type ShutdownState struct {
	ServiceStatus         ServiceStatus
	ScaleInOperationID    string
	LiveSandboxes         uint64
	SandboxStartsInFlight uint64
	LifecycleCleanups     uint64
	SnapshotUploads       uint64
	ActivityQuiet         bool
	Ready                 bool
}

func SnapshotShutdownState(info *ServiceInfo, sandboxes *sandbox.Map, activity ShutdownActivityProvider) ShutdownState {
	before := info.GetAdmissionState()
	state := ShutdownState{
		ServiceStatus:         before.Status,
		LiveSandboxes:         uint64(sandboxes.Count()),
		SandboxStartsInFlight: before.SandboxStartsInFlight,
		LifecycleCleanups:     uint64(sandboxes.LifecycleCount()),
		SnapshotUploads:       activity.SnapshotUploadsInFlight(),
	}
	after := info.GetAdmissionState()
	state.ServiceStatus = after.Status
	state.ScaleInOperationID = after.ControllerDrainOperationID
	state.SandboxStartsInFlight = after.SandboxStartsInFlight
	state.ActivityQuiet = state.LiveSandboxes == 0 &&
		before.SandboxStartsInFlight == 0 &&
		after.SandboxStartsInFlight == 0 &&
		state.LifecycleCleanups == 0 &&
		state.SnapshotUploads == 0
	state.Ready = before.ControllerDrainOwned &&
		after.ControllerDrainOwned &&
		before.ControllerDrainOperationID != "" &&
		before.ControllerDrainOperationID == after.ControllerDrainOperationID &&
		after.Status.Status == orchestratorinfo.ServiceInfoStatus_Draining &&
		state.ActivityQuiet

	return state
}

func NewInfoService(info *ServiceInfo, sandboxes *sandbox.Map, hostMetrics *metrics.HostMetrics, createLimitProvider SandboxCreateLimitProvider, shutdownActivity ShutdownActivityProvider) *Server {
	return &Server{
		info:                info,
		sandboxes:           sandboxes,
		hostMetrics:         hostMetrics,
		createLimitProvider: createLimitProvider,
		shutdownActivity:    shutdownActivity,
	}
}

func (s *Server) ServiceInfo(ctx context.Context, _ *emptypb.Empty) (*orchestratorinfo.ServiceInfoResponse, error) {
	info := s.info

	// Get host metrics for the orchestrator
	cpuMetrics, err := s.hostMetrics.GetCPUMetrics()
	if err != nil {
		logger.L().Warn(ctx, "Failed to get host metrics", zap.Error(err))
		cpuMetrics = &metrics.CPUMetrics{}
	}

	memoryMetrics, err := s.hostMetrics.GetMemoryMetrics()
	if err != nil {
		logger.L().Warn(ctx, "Failed to get host metrics", zap.Error(err))
		memoryMetrics = &metrics.MemoryMetrics{}
	}

	diskMetrics, err := s.hostMetrics.GetDiskMetrics()
	if err != nil {
		logger.L().Warn(ctx, "Failed to get host metrics", zap.Error(err))
		diskMetrics = []metrics.DiskInfo{}
	}

	// Calculate sandbox resource allocation
	sandboxVCpuAllocated := uint32(0)
	sandboxMemoryAllocated := uint64(0)
	sandboxDiskAllocated := uint64(0)

	for _, item := range s.sandboxes.Items() {
		sandboxVCpuAllocated += uint32(item.Config.Vcpu)
		sandboxMemoryAllocated += uint64(item.Config.RamMB) * 1024 * 1024
		sandboxDiskAllocated += uint64(item.Config.TotalDiskSizeMB) * 1024 * 1024
	}

	shutdownState := SnapshotShutdownState(info, s.sandboxes, s.shutdownActivity)

	return &orchestratorinfo.ServiceInfoResponse{
		NodeId:                        info.ClientId,
		ServiceId:                     info.ServiceId,
		ServiceStatus:                 shutdownState.ServiceStatus.Status,
		ServiceStatusChangedAt:        timestamppb.New(shutdownState.ServiceStatus.ChangedAt),
		SandboxCreateConcurrencyLimit: s.createLimitProvider.SandboxCreateConcurrencyLimit(),
		SafeScaleInSupported:          true,
		SandboxStartsInFlight:         shutdownState.SandboxStartsInFlight,
		LifecycleCleanupsInFlight:     shutdownState.LifecycleCleanups,
		SnapshotUploadsInFlight:       shutdownState.SnapshotUploads,
		ShutdownReady:                 shutdownState.Ready,
		ScaleInOperationId:            shutdownState.ScaleInOperationID,

		ServiceVersion: info.SourceVersion,
		ServiceCommit:  info.SourceCommit,

		ServiceStartup: timestamppb.New(info.Startup),
		ServiceRoles:   info.Roles,
		MachineInfo:    convertMachineInfo(info.MachineInfo),
		Labels:         info.Labels,

		// Allocated resources to sandboxes
		MetricCpuAllocated:         sandboxVCpuAllocated,
		MetricMemoryAllocatedBytes: sandboxMemoryAllocated,
		MetricDiskAllocatedBytes:   sandboxDiskAllocated,
		MetricSandboxesRunning:     uint32(shutdownState.LiveSandboxes),

		// Host system usage metrics
		MetricCpuPercent:      uint32(cpuMetrics.UsedPercent),
		MetricMemoryUsedBytes: memoryMetrics.UsedBytes,

		// Host system total resources
		MetricCpuCount:         cpuMetrics.Count,
		MetricMemoryTotalBytes: memoryMetrics.TotalBytes,

		// Hugepage pool metrics (page counts)
		MetricHugepagesTotal:    memoryMetrics.HugePagesTotal,
		MetricHugepagesUsed:     memoryMetrics.HugePagesUsed,
		MetricHugepagesReserved: memoryMetrics.HugePagesReserved,
		MetricHugepageSizeBytes: memoryMetrics.HugePageSizeBytes,

		// Detailed disk metrics
		MetricDisks: convertDiskMetrics(diskMetrics),

		// TODO: Remove when migrated
		MetricVcpuUsed:     int64(sandboxVCpuAllocated),
		MetricMemoryUsedMb: int64(sandboxMemoryAllocated / (1024 * 1024)),
		MetricDiskMb:       int64(sandboxDiskAllocated / (1024 * 1024)),
	}, nil
}

// convertDiskMetrics converts internal DiskInfo to protobuf DiskMetrics
func convertDiskMetrics(disks []metrics.DiskInfo) []*orchestratorinfo.DiskMetrics {
	result := make([]*orchestratorinfo.DiskMetrics, len(disks))
	for i, disk := range disks {
		result[i] = &orchestratorinfo.DiskMetrics{
			MountPoint:     disk.MountPoint,
			Device:         disk.Device,
			FilesystemType: disk.FilesystemType,
			UsedBytes:      disk.UsedBytes,
			TotalBytes:     disk.TotalBytes,
		}
	}

	return result
}

// convertDiskMetrics converts internal DiskInfo to protobuf DiskMetrics
func convertMachineInfo(machineInfo machineinfo.MachineInfo) *orchestratorinfo.MachineInfo {
	return &orchestratorinfo.MachineInfo{
		CpuArchitecture: machineInfo.Arch,
		CpuFamily:       machineInfo.Family,
		CpuModel:        machineInfo.Model,
		CpuModelName:    machineInfo.ModelName,
		CpuFlags:        machineInfo.Flags,
	}
}

func (s *Server) ServiceStatusOverride(ctx context.Context, req *orchestratorinfo.ServiceStatusChangeRequest) (*emptypb.Empty, error) {
	return s.overrideServiceStatus(ctx, req)
}

func (s *Server) ConditionalServiceStatusOverride(ctx context.Context, req *orchestratorinfo.ServiceStatusChangeRequest) (*emptypb.Empty, error) {
	if req.GetExpectedServiceId() == "" || req.GetOperationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "expected service instance identity and operation ID are required")
	}
	if req.GetServiceStatus() != orchestratorinfo.ServiceInfoStatus_Draining && req.GetServiceStatus() != orchestratorinfo.ServiceInfoStatus_Healthy {
		return nil, status.Error(codes.InvalidArgument, "controller drain status must be Draining or Healthy")
	}

	logger.L().Info(ctx, "controller drain status override request received", zap.String("status", req.GetServiceStatus().String()))
	applied, identityMatched := s.info.OverrideControllerDrain(ctx, req.GetServiceStatus(), req.GetExpectedServiceId(), req.GetOperationId())
	if !identityMatched {
		return nil, status.Error(codes.FailedPrecondition, "service instance identity changed")
	}
	if !applied {
		return nil, status.Error(codes.FailedPrecondition, "controller drain ownership conflict")
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) overrideServiceStatus(ctx context.Context, req *orchestratorinfo.ServiceStatusChangeRequest) (*emptypb.Empty, error) {
	logger.L().Info(ctx, "service status override request received", zap.String("status", req.GetServiceStatus().String()))
	applied, identityMatched := s.info.OverrideStatusForService(ctx, req.GetServiceStatus(), req.GetExpectedServiceId())
	if !identityMatched {
		return nil, status.Error(codes.FailedPrecondition, "service instance identity changed")
	}
	if !applied {
		return nil, status.Error(codes.FailedPrecondition, "service status transition is no longer valid")
	}

	return &emptypb.Empty{}, nil
}
