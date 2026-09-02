//go:build linux

package service

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service/machineinfo"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// ServiceStatus bundles the service status with the time of its last change.
type ServiceStatus struct {
	Status    orchestratorinfo.ServiceInfoStatus
	ChangedAt time.Time
}

type ServiceInfo struct {
	ClientId  string
	ServiceId string

	SourceVersion string
	SourceCommit  string

	Startup     time.Time
	Roles       []orchestratorinfo.ServiceInfoRole
	Labels      []string
	MachineInfo machineinfo.MachineInfo

	status                ServiceStatus
	sandboxStartsInFlight uint64
	statusMu              sync.RWMutex
}

// AdmissionState is a consistent snapshot of the service status and the
// create/resume operations that crossed the worker admission boundary.
type AdmissionState struct {
	Status                ServiceStatus
	SandboxStartsInFlight uint64
}

var serviceRolesMapper = map[cfg.ServiceType]orchestratorinfo.ServiceInfoRole{
	cfg.Orchestrator:    orchestratorinfo.ServiceInfoRole_Orchestrator,
	cfg.TemplateManager: orchestratorinfo.ServiceInfoRole_TemplateBuilder,
}

func (s *ServiceInfo) GetStatus() ServiceStatus {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()

	return s.status
}

func (s *ServiceInfo) GetAdmissionState() AdmissionState {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()

	return AdmissionState{
		Status:                s.status,
		SandboxStartsInFlight: s.sandboxStartsInFlight,
	}
}

// AdmitSandboxStart linearizes create/resume admission with status changes.
// FinishSandboxStart must be called exactly once after a successful admission.
func (s *ServiceInfo) AdmitSandboxStart() bool {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()

	if !s.status.Status.CanAcceptNewRequests() {
		return false
	}

	s.sandboxStartsInFlight++

	return true
}

func (s *ServiceInfo) FinishSandboxStart() {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()

	if s.sandboxStartsInFlight == 0 {
		panic("sandbox start admission released without acquisition")
	}
	s.sandboxStartsInFlight--
}

func (s *ServiceInfo) SetStatus(ctx context.Context, status orchestratorinfo.ServiceInfoStatus) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()

	s.setStatus(ctx, status)
}

func (s *ServiceInfo) OverrideStatus(ctx context.Context, status orchestratorinfo.ServiceInfoStatus) bool {
	applied, _ := s.OverrideStatusForService(ctx, status, "")

	return applied
}

// OverrideStatusForService changes status only for the expected process
// identity. Identity validation and the status change share the admission lock.
func (s *ServiceInfo) OverrideStatusForService(ctx context.Context, status orchestratorinfo.ServiceInfoStatus, expectedServiceID string) (applied bool, identityMatched bool) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()

	if expectedServiceID != "" && expectedServiceID != s.ServiceId {
		return false, false
	}

	// A controller cancellation may restore Healthy only while this exact worker
	// is still in the controller-owned Draining state. Never erase a concurrent
	// fail-closed transition to Unhealthy.
	if expectedServiceID != "" && status == orchestratorinfo.ServiceInfoStatus_Healthy && s.status.Status != orchestratorinfo.ServiceInfoStatus_Draining {
		return false, true
	}

	if s.status.Status == orchestratorinfo.ServiceInfoStatus_Draining && status == orchestratorinfo.ServiceInfoStatus_Standby {
		return false, true
	}

	s.setStatus(ctx, status)

	return true, true
}

func (s *ServiceInfo) setStatus(ctx context.Context, status orchestratorinfo.ServiceInfoStatus) {
	if s.status.Status != status {
		logger.L().Info(ctx, "Service status changed", zap.String("status", status.String()))
		s.status = ServiceStatus{Status: status, ChangedAt: time.Now()}
	}
}

func NewInfoContainer(clientId string, version string, commit string, instanceID string, machineInfo machineinfo.MachineInfo, config cfg.Config) *ServiceInfo {
	services := cfg.GetServices(config)
	serviceRoles := make([]orchestratorinfo.ServiceInfoRole, 0)

	for _, service := range services {
		if role, ok := serviceRolesMapper[service]; ok {
			serviceRoles = append(serviceRoles, role)
		}
	}

	startup := time.Now()
	serviceInfo := &ServiceInfo{
		ClientId:  clientId,
		ServiceId: instanceID,

		status: ServiceStatus{Status: orchestratorinfo.ServiceInfoStatus_Healthy, ChangedAt: startup},

		Startup:     startup,
		Roles:       serviceRoles,
		Labels:      config.NodeLabels,
		MachineInfo: machineInfo,

		SourceVersion: version,
		SourceCommit:  commit,
	}

	return serviceInfo
}
