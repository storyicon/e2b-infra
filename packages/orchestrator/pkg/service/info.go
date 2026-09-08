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

	status                     ServiceStatus
	sandboxStartsInFlight      uint64
	controllerDrainOwned       bool
	controllerDrainOperationID string
	completedDrainOperationID  string
	statusMu                   sync.RWMutex
}

// AdmissionState is a consistent snapshot of the service status and the
// create/resume operations that crossed the worker admission boundary.
type AdmissionState struct {
	Status                     ServiceStatus
	SandboxStartsInFlight      uint64
	ControllerDrainOwned       bool
	ControllerDrainOperationID string
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
		Status:                     s.status,
		SandboxStartsInFlight:      s.sandboxStartsInFlight,
		ControllerDrainOwned:       s.controllerDrainOwned,
		ControllerDrainOperationID: s.controllerDrainOperationID,
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

	// Internal lifecycle and health transitions always take ownership away
	// from the capacity controller, including a repeated Draining transition
	// during process shutdown.
	s.clearControllerDrainStateLocked()
	s.setStatus(ctx, status)
}

// BeginProcessShutdown enters a lifecycle-owned drain without overwriting a
// concurrent fail-closed status. The status check, ownership transfer, and
// transition are atomic with admission and controller overrides.
func (s *ServiceInfo) BeginProcessShutdown(ctx context.Context) (ServiceStatus, bool) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()

	previous := s.status
	switch previous.Status {
	case orchestratorinfo.ServiceInfoStatus_Healthy, orchestratorinfo.ServiceInfoStatus_Standby, orchestratorinfo.ServiceInfoStatus_Draining:
		s.clearControllerDrainStateLocked()
		s.setStatus(ctx, orchestratorinfo.ServiceInfoStatus_Draining)

		return previous, true
	default:
		return previous, false
	}
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
	if s.status.Status == orchestratorinfo.ServiceInfoStatus_Draining && status == orchestratorinfo.ServiceInfoStatus_Standby {
		return false, true
	}

	// Generic lifecycle/admin changes never acquire controller ownership and
	// cannot reopen a controller-owned drain.
	if s.controllerDrainOwned && status == orchestratorinfo.ServiceInfoStatus_Healthy {
		return false, true
	}
	s.clearControllerDrainStateLocked()
	s.setStatus(ctx, status)

	return true, true
}

// OverrideControllerDrain atomically begins or cancels a controller-owned
// drain for one exact worker process and operation.
func (s *ServiceInfo) OverrideControllerDrain(ctx context.Context, status orchestratorinfo.ServiceInfoStatus, expectedServiceID, operationID string) (applied bool, identityMatched bool) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()

	if expectedServiceID == "" || expectedServiceID != s.ServiceId || operationID == "" {
		return false, false
	}

	switch status {
	case orchestratorinfo.ServiceInfoStatus_Draining:
		switch {
		case s.status.Status == orchestratorinfo.ServiceInfoStatus_Healthy:
			if s.completedDrainOperationID == operationID {
				// A completed cancellation is terminal for this process and
				// operation. Reject a stale Begin replay instead of reopening the
				// admission boundary with an already-completed owner.
				return false, true
			}
			s.controllerDrainOwned = true
			s.controllerDrainOperationID = operationID
			s.completedDrainOperationID = ""
		case s.status.Status == orchestratorinfo.ServiceInfoStatus_Draining && s.controllerDrainOwned && s.controllerDrainOperationID == operationID:
			// Idempotent retry of the controller-owned transition.
		default:
			return false, true
		}
	case orchestratorinfo.ServiceInfoStatus_Healthy:
		if s.status.Status == orchestratorinfo.ServiceInfoStatus_Healthy && !s.controllerDrainOwned && s.completedDrainOperationID == operationID {
			// The previous cancellation may have succeeded while its response was
			// lost. Preserve an idempotent receipt for this process and operation.
			return true, true
		}
		if s.status.Status != orchestratorinfo.ServiceInfoStatus_Draining || !s.controllerDrainOwned || s.controllerDrainOperationID != operationID {
			return false, true
		}
		s.controllerDrainOwned = false
		s.controllerDrainOperationID = ""
		s.completedDrainOperationID = operationID
	default:
		return false, true
	}

	s.setStatus(ctx, status)

	return true, true
}

func (s *ServiceInfo) clearControllerDrainStateLocked() {
	s.controllerDrainOwned = false
	s.controllerDrainOperationID = ""
	s.completedDrainOperationID = ""
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
