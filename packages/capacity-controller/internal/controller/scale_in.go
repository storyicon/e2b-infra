package controller

import (
	"context"
	"errors"
	"math"
	"slices"
	"time"
)

const (
	ScaleInNomadOwner = "e2b-capacity-controller"
	ScaleInVersion    = "safe-empty-worker-v2"
)

type NomadScaleInOperation struct {
	OperationID       string
	ServiceInstanceID string
	StartedAt         time.Time
	Stage             string
}

type NomadScaleInNode struct {
	NomadNodeID string
	NodeID      string
	NodePool    string
	Ready       bool
	Eligible    bool
	Draining    bool
	CreateIndex uint64
	Operation   *NomadScaleInOperation
}

type ScaleInNodeInventory interface {
	Inventory(ctx context.Context, nodePool string) ([]NomadScaleInNode, error)
	MarkDrain(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error
	MarkOperationStage(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error
	RestoreDrain(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error
	CompleteRestore(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error
	CompleteTermination(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error
}

type ScaleInCandidateObservation struct {
	NodeID                 string
	NomadNodeID            string
	ServiceInstanceID      string
	ServiceStatus          string
	RunningSandboxes       int64
	KnownWorkload          int64
	ScaleInProtocolSupport bool
	ObservedAt             time.Time
}

type WorkerScaleInState struct {
	NodeID                    string
	ServiceInstanceID         string
	ServiceStatus             string
	ScaleInProtocolSupport    bool
	RunningSandboxes          int64
	StartsInFlight            int64
	LifecycleCleanupsInFlight int64
	SnapshotUploadsInFlight   int64
	ShutdownReady             bool
	SandboxListEmpty          bool
	ScaleInOperationID        string
}

type ScaleInWorkerControl interface {
	ListScaleInCandidates(ctx context.Context, clusterID string) ([]ScaleInCandidateObservation, error)
	BeginWorkerScaleIn(ctx context.Context, clusterID, nodeID, serviceInstanceID, operationID string) (WorkerScaleInState, error)
	VerifyWorkerScaleIn(ctx context.Context, clusterID, nodeID, serviceInstanceID, operationID string) (WorkerScaleInState, error)
	CancelWorkerScaleIn(ctx context.Context, clusterID, nodeID, serviceInstanceID, operationID string) (WorkerScaleInState, error)
}

type ScaleInASGInstance struct {
	ID                   string
	HealthStatus         string
	LifecycleState       string
	ProtectedFromScaleIn bool
	LaunchTime           *time.Time
}

type ScaleInASGSnapshot struct {
	Name                             string
	ARN                              string
	DesiredCapacity                  int32
	MinSize                          int32
	MaxSize                          int32
	NewInstancesProtectedFromScaleIn bool
	TerminateSuspended               bool
	ActiveInstanceRefresh            bool
	Instances                        map[string]ScaleInASGInstance
}

type ScaleInInfrastructure interface {
	Snapshot(ctx context.Context, asgName string) (ScaleInASGSnapshot, error)
	SetInstanceProtection(ctx context.Context, asgName string, instanceIDs []string, protected bool) error
}

type ScaleInMode string

const (
	ScaleInModeOff     ScaleInMode = "off"
	ScaleInModeObserve ScaleInMode = "observe"
	ScaleInModeEnforce ScaleInMode = "enforce"

	ScaleInGlobalBudget = int32(50)
)

func ParseScaleInMode(value string) (ScaleInMode, error) {
	mode := ScaleInMode(value)
	switch mode {
	case ScaleInModeOff, ScaleInModeObserve, ScaleInModeEnforce:
		return mode, nil
	default:
		return "", errors.New("scale-in mode must be off, observe, or enforce")
	}
}

// ScaleInNode contains only capacity and infrastructure state. LaunchTime must
// come from an authoritative infrastructure source; a missing value makes a
// node ineligible instead of being guessed from Nomad indexes or status time.
type ScaleInNode struct {
	NodeID                 string
	NomadNodeID            string
	ServiceInstanceID      string
	RunningSandboxes       int64
	KnownWorkload          int64
	NomadCreateIndex       uint64
	LaunchTime             *time.Time
	ScaleInProtocolSupport bool
	Ready                  bool
	Healthy                bool
	Eligible               bool
	Draining               bool
	Terminating            bool
}

type ScaleInPlan struct {
	RawRequired    int64
	HeadroomNodes  int64
	SafeRequired   int64
	ReadyNodes     int32
	AcceptingNodes int32
	DisruptionUsed int32
	Excess         int32
}

func BuildScaleInPlan(workloadCount int64, slotsPerNode int, minNodes int32, headroomPercent int, nodes []ScaleInNode) (ScaleInPlan, error) {
	if workloadCount < 0 {
		return ScaleInPlan{}, errors.New("workload count must be non-negative")
	}
	if slotsPerNode <= 0 {
		return ScaleInPlan{}, errors.New("slots per node must be positive")
	}
	if minNodes < 0 {
		return ScaleInPlan{}, errors.New("minimum nodes must be non-negative")
	}
	if headroomPercent < 0 {
		return ScaleInPlan{}, errors.New("headroom percent must be non-negative")
	}

	rawRequired := ceilDiv(workloadCount, int64(slotsPerNode))
	headroomNodes := ceilPercent(rawRequired, headroomPercent)
	safeRequired := saturatingAdd(rawRequired, headroomNodes)
	safeRequired = max(safeRequired, int64(minNodes))

	plan := ScaleInPlan{
		RawRequired:   rawRequired,
		HeadroomNodes: headroomNodes,
		SafeRequired:  safeRequired,
	}
	for _, node := range nodes {
		if node.Ready {
			plan.ReadyNodes++
		}
		if !node.Ready || node.Draining || node.Terminating {
			plan.DisruptionUsed++
		}
		if node.Ready && node.Healthy && node.Eligible && !node.Draining && !node.Terminating {
			plan.AcceptingNodes++
		}
	}
	if int64(plan.AcceptingNodes) > safeRequired {
		plan.Excess = int32(min(int64(math.MaxInt32), int64(plan.AcceptingNodes)-safeRequired))
	}

	return plan, nil
}

type ScaleInBudget struct {
	Global          int32
	Graceful        int32
	AvailableGlobal int32
	AllowedEmpty    int32
	AllowedNonEmpty int32
}

func BuildScaleInBudget(readyNodes, excess, emptyExcess, disruptionUsed int32) ScaleInBudget {
	readyNodes = max(readyNodes, 0)
	excess = max(excess, 0)
	emptyExcess = max(emptyExcess, 0)
	disruptionUsed = max(disruptionUsed, 0)

	graceful := min(ceilDivInt32(readyNodes, 10), ScaleInGlobalBudget)
	availableGlobal := max(int32(0), ScaleInGlobalBudget-disruptionUsed)

	return ScaleInBudget{
		Global:          ScaleInGlobalBudget,
		Graceful:        graceful,
		AvailableGlobal: availableGlobal,
		AllowedEmpty:    min(emptyExcess, availableGlobal),
		AllowedNonEmpty: min(excess, max(int32(0), graceful-disruptionUsed), availableGlobal),
	}
}

type CandidateBlockReason string

const (
	CandidateAllowed             CandidateBlockReason = ""
	CandidateUnsupportedProtocol CandidateBlockReason = "unsupported_worker_protocol"
	CandidateNotAccepting        CandidateBlockReason = "node_not_accepting"
	CandidateUnknownLaunchTime   CandidateBlockReason = "node_launch_time_unknown"
	CandidateTooYoung            CandidateBlockReason = "node_too_young"
)

func ScaleInCandidateBlockReason(node ScaleInNode, now time.Time, minimumAge time.Duration) CandidateBlockReason {
	if !node.ScaleInProtocolSupport {
		return CandidateUnsupportedProtocol
	}
	if !node.Ready || !node.Healthy || !node.Eligible || node.Draining || node.Terminating {
		return CandidateNotAccepting
	}
	if node.LaunchTime == nil || node.LaunchTime.IsZero() {
		return CandidateUnknownLaunchTime
	}
	if now.Before(node.LaunchTime.Add(minimumAge)) {
		return CandidateTooYoung
	}

	return CandidateAllowed
}

func EligibleScaleInCandidates(nodes []ScaleInNode, now time.Time, minimumAge time.Duration) []ScaleInNode {
	candidates := make([]ScaleInNode, 0, len(nodes))
	for _, node := range nodes {
		if ScaleInCandidateBlockReason(node, now, minimumAge) == CandidateAllowed {
			candidates = append(candidates, node)
		}
	}
	slices.SortStableFunc(candidates, func(left, right ScaleInNode) int {
		if left.KnownWorkload != right.KnownWorkload {
			if left.KnownWorkload < right.KnownWorkload {
				return -1
			}

			return 1
		}
		if left.NomadCreateIndex > right.NomadCreateIndex {
			return -1
		}
		if left.NomadCreateIndex < right.NomadCreateIndex {
			return 1
		}

		return 0
	})

	return candidates
}

// SurplusStabilizer deliberately keeps no durable timestamp. A fresh
// controller therefore observes a complete new stable interval after restart.
type SurplusStabilizer struct {
	firstObservedAt time.Time
}

func (s *SurplusStabilizer) Observe(now time.Time, excess int32, dependenciesKnown bool, duration time.Duration) bool {
	if excess <= 0 || !dependenciesKnown {
		s.Reset()

		return false
	}
	if s.firstObservedAt.IsZero() || now.Before(s.firstObservedAt) {
		s.firstObservedAt = now

		return false
	}

	return !now.Before(s.firstObservedAt.Add(duration))
}

func (s *SurplusStabilizer) Reset() {
	s.firstObservedAt = time.Time{}
}

func ScaleOutRequired(rawRequired int64, uncommittedOwnedDrains int32, scaleInMode ScaleInMode) int64 {
	if scaleInMode != ScaleInModeEnforce || uncommittedOwnedDrains <= 0 {
		return rawRequired
	}

	return saturatingAdd(rawRequired, int64(uncommittedOwnedDrains))
}

func ceilPercent(value int64, percent int) int64 {
	if value == 0 || percent == 0 {
		return 0
	}
	if value > math.MaxInt64/int64(percent) {
		return math.MaxInt64
	}

	return ceilDiv(value*int64(percent), 100)
}

func saturatingAdd(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}

	return left + right
}

func ceilDivInt32(numerator, denominator int32) int32 {
	if numerator == 0 {
		return 0
	}

	return (numerator-1)/denominator + 1
}
