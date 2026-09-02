package orchestrator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	sandboxcatalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
)

type workloadTestCatalog struct {
	onDelete func()
}

func (workloadTestCatalog) GetSandbox(context.Context, string) (*sandboxcatalog.SandboxInfo, error) {
	return nil, sandboxcatalog.ErrSandboxNotFound
}

func (workloadTestCatalog) StoreSandbox(context.Context, string, *sandboxcatalog.SandboxInfo, time.Duration) error {
	return nil
}

func (c workloadTestCatalog) DeleteSandbox(context.Context, string, string) error {
	if c.onDelete != nil {
		c.onDelete()
	}

	return nil
}

func (workloadTestCatalog) Close(context.Context) error { return nil }

type workloadTestSandboxClient struct {
	orchestratorgrpc.SandboxServiceClient

	onDelete  func()
	onUpdate  func()
	deleteErr error
}

func (c *workloadTestSandboxClient) Update(_ context.Context, _ *orchestratorgrpc.SandboxUpdateRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	if c.onUpdate != nil {
		c.onUpdate()
	}

	return &emptypb.Empty{}, nil
}

func (c *workloadTestSandboxClient) Create(_ context.Context, _ *orchestratorgrpc.SandboxCreateRequest, _ ...grpc.CallOption) (*orchestratorgrpc.SandboxCreateResponse, error) {
	return &orchestratorgrpc.SandboxCreateResponse{}, nil
}

func (c *workloadTestSandboxClient) Delete(_ context.Context, _ *orchestratorgrpc.SandboxDeleteRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	if c.onDelete != nil {
		c.onDelete()
	}
	if c.deleteErr != nil {
		return nil, c.deleteErr
	}

	return &emptypb.Empty{}, nil
}

type fakeWorkloadStore struct {
	mu          sync.Mutex
	history     []string
	acquired    bool
	renewed     bool
	promoted    bool
	removed     bool
	renewErr    error
	deadlineErr error
	onDeadline  func()
	lastExpires time.Time
}

func (s *fakeWorkloadStore) Acquire(context.Context, string, string, string, time.Time, time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = append(s.history, "acquire")
	s.acquired = true

	return true, nil
}

func (s *fakeWorkloadStore) Renew(_ context.Context, _, _, _ string, _ time.Time, expiresAt time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = append(s.history, "renew")
	s.lastExpires = expiresAt
	if s.renewErr != nil {
		return false, s.renewErr
	}
	s.renewed = true

	return true, nil
}

func (s *fakeWorkloadStore) Promote(_ context.Context, _, _, _ string, _ time.Time, expiresAt time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = append(s.history, "promote")
	s.lastExpires = expiresAt
	s.promoted = true

	return true, nil
}

func (s *fakeWorkloadStore) UpdateDeadline(context.Context, string, string, string, time.Time, time.Time) (bool, error) {
	s.mu.Lock()
	s.history = append(s.history, "deadline")
	onDeadline := s.onDeadline
	s.mu.Unlock()
	if onDeadline != nil {
		onDeadline()
	}
	if s.deadlineErr != nil {
		return false, s.deadlineErr
	}

	return true, nil
}

func (s *fakeWorkloadStore) Remove(context.Context, string, string, string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = append(s.history, "remove")
	s.removed = true

	return true, nil
}

func (s *fakeWorkloadStore) appendHistory(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = append(s.history, event)
}

func TestWorkloadLeasePreparesEndTimeBeforePromote(t *testing.T) {
	t.Parallel()

	store := &fakeWorkloadStore{}
	lease, err := beginWorkloadLease(t.Context(), store, "cluster", "sandbox", "execution", time.Minute, time.Hour)
	require.NoError(t, err)
	endTime := time.Now().Add(time.Hour).UTC()

	require.NoError(t, lease.PrepareCommit(t.Context(), endTime))
	require.NoError(t, lease.Promote(t.Context(), endTime))

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, []string{"acquire", "renew", "promote"}, store.history)
	require.True(t, store.acquired)
	require.True(t, store.renewed)
	require.True(t, store.promoted)
	require.False(t, store.removed)
	require.Equal(t, endTime, store.lastExpires)
}

func TestWorkloadLeasePrepareFailurePreventsPromote(t *testing.T) {
	t.Parallel()

	store := &fakeWorkloadStore{renewErr: errors.New("redis unavailable")}
	lease, err := beginWorkloadLease(t.Context(), store, "cluster", "sandbox", "execution", time.Minute, time.Hour)
	require.NoError(t, err)

	err = lease.PrepareCommit(t.Context(), time.Now().Add(time.Hour))
	require.ErrorContains(t, err, "prepare workload commit")
	lease.Remove(t.Context())

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, []string{"acquire", "renew", "remove"}, store.history)
	require.False(t, store.promoted)
}

func TestCreateSandboxWorkloadLifecycleCommitsBeforeSuccessCallback(t *testing.T) {
	t.Parallel()

	workloads := &fakeWorkloadStore{}
	o, reservations := newStartIntentTestOrchestratorWithReservation(t)
	o.capacityDemandMode = cfg.SandboxCapacityDemandModeWorkloadV2
	o.capacityPoolVCPU = testBuild().Vcpu
	o.capacityPoolMemoryMiB = testBuild().RamMb
	o.workloadLeaseStore = workloads
	o.routingCatalog = workloadTestCatalog{}
	o.startIntentLeaseTTL = time.Minute
	o.startIntentHeartbeatInterval = time.Hour
	o.sandboxStore = sandbox.NewStore(newMemorySandboxStorage(), reservations, sandbox.Callbacks{
		AddSandboxToRoutingTable: func(context.Context, sandbox.Sandbox) error {
			workloads.appendHistory("routing")

			return nil
		},
		AsyncNewlyCreatedSandbox: func(context.Context, sandbox.Sandbox, sandbox.CreationMetadata) {
			workloads.appendHistory("callback")
		},
	})
	node := o.GetClusterNodes(uuid.Nil)[0]
	node.SetSandboxClient(&nodemanager.MockSandboxClientCustom{CreateFunc: func() error {
		workloads.appendHistory("placement")

		return nil
	}})

	now := time.Now()
	_, apiErr := o.CreateSandbox(t.Context(), fixedSandboxID(t), uuid.NewString(), testTeam(), successFetcher(), now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
	require.Nil(t, apiErr)
	require.Eventually(t, func() bool {
		workloads.mu.Lock()
		defer workloads.mu.Unlock()

		return len(workloads.history) == 6
	}, time.Second, time.Millisecond)

	workloads.mu.Lock()
	defer workloads.mu.Unlock()
	require.Equal(t, []string{"acquire", "placement", "renew", "promote", "routing", "callback"}, workloads.history)
}

func TestCreateSandboxRollsBackNodeWhenWorkloadCommitFails(t *testing.T) {
	t.Parallel()

	workloads := &fakeWorkloadStore{renewErr: errors.New("redis unavailable")}
	o, _ := newStartIntentTestOrchestratorWithReservation(t)
	o.capacityDemandMode = cfg.SandboxCapacityDemandModeWorkloadV2
	o.capacityPoolVCPU = testBuild().Vcpu
	o.capacityPoolMemoryMiB = testBuild().RamMb
	o.workloadLeaseStore = workloads
	o.routingCatalog = workloadTestCatalog{}
	o.startIntentLeaseTTL = time.Minute
	o.startIntentHeartbeatInterval = time.Hour
	deleted := make(chan struct{}, 1)
	node := o.GetClusterNodes(uuid.Nil)[0]
	node.SetSandboxClient(&workloadTestSandboxClient{onDelete: func() { deleted <- struct{}{} }})

	now := time.Now()
	_, apiErr := o.CreateSandbox(t.Context(), fixedSandboxID(t), uuid.NewString(), testTeam(), successFetcher(), now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
	require.NotNil(t, apiErr)
	require.ErrorContains(t, apiErr.Err, "redis unavailable")
	require.Len(t, deleted, 1, "the created sandbox must be deleted before returning the failure")

	workloads.mu.Lock()
	defer workloads.mu.Unlock()
	require.Equal(t, []string{"acquire", "renew", "remove"}, workloads.history)
}

func TestCreateSandboxRetainsWorkloadWhenWorkerRollbackFails(t *testing.T) {
	t.Parallel()

	workloads := &fakeWorkloadStore{renewErr: errors.New("redis unavailable")}
	o, _ := newStartIntentTestOrchestratorWithReservation(t)
	o.capacityDemandMode = cfg.SandboxCapacityDemandModeWorkloadV2
	o.capacityPoolVCPU = testBuild().Vcpu
	o.capacityPoolMemoryMiB = testBuild().RamMb
	o.workloadLeaseStore = workloads
	o.routingCatalog = workloadTestCatalog{}
	o.startIntentLeaseTTL = time.Minute
	o.startIntentHeartbeatInterval = time.Hour
	node := o.GetClusterNodes(uuid.Nil)[0]
	node.SetSandboxClient(&workloadTestSandboxClient{deleteErr: errors.New("worker cleanup failed")})

	now := time.Now()
	_, apiErr := o.CreateSandbox(t.Context(), fixedSandboxID(t), uuid.NewString(), testTeam(), successFetcher(), now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
	require.NotNil(t, apiErr)
	require.ErrorContains(t, apiErr.Err, "redis unavailable")
	require.ErrorContains(t, apiErr.Err, "worker cleanup failed")

	workloads.mu.Lock()
	defer workloads.mu.Unlock()
	require.Equal(t, []string{"acquire", "renew"}, workloads.history, "uncertain worker cleanup must retain the conservative workload count")
}

func TestRemoveSandboxFromNodeDeletesRouteOnlyAfterWorkerSuccess(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		workerErr error
		expected  []string
	}{
		{name: "success", expected: []string{"worker", "route"}},
		{name: "worker failure", workerErr: errors.New("worker unavailable"), expected: []string{"worker"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			events := &keepAliveEvents{}
			o, _ := newStartIntentTestOrchestratorWithReservation(t)
			o.routingCatalog = workloadTestCatalog{onDelete: func() { events.add("route") }}
			node := o.GetClusterNodes(uuid.Nil)[0]
			node.SetSandboxClient(&workloadTestSandboxClient{
				onDelete:  func() { events.add("worker") },
				deleteErr: tc.workerErr,
			})
			sbx := sandbox.Sandbox{
				SandboxID:   "remove-sandbox",
				ExecutionID: "remove-execution",
				ClusterID:   uuid.Nil,
				NodeID:      node.ID,
			}

			err := o.removeSandboxFromNode(t.Context(), sbx, sandbox.StateActionKill, sandbox.KillReasonRequest, false)
			if tc.workerErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.workerErr.Error())
			}
			require.Equal(t, tc.expected, events.snapshot())
		})
	}
}
