package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/capacity-demand/startintent"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

type memorySandboxStorage struct {
	mu    sync.Mutex
	items map[string]sandbox.Sandbox
}

func TestValidateStartIntentPoolRejectsUnsupportedRequestBeforePersistence(t *testing.T) {
	t.Parallel()

	compatible := startintent.Intent{
		VCPU:          2,
		MemoryMiB:     4096,
		Compatibility: startintent.SinglePoolCompatibility,
	}
	require.NoError(t, validateStartIntentPool(compatible, 2, 4096))

	for _, incompatible := range []startintent.Intent{
		{VCPU: 4, MemoryMiB: 4096, Compatibility: startintent.SinglePoolCompatibility},
		{VCPU: 2, MemoryMiB: 8192, Compatibility: startintent.SinglePoolCompatibility},
		{VCPU: 2, MemoryMiB: 4096, Compatibility: startintent.SinglePoolCompatibility + ":labels"},
	} {
		require.ErrorContains(t, validateStartIntentPool(incompatible, 2, 4096), "does not match autoscaled pool")
	}
}

func TestStartIntentCompatibilityChecksBuildCPUAgainstPool(t *testing.T) {
	t.Parallel()

	pool := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: machineinfo.IceLakeModel}
	compatible := placement.CPURequirement{Build: pool}
	require.Equal(t, startintent.SinglePoolCompatibility, startIntentCompatibility(compatible, pool, false, nil))

	incompatible := placement.CPURequirement{Build: machineinfo.MachineInfo{
		CPUArchitecture: "x86_64",
		CPUFamily:       "6",
		CPUModel:        machineinfo.EmeraldRapidsModel,
	}}
	require.NotEqual(t, startintent.SinglePoolCompatibility, startIntentCompatibility(incompatible, pool, false, nil))
}

func TestBenchmarkRunHashFromMetadataIsScopedAndDoesNotLogRawIdentifier(t *testing.T) {
	t.Parallel()

	runHash, ok := benchmarkRunHashFromMetadata(map[string]string{"benchmarkRunId": "run-1"})
	require.True(t, ok)
	require.Equal(t, "4e65d3fbe8ad6535681b021b30785b12b6c0e3f8878859a4148b3f58b8835db0", runHash)
	require.NotContains(t, runHash, "run-1")

	_, ok = benchmarkRunHashFromMetadata(map[string]string{"benchmarkRunId": "invalid/run"})
	require.False(t, ok)
}

func newMemorySandboxStorage() *memorySandboxStorage {
	return &memorySandboxStorage{items: make(map[string]sandbox.Sandbox)}
}

func (s *memorySandboxStorage) Add(_ context.Context, sbx sandbox.Sandbox) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[sbx.SandboxID] = sbx

	return nil
}

func (s *memorySandboxStorage) AddCapacity(ctx context.Context, sbx sandbox.Sandbox) error {
	return s.Add(ctx, sbx)
}

func (s *memorySandboxStorage) Get(_ context.Context, _ uuid.UUID, sandboxID string) (sandbox.Sandbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sbx, ok := s.items[sandboxID]
	if !ok {
		return sandbox.Sandbox{}, sandbox.ErrNotFound
	}

	return sbx, nil
}

func (s *memorySandboxStorage) Remove(_ context.Context, _ uuid.UUID, sandboxID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, sandboxID)

	return nil
}

func (s *memorySandboxStorage) TeamItems(context.Context, uuid.UUID, []sandbox.State) ([]sandbox.Sandbox, error) {
	return nil, nil
}

func (s *memorySandboxStorage) ExpiredItems(context.Context) ([]sandbox.Sandbox, error) {
	return nil, nil
}

func (s *memorySandboxStorage) TeamsWithSandboxCount(context.Context) (map[uuid.UUID]int64, error) {
	return nil, nil
}

func (s *memorySandboxStorage) Update(ctx context.Context, _ uuid.UUID, sandboxID string, update func(sandbox.Sandbox) (sandbox.Sandbox, error)) (sandbox.Sandbox, error) {
	current, err := s.Get(ctx, uuid.Nil, sandboxID)
	if err != nil {
		return sandbox.Sandbox{}, err
	}
	updated, err := update(current)
	if err != nil {
		return sandbox.Sandbox{}, err
	}
	_ = s.Add(ctx, updated)

	return updated, nil
}

func (s *memorySandboxStorage) StartRemoving(context.Context, uuid.UUID, string, sandbox.RemoveOpts) (sandbox.Sandbox, bool, func(context.Context, error), error) {
	return sandbox.Sandbox{}, false, nil, errors.New("not implemented")
}

func (s *memorySandboxStorage) WaitForStateChange(context.Context, uuid.UUID, string) error {
	return errors.New("not implemented")
}

func (s *memorySandboxStorage) Reconcile(context.Context, []sandbox.NodeSandbox, string) []sandbox.NodeSandbox {
	return nil
}

type memoryReservation struct {
	mu          sync.Mutex
	entries     map[string]*memoryReservationEntry
	legacyCalls int
	ownedCalls  int
}

type memoryReservationEntry struct {
	done chan struct{}
	sbx  sandbox.Sandbox
	err  error
}

func newMemoryReservation() *memoryReservation {
	return &memoryReservation{entries: make(map[string]*memoryReservationEntry)}
}

func (r *memoryReservation) Reserve(_ context.Context, _ uuid.UUID, sandboxID string, _ int) (func(sandbox.Sandbox, error), func(context.Context) (sandbox.Sandbox, error), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.legacyCalls++

	finishStart, waitForStart := r.reserve(sandboxID)

	return finishStart, waitForStart, nil
}

func (r *memoryReservation) reserve(sandboxID string) (func(sandbox.Sandbox, error), func(context.Context) (sandbox.Sandbox, error)) {
	if entry, ok := r.entries[sandboxID]; ok {
		return nil, func(ctx context.Context) (sandbox.Sandbox, error) {
			select {
			case <-ctx.Done():
				return sandbox.Sandbox{}, ctx.Err()
			case <-entry.done:
				return entry.sbx, entry.err
			}
		}
	}

	entry := &memoryReservationEntry{done: make(chan struct{})}
	r.entries[sandboxID] = entry
	var once sync.Once
	finish := func(sbx sandbox.Sandbox, err error) {
		once.Do(func() {
			entry.sbx = sbx
			entry.err = err
			close(entry.done)
		})
	}

	return finish, nil
}

func (r *memoryReservation) ReserveOwned(_ context.Context, _ uuid.UUID, sandboxID string, _ int, _ sandbox.ReservationOwner) (func(sandbox.Sandbox, error), func(context.Context) (sandbox.Sandbox, error), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ownedCalls++

	finishStart, waitForStart := r.reserve(sandboxID)

	return finishStart, waitForStart, nil
}

func (r *memoryReservation) Release(_ context.Context, _ uuid.UUID, sandboxID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, sandboxID)

	return nil
}

func newStartIntentTestOrchestrator(t *testing.T) *Orchestrator {
	t.Helper()

	o, _ := newStartIntentTestOrchestratorWithReservation(t)

	return o
}

func newStartIntentTestOrchestratorWithReservation(t *testing.T) (*Orchestrator, *memoryReservation) {
	t.Helper()

	ffClient, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	meter := noop.NewMeterProvider().Meter("start-intent-test")
	counter, err := meter.Int64Counter("created")
	require.NoError(t, err)

	reservations := newMemoryReservation()
	store := sandbox.NewStore(newMemorySandboxStorage(), reservations, sandbox.Callbacks{
		AddSandboxToRoutingTable: func(context.Context, sandbox.Sandbox) {},
		AsyncNewlyCreatedSandbox: func(context.Context, sandbox.Sandbox, sandbox.CreationMetadata) {},
	})
	o := &Orchestrator{
		sandboxStore:            store,
		nodes:                   smap.New[*nodemanager.Node](),
		placementAlgorithm:      placement.NewBestOfK(placement.DefaultBestOfKConfig()).(*placement.BestOfK),
		featureFlagsClient:      ffClient,
		createdSandboxesCounter: counter,
		capacityDemandMode:      cfg.SandboxCapacityDemandModeLegacy,
	}
	node := nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 8)
	node.ClusterID = uuid.Nil
	o.registerNode(node)

	return o, reservations
}

type fakeStartIntentStore struct {
	mu               sync.Mutex
	records          map[string]startintent.Record
	history          []string
	upsertErr        error
	heartbeatErr     error
	handoffErr       error
	removeErr        error
	removeCtxErr     error
	activeErr        error
	heartbeatUpdated bool
}

func newFakeStartIntentStore() *fakeStartIntentStore {
	return &fakeStartIntentStore{records: make(map[string]startintent.Record), heartbeatUpdated: true}
}

func (s *fakeStartIntentStore) Upsert(_ context.Context, intent startintent.Intent, now, expiresAt time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = append(s.history, "upsert")
	if s.upsertErr != nil {
		return false, s.upsertErr
	}
	if _, ok := s.records[intent.SandboxID]; ok {
		return false, nil
	}
	s.records[intent.SandboxID] = startintent.Record{Intent: intent, State: startintent.StateOutstanding, CreatedAt: now, ExpiresAt: expiresAt}

	return true, nil
}

func (s *fakeStartIntentStore) Heartbeat(_ context.Context, clusterID, sandboxID, ownerToken string, _, expiresAt time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = append(s.history, "heartbeat")
	if s.heartbeatErr != nil {
		return false, s.heartbeatErr
	}
	record, ok := s.records[sandboxID]
	if !ok || record.ClusterID != clusterID || record.OwnerToken != ownerToken || record.State != startintent.StateOutstanding || !s.heartbeatUpdated {
		return false, nil
	}
	record.ExpiresAt = expiresAt
	s.records[sandboxID] = record

	return true, nil
}

func (s *fakeStartIntentStore) Handoff(_ context.Context, clusterID, sandboxID, ownerToken string, _, expiresAt time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = append(s.history, "handoff")
	if s.handoffErr != nil {
		return false, s.handoffErr
	}
	record, ok := s.records[sandboxID]
	if !ok || record.ClusterID != clusterID || record.OwnerToken != ownerToken || record.State != startintent.StateOutstanding {
		return false, nil
	}
	record.State = startintent.StateHandoff
	record.ExpiresAt = expiresAt
	s.records[sandboxID] = record

	return true, nil
}

func (s *fakeStartIntentStore) Remove(ctx context.Context, clusterID, sandboxID, ownerToken string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = append(s.history, "remove")
	s.removeCtxErr = ctx.Err()
	if s.removeErr != nil {
		return false, s.removeErr
	}
	record, ok := s.records[sandboxID]
	if !ok || record.ClusterID != clusterID || record.OwnerToken != ownerToken {
		return false, nil
	}
	delete(s.records, sandboxID)

	return true, nil
}

func (s *fakeStartIntentStore) Active(_ context.Context, clusterID string, _ time.Time) ([]startintent.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeErr != nil {
		return nil, s.activeErr
	}
	result := make([]startintent.Record, 0, len(s.records))
	for _, record := range s.records {
		if record.ClusterID == clusterID {
			result = append(result, record)
		}
	}

	return result, nil
}

func (s *fakeStartIntentStore) appendHistory(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = append(s.history, event)
}

func (s *fakeStartIntentStore) snapshotHistory() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.history...)
}

func configureStartIntentMode(o *Orchestrator, store startIntentStore) {
	o.capacityDemandMode = cfg.SandboxCapacityDemandModeStartIntentV1
	o.capacityPoolVCPU = testBuild().Vcpu
	o.capacityPoolMemoryMiB = testBuild().RamMb
	o.capacityPoolCPU = machineinfo.MachineInfo{
		CPUArchitecture: "x86_64",
		CPUFamily:       "6",
		CPUModel:        machineinfo.EmeraldRapidsModel,
	}
	o.startIntentStore = store
	o.startIntentLeaseTTL = time.Minute
	o.startIntentHeartbeatInterval = time.Hour
	o.startIntentHandoffTTL = time.Minute
}

func TestCreateSandboxReservationOwnershipIsFeatureGated(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name       string
		configure  func(*Orchestrator)
		wantLegacy int
		wantOwned  int
	}{
		{name: "legacy mode preserves legacy reservation protocol", configure: func(*Orchestrator) {}, wantLegacy: 1},
		{name: "start intent mode uses owner fencing", configure: func(o *Orchestrator) {
			configureStartIntentMode(o, newFakeStartIntentStore())
		}, wantOwned: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			o, reservations := newStartIntentTestOrchestratorWithReservation(t)
			testCase.configure(o)
			node := o.GetClusterNodes(uuid.Nil)[0]
			node.SetSandboxClient(&nodemanager.MockSandboxClientCustom{})

			now := time.Now()
			_, apiErr := o.CreateSandbox(t.Context(), fixedSandboxID(t), uuid.NewString(), testTeam(), successFetcher(), now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
			require.Nil(t, apiErr)
			require.Equal(t, testCase.wantLegacy, reservations.legacyCalls)
			require.Equal(t, testCase.wantOwned, reservations.ownedCalls)
		})
	}
}

func TestCreateSandboxStartIntentOwnerLifecycleAndOrdering(t *testing.T) {
	t.Parallel()

	store := newFakeStartIntentStore()
	o := newStartIntentTestOrchestrator(t)
	configureStartIntentMode(o, store)
	node := o.GetClusterNodes(uuid.Nil)[0]
	node.SetSandboxClient(&nodemanager.MockSandboxClientCustom{CreateFunc: func() error {
		store.appendHistory("placement")

		return nil
	}})

	now := time.Now()
	_, apiErr := o.CreateSandbox(t.Context(), fixedSandboxID(t), uuid.NewString(), testTeam(), successFetcher(), now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
	require.Nil(t, apiErr)
	require.Equal(t, []string{"upsert", "placement", "handoff", "remove"}, store.snapshotHistory())
}

func TestCreateSandboxRejectsIncompatibleBuildCPUBeforeIntentPersistence(t *testing.T) {
	t.Parallel()

	store := newFakeStartIntentStore()
	o := newStartIntentTestOrchestrator(t)
	configureStartIntentMode(o, store)
	o.capacityPoolCPU.CPUModel = machineinfo.IceLakeModel

	fetcher := func(ctx context.Context) (SandboxMetadata, *api.APIError) {
		metadata, fetchErr := successFetcher()(ctx)
		architecture := "x86_64"
		family := "6"
		model := machineinfo.EmeraldRapidsModel
		metadata.Build.CpuArchitecture = &architecture
		metadata.Build.CpuFamily = &family
		metadata.Build.CpuModel = &model

		return metadata, fetchErr
	}

	now := time.Now()
	_, apiErr := o.CreateSandbox(t.Context(), fixedSandboxID(t), uuid.NewString(), testTeam(), fetcher, now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
	require.NotNil(t, apiErr)
	require.Equal(t, http.StatusUnprocessableEntity, apiErr.Code)
	require.Equal(t, "sandbox_capacity_pool_incompatible", apiErr.ErrorCode)
	require.Empty(t, store.snapshotHistory())
}

func TestCreateSandboxJoinedRequestDoesNotRegisterAnotherIntent(t *testing.T) {
	t.Parallel()

	store := newFakeStartIntentStore()
	o := newStartIntentTestOrchestrator(t)
	configureStartIntentMode(o, store)
	team := testTeam()
	sandboxID := fixedSandboxID(t)
	now := time.Now()
	fetcherEntered := make(chan struct{})
	releaseFetcher := make(chan struct{})
	winnerDone := make(chan *api.APIError, 1)

	go func() {
		_, apiErr := o.CreateSandbox(t.Context(), sandboxID, uuid.NewString(), team, func(fetchCtx context.Context) (SandboxMetadata, *api.APIError) {
			close(fetcherEntered)
			<-releaseFetcher

			return successFetcher()(fetchCtx)
		}, now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
		winnerDone <- apiErr
	}()
	<-fetcherEntered

	joinerDone := make(chan *api.APIError, 1)
	go func() {
		_, apiErr := o.CreateSandbox(t.Context(), sandboxID, uuid.NewString(), team, func(context.Context) (SandboxMetadata, *api.APIError) {
			t.Error("joined request must not fetch metadata or register an intent")

			return SandboxMetadata{}, nil
		}, now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
		joinerDone <- apiErr
	}()

	close(releaseFetcher)
	require.Nil(t, <-winnerDone)
	require.Nil(t, <-joinerDone)
	require.Equal(t, []string{"upsert", "handoff", "remove"}, store.snapshotHistory())
}

func TestCreateSandboxStartIntentStoreFailureStopsBeforePlacement(t *testing.T) {
	t.Parallel()

	store := newFakeStartIntentStore()
	store.upsertErr = errors.New("redis unavailable")
	o := newStartIntentTestOrchestrator(t)
	configureStartIntentMode(o, store)
	placements := 0
	o.GetClusterNodes(uuid.Nil)[0].SetSandboxClient(&nodemanager.MockSandboxClientCustom{CreateFunc: func() error {
		placements++

		return nil
	}})

	now := time.Now()
	_, apiErr := o.CreateSandbox(t.Context(), fixedSandboxID(t), uuid.NewString(), testTeam(), successFetcher(), now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
	require.NotNil(t, apiErr)
	require.Equal(t, http.StatusServiceUnavailable, apiErr.Code)
	require.Equal(t, 0, placements)
	require.Equal(t, []string{"upsert"}, store.snapshotHistory())
}

func TestCreateSandboxStartIntentFailureRemovesWithDetachedContext(t *testing.T) {
	t.Parallel()

	store := newFakeStartIntentStore()
	o := newStartIntentTestOrchestrator(t)
	configureStartIntentMode(o, store)
	ctx, cancel := context.WithCancel(t.Context())
	o.GetClusterNodes(uuid.Nil)[0].SetSandboxClient(&nodemanager.MockSandboxClientCustom{CreateFunc: func() error {
		cancel()

		return errors.New("create failed")
	}})

	now := time.Now()
	_, apiErr := o.CreateSandbox(ctx, fixedSandboxID(t), uuid.NewString(), testTeam(), successFetcher(), now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
	require.NotNil(t, apiErr)
	require.NoError(t, store.removeCtxErr)
	require.Contains(t, store.snapshotHistory(), "remove")
}

func TestDualWriteUsesStartIntentAndLegacyLedgerWithoutChangingControllerReadMode(t *testing.T) {
	t.Parallel()

	intentStore := newFakeStartIntentStore()
	legacyStore := &fakeCapacityDemandStore{}
	o := &Orchestrator{capacityWaiter: newCapacityWaiter(legacyStore, 100*time.Millisecond, time.Millisecond)}
	intent := startintent.Intent{
		ClusterID: "cluster", SandboxID: "sandbox", OwnerToken: "owner",
		VCPU: 2, MemoryMiB: 512, Compatibility: "single-pool-v1",
	}
	lease, err := beginStartIntent(t.Context(), intentStore, intent, time.Minute, time.Hour, time.Minute)
	require.NoError(t, err)
	defer lease.Remove(t.Context())

	attempts := 0
	err = o.waitForCapacity(lease.Context(), cfg.SandboxCapacityDemandModeDualWrite, intent, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return placement.NoNodesAvailableError{}
		}

		return nil
	})
	require.NoError(t, err)
	require.NoError(t, lease.Handoff(t.Context()))
	lease.Remove(t.Context())
	require.Equal(t, []string{"upsert", "handoff", "remove"}, intentStore.snapshotHistory())
	require.Len(t, legacyStore.upserted, 1, "dual-write must populate the isolated legacy demand namespace")
	require.Len(t, legacyStore.fulfilled, 1, "legacy demand must be completed after placement succeeds")
	require.Empty(t, legacyStore.removed)
}

type cancelBlockingHeartbeatStore struct {
	*fakeStartIntentStore

	heartbeatEntered chan struct{}
}

func (s *cancelBlockingHeartbeatStore) Heartbeat(
	ctx context.Context,
	_, _, _ string,
	_, _ time.Time,
) (bool, error) {
	close(s.heartbeatEntered)
	<-ctx.Done()

	return false, ctx.Err()
}

func TestStartIntentHandoffDoesNotTreatItsHeartbeatCancellationAsLeaseLoss(t *testing.T) {
	t.Parallel()

	store := &cancelBlockingHeartbeatStore{
		fakeStartIntentStore: newFakeStartIntentStore(),
		heartbeatEntered:     make(chan struct{}),
	}
	intent := startintent.Intent{
		ClusterID: "cluster", SandboxID: "sandbox", OwnerToken: "owner",
		VCPU: 1, MemoryMiB: 512, Compatibility: startintent.SinglePoolCompatibility,
	}
	lease, err := beginStartIntent(t.Context(), store, intent, time.Minute, time.Millisecond, time.Minute)
	require.NoError(t, err)
	defer lease.Remove(t.Context())

	select {
	case <-store.heartbeatEntered:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not start")
	}

	require.NoError(t, lease.Handoff(t.Context()))
}

var _ startIntentStore = (*fakeStartIntentStore)(nil)
