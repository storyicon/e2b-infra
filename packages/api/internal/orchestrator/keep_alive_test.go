package orchestrator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
)

type keepAliveEvents struct {
	mu     sync.Mutex
	values []string
}

func (e *keepAliveEvents) add(value string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.values = append(e.values, value)
}

func (e *keepAliveEvents) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]string(nil), e.values...)
}

type recordingSandboxStorage struct {
	*memorySandboxStorage

	onUpdate func()
}

func (s *recordingSandboxStorage) Update(ctx context.Context, teamID uuid.UUID, sandboxID string, update func(sandbox.Sandbox) (sandbox.Sandbox, error)) (sandbox.Sandbox, error) {
	updated, err := s.memorySandboxStorage.Update(ctx, teamID, sandboxID, update)
	if err == nil && s.onUpdate != nil {
		s.onUpdate()
	}

	return updated, err
}

func TestGetMaxTTLNormal(t *testing.T) {
	t.Parallel()
	now := time.Now()
	ttl := getMaxAllowedTTL(now, now, 2*time.Hour, 3*time.Hour)
	if ttl != 2*time.Hour {
		t.Fatalf("expected 2 hours, got %v", ttl)
	}
}

func TestGetMaxTTLMax(t *testing.T) {
	t.Parallel()
	now := time.Now()
	ttl := getMaxAllowedTTL(now, now, 4*time.Hour, 3*time.Hour)
	if ttl != 3*time.Hour {
		t.Fatalf("expected 3 hours, got %v", ttl)
	}
}

func TestGetMaxTTLExpired(t *testing.T) {
	t.Parallel()
	now := time.Now()
	ttl := getMaxAllowedTTL(now, now.Add(-2*time.Hour), 4*time.Hour, time.Hour)
	if ttl != 0 {
		t.Fatalf("expected 0 hours, got %v", ttl)
	}
}

func TestKeepAliveWorkloadDeadlineOrdering(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		oldRemaining time.Duration
		requested    time.Duration
		deadlineErr  error
		wantError    bool
		expected     []string
	}{
		{name: "extend", oldRemaining: 10 * time.Minute, requested: 20 * time.Minute, expected: []string{"deadline", "product", "worker"}},
		{name: "shorten", oldRemaining: 20 * time.Minute, requested: 10 * time.Minute, expected: []string{"product", "worker", "deadline"}},
		{name: "extend ledger failure stops product update", oldRemaining: 10 * time.Minute, requested: 20 * time.Minute, deadlineErr: errors.New("ledger unavailable"), wantError: true, expected: []string{"deadline"}},
		{name: "shorten ledger failure keeps conservative overcount", oldRemaining: 20 * time.Minute, requested: 10 * time.Minute, deadlineErr: errors.New("ledger unavailable"), wantError: true, expected: []string{"product", "worker", "deadline"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			events := &keepAliveEvents{}
			workloads := &fakeWorkloadStore{deadlineErr: tc.deadlineErr, onDeadline: func() { events.add("deadline") }}
			storage := &recordingSandboxStorage{memorySandboxStorage: newMemorySandboxStorage(), onUpdate: func() { events.add("product") }}
			o, reservations := newStartIntentTestOrchestratorWithReservation(t)
			o.capacityDemandMode = cfg.SandboxCapacityDemandModeWorkloadV2
			o.workloadLeaseStore = workloads
			o.sandboxStore = sandbox.NewStore(storage, storage, reservations, sandbox.Callbacks{
				AddSandboxToRoutingTable: func(context.Context, sandbox.Sandbox) error { return nil },
				AsyncNewlyCreatedSandbox: func(context.Context, sandbox.Sandbox, sandbox.CreationMetadata) {},
			})
			node := o.GetClusterNodes(uuid.Nil)[0]
			node.SetSandboxClient(&workloadTestSandboxClient{onUpdate: func() { events.add("worker") }})

			now := time.Now()
			sbx := sandbox.Sandbox{
				SandboxID:         "keep-alive-sandbox",
				ExecutionID:       "keep-alive-execution",
				TeamID:            uuid.New(),
				ClusterID:         uuid.Nil,
				NodeID:            node.ID,
				StartTime:         now.Add(-time.Minute),
				EndTime:           now.Add(tc.oldRemaining),
				MaxInstanceLength: time.Hour,
				State:             sandbox.StateRunning,
			}
			require.NoError(t, storage.Add(t.Context(), sbx))

			_, apiErr := o.KeepAliveFor(t.Context(), sbx.TeamID, sbx.SandboxID, tc.requested, true)
			if tc.wantError {
				require.NotNil(t, apiErr)
				require.ErrorContains(t, apiErr.Err, "ledger unavailable")
			} else {
				require.Nil(t, apiErr)
			}
			require.Equal(t, tc.expected, events.snapshot())
		})
	}
}
