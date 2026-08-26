package orchestrator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	capacitydemand "github.com/e2b-dev/infra/packages/shared/pkg/capacity-demand"
)

type fakeCapacityDemandStore struct {
	mu         sync.Mutex
	upserted   []capacitydemand.Demand
	removed    []capacitydemand.Demand
	fulfilled  []capacitydemand.Demand
	succeeded  []string
	upsertErr  error
	removeErr  error
	fulfillErr error
	successErr error
}

func (s *fakeCapacityDemandStore) RecordSuccess(_ context.Context, clusterID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.succeeded = append(s.succeeded, clusterID)

	return s.successErr
}

func (s *fakeCapacityDemandStore) Fulfill(_ context.Context, clusterID, sandboxID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fulfilled = append(s.fulfilled, capacitydemand.Demand{ClusterID: clusterID, SandboxID: sandboxID})

	return s.fulfillErr
}

func (s *fakeCapacityDemandStore) Upsert(_ context.Context, demand capacitydemand.Demand, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserted = append(s.upserted, demand)

	return s.upsertErr
}

func (s *fakeCapacityDemandStore) Remove(_ context.Context, clusterID, sandboxID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removed = append(s.removed, capacitydemand.Demand{ClusterID: clusterID, SandboxID: sandboxID})

	return s.removeErr
}

func TestCapacityWaiterRegistersOnceAndFulfillsAfterSuccess(t *testing.T) {
	t.Parallel()

	store := &fakeCapacityDemandStore{}
	waiter := newCapacityWaiter(store, 100*time.Millisecond, time.Millisecond)
	demand := capacitydemand.Demand{ClusterID: "cluster", SandboxID: "sandbox", VCPU: 1, MemoryMiB: 4096}
	attempts := 0

	err := waiter.Wait(t.Context(), demand, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return placement.NoNodesAvailableError{}
		}

		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	require.Equal(t, []capacitydemand.Demand{demand}, store.upserted)
	require.Equal(t, []capacitydemand.Demand{{ClusterID: "cluster", SandboxID: "sandbox"}}, store.fulfilled)
	require.Empty(t, store.removed)
}

func TestCapacityWaiterRecordsImmediateSuccessForAnActiveBurst(t *testing.T) {
	t.Parallel()

	store := &fakeCapacityDemandStore{}
	waiter := newCapacityWaiter(store, 100*time.Millisecond, time.Millisecond)
	demand := capacitydemand.Demand{ClusterID: "cluster", SandboxID: "sandbox", VCPU: 1, MemoryMiB: 4096}

	err := waiter.Wait(t.Context(), demand, func(context.Context) error { return nil })

	require.NoError(t, err)
	require.Equal(t, []string{"cluster"}, store.succeeded)
	require.Empty(t, store.upserted)
	require.Empty(t, store.fulfilled)
	require.Empty(t, store.removed)
}

func TestCapacityWaiterQueuesTransientNodeUnavailable(t *testing.T) {
	t.Parallel()

	store := &fakeCapacityDemandStore{}
	waiter := newCapacityWaiter(store, 100*time.Millisecond, time.Millisecond)
	demand := capacitydemand.Demand{ClusterID: "cluster", SandboxID: "sandbox", VCPU: 1, MemoryMiB: 4096}
	attempts := 0

	err := waiter.Wait(t.Context(), demand, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return placement.SandboxCreateError{
				Attempts: 1,
				LastErr:  status.Error(codes.Unavailable, "worker connection timed out"),
			}
		}

		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	require.Equal(t, []capacitydemand.Demand{demand}, store.upserted)
	require.Equal(t, []capacitydemand.Demand{{ClusterID: "cluster", SandboxID: "sandbox"}}, store.fulfilled)
	require.Empty(t, store.removed)
}

func TestCapacityWaiterDoesNotQueueNonCapacityFailure(t *testing.T) {
	t.Parallel()

	store := &fakeCapacityDemandStore{}
	waiter := newCapacityWaiter(store, 100*time.Millisecond, time.Millisecond)
	wantErr := errors.New("template is incompatible")

	err := waiter.Wait(t.Context(), capacitydemand.Demand{
		ClusterID: "cluster", SandboxID: "sandbox", VCPU: 1, MemoryMiB: 4096,
	}, func(context.Context) error { return wantErr })

	require.ErrorIs(t, err, wantErr)
	require.Empty(t, store.upserted)
	require.Empty(t, store.removed)
}

func TestCapacityWaiterFailsClosedWhenDemandCannotBePersisted(t *testing.T) {
	t.Parallel()

	store := &fakeCapacityDemandStore{upsertErr: errors.New("redis unavailable")}
	waiter := newCapacityWaiter(store, 100*time.Millisecond, time.Millisecond)

	err := waiter.Wait(t.Context(), capacitydemand.Demand{
		ClusterID: "cluster", SandboxID: "sandbox", VCPU: 1, MemoryMiB: 4096,
	}, func(context.Context) error { return placement.NoNodesAvailableError{} })

	var storeErr CapacityDemandStoreError
	require.ErrorAs(t, err, &storeErr)
	require.Empty(t, store.removed)
}

func TestCapacityWaiterTimesOutAndRemovesDemand(t *testing.T) {
	t.Parallel()

	store := &fakeCapacityDemandStore{}
	waiter := newCapacityWaiter(store, 5*time.Millisecond, time.Millisecond)
	demand := capacitydemand.Demand{ClusterID: "cluster", SandboxID: "sandbox", VCPU: 1, MemoryMiB: 4096}

	err := waiter.Wait(t.Context(), demand, func(context.Context) error {
		return placement.NoNodesAvailableError{}
	})

	var timeoutErr CapacityWaitTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.Equal(t, []capacitydemand.Demand{demand}, store.upserted)
	require.Equal(t, []capacitydemand.Demand{{ClusterID: "cluster", SandboxID: "sandbox"}}, store.removed)
}

func TestCapacityWaiterKeepsSuccessfulPlacementWhenCleanupFails(t *testing.T) {
	t.Parallel()

	store := &fakeCapacityDemandStore{fulfillErr: errors.New("redis unavailable during cleanup")}
	waiter := newCapacityWaiter(store, 100*time.Millisecond, time.Millisecond)
	demand := capacitydemand.Demand{ClusterID: "cluster", SandboxID: "sandbox", VCPU: 1, MemoryMiB: 4096}
	attempts := 0

	err := waiter.Wait(t.Context(), demand, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return placement.NoNodesAvailableError{}
		}

		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	require.Equal(t, []capacitydemand.Demand{{ClusterID: "cluster", SandboxID: "sandbox"}}, store.fulfilled)
	require.Empty(t, store.removed)
}
