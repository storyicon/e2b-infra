package sandbox

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
)

type CreationMetadata struct {
	IsResume       bool
	TeamName       string
	RequestHeader  http.Header
	MCPServerNames []string
}

type (
	InsertCallback   func(ctx context.Context, sbx Sandbox) error
	OrphanCallback   func(ctx context.Context, sbx NodeSandbox)
	CreationCallback func(ctx context.Context, sbx Sandbox, meta CreationMetadata)
)

const sbxRemoveTimeout = 10 * time.Second

// Storage and ReservationStorage are re-exported from sandboxtypes so external
// callers can continue to use sandbox.Storage / sandbox.ReservationStorage.
// They live in sandboxtypes (a leaf package) so storage backends can implement
// them without creating an import cycle back into package sandbox.
type (
	Storage            = sandboxtypes.Storage
	CapacityStorage    = sandboxtypes.CapacityStorage
	ReservationStorage = sandboxtypes.ReservationStorage
	ReservationOwner   = sandboxtypes.ReservationOwner
)

type Callbacks struct {
	// AddSandboxToRoutingTable should be called sync to prevent race conditions where we would know where to route the sandbox
	AddSandboxToRoutingTable InsertCallback
	// AsyncNewlyCreatedSandbox is called asynchronously for newly created sandboxes (Add called with non-nil CreationMetadata).
	AsyncNewlyCreatedSandbox CreationCallback
	// KillOrphanSandbox kills an orphaned sandbox on the orchestrator node via gRPC.
	// Used during sync when the Redis backend detects sandboxes running on a node but not present in the store.
	KillOrphanSandbox OrphanCallback
}

type Store struct {
	storage         Storage
	capacityStorage CapacityStorage
	callbacks       Callbacks

	reservations ReservationStorage
}

func NewStore(
	backend Storage,
	capacityBackend CapacityStorage,
	reservations ReservationStorage,
	callbacks Callbacks,
) *Store {
	return &Store{
		storage:         backend,
		capacityStorage: capacityBackend,
		reservations:    reservations,
		callbacks:       callbacks,
	}
}

// Add inserts a sandbox into the store. A non-nil creation argument fires the
// AsyncNewlyCreatedSandbox callback; nil indicates a sync/reconcile re-add.
func (s *Store) Add(ctx context.Context, sandbox Sandbox, creation *CreationMetadata) error {
	return s.add(ctx, sandbox, creation, s.storage.Add)
}

func (s *Store) AddCapacity(ctx context.Context, sandbox Sandbox, creation *CreationMetadata) error {
	return s.add(ctx, sandbox, creation, s.capacityStorage.AddCapacity)
}

func (s *Store) add(ctx context.Context, sandbox Sandbox, creation *CreationMetadata, persist func(context.Context, Sandbox) error) error {
	sbxlogger.I(sandbox).Debug(ctx, "Adding sandbox to cache",
		zap.Bool("newly_created", creation != nil),
		logger.Time("start_time", sandbox.StartTime),
		logger.Time("end_time", sandbox.EndTime),
	)

	endTime := sandbox.EndTime

	if endTime.Sub(sandbox.StartTime) > sandbox.MaxInstanceLength {
		sandbox.EndTime = sandbox.StartTime.Add(sandbox.MaxInstanceLength)
	}

	err := persist(ctx, sandbox)
	if err != nil {
		return err
	}
	if err := s.callbacks.AddSandboxToRoutingTable(ctx, sandbox); err != nil {
		return fmt.Errorf("add sandbox to routing table: %w", err)
	}

	if creation != nil {
		meta := *creation
		go s.callbacks.AsyncNewlyCreatedSandbox(context.WithoutCancel(ctx), sandbox, meta)
	}

	return nil
}

func (s *Store) Get(ctx context.Context, teamID uuid.UUID, sandboxID string) (Sandbox, error) {
	return s.storage.Get(ctx, teamID, sandboxID)
}

func (s *Store) Remove(ctx context.Context, teamID uuid.UUID, sandboxID string) {
	if err := s.RemoveStrict(ctx, teamID, sandboxID); err != nil {
		logger.L().Error(ctx, "Failed to remove sandbox", zap.Error(err), logger.WithSandboxID(sandboxID))
	}
}

func (s *Store) RemoveStrict(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	storageErr := s.storage.Remove(ctx, teamID, sandboxID)
	reservationErr := s.reservations.Release(ctx, teamID, sandboxID)

	return errors.Join(storageErr, reservationErr)
}

func (s *Store) TeamItems(ctx context.Context, teamID uuid.UUID, states []State) ([]Sandbox, error) {
	return s.storage.TeamItems(ctx, teamID, states)
}

func (s *Store) ExpiredItems(ctx context.Context) ([]Sandbox, error) {
	return s.storage.ExpiredItems(ctx)
}

func (s *Store) TeamsWithSandboxes(ctx context.Context) (map[uuid.UUID]int64, error) {
	return s.storage.TeamsWithSandboxCount(ctx)
}

func (s *Store) Update(ctx context.Context, teamID uuid.UUID, sandboxID string, updateFunc func(sandbox Sandbox) (Sandbox, error)) (Sandbox, error) {
	return s.storage.Update(ctx, teamID, sandboxID, updateFunc)
}

func (s *Store) StartRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts RemoveOpts) (Sandbox, bool, func(context.Context, error), error) {
	return s.storage.StartRemoving(ctx, teamID, sandboxID, opts)
}

func (s *Store) WaitForStateChange(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	return s.storage.WaitForStateChange(ctx, teamID, sandboxID)
}

func (s *Store) Reconcile(ctx context.Context, sandboxes []NodeSandbox, nodeID string) {
	// Redis is the source of truth — divergent sandboxes are orphans running
	// on the node but not present in the store. Kill them.
	orphans := s.storage.Reconcile(ctx, sandboxes, nodeID)

	wg := sync.WaitGroup{}
	for _, sbx := range orphans {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sbxRemoveTimeout)
			defer cancel()
			s.callbacks.KillOrphanSandbox(ctx, sbx)
		})
	}

	wg.Wait()
}

func (s *Store) Reserve(ctx context.Context, teamID uuid.UUID, sandboxID string, limit int) (finishStart func(Sandbox, error), waitForStart func(ctx context.Context) (Sandbox, error), err error) {
	return s.reserve(ctx, teamID, sandboxID, func() (func(Sandbox, error), func(context.Context) (Sandbox, error), error) {
		return s.reservations.Reserve(ctx, teamID, sandboxID, limit)
	})
}

func (s *Store) ReserveOwned(ctx context.Context, teamID uuid.UUID, sandboxID string, limit int, owner sandboxtypes.ReservationOwner) (finishStart func(Sandbox, error), waitForStart func(ctx context.Context) (Sandbox, error), err error) {
	return s.reserve(ctx, teamID, sandboxID, func() (func(Sandbox, error), func(context.Context) (Sandbox, error), error) {
		return s.reservations.ReserveOwned(ctx, teamID, sandboxID, limit, owner)
	})
}

func (s *Store) reserve(_ context.Context, teamID uuid.UUID, sandboxID string, reserve func() (func(Sandbox, error), func(context.Context) (Sandbox, error), error)) (finishStart func(Sandbox, error), waitForStart func(ctx context.Context) (Sandbox, error), err error) {
	finishStart, waitForStart, err = reserve()
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			// Try to get the sandbox from the storage if already exists
			return nil, func(ctx context.Context) (Sandbox, error) {
				return s.storage.Get(ctx, teamID, sandboxID)
			}, nil
		}

		return nil, nil, err
	}

	return finishStart, waitForStart, nil
}
