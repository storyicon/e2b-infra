package adapters

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	nomadapi "github.com/hashicorp/nomad/api"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/capacity-controller/internal/controller"
)

func TestNomadCountsOnlyReadyEligibleNodesInPool(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/nodes" {
			t.Errorf("unexpected request path: %s", r.URL.Path)
			http.Error(w, "unexpected request path", http.StatusNotFound)

			return
		}
		if err := json.NewEncoder(w).Encode([]*nomadapi.NodeListStub{
			{ID: "ready", NodePool: "orchestrator", Status: nomadapi.NodeStatusReady, SchedulingEligibility: nomadapi.NodeSchedulingEligible},
			{ID: "draining", NodePool: "orchestrator", Status: nomadapi.NodeStatusReady, SchedulingEligibility: nomadapi.NodeSchedulingIneligible},
			{ID: "down", NodePool: "orchestrator", Status: nomadapi.NodeStatusDown, SchedulingEligibility: nomadapi.NodeSchedulingEligible},
			{ID: "other-pool", NodePool: "api", Status: nomadapi.NodeStatusReady, SchedulingEligibility: nomadapi.NodeSchedulingEligible},
		}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := nomadapi.NewClient(&nomadapi.Config{Address: server.URL})
	require.NoError(t, err)
	counter := NewNomad(client)

	count, err := counter.ReadyCount(t.Context(), "orchestrator")

	require.NoError(t, err)
	require.Equal(t, int32(1), count)
}

func TestOwnedNomadOperationRequiresCompleteControllerMetadata(t *testing.T) {
	t.Parallel()

	startedAt := time.Unix(100, 0).UTC()
	operation := controller.NomadScaleInOperation{
		OperationID:       "operation-1",
		ServiceInstanceID: "service-1",
		StartedAt:         startedAt,
		Stage:             "worker_draining",
		ActivityID:        "activity-1",
	}
	metadata := &nomadapi.DrainMetadata{Meta: nomadOperationMeta(operation)}

	parsed, err := ownedNomadOperation(metadata)
	require.NoError(t, err)
	require.Equal(t, operation, *parsed)

	manual, err := ownedNomadOperation(&nomadapi.DrainMetadata{Meta: map[string]string{nomadMetaOwner: "operator"}})
	require.NoError(t, err)
	require.Nil(t, manual)

	delete(metadata.Meta, nomadMetaServiceID)
	_, err = ownedNomadOperation(metadata)
	require.ErrorContains(t, err, "incomplete")
}

func TestOwnedNomadIsolationAcceptsCompletedIgnoreSystemDrain(t *testing.T) {
	t.Parallel()

	operation := &controller.NomadScaleInOperation{OperationID: "operation-1"}
	tests := []struct {
		name        string
		drain       bool
		eligibility string
		lastDrain   *nomadapi.DrainMetadata
		want        bool
	}{
		{
			name:  "active owned drain",
			drain: true,
			want:  true,
		},
		{
			name:        "completed ignore-system drain remains isolated",
			eligibility: nomadapi.NodeSchedulingIneligible,
			lastDrain:   &nomadapi.DrainMetadata{Status: nomadapi.DrainStatusComplete},
			want:        true,
		},
		{
			name:        "eligible completed drain is no longer isolated",
			eligibility: nomadapi.NodeSchedulingEligible,
			lastDrain:   &nomadapi.DrainMetadata{Status: nomadapi.DrainStatusComplete},
		},
		{
			name:        "canceled drain is not active isolation",
			eligibility: nomadapi.NodeSchedulingIneligible,
			lastDrain:   &nomadapi.DrainMetadata{Status: nomadapi.DrainStatusCanceled},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, hasOwnedNomadIsolation(test.drain, test.eligibility, test.lastDrain, operation, operation.OperationID))
		})
	}

	require.False(t, hasOwnedNomadIsolation(true, "", nil, operation, "another-operation"))
}

func TestNomadInventoryDerivesRestoringFromEligibleCompletedDrain(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name         string
		status       string
		eligibility  string
		wantStage    string
		wantDraining bool
	}{
		{name: "ready eligible node resumes restoring", status: nomadapi.NodeStatusReady, eligibility: nomadapi.NodeSchedulingEligible, wantStage: "restoring"},
		{name: "down eligible node leaves only history", status: nomadapi.NodeStatusDown, eligibility: nomadapi.NodeSchedulingEligible},
		{name: "down ineligible node retains owned isolation", status: nomadapi.NodeStatusDown, eligibility: nomadapi.NodeSchedulingIneligible, wantStage: "worker_draining", wantDraining: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			operation := controller.NomadScaleInOperation{
				OperationID:       "operation-1",
				ServiceInstanceID: "service-1",
				StartedAt:         time.Unix(100, 0).UTC(),
				Stage:             "worker_draining",
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/nodes" {
					t.Errorf("unexpected request path: %s", r.URL.Path)
					http.Error(w, "unexpected request path", http.StatusNotFound)

					return
				}
				if err := json.NewEncoder(w).Encode([]*nomadapi.NodeListStub{{
					ID:                    "nomad-1",
					Name:                  "i-1",
					NodePool:              "orchestrator",
					Status:                test.status,
					SchedulingEligibility: test.eligibility,
					LastDrain: &nomadapi.DrainMetadata{
						Status: nomadapi.DrainStatusComplete,
						Meta:   nomadOperationMeta(operation),
					},
				}}); err != nil {
					t.Errorf("encode response: %v", err)
				}
			}))
			defer server.Close()

			client, err := nomadapi.NewClient(&nomadapi.Config{Address: server.URL})
			require.NoError(t, err)

			nodes, err := NewNomad(client).Inventory(t.Context(), "orchestrator")
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			require.Equal(t, test.wantDraining, nodes[0].Draining)
			if test.wantStage == "" {
				require.Nil(t, nodes[0].Operation)
			} else {
				require.NotNil(t, nodes[0].Operation)
				require.Equal(t, test.wantStage, nodes[0].Operation.Stage)
			}
		})
	}
}

func TestNomadCompleteRestorePersistsTerminalMarker(t *testing.T) {
	t.Parallel()

	operation := controller.NomadScaleInOperation{
		OperationID:       "operation-1",
		ServiceInstanceID: "service-1",
		StartedAt:         time.Unix(100, 0).UTC(),
		Stage:             "restoring",
	}
	node := &nomadapi.Node{
		ID:                    "nomad-1",
		Name:                  "i-1",
		NodePool:              "orchestrator",
		SchedulingEligibility: nomadapi.NodeSchedulingEligible,
		LastDrain: &nomadapi.DrainMetadata{
			Status: nomadapi.DrainStatusComplete,
			Meta: func() map[string]string {
				previous := operation
				previous.Stage = "worker_draining"

				return nomadOperationMeta(previous)
			}(),
		},
	}
	var updates []*nomadapi.NodeUpdateDrainRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if r.URL.Path != "/v1/node/nomad-1" {
				t.Errorf("unexpected request path: %s", r.URL.Path)
				http.Error(w, "unexpected request path", http.StatusNotFound)

				return
			}
			if err := json.NewEncoder(w).Encode(node); err != nil {
				t.Errorf("encode node response: %v", err)
			}
		case http.MethodPut:
			if r.URL.Path != "/v1/node/nomad-1/drain" {
				t.Errorf("unexpected request path: %s", r.URL.Path)
				http.Error(w, "unexpected request path", http.StatusNotFound)

				return
			}
			var request nomadapi.NodeUpdateDrainRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode drain request: %v", err)
				http.Error(w, "invalid request", http.StatusBadRequest)

				return
			}
			updates = append(updates, &request)
			if request.DrainSpec != nil {
				node.Drain = false
				node.SchedulingEligibility = nomadapi.NodeSchedulingIneligible
				node.LastDrain = &nomadapi.DrainMetadata{Status: nomadapi.DrainStatusComplete, Meta: request.Meta}
			} else if request.MarkEligible {
				node.Drain = false
				node.SchedulingEligibility = nomadapi.NodeSchedulingEligible
			}
			if err := json.NewEncoder(w).Encode(&nomadapi.NodeDrainUpdateResponse{}); err != nil {
				t.Errorf("encode drain response: %v", err)
			}
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	client, err := nomadapi.NewClient(&nomadapi.Config{Address: server.URL})
	require.NoError(t, err)
	err = NewNomad(client).CompleteRestore(t.Context(), controller.NomadScaleInNode{
		NomadNodeID: "nomad-1",
		NodeID:      "i-1",
		NodePool:    "orchestrator",
	}, operation)

	require.NoError(t, err)
	require.Len(t, updates, 2)
	require.NotNil(t, updates[0].DrainSpec)
	require.True(t, updates[0].DrainSpec.IgnoreSystemJobs)
	require.False(t, updates[0].MarkEligible)
	require.Equal(t, "restored", updates[0].Meta[nomadMetaStage])
	require.Nil(t, updates[1].DrainSpec)
	require.True(t, updates[1].MarkEligible)
	require.Equal(t, "restored", node.LastDrain.Meta[nomadMetaStage])
	require.Equal(t, nomadapi.NodeSchedulingEligible, node.SchedulingEligibility)
}

func TestNomadCompleteRestoreResumesAfterTerminalMarker(t *testing.T) {
	t.Parallel()

	operation := controller.NomadScaleInOperation{
		OperationID:       "operation-1",
		ServiceInstanceID: "service-1",
		StartedAt:         time.Unix(100, 0).UTC(),
		Stage:             "restored",
	}
	node := &nomadapi.Node{
		ID:                    "nomad-1",
		Name:                  "i-1",
		NodePool:              "orchestrator",
		SchedulingEligibility: nomadapi.NodeSchedulingIneligible,
		LastDrain:             &nomadapi.DrainMetadata{Status: nomadapi.DrainStatusComplete, Meta: nomadOperationMeta(operation)},
	}
	updates := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if err := json.NewEncoder(w).Encode(node); err != nil {
				t.Errorf("encode node response: %v", err)
			}
		case http.MethodPut:
			var request nomadapi.NodeUpdateDrainRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode drain request: %v", err)
				http.Error(w, "invalid request", http.StatusBadRequest)

				return
			}
			updates++
			if request.DrainSpec != nil || !request.MarkEligible {
				t.Errorf("unexpected restore request: drain=%v mark_eligible=%t", request.DrainSpec, request.MarkEligible)
			}
			node.SchedulingEligibility = nomadapi.NodeSchedulingEligible
			if err := json.NewEncoder(w).Encode(&nomadapi.NodeDrainUpdateResponse{}); err != nil {
				t.Errorf("encode drain response: %v", err)
			}
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	client, err := nomadapi.NewClient(&nomadapi.Config{Address: server.URL})
	require.NoError(t, err)
	err = NewNomad(client).CompleteRestore(t.Context(), controller.NomadScaleInNode{
		NomadNodeID: "nomad-1",
		NodeID:      "i-1",
		NodePool:    "orchestrator",
	}, operation)

	require.NoError(t, err)
	require.Equal(t, 1, updates)
	require.Equal(t, nomadapi.NodeSchedulingEligible, node.SchedulingEligibility)
}
