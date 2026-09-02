package adapters

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	nomadapi "github.com/hashicorp/nomad/api"
	"github.com/stretchr/testify/require"
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
