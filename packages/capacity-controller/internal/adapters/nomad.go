package adapters

import (
	"context"
	"fmt"

	nomadapi "github.com/hashicorp/nomad/api"
)

type Nomad struct {
	client *nomadapi.Client
}

func NewNomad(client *nomadapi.Client) *Nomad {
	return &Nomad{client: client}
}

func (n *Nomad) ReadyCount(ctx context.Context, nodePool string) (int32, error) {
	query := (&nomadapi.QueryOptions{Namespace: "*", AllowStale: false}).WithContext(ctx)
	nodes, _, err := n.client.Nodes().List(query)
	if err != nil {
		return 0, fmt.Errorf("list Nomad nodes: %w", err)
	}

	var ready int32
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if node.NodePool == nodePool && node.Status == nomadapi.NodeStatusReady && node.SchedulingEligibility == nomadapi.NodeSchedulingEligible {
			ready++
		}
	}

	return ready, nil
}
