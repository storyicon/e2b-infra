package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestServiceInfoResponseMissingCreateLimitRemainsCompatible(t *testing.T) {
	t.Parallel()

	legacyPayload, err := proto.Marshal(&ServiceInfoResponse{
		NodeId:    "legacy-node",
		ServiceId: "legacy-service",
	})
	require.NoError(t, err)

	var decoded ServiceInfoResponse
	require.NoError(t, proto.Unmarshal(legacyPayload, &decoded))
	require.Equal(t, "legacy-node", decoded.GetNodeId())
	require.Zero(t, decoded.GetSandboxCreateConcurrencyLimit())
}

func TestServiceInfoResponseCreateLimitRoundTrips(t *testing.T) {
	t.Parallel()

	payload, err := proto.Marshal(&ServiceInfoResponse{SandboxCreateConcurrencyLimit: 16})
	require.NoError(t, err)

	var decoded ServiceInfoResponse
	require.NoError(t, proto.Unmarshal(payload, &decoded))
	require.Equal(t, uint64(16), decoded.GetSandboxCreateConcurrencyLimit())
}
