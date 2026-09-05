package controller

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseScaleInMode(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"off", "observe", "enforce"} {
		mode, err := ParseScaleInMode(value)
		require.NoError(t, err)
		require.Equal(t, value, string(mode))
	}
	_, err := ParseScaleInMode("enabled")
	require.ErrorContains(t, err, "off, observe, or enforce")
}

func TestBuildScaleInPlanCountsOnlyAcceptingNodes(t *testing.T) {
	t.Parallel()

	nodes := []ScaleInNode{
		{Ready: true, Healthy: true, Eligible: true},
		{Ready: true, Healthy: true, Eligible: true},
		{Ready: true, Healthy: false, Eligible: true},
		{Ready: true, Healthy: true, Eligible: false},
		{Ready: true, Healthy: true, Eligible: true, Draining: true},
		{Ready: true, Healthy: true, Eligible: true, Terminating: true},
		{Ready: false, Healthy: true, Eligible: true},
	}

	plan, err := BuildScaleInPlan(40, 20, 1, 10, nodes)
	require.NoError(t, err)
	require.Equal(t, int64(2), plan.RawRequired)
	require.Equal(t, int64(1), plan.HeadroomNodes)
	require.Equal(t, int64(3), plan.SafeRequired)
	require.Equal(t, int32(6), plan.ReadyNodes)
	require.Equal(t, int32(2), plan.AcceptingNodes)
	require.Equal(t, int32(3), plan.DisruptionUsed)
	require.Zero(t, plan.Excess)
}

func TestBuildScaleInPlanComputesExcessWithoutChangingRawRequired(t *testing.T) {
	t.Parallel()

	nodes := make([]ScaleInNode, 6)
	for index := range nodes {
		nodes[index] = ScaleInNode{Ready: true, Healthy: true, Eligible: true}
	}

	plan, err := BuildScaleInPlan(60, 20, 1, 10, nodes)
	require.NoError(t, err)
	require.Equal(t, int64(3), plan.RawRequired)
	require.Equal(t, int64(1), plan.HeadroomNodes)
	require.Equal(t, int64(4), plan.SafeRequired)
	require.Equal(t, int32(2), plan.Excess)
}

func TestBuildScaleInPlanSaturatesExtremeHeadroom(t *testing.T) {
	t.Parallel()

	plan, err := BuildScaleInPlan(math.MaxInt64, 1, 1, math.MaxInt, nil)
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), plan.SafeRequired)
}

func TestBuildScaleInBudgetUsesOneGlobalWindow(t *testing.T) {
	t.Parallel()

	large := BuildScaleInBudget(500, 499, 499, 0)
	require.Equal(t, int32(50), large.Graceful)
	require.Equal(t, int32(50), large.AllowedEmpty)
	require.Equal(t, int32(50), large.AllowedNonEmpty)

	small := BuildScaleInBudget(8, 4, 7, 0)
	require.Equal(t, int32(1), small.Graceful)
	require.Equal(t, int32(7), small.AllowedEmpty)
	require.Equal(t, int32(1), small.AllowedNonEmpty)

	nearlyFull := BuildScaleInBudget(500, 499, 499, 47)
	require.Equal(t, int32(3), nearlyFull.AvailableGlobal)
	require.Equal(t, int32(3), nearlyFull.AllowedEmpty)
	require.Equal(t, int32(3), nearlyFull.AllowedNonEmpty)
}

func TestEligibleScaleInCandidatesFailsClosedAndSortsByLoadThenNewest(t *testing.T) {
	t.Parallel()

	now := time.Unix(10_000, 0)
	old := now.Add(-time.Hour)
	young := now.Add(-time.Minute)
	nodes := []ScaleInNode{
		{NodeID: "busy", RunningSandboxes: 2, KnownWorkload: 2, NomadCreateIndex: 10, ScaleInProtocolSupport: true, Ready: true, Healthy: true, Eligible: true, LaunchTime: &old},
		{NodeID: "start-in-flight", KnownWorkload: 1, NomadCreateIndex: 40, ScaleInProtocolSupport: true, Ready: true, Healthy: true, Eligible: true, LaunchTime: &old},
		{NodeID: "older-empty", NomadCreateIndex: 20, ScaleInProtocolSupport: true, Ready: true, Healthy: true, Eligible: true, LaunchTime: &old},
		{NodeID: "newer-empty", NomadCreateIndex: 30, ScaleInProtocolSupport: true, Ready: true, Healthy: true, Eligible: true, LaunchTime: &old},
		{NodeID: "unsupported", Ready: true, Healthy: true, Eligible: true, LaunchTime: &old},
		{NodeID: "unknown-age", ScaleInProtocolSupport: true, Ready: true, Healthy: true, Eligible: true},
		{NodeID: "young", ScaleInProtocolSupport: true, Ready: true, Healthy: true, Eligible: true, LaunchTime: &young},
	}

	candidates := EligibleScaleInCandidates(nodes, now, 10*time.Minute)
	require.Equal(t, []string{"newer-empty", "older-empty", "start-in-flight", "busy"}, []string{
		candidates[0].NodeID,
		candidates[1].NodeID,
		candidates[2].NodeID,
		candidates[3].NodeID,
	})
	require.Equal(t, CandidateUnsupportedProtocol, ScaleInCandidateBlockReason(nodes[4], now, 10*time.Minute))
	require.Equal(t, CandidateUnknownLaunchTime, ScaleInCandidateBlockReason(nodes[5], now, 10*time.Minute))
	require.Equal(t, CandidateTooYoung, ScaleInCandidateBlockReason(nodes[6], now, 10*time.Minute))
}

func TestSurplusStabilizerRequiresContinuousKnownExcess(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(100, 0)
	var stabilizer SurplusStabilizer
	require.False(t, stabilizer.Observe(t0, 2, true, 2*time.Minute))
	require.False(t, stabilizer.Observe(t0.Add(time.Minute), 4, true, 2*time.Minute))
	require.True(t, stabilizer.Observe(t0.Add(2*time.Minute), 1, true, 2*time.Minute))
	require.False(t, stabilizer.Observe(t0.Add(3*time.Minute), 0, true, 2*time.Minute))
	require.False(t, stabilizer.Observe(t0.Add(4*time.Minute), 1, true, 2*time.Minute))
	require.False(t, stabilizer.Observe(t0.Add(5*time.Minute), 1, false, 2*time.Minute))
	require.False(t, stabilizer.Observe(t0.Add(6*time.Minute), 1, true, 2*time.Minute))
}

func TestScaleOutRequiredOnlyCompensatesEnforcedUncommittedDrains(t *testing.T) {
	t.Parallel()

	require.Equal(t, int64(10), ScaleOutRequired(10, 3, ScaleInModeOff))
	require.Equal(t, int64(10), ScaleOutRequired(10, 3, ScaleInModeObserve))
	require.Equal(t, int64(13), ScaleOutRequired(10, 3, ScaleInModeEnforce))
	require.Equal(t, int64(math.MaxInt64), ScaleOutRequired(math.MaxInt64, 3, ScaleInModeEnforce))
}
