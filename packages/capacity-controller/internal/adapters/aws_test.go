package adapters

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/require"
)

type fakeAutoScalingClient struct {
	groups          []types.AutoScalingGroup
	refreshes       []types.InstanceRefresh
	setDesiredInput *autoscaling.SetDesiredCapacityInput
	protectionInput *autoscaling.SetInstanceProtectionInput
	protectionErr   error
}

func (f *fakeAutoScalingClient) DescribeAutoScalingGroups(context.Context, *autoscaling.DescribeAutoScalingGroupsInput, ...func(*autoscaling.Options)) (*autoscaling.DescribeAutoScalingGroupsOutput, error) {
	return &autoscaling.DescribeAutoScalingGroupsOutput{AutoScalingGroups: f.groups}, nil
}

func (f *fakeAutoScalingClient) DescribeInstanceRefreshes(context.Context, *autoscaling.DescribeInstanceRefreshesInput, ...func(*autoscaling.Options)) (*autoscaling.DescribeInstanceRefreshesOutput, error) {
	return &autoscaling.DescribeInstanceRefreshesOutput{InstanceRefreshes: f.refreshes}, nil
}

func (f *fakeAutoScalingClient) SetDesiredCapacity(_ context.Context, input *autoscaling.SetDesiredCapacityInput, _ ...func(*autoscaling.Options)) (*autoscaling.SetDesiredCapacityOutput, error) {
	f.setDesiredInput = input

	return &autoscaling.SetDesiredCapacityOutput{}, nil
}

func (f *fakeAutoScalingClient) SetInstanceProtection(_ context.Context, input *autoscaling.SetInstanceProtectionInput, _ ...func(*autoscaling.Options)) (*autoscaling.SetInstanceProtectionOutput, error) {
	f.protectionInput = input

	return &autoscaling.SetInstanceProtectionOutput{}, f.protectionErr
}

type fakeEC2Client struct{ launchTimes map[string]time.Time }

func (f fakeEC2Client) DescribeInstances(_ context.Context, input *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	instances := make([]ec2types.Instance, 0, len(input.InstanceIds))
	for _, id := range input.InstanceIds {
		if launch, ok := f.launchTimes[id]; ok {
			instances = append(instances, ec2types.Instance{InstanceId: aws.String(id), LaunchTime: aws.Time(launch)})
		}
	}

	return &ec2.DescribeInstancesOutput{Reservations: []ec2types.Reservation{{Instances: instances}}}, nil
}

func completeASG(count int) *fakeAutoScalingClient {
	instances := make([]types.Instance, 0, count)
	for i := range count {
		instances = append(instances, types.Instance{InstanceId: aws.String(fmt.Sprintf("i-%d", i)), HealthStatus: aws.String("Healthy"), LifecycleState: types.LifecycleStateInService, ProtectedFromScaleIn: aws.Bool(true)})
	}

	return &fakeAutoScalingClient{groups: []types.AutoScalingGroup{{AutoScalingGroupName: aws.String("workers"), AutoScalingGroupARN: aws.String("arn:workers"), DesiredCapacity: aws.Int32(int32(count)), MinSize: aws.Int32(1), MaxSize: aws.Int32(1000), NewInstancesProtectedFromScaleIn: aws.Bool(true), Instances: instances}}}
}

func TestASGSnapshotReadsProtectionMembershipAndLaunchTime(t *testing.T) {
	t.Parallel()

	client := completeASG(1)
	launch := time.Now().Add(-time.Hour).UTC()
	snapshot, err := NewASG(client, fakeEC2Client{launchTimes: map[string]time.Time{"i-0": launch}}).Snapshot(t.Context(), "workers")
	require.NoError(t, err)
	require.True(t, snapshot.NewInstancesProtectedFromScaleIn)
	require.True(t, snapshot.Instances["i-0"].ProtectedFromScaleIn)
	require.Equal(t, launch, *snapshot.Instances["i-0"].LaunchTime)
}

func TestASGSnapshotRejectsUnknownGroupProtection(t *testing.T) {
	t.Parallel()

	client := completeASG(0)
	client.groups[0].NewInstancesProtectedFromScaleIn = nil
	_, err := NewASG(client, fakeEC2Client{}).Snapshot(t.Context(), "workers")
	require.ErrorContains(t, err, "incomplete capacity metadata")
}

func TestASGDesiredCapacityRequiresExactCompleteGroup(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		groups []types.AutoScalingGroup
		want   string
	}{
		{"missing", nil, "not found uniquely"},
		{"duplicate", []types.AutoScalingGroup{{AutoScalingGroupName: aws.String("workers")}, {AutoScalingGroupName: aws.String("workers")}}, "not found uniquely"},
		{"wrong identity", []types.AutoScalingGroup{{AutoScalingGroupName: aws.String("other"), DesiredCapacity: aws.Int32(1)}}, "not found uniquely"},
		{"missing desired", []types.AutoScalingGroup{{AutoScalingGroupName: aws.String("workers")}}, "no desired capacity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewASG(&fakeAutoScalingClient{groups: tc.groups}, nil).DesiredCapacity(t.Context(), "workers")
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestASGSnapshotFailsClosedWhenEC2LaunchTimeIsMissing(t *testing.T) {
	t.Parallel()

	_, err := NewASG(completeASG(1), fakeEC2Client{launchTimes: map[string]time.Time{}}).Snapshot(t.Context(), "workers")
	require.ErrorContains(t, err, "returned 0 of 1")
}

func TestSetInstanceProtectionValidatesAndForwardsBatch(t *testing.T) {
	t.Parallel()

	client := completeASG(0)
	adapter := NewASG(client, nil)
	require.NoError(t, adapter.SetInstanceProtection(t.Context(), "workers", []string{"i-1", "i-2"}, false))
	require.Equal(t, []string{"i-1", "i-2"}, client.protectionInput.InstanceIds)
	require.False(t, *client.protectionInput.ProtectedFromScaleIn)
	require.Equal(t, "workers", *client.protectionInput.AutoScalingGroupName)

	require.Error(t, adapter.SetInstanceProtection(t.Context(), "workers", nil, true))
	require.Error(t, adapter.SetInstanceProtection(t.Context(), "workers", make([]string, 51), true))
	require.ErrorContains(t, adapter.SetInstanceProtection(t.Context(), "workers", []string{"i-1", "i-1"}, true), "duplicate")
}

func TestSetInstanceProtectionDoesNotInterpretAmbiguousError(t *testing.T) {
	t.Parallel()

	client := completeASG(0)
	client.protectionErr = errors.New("response lost")
	err := NewASG(client, nil).SetInstanceProtection(t.Context(), "workers", []string{"i-1"}, false)
	require.ErrorContains(t, err, "response lost")
}

func TestSetDesiredCapacityUsesAbsoluteTarget(t *testing.T) {
	t.Parallel()

	client := completeASG(0)
	_, err := NewASG(client, nil).SetDesiredCapacity(t.Context(), "workers", 450)
	require.NoError(t, err)
	require.Equal(t, int32(450), *client.setDesiredInput.DesiredCapacity)
	require.False(t, *client.setDesiredInput.HonorCooldown)
}

func TestASGSnapshotTreatsUnknownRefreshAsActive(t *testing.T) {
	t.Parallel()

	client := completeASG(0)
	client.refreshes = []types.InstanceRefresh{{Status: types.InstanceRefreshStatus("FutureState")}}
	snapshot, err := NewASG(client, fakeEC2Client{}).Snapshot(t.Context(), "workers")
	require.NoError(t, err)
	require.True(t, snapshot.ActiveInstanceRefresh)
}
