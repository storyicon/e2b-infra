package adapters

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/stretchr/testify/require"
)

type fakeAutoScalingClient struct {
	describeOutput *autoscaling.DescribeAutoScalingGroupsOutput
	describeErr    error
	setInput       *autoscaling.SetDesiredCapacityInput
}

func (f *fakeAutoScalingClient) DescribeAutoScalingGroups(context.Context, *autoscaling.DescribeAutoScalingGroupsInput, ...func(*autoscaling.Options)) (*autoscaling.DescribeAutoScalingGroupsOutput, error) {
	return f.describeOutput, f.describeErr
}

func (f *fakeAutoScalingClient) SetDesiredCapacity(_ context.Context, input *autoscaling.SetDesiredCapacityInput, _ ...func(*autoscaling.Options)) (*autoscaling.SetDesiredCapacityOutput, error) {
	f.setInput = input

	return &autoscaling.SetDesiredCapacityOutput{}, nil
}

func TestASGDesiredCapacityRequiresExactGroup(t *testing.T) {
	t.Parallel()

	client := &fakeAutoScalingClient{describeOutput: &autoscaling.DescribeAutoScalingGroupsOutput{}}
	target := NewASG(client)

	_, err := target.DesiredCapacity(t.Context(), "workers")

	require.ErrorContains(t, err, "workers")
}

func TestASGReadsAndSetsDesiredCapacityWithoutCooldown(t *testing.T) {
	t.Parallel()

	client := &fakeAutoScalingClient{describeOutput: &autoscaling.DescribeAutoScalingGroupsOutput{
		AutoScalingGroups: []types.AutoScalingGroup{{AutoScalingGroupName: aws.String("workers"), DesiredCapacity: aws.Int32(3)}},
	}}
	target := NewASG(client)

	desired, err := target.DesiredCapacity(t.Context(), "workers")
	require.NoError(t, err)
	require.Equal(t, int32(3), desired)
	_, err = target.SetDesiredCapacity(t.Context(), "workers", 7)
	require.NoError(t, err)
	require.Equal(t, int32(7), *client.setInput.DesiredCapacity)
	require.False(t, *client.setInput.HonorCooldown)
}
