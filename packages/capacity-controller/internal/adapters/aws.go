package adapters

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
)

type AutoScalingClient interface {
	DescribeAutoScalingGroups(ctx context.Context, params *autoscaling.DescribeAutoScalingGroupsInput, optFns ...func(*autoscaling.Options)) (*autoscaling.DescribeAutoScalingGroupsOutput, error)
	SetDesiredCapacity(ctx context.Context, params *autoscaling.SetDesiredCapacityInput, optFns ...func(*autoscaling.Options)) (*autoscaling.SetDesiredCapacityOutput, error)
}

type ASG struct {
	client AutoScalingClient
}

func NewASG(client AutoScalingClient) *ASG {
	return &ASG{client: client}
}

func (a *ASG) DesiredCapacity(ctx context.Context, asgName string) (int32, error) {
	output, err := a.client.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{asgName},
	})
	if err != nil {
		return 0, fmt.Errorf("describe Auto Scaling group: %w", err)
	}
	if len(output.AutoScalingGroups) != 1 || output.AutoScalingGroups[0].AutoScalingGroupName == nil || *output.AutoScalingGroups[0].AutoScalingGroupName != asgName {
		return 0, fmt.Errorf("auto scaling group %q was not found uniquely", asgName)
	}
	if output.AutoScalingGroups[0].DesiredCapacity == nil {
		return 0, fmt.Errorf("auto scaling group %q has no desired capacity", asgName)
	}

	return *output.AutoScalingGroups[0].DesiredCapacity, nil
}

func (a *ASG) SetDesiredCapacity(ctx context.Context, asgName string, desired int32) error {
	_, err := a.client.SetDesiredCapacity(ctx, &autoscaling.SetDesiredCapacityInput{
		AutoScalingGroupName: aws.String(asgName),
		DesiredCapacity:      aws.Int32(desired),
		HonorCooldown:        aws.Bool(false),
	})
	if err != nil {
		return fmt.Errorf("set Auto Scaling group desired capacity: %w", err)
	}

	return nil
}
