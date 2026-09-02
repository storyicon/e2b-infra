package adapters

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/e2b-dev/infra/packages/capacity-controller/internal/controller"
)

type AutoScalingClient interface {
	DescribeAutoScalingGroups(ctx context.Context, params *autoscaling.DescribeAutoScalingGroupsInput, optFns ...func(*autoscaling.Options)) (*autoscaling.DescribeAutoScalingGroupsOutput, error)
	DescribeInstanceRefreshes(ctx context.Context, params *autoscaling.DescribeInstanceRefreshesInput, optFns ...func(*autoscaling.Options)) (*autoscaling.DescribeInstanceRefreshesOutput, error)
	DescribeScalingActivities(ctx context.Context, params *autoscaling.DescribeScalingActivitiesInput, optFns ...func(*autoscaling.Options)) (*autoscaling.DescribeScalingActivitiesOutput, error)
	SetDesiredCapacity(ctx context.Context, params *autoscaling.SetDesiredCapacityInput, optFns ...func(*autoscaling.Options)) (*autoscaling.SetDesiredCapacityOutput, error)
	TerminateInstanceInAutoScalingGroup(ctx context.Context, params *autoscaling.TerminateInstanceInAutoScalingGroupInput, optFns ...func(*autoscaling.Options)) (*autoscaling.TerminateInstanceInAutoScalingGroupOutput, error)
}

type EC2Client interface {
	DescribeInstances(ctx context.Context, params *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
}

type ASGInstance = controller.ScaleInASGInstance

type ASGSnapshot = controller.ScaleInASGSnapshot

type TerminationReceipt = controller.ScaleInTerminationReceipt

type ScalingActivity = controller.ScaleInActivity

type TerminationReconciliation string

const (
	TerminationUnknown   TerminationReconciliation = "unknown"
	TerminationPending   TerminationReconciliation = "pending"
	TerminationCompleted TerminationReconciliation = "completed"
	TerminationRejected  TerminationReconciliation = "rejected"
)

type TerminationBlockReason string

const (
	TerminationAllowed               TerminationBlockReason = ""
	TerminationAtMinimum             TerminationBlockReason = "asg_at_minimum"
	TerminationProcessSuspended      TerminationBlockReason = "terminate_process_suspended"
	TerminationInstanceRefreshActive TerminationBlockReason = "instance_refresh_active"
	TerminationInstanceMissing       TerminationBlockReason = "instance_not_in_asg"
	TerminationInstanceNotInService  TerminationBlockReason = "instance_not_in_service"
	TerminationInstanceUnhealthy     TerminationBlockReason = "instance_unhealthy"
	TerminationInstanceProtected     TerminationBlockReason = "instance_protected"
)

type ASG struct {
	client    AutoScalingClient
	ec2Client EC2Client
}

func NewASG(client AutoScalingClient, ec2Client EC2Client) *ASG {
	return &ASG{client: client, ec2Client: ec2Client}
}

func (a *ASG) DesiredCapacity(ctx context.Context, asgName string) (int32, error) {
	group, err := a.describeGroup(ctx, asgName)
	if err != nil {
		return 0, err
	}
	if group.DesiredCapacity == nil {
		return 0, fmt.Errorf("auto scaling group %q has no desired capacity", asgName)
	}

	return *group.DesiredCapacity, nil
}

func (a *ASG) Snapshot(ctx context.Context, asgName string) (ASGSnapshot, error) {
	group, err := a.describeGroup(ctx, asgName)
	if err != nil {
		return ASGSnapshot{}, err
	}
	if group.AutoScalingGroupARN == nil || group.DesiredCapacity == nil || group.MinSize == nil || group.MaxSize == nil {
		return ASGSnapshot{}, fmt.Errorf("auto scaling group %q returned incomplete capacity metadata", asgName)
	}

	refreshes, err := a.client.DescribeInstanceRefreshes(ctx, &autoscaling.DescribeInstanceRefreshesInput{
		AutoScalingGroupName: aws.String(asgName),
		MaxRecords:           aws.Int32(100),
	})
	if err != nil {
		return ASGSnapshot{}, fmt.Errorf("describe Auto Scaling group instance refreshes: %w", err)
	}

	snapshot := ASGSnapshot{
		Name:            asgName,
		ARN:             *group.AutoScalingGroupARN,
		DesiredCapacity: *group.DesiredCapacity,
		MinSize:         *group.MinSize,
		MaxSize:         *group.MaxSize,
		Instances:       make(map[string]ASGInstance, len(group.Instances)),
	}
	for _, process := range group.SuspendedProcesses {
		if process.ProcessName != nil && *process.ProcessName == "Terminate" {
			snapshot.TerminateSuspended = true
		}
	}
	for _, refresh := range refreshes.InstanceRefreshes {
		if instanceRefreshIsActive(refresh.Status) {
			snapshot.ActiveInstanceRefresh = true
			break
		}
	}
	for _, instance := range group.Instances {
		if instance.InstanceId == nil || instance.HealthStatus == nil || instance.ProtectedFromScaleIn == nil || instance.LifecycleState == "" {
			return ASGSnapshot{}, fmt.Errorf("auto scaling group %q returned incomplete instance metadata", asgName)
		}
		snapshot.Instances[*instance.InstanceId] = ASGInstance{
			ID:                   *instance.InstanceId,
			HealthStatus:         *instance.HealthStatus,
			LifecycleState:       string(instance.LifecycleState),
			ProtectedFromScaleIn: *instance.ProtectedFromScaleIn,
		}
	}
	if err := a.populateLaunchTimes(ctx, snapshot.Instances); err != nil {
		return ASGSnapshot{}, err
	}

	return snapshot, nil
}

func (a *ASG) SetDesiredCapacity(ctx context.Context, asgName string, desired int32) (controller.ScaleWriteMetadata, error) {
	output, err := a.client.SetDesiredCapacity(ctx, &autoscaling.SetDesiredCapacityInput{
		AutoScalingGroupName: aws.String(asgName),
		DesiredCapacity:      aws.Int32(desired),
		HonorCooldown:        aws.Bool(false),
	})
	if err != nil {
		return controller.ScaleWriteMetadata{}, fmt.Errorf("set Auto Scaling group desired capacity: %w", err)
	}

	requestID, _ := awsmiddleware.GetRequestIDMetadata(output.ResultMetadata)

	return controller.ScaleWriteMetadata{RequestID: requestID}, nil
}

func (a *ASG) TerminateInstance(ctx context.Context, instanceID string) (TerminationReceipt, error) {
	output, err := a.client.TerminateInstanceInAutoScalingGroup(ctx, &autoscaling.TerminateInstanceInAutoScalingGroupInput{
		InstanceId:                     aws.String(instanceID),
		ShouldDecrementDesiredCapacity: aws.Bool(true),
	}, func(options *autoscaling.Options) {
		// This destructive API has no idempotency token. An ambiguous response is
		// reconciled by reads and must never be retried by the SDK.
		options.Retryer = aws.NopRetryer{}
	})
	if err != nil {
		outcome := controller.ScaleInTerminationAmbiguous
		var sendErr *smithyhttp.RequestSendError
		var responseErr *smithyhttp.ResponseError
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.As(err, &sendErr):
			// The client cannot prove whether AWS received the request.
		case errors.As(err, &responseErr) && responseErr.HTTPStatusCode() >= 400 && responseErr.HTTPStatusCode() < 500:
			// A complete 4xx response is an authoritative API rejection.
			outcome = controller.ScaleInTerminationRejected
		}
		return TerminationReceipt{}, &controller.ScaleInTerminationError{
			Outcome: outcome,
			Err:     fmt.Errorf("terminate instance in Auto Scaling group: %w", err),
		}
	}
	if output.Activity == nil || output.Activity.ActivityId == nil || *output.Activity.ActivityId == "" {
		return TerminationReceipt{}, &controller.ScaleInTerminationError{
			Outcome: controller.ScaleInTerminationAmbiguous,
			Err:     errorsNewIncompleteTermination(instanceID),
		}
	}
	requestID, _ := awsmiddleware.GetRequestIDMetadata(output.ResultMetadata)

	return TerminationReceipt{
		RequestID:  requestID,
		ActivityID: *output.Activity.ActivityId,
		Status:     string(output.Activity.StatusCode),
	}, nil
}

func (a *ASG) populateLaunchTimes(ctx context.Context, instances map[string]ASGInstance) error {
	if len(instances) == 0 {
		return nil
	}
	if a.ec2Client == nil {
		return errors.New("EC2 client is required to read authoritative instance launch times")
	}
	instanceIDs := make([]string, 0, len(instances))
	for instanceID := range instances {
		instanceIDs = append(instanceIDs, instanceID)
	}
	output, err := a.ec2Client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: instanceIDs})
	if err != nil {
		return fmt.Errorf("describe EC2 instances for launch times: %w", err)
	}
	if output == nil {
		return errors.New("describe EC2 instances for launch times returned an empty response")
	}
	seen := make(map[string]struct{}, len(instances))
	for _, reservation := range output.Reservations {
		for _, ec2Instance := range reservation.Instances {
			if ec2Instance.InstanceId == nil || ec2Instance.LaunchTime == nil || ec2Instance.LaunchTime.IsZero() {
				return errors.New("describe EC2 instances returned incomplete launch-time metadata")
			}
			instanceID := *ec2Instance.InstanceId
			asgInstance, expected := instances[instanceID]
			if !expected {
				return fmt.Errorf("describe EC2 instances returned unexpected instance %q", instanceID)
			}
			if _, duplicate := seen[instanceID]; duplicate {
				return fmt.Errorf("describe EC2 instances returned duplicate instance %q", instanceID)
			}
			launchTime := *ec2Instance.LaunchTime
			asgInstance.LaunchTime = &launchTime
			instances[instanceID] = asgInstance
			seen[instanceID] = struct{}{}
		}
	}
	if len(seen) != len(instances) {
		return fmt.Errorf("describe EC2 instances returned %d of %d requested instances", len(seen), len(instances))
	}

	return nil
}

func (a *ASG) Activity(ctx context.Context, asgName, activityID string) (*ScalingActivity, error) {
	output, err := a.client.DescribeScalingActivities(ctx, &autoscaling.DescribeScalingActivitiesInput{
		ActivityIds:          []string{activityID},
		AutoScalingGroupName: aws.String(asgName),
		MaxRecords:           aws.Int32(1),
	})
	if err != nil {
		return nil, fmt.Errorf("describe Auto Scaling group activity: %w", err)
	}
	if len(output.Activities) == 0 {
		return nil, nil
	}
	if len(output.Activities) != 1 || output.Activities[0].ActivityId == nil || output.Activities[0].AutoScalingGroupName == nil || output.Activities[0].StartTime == nil || *output.Activities[0].ActivityId != activityID || *output.Activities[0].AutoScalingGroupName != asgName {
		return nil, fmt.Errorf("auto scaling activity %q returned conflicting identity", activityID)
	}

	return &ScalingActivity{
		ID:        activityID,
		ASGName:   asgName,
		Status:    string(output.Activities[0].StatusCode),
		StartedAt: *output.Activities[0].StartTime,
	}, nil
}

func TerminationEligibility(snapshot ASGSnapshot, instanceID string) TerminationBlockReason {
	if snapshot.DesiredCapacity <= snapshot.MinSize {
		return TerminationAtMinimum
	}
	if snapshot.TerminateSuspended {
		return TerminationProcessSuspended
	}
	if snapshot.ActiveInstanceRefresh {
		return TerminationInstanceRefreshActive
	}
	instance, found := snapshot.Instances[instanceID]
	if !found {
		return TerminationInstanceMissing
	}
	if instance.LifecycleState != "InService" {
		return TerminationInstanceNotInService
	}
	if instance.HealthStatus != "Healthy" {
		return TerminationInstanceUnhealthy
	}
	if instance.ProtectedFromScaleIn {
		return TerminationInstanceProtected
	}

	return TerminationAllowed
}

func ReconcileTermination(snapshot ASGSnapshot, instanceID string, activity *ScalingActivity) TerminationReconciliation {
	if _, found := snapshot.Instances[instanceID]; !found {
		return TerminationCompleted
	}
	if activity == nil || activity.ASGName != snapshot.Name {
		return TerminationUnknown
	}
	switch activity.Status {
	case "Failed", "Cancelled":
		return TerminationRejected
	case "PendingSpotBidPlacement", "WaitingForSpotInstanceRequestId", "WaitingForSpotInstanceId", "WaitingForInstanceId", "PreInService", "InProgress", "WaitingForELBConnectionDraining", "MidLifecycleAction", "WaitingForInstanceWarmup", "Successful", "WaitingForConnectionDraining", "WaitingForInPlaceUpdateToStart", "WaitingForInPlaceUpdateToFinalize", "InPlaceUpdateInProgress":
		return TerminationPending
	default:
		return TerminationUnknown
	}
}

func (a *ASG) describeGroup(ctx context.Context, asgName string) (*types.AutoScalingGroup, error) {
	output, err := a.client.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{asgName},
	})
	if err != nil {
		return nil, fmt.Errorf("describe Auto Scaling group: %w", err)
	}
	if len(output.AutoScalingGroups) != 1 || output.AutoScalingGroups[0].AutoScalingGroupName == nil || *output.AutoScalingGroups[0].AutoScalingGroupName != asgName {
		return nil, fmt.Errorf("auto scaling group %q was not found uniquely", asgName)
	}

	return &output.AutoScalingGroups[0], nil
}

func instanceRefreshIsActive(status types.InstanceRefreshStatus) bool {
	switch status {
	case types.InstanceRefreshStatusSuccessful,
		types.InstanceRefreshStatusFailed,
		types.InstanceRefreshStatusCancelled,
		types.InstanceRefreshStatusRollbackFailed,
		types.InstanceRefreshStatusRollbackSuccessful:
		return false
	case types.InstanceRefreshStatusPending,
		types.InstanceRefreshStatusInProgress,
		types.InstanceRefreshStatusCancelling,
		types.InstanceRefreshStatusRollbackInProgress,
		types.InstanceRefreshStatusBaking:
		return true
	default:
		// New AWS states fail closed until explicitly classified.
		return true
	}
}

func errorsNewIncompleteTermination(instanceID string) error {
	return fmt.Errorf("terminate instance %q returned no scaling activity", instanceID)
}
