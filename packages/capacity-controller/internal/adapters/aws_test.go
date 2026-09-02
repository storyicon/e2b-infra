package adapters

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/capacity-controller/internal/controller"
)

type fakeAutoScalingClient struct {
	describeOutput         *autoscaling.DescribeAutoScalingGroupsOutput
	describeErr            error
	refreshOutput          *autoscaling.DescribeInstanceRefreshesOutput
	refreshErr             error
	activitiesOutput       *autoscaling.DescribeScalingActivitiesOutput
	activitiesErr          error
	setInput               *autoscaling.SetDesiredCapacityInput
	terminateInput         *autoscaling.TerminateInstanceInAutoScalingGroupInput
	terminateOutput        *autoscaling.TerminateInstanceInAutoScalingGroupOutput
	terminateErr           error
	terminateRetryDisabled bool
}

type fakeEC2Client struct {
	describeInput  *ec2.DescribeInstancesInput
	describeOutput *ec2.DescribeInstancesOutput
	describeErr    error
}

func (f *fakeEC2Client) DescribeInstances(_ context.Context, input *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	f.describeInput = input

	return f.describeOutput, f.describeErr
}

func (f *fakeAutoScalingClient) DescribeAutoScalingGroups(context.Context, *autoscaling.DescribeAutoScalingGroupsInput, ...func(*autoscaling.Options)) (*autoscaling.DescribeAutoScalingGroupsOutput, error) {
	return f.describeOutput, f.describeErr
}

func (f *fakeAutoScalingClient) SetDesiredCapacity(_ context.Context, input *autoscaling.SetDesiredCapacityInput, _ ...func(*autoscaling.Options)) (*autoscaling.SetDesiredCapacityOutput, error) {
	f.setInput = input

	return &autoscaling.SetDesiredCapacityOutput{}, nil
}

func (f *fakeAutoScalingClient) DescribeInstanceRefreshes(context.Context, *autoscaling.DescribeInstanceRefreshesInput, ...func(*autoscaling.Options)) (*autoscaling.DescribeInstanceRefreshesOutput, error) {
	return f.refreshOutput, f.refreshErr
}

func (f *fakeAutoScalingClient) DescribeScalingActivities(context.Context, *autoscaling.DescribeScalingActivitiesInput, ...func(*autoscaling.Options)) (*autoscaling.DescribeScalingActivitiesOutput, error) {
	return f.activitiesOutput, f.activitiesErr
}

func (f *fakeAutoScalingClient) TerminateInstanceInAutoScalingGroup(_ context.Context, input *autoscaling.TerminateInstanceInAutoScalingGroupInput, optFns ...func(*autoscaling.Options)) (*autoscaling.TerminateInstanceInAutoScalingGroupOutput, error) {
	f.terminateInput = input
	options := autoscaling.Options{}
	for _, option := range optFns {
		option(&options)
	}
	_, f.terminateRetryDisabled = options.Retryer.(aws.NopRetryer)
	if f.terminateErr != nil {
		return nil, f.terminateErr
	}

	return f.terminateOutput, nil
}

func TestASGDesiredCapacityRequiresExactGroup(t *testing.T) {
	t.Parallel()

	client := &fakeAutoScalingClient{describeOutput: &autoscaling.DescribeAutoScalingGroupsOutput{}}
	target := NewASG(client, nil)

	_, err := target.DesiredCapacity(t.Context(), "workers")

	require.ErrorContains(t, err, "workers")
}

func TestASGReadsAndSetsDesiredCapacityWithoutCooldown(t *testing.T) {
	t.Parallel()

	client := &fakeAutoScalingClient{describeOutput: &autoscaling.DescribeAutoScalingGroupsOutput{
		AutoScalingGroups: []types.AutoScalingGroup{{AutoScalingGroupName: aws.String("workers"), DesiredCapacity: aws.Int32(3)}},
	}}
	target := NewASG(client, nil)

	desired, err := target.DesiredCapacity(t.Context(), "workers")
	require.NoError(t, err)
	require.Equal(t, int32(3), desired)
	_, err = target.SetDesiredCapacity(t.Context(), "workers", 7)
	require.NoError(t, err)
	require.Equal(t, int32(7), *client.setInput.DesiredCapacity)
	require.False(t, *client.setInput.HonorCooldown)
}

func TestASGSnapshotReturnsSafetyStateAndAuthoritativeLaunchTime(t *testing.T) {
	t.Parallel()

	client := &fakeAutoScalingClient{
		describeOutput: &autoscaling.DescribeAutoScalingGroupsOutput{AutoScalingGroups: []types.AutoScalingGroup{{
			AutoScalingGroupName: aws.String("workers"),
			AutoScalingGroupARN:  aws.String("arn:aws:autoscaling:region:account:autoScalingGroup:group-id:autoScalingGroupName/workers"),
			DesiredCapacity:      aws.Int32(3),
			MinSize:              aws.Int32(1),
			MaxSize:              aws.Int32(30),
			SuspendedProcesses:   []types.SuspendedProcess{{ProcessName: aws.String("Terminate")}},
			Instances: []types.Instance{{
				InstanceId:           aws.String("i-safe"),
				HealthStatus:         aws.String("Healthy"),
				LifecycleState:       types.LifecycleStateInService,
				ProtectedFromScaleIn: aws.Bool(false),
			}},
		}}},
		refreshOutput: &autoscaling.DescribeInstanceRefreshesOutput{InstanceRefreshes: []types.InstanceRefresh{{
			Status: types.InstanceRefreshStatusInProgress,
		}}},
	}

	launchTime := time.Unix(100, 0)
	ec2Client := &fakeEC2Client{describeOutput: &ec2.DescribeInstancesOutput{Reservations: []ec2types.Reservation{{
		Instances: []ec2types.Instance{{InstanceId: aws.String("i-safe"), LaunchTime: &launchTime}},
	}}}}
	snapshot, err := NewASG(client, ec2Client).Snapshot(t.Context(), "workers")
	require.NoError(t, err)
	require.Equal(t, int32(3), snapshot.DesiredCapacity)
	require.Equal(t, int32(1), snapshot.MinSize)
	require.True(t, snapshot.TerminateSuspended)
	require.True(t, snapshot.ActiveInstanceRefresh)
	require.Equal(t, launchTime, *snapshot.Instances["i-safe"].LaunchTime)
	require.Equal(t, []string{"i-safe"}, ec2Client.describeInput.InstanceIds)
}

func TestASGSnapshotFailsClosedWhenEC2LaunchTimeIsMissing(t *testing.T) {
	t.Parallel()

	_, err := NewASG(newCompleteASGClient(), &fakeEC2Client{describeOutput: &ec2.DescribeInstancesOutput{}}).Snapshot(t.Context(), "workers")

	require.ErrorContains(t, err, "returned 0 of 1 requested instances")
}

func TestASGSnapshotFailsClosedOnUnknownRefreshState(t *testing.T) {
	t.Parallel()

	client := newCompleteASGClient()
	client.refreshOutput.InstanceRefreshes = []types.InstanceRefresh{{Status: types.InstanceRefreshStatus("FutureState")}}

	snapshot, err := NewASG(client, newCompleteEC2Client()).Snapshot(t.Context(), "workers")
	require.NoError(t, err)
	require.True(t, snapshot.ActiveInstanceRefresh)
}

func TestTerminationEligibilityChecksEveryASGGuard(t *testing.T) {
	t.Parallel()

	base := ASGSnapshot{
		Name:            "workers",
		DesiredCapacity: 2,
		MinSize:         1,
		Instances: map[string]ASGInstance{
			"i-safe": {ID: "i-safe", HealthStatus: "Healthy", LifecycleState: "InService"},
		},
	}
	require.Equal(t, TerminationAllowed, TerminationEligibility(base, "i-safe"))

	atMinimum := base
	atMinimum.DesiredCapacity = 1
	require.Equal(t, TerminationAtMinimum, TerminationEligibility(atMinimum, "i-safe"))
	suspended := base
	suspended.TerminateSuspended = true
	require.Equal(t, TerminationProcessSuspended, TerminationEligibility(suspended, "i-safe"))
	refresh := base
	refresh.ActiveInstanceRefresh = true
	require.Equal(t, TerminationInstanceRefreshActive, TerminationEligibility(refresh, "i-safe"))
	require.Equal(t, TerminationInstanceMissing, TerminationEligibility(base, "i-missing"))

	notInService := base
	notInService.Instances = map[string]ASGInstance{"i-safe": {ID: "i-safe", HealthStatus: "Healthy", LifecycleState: "Pending"}}
	require.Equal(t, TerminationInstanceNotInService, TerminationEligibility(notInService, "i-safe"))
	unhealthy := base
	unhealthy.Instances = map[string]ASGInstance{"i-safe": {ID: "i-safe", HealthStatus: "Unhealthy", LifecycleState: "InService"}}
	require.Equal(t, TerminationInstanceUnhealthy, TerminationEligibility(unhealthy, "i-safe"))
	protected := base
	protected.Instances = map[string]ASGInstance{"i-safe": {ID: "i-safe", HealthStatus: "Healthy", LifecycleState: "InService", ProtectedFromScaleIn: true}}
	require.Equal(t, TerminationInstanceProtected, TerminationEligibility(protected, "i-safe"))
}

func TestASGTerminatesExactInstanceWithoutSDKRetry(t *testing.T) {
	t.Parallel()

	client := newCompleteASGClient()
	client.terminateOutput = &autoscaling.TerminateInstanceInAutoScalingGroupOutput{Activity: &types.Activity{
		ActivityId: aws.String("activity-1"),
		StatusCode: types.ScalingActivityStatusCodeInProgress,
	}}

	receipt, err := NewASG(client, nil).TerminateInstance(t.Context(), "i-safe")
	require.NoError(t, err)
	require.Equal(t, "i-safe", *client.terminateInput.InstanceId)
	require.True(t, *client.terminateInput.ShouldDecrementDesiredCapacity)
	require.True(t, client.terminateRetryDisabled)
	require.Equal(t, "activity-1", receipt.ActivityID)
	require.Equal(t, "InProgress", receipt.Status)
	require.Nil(t, client.setInput)
}

func TestASGTerminationFailureHasNoDesiredCapacityFallback(t *testing.T) {
	t.Parallel()

	client := newCompleteASGClient()
	client.terminateErr = &smithyhttp.RequestSendError{Err: errors.New("connection reset")}

	_, err := NewASG(client, nil).TerminateInstance(t.Context(), "i-safe")
	require.ErrorContains(t, err, "connection reset")
	var terminationErr *controller.ScaleInTerminationError
	require.ErrorAs(t, err, &terminationErr)
	require.Equal(t, controller.ScaleInTerminationAmbiguous, terminationErr.Outcome)
	require.True(t, client.terminateRetryDisabled)
	require.Nil(t, client.setInput)
}

func TestASGTerminationAPIRejectionIsExplicit(t *testing.T) {
	t.Parallel()

	client := newCompleteASGClient()
	client.terminateErr = &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusBadRequest}},
		Err:      errors.New("validation rejected"),
	}

	_, err := NewASG(client, nil).TerminateInstance(t.Context(), "i-safe")
	var terminationErr *controller.ScaleInTerminationError
	require.ErrorAs(t, err, &terminationErr)
	require.Equal(t, controller.ScaleInTerminationRejected, terminationErr.Outcome)
}

func TestASGTerminationServerResponseRemainsAmbiguous(t *testing.T) {
	t.Parallel()

	client := newCompleteASGClient()
	client.terminateErr = &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusInternalServerError}},
		Err:      errors.New("response decode failed"),
	}

	_, err := NewASG(client, nil).TerminateInstance(t.Context(), "i-safe")
	var terminationErr *controller.ScaleInTerminationError
	require.ErrorAs(t, err, &terminationErr)
	require.Equal(t, controller.ScaleInTerminationAmbiguous, terminationErr.Outcome)
}

func TestASGReadsExactScalingActivity(t *testing.T) {
	t.Parallel()

	startedAt := time.Unix(100, 0)
	client := newCompleteASGClient()
	client.activitiesOutput = &autoscaling.DescribeScalingActivitiesOutput{Activities: []types.Activity{{
		ActivityId:           aws.String("activity-1"),
		AutoScalingGroupName: aws.String("workers"),
		StartTime:            &startedAt,
		StatusCode:           types.ScalingActivityStatusCodeSuccessful,
	}}}

	activity, err := NewASG(client, nil).Activity(t.Context(), "workers", "activity-1")
	require.NoError(t, err)
	require.Equal(t, "Successful", activity.Status)
	require.Equal(t, startedAt, activity.StartedAt)
}

func newCompleteEC2Client() *fakeEC2Client {
	launchTime := time.Unix(100, 0)

	return &fakeEC2Client{describeOutput: &ec2.DescribeInstancesOutput{Reservations: []ec2types.Reservation{{
		Instances: []ec2types.Instance{{InstanceId: aws.String("i-safe"), LaunchTime: &launchTime}},
	}}}}
}

func TestReconcileTerminationNeverConvertsAmbiguityIntoASecondWrite(t *testing.T) {
	t.Parallel()

	present := ASGSnapshot{Name: "workers", Instances: map[string]ASGInstance{"i-safe": {ID: "i-safe"}}}
	require.Equal(t, TerminationUnknown, ReconcileTermination(present, "i-safe", nil))
	require.Equal(t, TerminationPending, ReconcileTermination(present, "i-safe", &ScalingActivity{ASGName: "workers", Status: "InProgress"}))
	require.Equal(t, TerminationRejected, ReconcileTermination(present, "i-safe", &ScalingActivity{ASGName: "workers", Status: "Failed"}))
	require.Equal(t, TerminationUnknown, ReconcileTermination(present, "i-safe", &ScalingActivity{ASGName: "workers", Status: "FutureState"}))

	absent := ASGSnapshot{Name: "workers", Instances: map[string]ASGInstance{}}
	require.Equal(t, TerminationCompleted, ReconcileTermination(absent, "i-safe", nil))
}

func newCompleteASGClient() *fakeAutoScalingClient {
	return &fakeAutoScalingClient{
		describeOutput: &autoscaling.DescribeAutoScalingGroupsOutput{AutoScalingGroups: []types.AutoScalingGroup{{
			AutoScalingGroupName: aws.String("workers"),
			AutoScalingGroupARN:  aws.String("arn:aws:autoscaling:region:account:autoScalingGroup:group-id:autoScalingGroupName/workers"),
			DesiredCapacity:      aws.Int32(2),
			MinSize:              aws.Int32(1),
			MaxSize:              aws.Int32(30),
			Instances: []types.Instance{{
				InstanceId:           aws.String("i-safe"),
				HealthStatus:         aws.String("Healthy"),
				LifecycleState:       types.LifecycleStateInService,
				ProtectedFromScaleIn: aws.Bool(false),
			}},
		}}},
		refreshOutput:    &autoscaling.DescribeInstanceRefreshesOutput{},
		activitiesOutput: &autoscaling.DescribeScalingActivitiesOutput{},
	}
}
