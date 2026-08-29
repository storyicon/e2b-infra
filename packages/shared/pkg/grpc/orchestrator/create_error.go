package orchestrator

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	SandboxCreateErrorDomain           = "e2b.orchestrator.sandbox.create"
	SandboxGuestReadinessTimeoutReason = "SANDBOX_GUEST_READINESS_TIMEOUT"
	SandboxCreateCleanupFailedReason   = "SANDBOX_CREATE_CLEANUP_FAILED"
	SandboxCreateRetrySafeMetadataKey  = "retry_safe"
)

// NewSandboxCreateError attaches the machine-readable create outcome used by
// the API placement policy. retrySafe is intentionally explicit and defaults
// closed when the detail or metadata is absent during a rolling deployment.
func NewSandboxCreateError(code codes.Code, reason, message string, retrySafe bool) error {
	metadata := map[string]string{}
	if retrySafe {
		metadata[SandboxCreateRetrySafeMetadataKey] = "true"
	}

	st, err := status.New(code, message).WithDetails(&errdetails.ErrorInfo{
		Domain:   SandboxCreateErrorDomain,
		Reason:   reason,
		Metadata: metadata,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "encode sandbox create error detail: %v", err)
	}

	return st.Err()
}

// SandboxCreateErrorInfo returns only details from this protocol domain.
func SandboxCreateErrorInfo(err error) (*errdetails.ErrorInfo, bool) {
	for _, detail := range status.Convert(err).Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if ok && info.GetDomain() == SandboxCreateErrorDomain {
			return info, true
		}
	}

	return nil, false
}

func IsRetrySafeGuestReadinessTimeout(err error) bool {
	if status.Code(err) != codes.DeadlineExceeded {
		return false
	}

	info, ok := SandboxCreateErrorInfo(err)

	return ok &&
		info.GetReason() == SandboxGuestReadinessTimeoutReason &&
		info.GetMetadata()[SandboxCreateRetrySafeMetadataKey] == "true"
}

func IsSandboxCreateCleanupFailed(err error) bool {
	info, ok := SandboxCreateErrorInfo(err)

	return ok && info.GetReason() == SandboxCreateCleanupFailedReason
}
