package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRetrySafeGuestReadinessTimeoutRequiresExactDetails(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		err  error
		want bool
	}{
		"exact contract": {
			err: NewSandboxCreateError(
				codes.DeadlineExceeded,
				SandboxGuestReadinessTimeoutReason,
				"guest readiness timed out",
				true,
			),
			want: true,
		},
		"wrong code": {
			err: NewSandboxCreateError(
				codes.Internal,
				SandboxGuestReadinessTimeoutReason,
				"guest readiness timed out",
				true,
			),
		},
		"missing metadata": {
			err: NewSandboxCreateError(
				codes.DeadlineExceeded,
				SandboxGuestReadinessTimeoutReason,
				"guest readiness timed out",
				false,
			),
		},
		"plain status": {
			err: status.Error(codes.DeadlineExceeded, "guest readiness timed out"),
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, IsRetrySafeGuestReadinessTimeout(tt.err))
		})
	}
}

func TestSandboxCreateErrorInfoRejectsAnotherDomain(t *testing.T) {
	t.Parallel()

	st, err := status.New(codes.DeadlineExceeded, "guest readiness timed out").WithDetails(&errdetails.ErrorInfo{
		Domain: "another.domain",
		Reason: SandboxGuestReadinessTimeoutReason,
		Metadata: map[string]string{
			SandboxCreateRetrySafeMetadataKey: "true",
		},
	})
	require.NoError(t, err)

	_, ok := SandboxCreateErrorInfo(st.Err())
	require.False(t, ok)
}
