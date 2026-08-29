package workload

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	SchemaVersion           = 2
	maxClusterIDLen         = 128
	maxSandboxIDLen         = 256
	maxExecutionIDLen       = 256
	maxSweepBatch     int64 = 256
)

type State string

const (
	StateStarting State = "starting"
	StateRunning  State = "running"
)

var ErrInvalidTransition = errors.New("invalid workload state transition")

func validateMutation(clusterID, sandboxID, executionID string, now, expiresAt time.Time) error {
	if err := validateClusterID(clusterID); err != nil {
		return err
	}
	if err := validateIdentifier("sandbox ID", sandboxID, maxSandboxIDLen); err != nil {
		return err
	}
	if err := validateIdentifier("execution ID", executionID, maxExecutionIDLen); err != nil {
		return err
	}
	if now.IsZero() {
		return errors.New("workload current time is required")
	}
	if expiresAt.IsZero() || !expiresAt.After(now) {
		return errors.New("workload expiry must be after current time")
	}

	return nil
}

func validateIdentity(clusterID, sandboxID, executionID string) error {
	if err := validateClusterID(clusterID); err != nil {
		return err
	}
	if err := validateIdentifier("sandbox ID", sandboxID, maxSandboxIDLen); err != nil {
		return err
	}

	return validateIdentifier("execution ID", executionID, maxExecutionIDLen)
}

func validateClusterID(clusterID string) error {
	if err := validateIdentifier("cluster ID", clusterID, maxClusterIDLen); err != nil {
		return err
	}
	if strings.ContainsAny(clusterID, "{}") {
		return errors.New("workload cluster ID cannot contain braces")
	}

	return nil
}

func validateIdentifier(name, value string, maxLength int) error {
	if value == "" || strings.TrimSpace(value) == "" || len(value) > maxLength || !utf8.ValidString(value) {
		return fmt.Errorf("workload %s is required and must be valid UTF-8 at most %d bytes", name, maxLength)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("workload %s cannot contain control characters", name)
		}
	}

	return nil
}
