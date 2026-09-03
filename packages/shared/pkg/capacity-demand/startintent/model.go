package startintent

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	SchemaVersion           = 1
	SinglePoolCompatibility = "single-pool-v1"
	maxClusterIDLen         = 128
	maxSandboxIDLen         = 256
	maxOwnerTokenLen        = 256
	maxCompatibilityLen     = 512
)

type State string

const (
	StateOutstanding State = "outstanding"
	StateHandoff     State = "handoff"
)

type Intent struct {
	ClusterID     string
	SandboxID     string
	OwnerToken    string
	VCPU          int64
	MemoryMiB     int64
	Compatibility string
}

// Record is a validated, active start-intent lease returned from Redis.
type Record struct {
	Intent

	SchemaVersion int
	State         State
	CreatedAt     time.Time
	ExpiresAt     time.Time
}

type persistedRecord struct {
	SchemaVersion int    `json:"schema_version"`
	State         State  `json:"state"`
	ClusterID     string `json:"cluster_id"`
	SandboxID     string `json:"sandbox_id"`
	OwnerToken    string `json:"owner_token"`
	VCPU          int64  `json:"vcpu"`
	MemoryMiB     int64  `json:"memory_mib"`
	Compatibility string `json:"compatibility"`
	CreatedAtMs   int64  `json:"created_at_ms"`
	ExpiresAtMs   int64  `json:"expires_at_ms"`
}

func newPersistedRecord(intent Intent, now, expiresAt time.Time) persistedRecord {
	return persistedRecord{
		SchemaVersion: SchemaVersion,
		State:         StateOutstanding,
		ClusterID:     intent.ClusterID,
		SandboxID:     intent.SandboxID,
		OwnerToken:    intent.OwnerToken,
		VCPU:          intent.VCPU,
		MemoryMiB:     intent.MemoryMiB,
		Compatibility: intent.Compatibility,
		CreatedAtMs:   now.UnixMilli(),
		ExpiresAtMs:   expiresAt.UnixMilli(),
	}
}

func (r persistedRecord) record() Record {
	return Record{
		Intent: Intent{
			ClusterID:     r.ClusterID,
			SandboxID:     r.SandboxID,
			OwnerToken:    r.OwnerToken,
			VCPU:          r.VCPU,
			MemoryMiB:     r.MemoryMiB,
			Compatibility: r.Compatibility,
		},
		SchemaVersion: r.SchemaVersion,
		State:         r.State,
		CreatedAt:     time.UnixMilli(r.CreatedAtMs).UTC(),
		ExpiresAt:     time.UnixMilli(r.ExpiresAtMs).UTC(),
	}
}

func validateIntent(intent Intent) error {
	if err := validateClusterID(intent.ClusterID); err != nil {
		return err
	}
	if err := validateIdentifier("sandbox ID", intent.SandboxID, maxSandboxIDLen, false); err != nil {
		return err
	}
	if err := validateIdentifier("owner token", intent.OwnerToken, maxOwnerTokenLen, false); err != nil {
		return err
	}
	if intent.VCPU <= 0 {
		return errors.New("start intent vCPU must be positive")
	}
	if intent.MemoryMiB <= 0 {
		return errors.New("start intent memory must be positive")
	}

	return validateIdentifier("compatibility", intent.Compatibility, maxCompatibilityLen, false)
}

func validateRecord(record persistedRecord, expectedClusterID, expectedSandboxID string, nowMs, deadlineMs int64) error {
	if record.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported start intent schema version %d", record.SchemaVersion)
	}
	if record.State != StateOutstanding && record.State != StateHandoff {
		return fmt.Errorf("invalid start intent state %q", record.State)
	}
	if err := validateIntent(Intent{
		ClusterID:     record.ClusterID,
		SandboxID:     record.SandboxID,
		OwnerToken:    record.OwnerToken,
		VCPU:          record.VCPU,
		MemoryMiB:     record.MemoryMiB,
		Compatibility: record.Compatibility,
	}); err != nil {
		return err
	}
	if record.ClusterID != expectedClusterID || record.SandboxID != expectedSandboxID {
		return errors.New("start intent key does not match stored identifiers")
	}
	if record.CreatedAtMs <= 0 || record.ExpiresAtMs <= record.CreatedAtMs {
		return errors.New("start intent timestamps are invalid")
	}
	if record.ExpiresAtMs != deadlineMs {
		return errors.New("start intent expiry does not match its deadline index")
	}
	if record.ExpiresAtMs <= nowMs {
		return errors.New("expired start intent remained in active result")
	}

	return nil
}

func validateMutation(clusterID, sandboxID, ownerToken string, now, expiresAt time.Time) error {
	if err := validateClusterID(clusterID); err != nil {
		return err
	}
	if err := validateIdentifier("sandbox ID", sandboxID, maxSandboxIDLen, false); err != nil {
		return err
	}
	if err := validateIdentifier("owner token", ownerToken, maxOwnerTokenLen, false); err != nil {
		return err
	}

	return validateLease(now, expiresAt)
}

func validateLease(now, expiresAt time.Time) error {
	if now.IsZero() {
		return errors.New("start intent current time is required")
	}
	if expiresAt.IsZero() || expiresAt.UnixMilli() <= now.UnixMilli() {
		return errors.New("start intent expiry must be after the current time")
	}

	return nil
}

func validateClusterID(clusterID string) error {
	return validateIdentifier("cluster ID", clusterID, maxClusterIDLen, true)
}

func validateIdentifier(name, value string, maxLength int, rejectBraces bool) error {
	if value == "" || strings.TrimSpace(value) == "" || len(value) > maxLength || !utf8.ValidString(value) {
		return fmt.Errorf("start intent %s is required and must be valid UTF-8 at most %d bytes", name, maxLength)
	}
	if rejectBraces && strings.ContainsAny(value, "{}") {
		return fmt.Errorf("start intent %s cannot contain braces", name)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("start intent %s cannot contain control characters", name)
		}
	}

	return nil
}
