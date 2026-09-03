package controller

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	AuditEventControllerStarted  = "controller_started"
	AuditEventScaleWriteStarted  = "scale_write_started"
	AuditEventScaleWriteFinished = "scale_write_finished"
	AuditEventCheckpoint         = "audit_checkpoint"
	AuditEventScaleInTransition  = "scale_in_transition"
)

type ScaleWriteMetadata struct {
	RequestID string
}

// ScaleAuditEvent intentionally contains only capacity decision metadata. It
// must never include service credentials, customer metadata, or sandbox IDs.
type ScaleAuditEvent struct {
	Event                 string
	ControllerInstanceID  string
	ScaleWriteSequence    uint64
	Mode                  Mode
	WorkloadCount         int64
	CurrentDesired        int32
	Target                int32
	BatchTrigger          string
	BatchAge              time.Duration
	BatchIdleAge          time.Duration
	Outcome               string
	Duration              time.Duration
	AWSRequestID          string
	Error                 string
	AuditDroppedTotal     uint64
	CheckpointGeneratedAt time.Time
	ScaleInOperationID    string
	ScaleInNodeID         string
	ScaleInStage          string
	ScaleInReason         string
}

// AuditSink has no error return so evidence collection cannot change a scale
// decision. The production implementation writes only to the process logger.
type AuditSink interface {
	Record(event ScaleAuditEvent)
}

// AsyncAuditSink bounds audit overhead on the reconciliation path. A full
// buffer drops evidence instead of delaying a scale write; periodic checkpoints
// expose cumulative delivery loss so the benchmark fails closed even when the
// dropped event is at the sequence tail.
type AsyncAuditSink struct {
	delegate AuditSink
	events   chan ScaleAuditEvent
	done     chan struct{}
	close    sync.Once
	dropped  atomic.Uint64
}

func NewAsyncAuditSink(delegate AuditSink, bufferSize int) *AsyncAuditSink {
	if delegate == nil {
		panic("async audit delegate is required")
	}
	if bufferSize <= 0 {
		panic("async audit buffer size must be positive")
	}
	sink := &AsyncAuditSink{
		delegate: delegate,
		events:   make(chan ScaleAuditEvent, bufferSize),
		done:     make(chan struct{}),
	}
	go sink.run()

	return sink
}

func (s *AsyncAuditSink) Record(event ScaleAuditEvent) {
	select {
	case s.events <- event:
	default:
		s.dropped.Add(1)
	}
}

func (s *AsyncAuditSink) Dropped() uint64 {
	return s.dropped.Load()
}

func (s *AsyncAuditSink) Close() {
	s.close.Do(func() {
		close(s.events)
		<-s.done
	})
}

func (s *AsyncAuditSink) run() {
	defer close(s.done)
	for event := range s.events {
		func() {
			defer func() {
				if recover() != nil {
					s.dropped.Add(1)
				}
			}()
			if event.Event == AuditEventCheckpoint {
				event.AuditDroppedTotal = s.dropped.Load()
			}
			s.delegate.Record(event)
		}()
	}
}

func newControllerInstanceID() string {
	return uuid.NewString()
}
