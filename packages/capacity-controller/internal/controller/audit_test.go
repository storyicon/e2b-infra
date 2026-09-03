package controller

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingAuditSink struct {
	started chan struct{}
	release chan struct{}
}

func (s *blockingAuditSink) Record(ScaleAuditEvent) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.release
}

func TestAsyncAuditSinkNeverBlocksProducerWhenDelegateStalls(t *testing.T) {
	t.Parallel()

	delegate := &blockingAuditSink{started: make(chan struct{}, 1), release: make(chan struct{})}
	sink := NewAsyncAuditSink(delegate, 1)
	sink.Record(ScaleAuditEvent{Event: AuditEventControllerStarted})
	select {
	case <-delegate.started:
	case <-time.After(time.Second):
		t.Fatal("audit delegate did not start")
	}

	completed := make(chan struct{})
	go func() {
		for sequence := uint64(1); sequence <= 100; sequence++ {
			sink.Record(ScaleAuditEvent{ScaleWriteSequence: sequence})
		}
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("audit producer blocked behind delegate")
	}
	require.Positive(t, sink.Dropped())

	close(delegate.release)
	sink.Close()
}

type panickingAuditSink struct{}

func (panickingAuditSink) Record(ScaleAuditEvent) {
	panic("log handler failed")
}

func TestAsyncAuditSinkContainsDelegatePanic(t *testing.T) {
	t.Parallel()

	sink := NewAsyncAuditSink(panickingAuditSink{}, 1)
	sink.Record(ScaleAuditEvent{})
	sink.Close()
	require.Equal(t, uint64(1), sink.Dropped())
}

type blockingRecordingAuditSink struct {
	started chan struct{}
	release chan struct{}
	events  chan ScaleAuditEvent
	once    sync.Once
}

func (s *blockingRecordingAuditSink) Record(event ScaleAuditEvent) {
	s.once.Do(func() {
		close(s.started)
		<-s.release
	})
	s.events <- event
}

func TestAsyncAuditCheckpointReportsPriorDeliveryLoss(t *testing.T) {
	t.Parallel()

	delegate := &blockingRecordingAuditSink{
		started: make(chan struct{}),
		release: make(chan struct{}),
		events:  make(chan ScaleAuditEvent, 3),
	}
	sink := NewAsyncAuditSink(delegate, 1)
	sink.Record(ScaleAuditEvent{Event: AuditEventScaleWriteStarted, ScaleWriteSequence: 1})
	select {
	case <-delegate.started:
	case <-time.After(time.Second):
		t.Fatal("audit delegate did not start")
	}
	sink.Record(ScaleAuditEvent{Event: AuditEventScaleWriteFinished, ScaleWriteSequence: 1})
	sink.Record(ScaleAuditEvent{Event: AuditEventScaleWriteStarted, ScaleWriteSequence: 2})
	close(delegate.release)
	<-delegate.events
	<-delegate.events
	require.Equal(t, uint64(1), sink.Dropped())

	sink.Record(ScaleAuditEvent{Event: AuditEventCheckpoint, ScaleWriteSequence: 2})
	sink.Close()
	checkpoint := <-delegate.events
	require.Equal(t, AuditEventCheckpoint, checkpoint.Event)
	require.Equal(t, sink.Dropped(), checkpoint.AuditDroppedTotal)
}
