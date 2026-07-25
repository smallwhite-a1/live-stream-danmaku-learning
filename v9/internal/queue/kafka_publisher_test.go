package queue

import "testing"

func TestPublisherHealthDegradesAfterConsecutiveErrorsAndRecoversOnAck(t *testing.T) {
	health := newPublisherHealth(3)

	health.RecordError()
	health.RecordError()
	if snapshot := health.Snapshot(); snapshot.Status != PublisherHealthy {
		t.Fatalf("status after two errors = %q, want healthy", snapshot.Status)
	}

	health.RecordError()
	snapshot := health.Snapshot()
	if snapshot.Status != PublisherDegraded {
		t.Fatalf("status after threshold = %q, want degraded", snapshot.Status)
	}
	if snapshot.ConsecutiveErrors != 3 || snapshot.DegradedTransitions != 1 {
		t.Fatalf("unexpected degraded snapshot: %+v", snapshot)
	}

	health.RecordError()
	if transitions := health.Snapshot().DegradedTransitions; transitions != 1 {
		t.Fatalf("degraded transitions = %d, want 1", transitions)
	}

	health.RecordSuccess()
	snapshot = health.Snapshot()
	if snapshot.Status != PublisherHealthy {
		t.Fatalf("status after ack = %q, want healthy", snapshot.Status)
	}
	if snapshot.ConsecutiveErrors != 0 || snapshot.Recoveries != 1 {
		t.Fatalf("unexpected recovered snapshot: %+v", snapshot)
	}
}

func TestPublisherHealthUsesDefaultThreshold(t *testing.T) {
	health := newPublisherHealth(0)
	for i := 0; i < DefaultDegradeAfterErrors-1; i++ {
		health.RecordError()
	}
	if snapshot := health.Snapshot(); snapshot.Status != PublisherHealthy {
		t.Fatalf("status before default threshold = %q, want healthy", snapshot.Status)
	}
	health.RecordError()
	if snapshot := health.Snapshot(); snapshot.Status != PublisherDegraded {
		t.Fatalf("status at default threshold = %q, want degraded", snapshot.Status)
	}
}
