package queue

import (
	"sync"
	"time"
)

const DefaultDegradeAfterErrors = 3

type PublisherStatus string

const (
	PublisherHealthy  PublisherStatus = "healthy"
	PublisherDegraded PublisherStatus = "degraded"
)

type PublisherHealthSnapshot struct {
	Status              PublisherStatus `json:"status"`
	ConsecutiveErrors   uint64          `json:"consecutive_errors"`
	DegradedTransitions uint64          `json:"degraded_transitions"`
	Recoveries          uint64          `json:"recoveries"`
	LastAckUnixMillis   int64           `json:"last_ack_unix_millis"`
	LastErrorUnixMillis int64           `json:"last_error_unix_millis"`
}

type publisherHealth struct {
	mu             sync.Mutex
	degradeAfter   uint64
	status         PublisherStatus
	consecutive    uint64
	transitions    uint64
	recoveries     uint64
	lastAckMillis  int64
	lastErrorMilli int64
}

func newPublisherHealth(degradeAfter int) *publisherHealth {
	if degradeAfter <= 0 {
		degradeAfter = DefaultDegradeAfterErrors
	}
	return &publisherHealth{
		degradeAfter: uint64(degradeAfter),
		status:       PublisherHealthy,
	}
}

func (h *publisherHealth) RecordError() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.consecutive++
	h.lastErrorMilli = time.Now().UnixMilli()
	if h.consecutive >= h.degradeAfter && h.status != PublisherDegraded {
		h.status = PublisherDegraded
		h.transitions++
	}
}

func (h *publisherHealth) RecordSuccess() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastAckMillis = time.Now().UnixMilli()
	h.consecutive = 0
	if h.status == PublisherDegraded {
		h.status = PublisherHealthy
		h.recoveries++
	}
}

func (h *publisherHealth) Snapshot() PublisherHealthSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()

	return PublisherHealthSnapshot{
		Status:              h.status,
		ConsecutiveErrors:   h.consecutive,
		DegradedTransitions: h.transitions,
		Recoveries:          h.recoveries,
		LastAckUnixMillis:   h.lastAckMillis,
		LastErrorUnixMillis: h.lastErrorMilli,
	}
}
