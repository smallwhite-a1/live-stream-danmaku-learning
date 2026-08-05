package jsonl

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
)

func TestRunDecodesValidLinesSkipsBlanksAndNormalizesUTC(t *testing.T) {
	source := New(strings.NewReader(`
{"event_id":"event-1","room_id":"room-1","user_id":"user-1","content":"hello","occurred_at":"2020-01-02T03:04:05+08:00"}

{"event_id":"event-2","room_id":"room-1","user_id":"user-2","content":"world","occurred_at":"2020-01-02T03:04:06Z"}
`))

	var events []domain.MessageEvent
	err := source.Run(context.Background(), func(_ context.Context, event domain.MessageEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("Run() delivered %d events, want 2", len(events))
	}
	if got, want := events[0].OccurredAt, time.Date(2020, 1, 1, 19, 4, 5, 0, time.UTC); !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("first OccurredAt = %v (%v), want %v UTC", got, got.Location(), want)
	}
}

func TestRunRejectsMalformedJSONWithLineNumber(t *testing.T) {
	source := New(strings.NewReader("\n\n{not-json}\n"))

	err := source.Run(context.Background(), func(context.Context, domain.MessageEvent) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "line 3") {
		t.Fatalf("Run() error = %v, want malformed JSON error naming line 3", err)
	}
}

func TestRunRejectsInvalidEventWithLineNumber(t *testing.T) {
	source := New(strings.NewReader("{\"event_id\":\"event-1\",\"room_id\":\"room-1\",\"user_id\":\"user-1\",\"content\":\"\",\"occurred_at\":\"2020-01-01T00:00:00Z\"}\n"))

	err := source.Run(context.Background(), func(context.Context, domain.MessageEvent) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "line 1") || !strings.Contains(err.Error(), "content is required") {
		t.Fatalf("Run() error = %v, want validation error naming line 1", err)
	}
}

func TestRunStopsWhenCancelled(t *testing.T) {
	source := New(strings.NewReader("{\"event_id\":\"event-1\",\"room_id\":\"room-1\",\"user_id\":\"user-1\",\"content\":\"hello\",\"occurred_at\":\"2020-01-01T00:00:00Z\"}\n"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := source.Run(ctx, func(context.Context, domain.MessageEvent) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}
