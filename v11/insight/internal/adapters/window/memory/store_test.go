package memory

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/ports"
)

var testStart = time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

func TestAddFirstEventCreatesWindow(t *testing.T) {
	store := newTestStore(10)
	event := testEvent("event-1", "room-1", testStart.Add(5*time.Second))

	result, err := store.Add(context.Background(), event, testStart.Add(6*time.Second))
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if !result.Added || result.Duplicate || result.Late || result.Completed {
		t.Fatalf("Add() result = %+v, want first event added on time", result)
	}

	window, err := store.Load(context.Background(), result.Ref)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if window.TotalMessages != 1 || len(window.Events) != 1 {
		t.Fatalf("Load() = %+v, want one retained message", window)
	}
}

func TestAddSameEventIDIsCountedOnce(t *testing.T) {
	store := newTestStore(10)
	event := testEvent("event-1", "room-1", testStart.Add(5*time.Second))
	first, err := store.Add(context.Background(), event, testStart.Add(6*time.Second))
	if err != nil {
		t.Fatalf("first Add() error = %v", err)
	}

	result, err := store.Add(context.Background(), event, first.Ref.End.Add(time.Hour))
	if err != nil {
		t.Fatalf("duplicate Add() error = %v", err)
	}
	if !result.Duplicate || result.Added || result.Late {
		t.Fatalf("duplicate Add() result = %+v", result)
	}

	window, err := store.Load(context.Background(), first.Ref)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if window.TotalMessages != 1 || window.DuplicateMessages != 1 || len(window.Events) != 1 {
		t.Fatalf("Load() = %+v, want one message and one duplicate", window)
	}
}

func TestLoadSortsEventsAndReturnsCopy(t *testing.T) {
	store := newTestStore(10)
	events := []domain.MessageEvent{
		testEvent("event-c", "room-1", testStart.Add(20*time.Second)),
		testEvent("event-b", "room-1", testStart.Add(10*time.Second)),
		testEvent("event-a", "room-1", testStart.Add(10*time.Second)),
	}
	var ref domain.WindowRef
	for _, event := range events {
		result, err := store.Add(context.Background(), event, event.OccurredAt)
		if err != nil {
			t.Fatalf("Add() error = %v", err)
		}
		ref = result.Ref
	}

	window, err := store.Load(context.Background(), ref)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	got := []string{window.Events[0].EventID, window.Events[1].EventID, window.Events[2].EventID}
	want := []string{"event-a", "event-b", "event-c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event order = %v, want %v", got, want)
		}
	}

	window.Events[0].Content = "changed"
	again, err := store.Load(context.Background(), ref)
	if err != nil {
		t.Fatalf("second Load() error = %v", err)
	}
	if again.Events[0].Content == "changed" {
		t.Fatal("Load() exposed the internal event slice")
	}
}

func TestClaimDueWaitsUntilEndPlusLatenessAndMarksClaimed(t *testing.T) {
	store := newTestStore(10)
	result := mustAdd(t, store, testEvent("event-1", "room-1", testStart.Add(time.Second)), testStart.Add(time.Second))

	refs, err := store.ClaimDue(context.Background(), result.Ref.End.Add(9*time.Second), 10)
	if err != nil {
		t.Fatalf("ClaimDue() before due error = %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("ClaimDue() before due = %v, want no work", refs)
	}

	refs, err = store.ClaimDue(context.Background(), result.Ref.End.Add(10*time.Second), 10)
	if err != nil {
		t.Fatalf("ClaimDue() at due error = %v", err)
	}
	if len(refs) != 1 || refs[0].Key() != result.Ref.Key() {
		t.Fatalf("ClaimDue() at due = %v, want %v", refs, result.Ref)
	}

	again, err := store.ClaimDue(context.Background(), result.Ref.End.Add(10*time.Second), 10)
	if err != nil {
		t.Fatalf("second ClaimDue() error = %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("second ClaimDue() = %v, want claimed window omitted", again)
	}
}

func TestClaimDueSortsByStartThenRoomAndHonorsLimit(t *testing.T) {
	store := newTestStore(10)
	mustAdd(t, store, testEvent("event-2", "room-b", testStart.Add(time.Second)), testStart)
	mustAdd(t, store, testEvent("event-1", "room-a", testStart.Add(time.Second)), testStart)
	mustAdd(t, store, testEvent("event-3", "room-a", testStart.Add(time.Minute+time.Second)), testStart)

	refs, err := store.ClaimDue(context.Background(), testStart.Add(3*time.Minute), 2)
	if err != nil {
		t.Fatalf("ClaimDue() error = %v", err)
	}
	if len(refs) != 2 || refs[0].RoomID != "room-a" || refs[1].RoomID != "room-b" {
		t.Fatalf("ClaimDue() = %v, want first two ordered by start then room", refs)
	}

	none, err := store.ClaimDue(context.Background(), testStart.Add(3*time.Minute), 0)
	if err != nil {
		t.Fatalf("ClaimDue() zero limit error = %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("ClaimDue() zero limit = %v, want no work", none)
	}
}

func TestReleaseMakesWindowClaimableAtRetryAt(t *testing.T) {
	store := newTestStore(10)
	result := mustAdd(t, store, testEvent("event-1", "room-1", testStart.Add(time.Second)), testStart)
	due := result.Ref.End.Add(10 * time.Second)
	mustClaim(t, store, due)
	retryAt := due.Add(time.Minute)

	if err := store.Release(context.Background(), result.Ref, retryAt); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	refs, err := store.ClaimDue(context.Background(), retryAt.Add(-time.Nanosecond), 1)
	if err != nil {
		t.Fatalf("ClaimDue() before retry error = %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("ClaimDue() before retry = %v, want no work", refs)
	}
	refs, err = store.ClaimDue(context.Background(), retryAt, 1)
	if err != nil {
		t.Fatalf("ClaimDue() at retry error = %v", err)
	}
	if len(refs) != 1 || refs[0].Key() != result.Ref.Key() {
		t.Fatalf("ClaimDue() at retry = %v, want %v", refs, result.Ref)
	}
}

func TestCompletePreventsDuplicateReplay(t *testing.T) {
	store := newTestStore(10)
	event := testEvent("event-1", "room-1", testStart.Add(time.Second))
	result := mustAdd(t, store, event, testStart)
	mustClaim(t, store, result.Ref.End.Add(10*time.Second))

	if err := store.Complete(context.Background(), result.Ref); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if err := store.Complete(context.Background(), result.Ref); err != nil {
		t.Fatalf("second Complete() error = %v", err)
	}
	replayed, err := store.Add(context.Background(), event, result.Ref.End.Add(time.Hour))
	if err != nil {
		t.Fatalf("replayed Add() error = %v", err)
	}
	if !replayed.Duplicate || !replayed.Completed || replayed.Added {
		t.Fatalf("replayed Add() = %+v, want duplicate completed result", replayed)
	}
	refs, err := store.ClaimDue(context.Background(), result.Ref.End.Add(2*time.Hour), 1)
	if err != nil {
		t.Fatalf("ClaimDue() after complete error = %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("ClaimDue() after complete = %v, want no work", refs)
	}
	if _, err := store.Load(context.Background(), result.Ref); err != nil {
		t.Fatalf("Load() completed window error = %v", err)
	}
}

func TestLateEventIsCountedButNotRetained(t *testing.T) {
	store := newTestStore(10)
	first := mustAdd(t, store, testEvent("event-1", "room-1", testStart.Add(time.Second)), testStart)
	lateEvent := testEvent("event-2", "room-1", testStart.Add(2*time.Second))

	result, err := store.Add(context.Background(), lateEvent, first.Ref.End.Add(10*time.Second))
	if err != nil {
		t.Fatalf("late Add() error = %v", err)
	}
	if !result.Added || !result.Late || result.Duplicate {
		t.Fatalf("late Add() = %+v, want unique late event", result)
	}
	window, err := store.Load(context.Background(), first.Ref)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if window.TotalMessages != 2 || window.LateMessages != 1 || len(window.Events) != 1 {
		t.Fatalf("Load() = %+v, want late event counted but not retained", window)
	}
}

func TestMaxEventsBoundsRetainedEvents(t *testing.T) {
	store := newTestStore(2)
	var ref domain.WindowRef
	for i := 0; i < 5; i++ {
		result := mustAdd(t, store, testEvent(fmt.Sprintf("event-%d", i), "room-1", testStart.Add(time.Duration(i)*time.Second)), testStart)
		ref = result.Ref
	}

	window, err := store.Load(context.Background(), ref)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if window.TotalMessages != 5 || len(window.Events) != 2 {
		t.Fatalf("Load() = %+v, want total 5 and retained 2", window)
	}
}

func TestConcurrentAdd(t *testing.T) {
	store := newTestStore(100)
	const count = 50
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			event := testEvent(fmt.Sprintf("event-%d", i), "room-1", testStart.Add(time.Duration(i)*time.Millisecond))
			if _, err := store.Add(context.Background(), event, testStart); err != nil {
				t.Errorf("Add() error = %v", err)
			}
		}(i)
	}
	wg.Wait()

	ref, err := domain.NewWindowRef("room-1", testStart, time.Minute)
	if err != nil {
		t.Fatalf("NewWindowRef() error = %v", err)
	}
	window, err := store.Load(context.Background(), ref)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if window.TotalMessages != count || len(window.Events) != count {
		t.Fatalf("Load() totals = %d/%d, want %d/%d", window.TotalMessages, len(window.Events), count, count)
	}
}

func newTestStore(maxEvents int) *Store {
	return New(Config{WindowSize: time.Minute, Lateness: 10 * time.Second, MaxEvents: maxEvents})
}

func testEvent(eventID, roomID string, occurredAt time.Time) domain.MessageEvent {
	return domain.MessageEvent{
		EventID: eventID, RoomID: roomID, UserID: "user-1",
		Content: "hello", OccurredAt: occurredAt,
	}
}

func mustAdd(t *testing.T, store *Store, event domain.MessageEvent, now time.Time) ports.AddResult {
	t.Helper()
	result, err := store.Add(context.Background(), event, now)
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	return result
}

func mustClaim(t *testing.T, store *Store, now time.Time) domain.WindowRef {
	t.Helper()
	refs, err := store.ClaimDue(context.Background(), now, 1)
	if err != nil {
		t.Fatalf("ClaimDue() error = %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("ClaimDue() = %v, want one window", refs)
	}
	return refs[0]
}
