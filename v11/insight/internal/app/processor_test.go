package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/analyzer/rule"
	repositorymemory "github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/repository/memory"
	windowmemory "github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/window/memory"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/ports"
)

func TestIngestorAddsEventUsingInjectedClock(t *testing.T) {
	store := &recordingStore{}
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	ingestor := NewIngestor(store, func() time.Time { return now })
	event := appEvent("event-1", "room-1", now)

	if err := ingestor.Handle(context.Background(), event); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if !store.now.Equal(now) || store.event.EventID != event.EventID {
		t.Fatalf("Add() received (%+v, %v), want (%+v, %v)", store.event, store.now, event, now)
	}
}

func TestProcessDueSavesNormalInsightAndCompletesWindow(t *testing.T) {
	store, ref := dueStore(t, 1)
	repository := repositorymemory.New()
	processor := mustProcessor(t, store, staticAnalyzer{result: analysisResult("primary.v1")}, rule.NewAnalyzer(), repository, 1)

	summary, err := processor.ProcessDue(context.Background(), dueTime)
	if err != nil {
		t.Fatalf("ProcessDue() error = %v", err)
	}
	if summary != (Summary{Completed: 1}) {
		t.Fatalf("ProcessDue() summary = %+v, want one completed", summary)
	}
	insight, ok, err := repository.Latest(context.Background(), ref.RoomID)
	if err != nil || !ok || insight.Status != domain.InsightStatusNormal || !insight.WindowStart.Equal(ref.Start) {
		t.Fatalf("saved insight = (%+v, %v, %v), want normal insight for claimed window", insight, ok, err)
	}
	if refs, err := store.ClaimDue(context.Background(), dueTime, 1); err != nil || len(refs) != 0 {
		t.Fatalf("ClaimDue() after completion = (%v, %v), want no windows", refs, err)
	}
}

func TestProcessDueFallsBackToRulesAndSavesDegradedInsight(t *testing.T) {
	store, ref := dueStore(t, 1)
	repository := repositorymemory.New()
	processor := mustProcessor(t, store, staticAnalyzer{err: errors.New("model unavailable")}, rule.NewAnalyzer(), repository, 1)

	summary, err := processor.ProcessDue(context.Background(), dueTime)
	if err != nil {
		t.Fatalf("ProcessDue() error = %v", err)
	}
	if summary != (Summary{Degraded: 1}) {
		t.Fatalf("ProcessDue() summary = %+v, want one degraded", summary)
	}
	insight, ok, err := repository.Latest(context.Background(), ref.RoomID)
	if err != nil || !ok || insight.Status != domain.InsightStatusDegraded || insight.DegradedReason != "model unavailable" || insight.Rules.MessageCount != 1 {
		t.Fatalf("saved fallback insight = (%+v, %v, %v), want degraded rule result", insight, ok, err)
	}
}

func TestProcessDueReleasesWindowWhenSaveFails(t *testing.T) {
	store, ref := dueStore(t, 1)
	processor := mustProcessor(t, store, staticAnalyzer{result: analysisResult("primary.v1")}, rule.NewAnalyzer(), failingRepository{}, 1)

	summary, err := processor.ProcessDue(context.Background(), dueTime)
	if err != nil {
		t.Fatalf("ProcessDue() error = %v", err)
	}
	if summary != (Summary{Failed: 1}) {
		t.Fatalf("ProcessDue() summary = %+v, want one failed", summary)
	}
	refs, err := store.ClaimDue(context.Background(), dueTime, 1)
	if err != nil || len(refs) != 1 || refs[0].Key() != ref.Key() {
		t.Fatalf("ClaimDue() after save failure = (%v, %v), want released window", refs, err)
	}
}

func TestProcessDueReportsWindowReleaseFailure(t *testing.T) {
	releaseErr := errors.New("release unavailable")
	store := &releaseErrorStore{Store: windowmemory.New(windowmemory.Config{WindowSize: time.Minute, Lateness: time.Second, MaxEvents: 10}), err: releaseErr}
	event := appEvent("event-0", "room-0", dueTime.Add(-2*time.Minute))
	if _, err := store.Add(context.Background(), event, event.OccurredAt); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	processor := mustProcessor(t, store, staticAnalyzer{result: analysisResult("primary.v1")}, rule.NewAnalyzer(), failingRepository{}, 1)

	summary, err := processor.ProcessDue(context.Background(), dueTime)
	if summary != (Summary{Failed: 1}) {
		t.Fatalf("ProcessDue() summary = %+v, want one failed", summary)
	}
	if !errors.Is(err, releaseErr) {
		t.Fatalf("ProcessDue() error = %v, want wrapping release error", err)
	}
}

func TestProcessDueTreatsDuplicateSaveAsSuccess(t *testing.T) {
	repository := repositorymemory.New()
	firstStore, _ := dueStore(t, 1)
	secondStore, _ := dueStore(t, 1)
	first := mustProcessor(t, firstStore, staticAnalyzer{result: analysisResult("primary.v1")}, rule.NewAnalyzer(), repository, 1)
	second := mustProcessor(t, secondStore, staticAnalyzer{result: analysisResult("primary.v1")}, rule.NewAnalyzer(), repository, 1)

	if summary, err := first.ProcessDue(context.Background(), dueTime); err != nil || summary.Completed != 1 {
		t.Fatalf("first ProcessDue() = (%+v, %v), want completed", summary, err)
	}
	if summary, err := second.ProcessDue(context.Background(), dueTime); err != nil || summary.Completed != 1 {
		t.Fatalf("second ProcessDue() = (%+v, %v), want duplicate completed", summary, err)
	}
	listed, err := repository.List(context.Background(), "room-0", 10)
	if err != nil || len(listed) != 1 {
		t.Fatalf("List() = (%d records, %v), want one idempotent record", len(listed), err)
	}
}

func TestNewProcessorUsesDefaultJobCapacity(t *testing.T) {
	store, _ := dueStore(t, 1)
	processor, err := NewProcessor(store, staticAnalyzer{result: analysisResult("primary.v1")}, rule.NewAnalyzer(), repositorymemory.New(), Config{Workers: 1})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	if processor.config.JobCapacity != 128 {
		t.Fatalf("job capacity = %d, want default 128", processor.config.JobCapacity)
	}
}

func TestNewProcessorKeepsPositiveJobCapacity(t *testing.T) {
	store, _ := dueStore(t, 1)
	processor, err := NewProcessor(store, staticAnalyzer{result: analysisResult("primary.v1")}, rule.NewAnalyzer(), repositorymemory.New(), Config{Workers: 1, JobCapacity: 7})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	if processor.config.JobCapacity != 7 {
		t.Fatalf("job capacity = %d, want 7", processor.config.JobCapacity)
	}
}

func TestNewProcessorRejectsNegativeJobCapacity(t *testing.T) {
	store, _ := dueStore(t, 1)
	_, err := NewProcessor(store, staticAnalyzer{result: analysisResult("primary.v1")}, rule.NewAnalyzer(), repositorymemory.New(), Config{Workers: 1, JobCapacity: -1})
	if err == nil {
		t.Fatal("NewProcessor() error = nil, want negative job capacity rejected")
	}
}

func TestProcessDueUsesTwoWorkersForMultipleWindows(t *testing.T) {
	store, _ := dueStore(t, 8)
	repository := repositorymemory.New()
	analyzer := &blockingAnalyzer{result: analysisResult("primary.v1"), started: make(chan struct{}, 2), release: make(chan struct{})}
	processor := mustProcessor(t, store, analyzer, rule.NewAnalyzer(), repository, 2)

	done := make(chan struct{})
	var summary Summary
	var processErr error
	go func() {
		summary, processErr = processor.ProcessDue(context.Background(), dueTime)
		close(done)
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-analyzer.started:
		case <-time.After(time.Second):
			t.Fatal("two workers did not start analyzing concurrently")
		}
	}
	close(analyzer.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ProcessDue() did not finish")
	}
	if processErr != nil || summary.Completed != 8 || analyzer.maxActive > 2 {
		t.Fatalf("ProcessDue() = (%+v, %v), max active = %d; want eight completed with two workers", summary, processErr, analyzer.maxActive)
	}
}

var dueTime = time.Date(2026, 8, 4, 13, 0, 0, 0, time.UTC)

func dueStore(t *testing.T, count int) (*windowmemory.Store, domain.WindowRef) {
	t.Helper()
	store := windowmemory.New(windowmemory.Config{WindowSize: time.Minute, Lateness: time.Second, MaxEvents: 10})
	var first domain.WindowRef
	for i := 0; i < count; i++ {
		event := appEvent(fmt.Sprintf("event-%d", i), fmt.Sprintf("room-%d", i), dueTime.Add(-2*time.Minute))
		result, err := store.Add(context.Background(), event, event.OccurredAt)
		if err != nil {
			t.Fatalf("Add() error = %v", err)
		}
		if i == 0 {
			first = result.Ref
		}
	}
	return store, first
}

func mustProcessor(t *testing.T, store ports.WindowStore, primary, fallback ports.InsightAnalyzer, repository ports.InsightRepository, workers int) *Processor {
	t.Helper()
	processor, err := NewProcessor(store, primary, fallback, repository, Config{Workers: workers})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	return processor
}

func appEvent(eventID, roomID string, occurredAt time.Time) domain.MessageEvent {
	return domain.MessageEvent{EventID: eventID, RoomID: roomID, UserID: "user-1", Content: "hello", OccurredAt: occurredAt}
}

func analysisResult(promptVersion string) domain.AnalysisResult {
	return domain.AnalysisResult{
		Rules:    domain.RuleStats{MessageCount: 1},
		Semantic: domain.SemanticInsight{Sentiment: domain.Sentiment{Label: "neutral"}},
		Model:    domain.ModelMeta{Provider: "test", Model: "test", PromptVersion: promptVersion},
	}
}

type staticAnalyzer struct {
	result domain.AnalysisResult
	err    error
}

func (a staticAnalyzer) Analyze(context.Context, domain.InsightWindow) (domain.AnalysisResult, error) {
	return a.result, a.err
}

type failingRepository struct{}

func (failingRepository) Save(context.Context, domain.RoomInsight) (bool, error) {
	return false, errors.New("repository unavailable")
}

func (failingRepository) Latest(context.Context, string) (domain.RoomInsight, bool, error) {
	return domain.RoomInsight{}, false, nil
}

func (failingRepository) List(context.Context, string, int) ([]domain.RoomInsight, error) {
	return nil, nil
}

type recordingStore struct {
	event domain.MessageEvent
	now   time.Time
}

type releaseErrorStore struct {
	*windowmemory.Store
	err error
}

func (s *releaseErrorStore) Release(context.Context, domain.WindowRef, time.Time) error {
	return s.err
}

func (s *recordingStore) Add(_ context.Context, event domain.MessageEvent, now time.Time) (ports.AddResult, error) {
	s.event = event
	s.now = now
	return ports.AddResult{}, nil
}

func (*recordingStore) ClaimDue(context.Context, time.Time, int) ([]domain.WindowRef, error) {
	return nil, nil
}
func (*recordingStore) Load(context.Context, domain.WindowRef) (domain.InsightWindow, error) {
	return domain.InsightWindow{}, nil
}
func (*recordingStore) Complete(context.Context, domain.WindowRef) error           { return nil }
func (*recordingStore) Release(context.Context, domain.WindowRef, time.Time) error { return nil }

type blockingAnalyzer struct {
	result    domain.AnalysisResult
	started   chan struct{}
	release   chan struct{}
	mu        sync.Mutex
	active    int
	maxActive int
}

func (a *blockingAnalyzer) Analyze(ctx context.Context, _ domain.InsightWindow) (domain.AnalysisResult, error) {
	a.mu.Lock()
	a.active++
	if a.active > a.maxActive {
		a.maxActive = a.active
	}
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.active--
		a.mu.Unlock()
	}()
	select {
	case a.started <- struct{}{}:
	default:
	}
	select {
	case <-a.release:
	case <-ctx.Done():
		return domain.AnalysisResult{}, ctx.Err()
	}
	return a.result, nil
}
