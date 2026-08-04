package eino

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/ports"
)

func TestAnalyzerReturnsValidatedFakeResponseAndMetadata(t *testing.T) {
	analyzer, err := NewAnalyzer(&FakeModel{
		Provider:     "fake",
		Model:        "fake-v1",
		InputTokens:  21,
		OutputTokens: 13,
		Latency:      25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewAnalyzer() error = %v", err)
	}

	result, err := analyzer.Analyze(context.Background(), testWindow())
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if result.Rules.MessageCount != 2 {
		t.Fatalf("Analyze().Rules.MessageCount = %d, want 2", result.Rules.MessageCount)
	}
	if result.Semantic.Sentiment.Label != "neutral" || len(result.Semantic.Sentiment.EvidenceEventIDs) != 1 || result.Semantic.Sentiment.EvidenceEventIDs[0] != "event-1" {
		t.Fatalf("Analyze().Semantic.Sentiment = %+v, want neutral sentiment citing event-1", result.Semantic.Sentiment)
	}
	wantModel := domain.ModelMeta{
		Provider: "fake", Model: "fake-v1", PromptVersion: "insight.v1",
		InputTokens: 21, OutputTokens: 13, LatencyMillis: 25,
	}
	if result.Model != wantModel {
		t.Fatalf("Analyze().Model = %+v, want %+v", result.Model, wantModel)
	}
}

func TestAnalyzerRejectsUnknownEvidenceEventID(t *testing.T) {
	analyzer, err := NewAnalyzer(&FakeModel{Response: semanticJSON("unknown-event")})
	if err != nil {
		t.Fatalf("NewAnalyzer() error = %v", err)
	}

	_, err = analyzer.Analyze(context.Background(), testWindow())
	if err == nil {
		t.Fatal("Analyze() error = nil, want unknown evidence error")
	}
}

func TestAnalyzerRejectsInvalidJSON(t *testing.T) {
	analyzer, err := NewAnalyzer(&FakeModel{InvalidJSON: true})
	if err != nil {
		t.Fatalf("NewAnalyzer() error = %v", err)
	}

	_, err = analyzer.Analyze(context.Background(), testWindow())
	if err == nil {
		t.Fatal("Analyze() error = nil, want JSON validation error")
	}
}

func TestAnalyzerPropagatesCancellation(t *testing.T) {
	analyzer, err := NewAnalyzer(&FakeModel{Delay: time.Hour})
	if err != nil {
		t.Fatalf("NewAnalyzer() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = analyzer.Analyze(ctx, testWindow())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Analyze() error = %v, want context.Canceled", err)
	}
}

func TestFakeModelReturnsValidJSONWithCurrentEventID(t *testing.T) {
	window := testWindow()
	window.Events = window.Events[:1]
	window.Events[0].EventID = `event-"2`
	request := buildCompletionRequest(window)

	response, err := (&FakeModel{}).Complete(context.Background(), request)
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if _, err := parseAndValidate(response.Content, window); err != nil {
		t.Fatalf("fake response validation error = %v", err)
	}
}

func TestBuildCompletionRequestSortsEventsAndBoundsPrompts(t *testing.T) {
	start := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	window := domain.InsightWindow{Events: []domain.MessageEvent{
		{EventID: "event-c", Username: "c", Content: strings.Repeat("z", 9000), OccurredAt: start.Add(2 * time.Second)},
		{EventID: "event-b", Username: "b", Content: "second", OccurredAt: start.Add(time.Second)},
		{EventID: "event-a", Username: "a", Content: "first", OccurredAt: start},
	}}

	request := buildCompletionRequest(window)
	if utf8.RuneCountInString(request.SystemPrompt) > maxPromptRunes || utf8.RuneCountInString(request.UserPrompt) > maxPromptRunes {
		t.Fatalf("prompt exceeds %d runes", maxPromptRunes)
	}
	if strings.Index(request.UserPrompt, "[event-a]") > strings.Index(request.UserPrompt, "[event-b]") {
		t.Fatalf("event order = %q, want event-a before event-b", request.UserPrompt)
	}
}

func TestTruncateRunesReturnsEmptyStringAtZeroLimit(t *testing.T) {
	if got := truncateRunes("hello", 0); got != "" {
		t.Fatalf("truncateRunes() = %q, want empty string", got)
	}
}

func testWindow() domain.InsightWindow {
	start := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	return domain.InsightWindow{Events: []domain.MessageEvent{
		{EventID: "event-2", UserID: "user-2", Username: "beta", Content: "What is next?", OccurredAt: start.Add(time.Second)},
		{EventID: "event-1", UserID: "user-1", Username: "alpha", Content: "hello", OccurredAt: start},
	}}
}

func semanticJSON(evidenceID string) string {
	return `{"summary":"summary","topics":[{"name":"topic","confidence":0.5,"evidence_event_ids":["` + evidenceID + `"]}],"sentiment":{"label":"neutral","confidence":0.5,"evidence_event_ids":["` + evidenceID + `"]},"questions":[],"alerts":[]}`
}

var _ ports.InsightAnalyzer = (*Analyzer)(nil)
