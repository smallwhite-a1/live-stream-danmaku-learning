package rule

import (
	"context"
	"testing"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/ports"
)

func TestAnalyzerComputesDeterministicRuleStats(t *testing.T) {
	start := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	window := domain.InsightWindow{Events: []domain.MessageEvent{
		{EventID: "event-4", UserID: "user-4", Content: "  什么时候开始？ ", OccurredAt: start.Add(2 * time.Second)},
		{EventID: "event-2", UserID: "user-2", Content: "画面   卡顿", OccurredAt: start},
		{EventID: "event-5", UserID: "user-1", Content: "正常消息", OccurredAt: start.Add(2 * time.Second)},
		{EventID: "event-1", UserID: "user-1", Content: "画面 卡顿", OccurredAt: start},
		{EventID: "event-6", UserID: "user-5", Content: "有优惠吗", OccurredAt: start.Add(3 * time.Second)},
		{EventID: "event-3", UserID: "user-3", Content: "画面 卡顿", OccurredAt: start},
	}}

	result, err := NewAnalyzer().Analyze(context.Background(), window)
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}

	want := domain.RuleStats{
		MessageCount:          6,
		UniqueUsers:           5,
		QuestionCount:         2,
		RepeatedMessageRatio:  0.5,
		PeakMessagesPerSecond: 3,
		TopRepeatedText:       "画面 卡顿",
		TopRepeatedCount:      3,
	}
	if result.Rules != want {
		t.Fatalf("Analyze().Rules = %+v, want %+v", result.Rules, want)
	}
}

func TestAnalyzerBreaksRepeatedTextTiesLexicographically(t *testing.T) {
	start := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	window := domain.InsightWindow{Events: []domain.MessageEvent{
		{EventID: "event-1", UserID: "user-1", Content: "zeta", OccurredAt: start},
		{EventID: "event-2", UserID: "user-2", Content: "alpha", OccurredAt: start},
		{EventID: "event-3", UserID: "user-3", Content: "zeta", OccurredAt: start},
		{EventID: "event-4", UserID: "user-4", Content: "alpha", OccurredAt: start},
	}}

	result, err := NewAnalyzer().Analyze(context.Background(), window)
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if result.Rules.TopRepeatedText != "alpha" || result.Rules.TopRepeatedCount != 2 {
		t.Fatalf("top repeated = %q (%d), want alpha (2)", result.Rules.TopRepeatedText, result.Rules.TopRepeatedCount)
	}
}

var _ ports.InsightAnalyzer = NewAnalyzer()
