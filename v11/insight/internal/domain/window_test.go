package domain

import (
	"strings"
	"testing"
	"time"
)

func TestNewWindowRef(t *testing.T) {
	at := time.Date(2026, 8, 4, 12, 0, 59, 0, time.UTC)
	ref, err := NewWindowRef("room-1", at, time.Minute)
	if err != nil {
		t.Fatalf("NewWindowRef() error = %v", err)
	}
	wantStart := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	if !ref.Start.Equal(wantStart) || !ref.End.Equal(wantStart.Add(time.Minute)) {
		t.Fatalf("window = [%s, %s), want [%s, %s)", ref.Start, ref.End, wantStart, wantStart.Add(time.Minute))
	}
	if got, want := ref.Key(), "room-1:2026-08-04T12:00:00Z:2026-08-04T12:01:00Z"; got != want {
		t.Fatalf("Key() = %q, want %q", got, want)
	}
	if got := ref.Key(); got != ref.Key() {
		t.Fatalf("Key() is unstable: %q", got)
	}
}

func TestRoomInsightRejectsOversizedAlertType(t *testing.T) {
	start := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	insight := RoomInsight{
		RoomID: "room-1", WindowStart: start, WindowEnd: start.Add(time.Minute),
		Status: InsightStatusNormal,
		Semantic: SemanticInsight{
			Sentiment: Sentiment{Label: "neutral"},
			Alerts:    []Alert{{Type: strings.Repeat("a", 65), Severity: "low"}},
		},
		Model: ModelMeta{PromptVersion: "v1"}, GeneratedAt: start.Add(2 * time.Minute),
	}
	if err := insight.Validate(); err == nil {
		t.Fatal("Validate() error = nil for oversized alert type")
	}
}

func TestRoomInsightRejectsMalformedUTF8AlertType(t *testing.T) {
	start := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	insight := RoomInsight{
		RoomID: "room-1", WindowStart: start, WindowEnd: start.Add(time.Minute),
		Status: InsightStatusNormal,
		Semantic: SemanticInsight{
			Sentiment: Sentiment{Label: "neutral"},
			Alerts:    []Alert{{Type: string([]byte{0xff}), Severity: "low"}},
		},
		Model: ModelMeta{PromptVersion: "v1"}, GeneratedAt: start.Add(2 * time.Minute),
	}
	if err := insight.Validate(); err == nil {
		t.Fatal("Validate() error = nil for malformed UTF-8 alert type")
	}
}

func TestNewWindowRefRejectsInvalidInput(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name string
		room string
		size time.Duration
	}{
		{name: "blank room", room: " ", size: time.Minute},
		{name: "zero size", room: "room-1", size: 0},
		{name: "negative size", room: "room-1", size: -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewWindowRef(tc.room, now, tc.size); err == nil {
				t.Fatal("NewWindowRef() error = nil, want error")
			}
		})
	}
}

func TestRoomInsightValidateAndIdempotencyKey(t *testing.T) {
	start := time.Date(2026, 8, 4, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	insight := RoomInsight{
		RoomID: "room-1", WindowStart: start, WindowEnd: start.Add(time.Minute),
		Status: InsightStatusNormal, Semantic: SemanticInsight{Sentiment: Sentiment{Label: "neutral"}},
		Model: ModelMeta{PromptVersion: "v1"}, GeneratedAt: start.Add(2 * time.Minute),
	}
	if err := insight.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if got, want := insight.IdempotencyKey(), "room-1:2026-08-04T04:00:00Z:2026-08-04T04:01:00Z:v1"; got != want {
		t.Fatalf("IdempotencyKey() = %q, want %q", got, want)
	}
	if got := insight.IdempotencyKey(); got != insight.IdempotencyKey() {
		t.Fatalf("IdempotencyKey() is unstable: %q", got)
	}

	for _, tc := range []struct {
		name      string
		mutate    func(*RoomInsight)
		wantError bool
	}{
		{name: "normal status", mutate: func(i *RoomInsight) { i.Status = InsightStatusNormal }},
		{name: "degraded status", mutate: func(i *RoomInsight) { i.Status = InsightStatusDegraded }},
		{name: "unsupported status", mutate: func(i *RoomInsight) { i.Status = InsightStatus("unknown") }, wantError: true},
		{name: "blank status", mutate: func(i *RoomInsight) { i.Status = "" }, wantError: true},
		{name: "positive sentiment", mutate: func(i *RoomInsight) { i.Semantic.Sentiment.Label = "positive" }},
		{name: "neutral sentiment", mutate: func(i *RoomInsight) { i.Semantic.Sentiment.Label = "neutral" }},
		{name: "negative sentiment", mutate: func(i *RoomInsight) { i.Semantic.Sentiment.Label = "negative" }},
		{name: "mixed sentiment", mutate: func(i *RoomInsight) { i.Semantic.Sentiment.Label = "mixed" }},
		{name: "unsupported sentiment", mutate: func(i *RoomInsight) { i.Semantic.Sentiment.Label = "unknown" }, wantError: true},
		{name: "blank sentiment", mutate: func(i *RoomInsight) { i.Semantic.Sentiment.Label = "" }, wantError: true},
		{name: "low alert severity", mutate: func(i *RoomInsight) { i.Semantic.Alerts = []Alert{{Type: "risk", Severity: "low"}} }},
		{name: "medium alert severity", mutate: func(i *RoomInsight) { i.Semantic.Alerts = []Alert{{Type: "risk", Severity: "medium"}} }},
		{name: "high alert severity", mutate: func(i *RoomInsight) { i.Semantic.Alerts = []Alert{{Type: "risk", Severity: "high"}} }},
		{name: "unsupported alert severity", mutate: func(i *RoomInsight) { i.Semantic.Alerts = []Alert{{Type: "risk", Severity: "unknown"}} }, wantError: true},
		{name: "blank alert severity", mutate: func(i *RoomInsight) { i.Semantic.Alerts = []Alert{{Type: "risk", Severity: ""}} }, wantError: true},
		{name: "blank alert type", mutate: func(i *RoomInsight) { i.Semantic.Alerts = []Alert{{Type: " ", Severity: "low"}} }, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := insight
			if tc.mutate != nil {
				tc.mutate(&candidate)
			}
			if err := candidate.Validate(); (err != nil) != tc.wantError {
				t.Fatalf("Validate() error = %v, wantError %v", err, tc.wantError)
			}
		})
	}
}
