package memory

import (
	"context"
	"testing"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
)

func TestSaveIsIdempotentAndReturnsClones(t *testing.T) {
	repository := New()
	insight := testInsight("room-1", time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC))

	created, err := repository.Save(context.Background(), insight)
	if err != nil || !created {
		t.Fatalf("first Save() = (%v, %v), want (true, nil)", created, err)
	}
	created, err = repository.Save(context.Background(), insight)
	if err != nil || created {
		t.Fatalf("duplicate Save() = (%v, %v), want (false, nil)", created, err)
	}

	listed, err := repository.List(context.Background(), "room-1", 10)
	if err != nil || len(listed) != 1 {
		t.Fatalf("List() = (%d records, %v), want one record", len(listed), err)
	}
	listed[0].Semantic.Topics[0].EvidenceEventIDs[0] = "mutated"
	listed[0].Semantic.Sentiment.EvidenceEventIDs[0] = "mutated"
	listed[0].Semantic.Questions[0].EvidenceEventIDs[0] = "mutated"
	listed[0].Semantic.Alerts[0].EvidenceEventIDs[0] = "mutated"

	again, ok, err := repository.Latest(context.Background(), "room-1")
	if err != nil || !ok {
		t.Fatalf("Latest() = (%+v, %v, %v), want record", again, ok, err)
	}
	if got := []string{
		again.Semantic.Topics[0].EvidenceEventIDs[0],
		again.Semantic.Sentiment.EvidenceEventIDs[0],
		again.Semantic.Questions[0].EvidenceEventIDs[0],
		again.Semantic.Alerts[0].EvidenceEventIDs[0],
	}; got[0] == "mutated" || got[1] == "mutated" || got[2] == "mutated" || got[3] == "mutated" {
		t.Fatalf("returned insight mutated stored evidence: %v", got)
	}
}

func TestLatestAndListReturnNewestWindowsFirst(t *testing.T) {
	repository := New()
	older := testInsight("room-1", time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC))
	newer := testInsight("room-1", older.WindowStart.Add(time.Minute))
	otherRoom := testInsight("room-2", newer.WindowStart.Add(time.Minute))
	for _, insight := range []domain.RoomInsight{older, newer, otherRoom} {
		if _, err := repository.Save(context.Background(), insight); err != nil {
			t.Fatalf("Save() error = %v", err)
		}
	}

	latest, ok, err := repository.Latest(context.Background(), "room-1")
	if err != nil || !ok || !latest.WindowStart.Equal(newer.WindowStart) {
		t.Fatalf("Latest() = (%+v, %v, %v), want newest room-1 window", latest, ok, err)
	}
	listed, err := repository.List(context.Background(), "room-1", 1)
	if err != nil || len(listed) != 1 || !listed[0].WindowStart.Equal(newer.WindowStart) {
		t.Fatalf("List() = (%+v, %v), want newest room-1 window", listed, err)
	}
}

func testInsight(roomID string, start time.Time) domain.RoomInsight {
	return domain.RoomInsight{
		RoomID: roomID, WindowStart: start, WindowEnd: start.Add(time.Minute), Status: domain.InsightStatusNormal,
		Rules: domain.RuleStats{MessageCount: 1},
		Semantic: domain.SemanticInsight{
			Summary: "summary", Topics: []domain.Topic{{Name: "topic", Confidence: 0.5, EvidenceEventIDs: []string{"event-1"}}},
			Sentiment: domain.Sentiment{Label: "neutral", Confidence: 0.5, EvidenceEventIDs: []string{"event-1"}},
			Questions: []domain.Question{{Text: "question", EvidenceEventIDs: []string{"event-1"}}},
			Alerts:    []domain.Alert{{Type: "question", Severity: "low", Description: "description", EvidenceEventIDs: []string{"event-1"}}},
		},
		Model: domain.ModelMeta{PromptVersion: "insight.v1"}, GeneratedAt: start.Add(time.Minute),
	}
}
