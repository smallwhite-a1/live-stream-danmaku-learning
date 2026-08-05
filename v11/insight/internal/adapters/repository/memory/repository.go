package memory

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/ports"
)

type Repository struct {
	mu       sync.RWMutex
	insights map[string]domain.RoomInsight
}

var _ ports.InsightRepository = (*Repository)(nil)

func New() *Repository {
	return &Repository{insights: make(map[string]domain.RoomInsight)}
}

func (r *Repository) Save(_ context.Context, insight domain.RoomInsight) (bool, error) {
	if err := insight.Validate(); err != nil {
		return false, err
	}
	key := insight.IdempotencyKey()
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.insights[key]; exists {
		return false, nil
	}
	r.insights[key] = cloneInsight(insight)
	return true, nil
}

func (r *Repository) Latest(_ context.Context, roomID string) (domain.RoomInsight, bool, error) {
	roomID = strings.TrimSpace(roomID)
	r.mu.RLock()
	defer r.mu.RUnlock()
	var latest domain.RoomInsight
	found := false
	for _, insight := range r.insights {
		if insight.RoomID != roomID || (found && !newer(insight, latest)) {
			continue
		}
		latest = insight
		found = true
	}
	if !found {
		return domain.RoomInsight{}, false, nil
	}
	return cloneInsight(latest), true, nil
}

func (r *Repository) List(_ context.Context, roomID string, limit int) ([]domain.RoomInsight, error) {
	if limit <= 0 {
		return []domain.RoomInsight{}, nil
	}
	roomID = strings.TrimSpace(roomID)
	r.mu.RLock()
	insights := make([]domain.RoomInsight, 0)
	for _, insight := range r.insights {
		if insight.RoomID == roomID {
			insights = append(insights, cloneInsight(insight))
		}
	}
	r.mu.RUnlock()
	sort.Slice(insights, func(i, j int) bool { return newer(insights[i], insights[j]) })
	if len(insights) > limit {
		insights = insights[:limit]
	}
	return insights, nil
}

func newer(left, right domain.RoomInsight) bool {
	if left.WindowEnd.Equal(right.WindowEnd) {
		if left.WindowStart.Equal(right.WindowStart) {
			return left.IdempotencyKey() > right.IdempotencyKey()
		}
		return left.WindowStart.After(right.WindowStart)
	}
	return left.WindowEnd.After(right.WindowEnd)
}

func cloneInsight(insight domain.RoomInsight) domain.RoomInsight {
	clone := insight
	clone.Semantic.Topics = make([]domain.Topic, len(insight.Semantic.Topics))
	for i, topic := range insight.Semantic.Topics {
		clone.Semantic.Topics[i] = topic
		clone.Semantic.Topics[i].EvidenceEventIDs = append([]string(nil), topic.EvidenceEventIDs...)
	}
	clone.Semantic.Sentiment.EvidenceEventIDs = append([]string(nil), insight.Semantic.Sentiment.EvidenceEventIDs...)
	clone.Semantic.Questions = make([]domain.Question, len(insight.Semantic.Questions))
	for i, question := range insight.Semantic.Questions {
		clone.Semantic.Questions[i] = question
		clone.Semantic.Questions[i].EvidenceEventIDs = append([]string(nil), question.EvidenceEventIDs...)
	}
	clone.Semantic.Alerts = make([]domain.Alert, len(insight.Semantic.Alerts))
	for i, alert := range insight.Semantic.Alerts {
		clone.Semantic.Alerts[i] = alert
		clone.Semantic.Alerts[i].EvidenceEventIDs = append([]string(nil), alert.EvidenceEventIDs...)
	}
	return clone
}
