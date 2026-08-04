package ports

import (
	"context"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
)

type AddResult struct {
	Ref       domain.WindowRef
	Added     bool
	Duplicate bool
	Late      bool
	Completed bool
}

type EventSource interface {
	Run(context.Context, func(context.Context, domain.MessageEvent) error) error
}

type WindowStore interface {
	Add(context.Context, domain.MessageEvent, time.Time) (AddResult, error)
	ClaimDue(context.Context, time.Time, int) ([]domain.WindowRef, error)
	Load(context.Context, domain.WindowRef) (domain.InsightWindow, error)
	Complete(context.Context, domain.WindowRef) error
	Release(context.Context, domain.WindowRef, time.Time) error
}

type InsightAnalyzer interface {
	Analyze(context.Context, domain.InsightWindow) (domain.AnalysisResult, error)
}

type InsightRepository interface {
	Save(context.Context, domain.RoomInsight) (bool, error)
	Latest(context.Context, string) (domain.RoomInsight, bool, error)
	List(context.Context, string, int) ([]domain.RoomInsight, error)
}
