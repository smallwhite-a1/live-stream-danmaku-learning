package app

import (
	"context"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/ports"
)

type Ingestor struct {
	store ports.WindowStore
	now   func() time.Time
}

func NewIngestor(store ports.WindowStore, now func() time.Time) *Ingestor {
	return &Ingestor{store: store, now: now}
}

func (i *Ingestor) Handle(ctx context.Context, event domain.MessageEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	_, err := i.store.Add(ctx, event, i.now())
	return err
}
