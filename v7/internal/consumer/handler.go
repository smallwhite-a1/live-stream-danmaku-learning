package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/v7/internal/model"
	"github.com/charlesAcmen/livestream-danmaku/v7/internal/repo"
)

const (
	DefaultBatchSize     = 100
	DefaultFlushInterval = time.Second
	DefaultMaxRetries    = 3
	DefaultRetryBackoff  = 300 * time.Millisecond
)

type Metrics struct {
	Parsed        uint64 `json:"parsed"`
	Saved         uint64 `json:"saved"`
	Batches       uint64 `json:"batches"`
	Skipped       uint64 `json:"skipped"`
	FailedBatches uint64 `json:"failed_batches"`
}

type Handler struct {
	repo          *repo.MessageRepo
	batchSize     int
	flushInterval time.Duration
	maxRetries    int
	retryBackoff  time.Duration

	parsed        atomic.Uint64
	saved         atomic.Uint64
	batches       atomic.Uint64
	skipped       atomic.Uint64
	failedBatches atomic.Uint64
}

type Config struct {
	BatchSize     int
	FlushInterval time.Duration
	MaxRetries    int
	RetryBackoff  time.Duration
}

type pendingMessage struct {
	danmaku *model.Danmaku
	message *sarama.ConsumerMessage
}

func NewHandler(repo *repo.MessageRepo, config Config) *Handler {
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultBatchSize
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = DefaultFlushInterval
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = DefaultMaxRetries
	}
	if config.RetryBackoff <= 0 {
		config.RetryBackoff = DefaultRetryBackoff
	}

	return &Handler{
		repo:          repo,
		batchSize:     config.BatchSize,
		flushInterval: config.FlushInterval,
		maxRetries:    config.MaxRetries,
		retryBackoff:  config.RetryBackoff,
	}
}

func (h *Handler) Setup(sarama.ConsumerGroupSession) error {
	log.Printf("[consumer] session started")
	return nil
}

func (h *Handler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Printf("[consumer] session ended")
	return nil
}

func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ticker := time.NewTicker(h.flushInterval)
	defer ticker.Stop()

	pending := make([]pendingMessage, 0, h.batchSize)

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return h.flushPending(context.Background(), session, pending)
			}

			danmaku, err := h.parseMessage(msg)
			if err != nil {
				h.skipped.Add(1)
				session.MarkMessage(msg, "")
				continue
			}

			pending = append(pending, pendingMessage{
				danmaku: danmaku,
				message: msg,
			})
			h.parsed.Add(1)

			if len(pending) >= h.batchSize {
				if err := h.flushPending(session.Context(), session, pending); err != nil {
					return err
				}
				clearPending(pending)
				pending = pending[:0]
			}

		case <-ticker.C:
			if len(pending) > 0 {
				if err := h.flushPending(session.Context(), session, pending); err != nil {
					return err
				}
				clearPending(pending)
				pending = pending[:0]
			}

		case <-session.Context().Done():
			if len(pending) > 0 {
				_ = h.flushPending(context.Background(), session, pending)
				clearPending(pending)
			}
			return nil
		}
	}
}

func (h *Handler) Metrics() Metrics {
	return Metrics{
		Parsed:        h.parsed.Load(),
		Saved:         h.saved.Load(),
		Batches:       h.batches.Load(),
		Skipped:       h.skipped.Load(),
		FailedBatches: h.failedBatches.Load(),
	}
}

func (h *Handler) parseMessage(msg *sarama.ConsumerMessage) (*model.Danmaku, error) {
	var packet model.Packet
	if err := json.Unmarshal(msg.Value, &packet); err != nil {
		return nil, fmt.Errorf("unmarshal packet: %w", err)
	}
	if packet.Type != model.TypeDanmaku {
		return nil, fmt.Errorf("skip packet type=%d", packet.Type)
	}

	var danmaku model.Danmaku
	if err := json.Unmarshal(packet.Data, &danmaku); err != nil {
		return nil, fmt.Errorf("unmarshal danmaku: %w", err)
	}
	return &danmaku, nil
}

func (h *Handler) flushPending(ctx context.Context, session sarama.ConsumerGroupSession, pending []pendingMessage) error {
	if len(pending) == 0 {
		return nil
	}

	batch := make([]*model.Danmaku, 0, len(pending))
	for _, item := range pending {
		batch = append(batch, item.danmaku)
	}

	for attempt := 1; attempt <= h.maxRetries; attempt++ {
		err := h.repo.CreateInBatches(ctx, batch)
		if err == nil {
			for _, item := range pending {
				session.MarkMessage(item.message, "")
			}
			h.saved.Add(uint64(len(batch)))
			h.batches.Add(1)
			return nil
		}

		log.Printf("[consumer] save batch failed attempt=%d count=%d err=%v", attempt, len(batch), err)

		select {
		case <-ctx.Done():
			h.failedBatches.Add(1)
			return ctx.Err()
		case <-time.After(h.retryBackoff):
		}
	}

	h.failedBatches.Add(1)
	return fmt.Errorf("save batch failed after retries count=%d", len(batch))
}

func clearPending(pending []pendingMessage) {
	for i := range pending {
		pending[i] = pendingMessage{}
	}
}
