package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/v10/internal/model"
)

const (
	DefaultBatchSize     = 500
	DefaultFlushInterval = 200 * time.Millisecond
	DefaultFlushTimeout  = 5 * time.Second
	DefaultMaxRetries    = 3
	DefaultRetryBackoff  = 300 * time.Millisecond
	DefaultRecoveryMin   = time.Second
	DefaultRecoveryMax   = 30 * time.Second
)

var ErrUnsupportedPacket = errors.New("unsupported packet")

type BatchRepository interface {
	CreateIdempotent(ctx context.Context, messages []*model.Danmaku) (int64, error)
}

type DeadLetterPublisher interface {
	Publish(ctx context.Context, record model.DeadLetter) error
}

type Metrics struct {
	Parsed             uint64 `json:"parsed"`
	Saved              uint64 `json:"saved"`
	Inserted           uint64 `json:"inserted"`
	Duplicates         uint64 `json:"duplicates"`
	Batches            uint64 `json:"batches"`
	Skipped            uint64 `json:"skipped"`
	DeadLetters        uint64 `json:"dead_letters"`
	DeadLetterFailures uint64 `json:"dead_letter_failures"`
	RetryAttempts      uint64 `json:"retry_attempts"`
	FailedBatches      uint64 `json:"failed_batches"`
	LastBatchMillis    int64  `json:"last_batch_millis"`
	PausedPartitions   int64  `json:"paused_partitions"`
	PauseEvents        uint64 `json:"pause_events"`
	Recoveries         uint64 `json:"recoveries"`
	RecoveryWaitMillis uint64 `json:"recovery_wait_millis"`
}

type Handler struct {
	repo          BatchRepository
	deadLetters   DeadLetterPublisher
	batchSize     int
	flushInterval time.Duration
	flushTimeout  time.Duration
	maxRetries    int
	retryBackoff  time.Duration
	recoveryMin   time.Duration
	recoveryMax   time.Duration

	parsed             atomic.Uint64
	saved              atomic.Uint64
	inserted           atomic.Uint64
	duplicates         atomic.Uint64
	batches            atomic.Uint64
	skipped            atomic.Uint64
	deadLetterCount    atomic.Uint64
	deadLetterFailures atomic.Uint64
	retryAttempts      atomic.Uint64
	failedBatches      atomic.Uint64
	lastBatchMillis    atomic.Int64
	pausedPartitions   atomic.Int64
	pauseEvents        atomic.Uint64
	recoveries         atomic.Uint64
	recoveryWaitMillis atomic.Uint64
}

type Config struct {
	BatchSize          int
	FlushInterval      time.Duration
	FlushTimeout       time.Duration
	MaxRetries         int
	RetryBackoff       time.Duration
	RecoveryBackoffMin time.Duration
	RecoveryBackoffMax time.Duration
}

type pendingMessage struct {
	danmaku *model.Danmaku
	message *sarama.ConsumerMessage
}

func NewHandler(repo BatchRepository, deadLetters DeadLetterPublisher, config Config) *Handler {
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultBatchSize
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = DefaultFlushInterval
	}
	if config.FlushTimeout <= 0 {
		config.FlushTimeout = DefaultFlushTimeout
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = DefaultMaxRetries
	}
	if config.RetryBackoff <= 0 {
		config.RetryBackoff = DefaultRetryBackoff
	}
	if config.RecoveryBackoffMin <= 0 {
		config.RecoveryBackoffMin = DefaultRecoveryMin
	}
	if config.RecoveryBackoffMax <= 0 {
		config.RecoveryBackoffMax = DefaultRecoveryMax
	}
	if config.RecoveryBackoffMax < config.RecoveryBackoffMin {
		config.RecoveryBackoffMax = config.RecoveryBackoffMin
	}

	return &Handler{
		repo:          repo,
		deadLetters:   deadLetters,
		batchSize:     config.BatchSize,
		flushInterval: config.FlushInterval,
		flushTimeout:  config.FlushTimeout,
		maxRetries:    config.MaxRetries,
		retryBackoff:  config.RetryBackoff,
		recoveryMin:   config.RecoveryBackoffMin,
		recoveryMax:   config.RecoveryBackoffMax,
	}
}

func (h *Handler) Setup(session sarama.ConsumerGroupSession) error {
	log.Printf("[consumer] session started member=%s generation=%d", session.MemberID(), session.GenerationID())
	return nil
}

func (h *Handler) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Printf("[consumer] session ended member=%s generation=%d", session.MemberID(), session.GenerationID())
	return nil
}

// ConsumeClaim owns one partition-local batch for exactly one consumer-group
// session. No queue or worker survives a rebalance, which makes its lifecycle
// match Sarama's claim lifecycle.
func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ticker := time.NewTicker(h.flushInterval)
	defer ticker.Stop()

	pending := make([]pendingMessage, 0, h.batchSize)
	defer func() { clearPending(pending) }()

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return h.flushBeforeClaimEnds(session, pending)
			}

			danmaku, err := h.parseMessage(msg)
			// Kafka offsets are contiguous. Before marking a later poison or
			// unsupported record, persist all earlier valid records in this claim.
			if err != nil && len(pending) > 0 {
				if flushErr := h.flushWithRecovery(session.Context(), session, pending); flushErr != nil {
					return flushErr
				}
				clearPending(pending)
				pending = pending[:0]
			}
			switch {
			case errors.Is(err, ErrUnsupportedPacket):
				h.skipped.Add(1)
				session.MarkMessage(msg, "unsupported packet")
				continue
			case err != nil:
				if dlqErr := h.publishDeadLetter(session.Context(), msg, err); dlqErr != nil {
					return dlqErr
				}
				session.MarkMessage(msg, "stored in DLQ")
				continue
			}

			pending = append(pending, pendingMessage{danmaku: danmaku, message: msg})
			h.parsed.Add(1)

			if len(pending) >= h.batchSize {
				if err := h.flushWithRecovery(session.Context(), session, pending); err != nil {
					return err
				}
				clearPending(pending)
				pending = pending[:0]
			}

		case <-ticker.C:
			if len(pending) == 0 {
				continue
			}
			if err := h.flushWithRecovery(session.Context(), session, pending); err != nil {
				return err
			}
			clearPending(pending)
			pending = pending[:0]

		case <-session.Context().Done():
			return h.flushBeforeClaimEnds(session, pending)
		}
	}
}

func (h *Handler) Metrics() Metrics {
	return Metrics{
		Parsed:             h.parsed.Load(),
		Saved:              h.saved.Load(),
		Inserted:           h.inserted.Load(),
		Duplicates:         h.duplicates.Load(),
		Batches:            h.batches.Load(),
		Skipped:            h.skipped.Load(),
		DeadLetters:        h.deadLetterCount.Load(),
		DeadLetterFailures: h.deadLetterFailures.Load(),
		RetryAttempts:      h.retryAttempts.Load(),
		FailedBatches:      h.failedBatches.Load(),
		LastBatchMillis:    h.lastBatchMillis.Load(),
		PausedPartitions:   h.pausedPartitions.Load(),
		PauseEvents:        h.pauseEvents.Load(),
		Recoveries:         h.recoveries.Load(),
		RecoveryWaitMillis: h.recoveryWaitMillis.Load(),
	}
}

// flushWithRecovery pauses only the current Kafka partition after MySQL
// retries are exhausted. It keeps offsets unchanged, so Kafka remains the
// durable backlog while other partitions can continue on their own claims.
func (h *Handler) flushWithRecovery(ctx context.Context, session sarama.ConsumerGroupSession, pending []pendingMessage) error {
	err := h.flushPending(ctx, session, pending)
	if err == nil {
		return nil
	}

	h.pauseEvents.Add(1)
	h.pausedPartitions.Add(1)
	started := time.Now()
	defer func() {
		h.pausedPartitions.Add(-1)
		h.recoveryWaitMillis.Add(uint64(time.Since(started).Milliseconds()))
	}()

	backoff := h.recoveryMin
	for {
		log.Printf("[consumer] partition paused for MySQL recovery backoff=%s", backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("mysql recovery interrupted: %w", ctx.Err())
		case <-timer.C:
		}

		if err = h.flushPending(ctx, session, pending); err == nil {
			h.recoveries.Add(1)
			log.Printf("[consumer] partition resumed after MySQL recovery")
			return nil
		}
		backoff = growBackoff(backoff, h.recoveryMax)
	}
}

func growBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum {
		return maximum
	}
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

func (h *Handler) parseMessage(msg *sarama.ConsumerMessage) (*model.Danmaku, error) {
	var packet model.Packet
	if err := json.Unmarshal(msg.Value, &packet); err != nil {
		return nil, fmt.Errorf("unmarshal packet: %w", err)
	}
	if packet.Type != model.TypeDanmaku {
		return nil, fmt.Errorf("%w: type=%d", ErrUnsupportedPacket, packet.Type)
	}

	var danmaku model.Danmaku
	if err := json.Unmarshal(packet.Data, &danmaku); err != nil {
		return nil, fmt.Errorf("unmarshal danmaku: %w", err)
	}
	if strings.TrimSpace(danmaku.MessageID) == "" {
		return nil, errors.New("missing message_id")
	}
	if strings.TrimSpace(danmaku.RoomID) == "" {
		return nil, errors.New("missing room_id")
	}
	if strings.TrimSpace(danmaku.UserID) == "" {
		return nil, errors.New("missing user_id")
	}
	if danmaku.SendTime.IsZero() {
		return nil, errors.New("missing send_time")
	}
	if err := model.ValidateDanmakuStorage(&danmaku); err != nil {
		return nil, fmt.Errorf("invalid danmaku storage fields: %w", err)
	}
	return &danmaku, nil
}

func (h *Handler) publishDeadLetter(ctx context.Context, msg *sarama.ConsumerMessage, reason error) error {
	if h.deadLetters == nil {
		h.deadLetterFailures.Add(1)
		return fmt.Errorf("invalid message without DLQ publisher topic=%s partition=%d offset=%d: %w",
			msg.Topic, msg.Partition, msg.Offset, reason)
	}

	record := model.DeadLetter{
		SourceTopic:     msg.Topic,
		SourcePartition: msg.Partition,
		SourceOffset:    msg.Offset,
		OriginalKey:     append([]byte(nil), msg.Key...),
		OriginalValue:   append([]byte(nil), msg.Value...),
		Reason:          reason.Error(),
		FailedAt:        time.Now().UTC(),
	}
	if err := h.deadLetters.Publish(ctx, record); err != nil {
		h.deadLetterFailures.Add(1)
		return fmt.Errorf("publish DLQ topic=%s partition=%d offset=%d: %w",
			msg.Topic, msg.Partition, msg.Offset, err)
	}

	h.deadLetterCount.Add(1)
	return nil
}

func (h *Handler) flushBeforeClaimEnds(session sarama.ConsumerGroupSession, pending []pendingMessage) error {
	if len(pending) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.flushTimeout)
	defer cancel()
	return h.flushPending(ctx, session, pending)
}

// flushPending establishes the important ordering invariant:
// MySQL idempotent commit succeeds first, then Kafka offsets are marked.
func (h *Handler) flushPending(ctx context.Context, session sarama.ConsumerGroupSession, pending []pendingMessage) error {
	if len(pending) == 0 {
		return nil
	}

	batch := uniqueBatch(pending)

	started := time.Now()
	for attempt := 1; attempt <= h.maxRetries; attempt++ {
		inserted, err := h.repo.CreateIdempotent(ctx, batch)
		if err == nil {
			if inserted < 0 || inserted > int64(len(batch)) {
				err = fmt.Errorf("invalid inserted row count %d for batch %d", inserted, len(batch))
			} else {
				for _, item := range pending {
					session.MarkMessage(item.message, "mysql committed")
				}

				duplicates := uint64(len(pending)) - uint64(inserted)
				h.saved.Add(uint64(len(pending)))
				h.inserted.Add(uint64(inserted))
				h.duplicates.Add(duplicates)
				h.batches.Add(1)
				h.lastBatchMillis.Store(time.Since(started).Milliseconds())
				return nil
			}
		}

		log.Printf("[consumer] save batch failed attempt=%d count=%d err=%v", attempt, len(batch), err)
		if attempt == h.maxRetries {
			break
		}
		h.retryAttempts.Add(1)

		backoff := h.retryBackoff * time.Duration(attempt)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			h.failedBatches.Add(1)
			return ctx.Err()
		case <-timer.C:
		}
	}

	h.failedBatches.Add(1)
	h.lastBatchMillis.Store(time.Since(started).Milliseconds())
	return fmt.Errorf("save batch failed after retries count=%d", len(batch))
}

func uniqueBatch(pending []pendingMessage) []*model.Danmaku {
	unique := make([]*model.Danmaku, 0, len(pending))
	seen := make(map[string]struct{}, len(pending))
	for _, item := range pending {
		if _, exists := seen[item.danmaku.MessageID]; exists {
			continue
		}
		seen[item.danmaku.MessageID] = struct{}{}
		unique = append(unique, item.danmaku)
	}
	return unique
}

func clearPending(pending []pendingMessage) {
	for i := range pending {
		pending[i] = pendingMessage{}
	}
}
