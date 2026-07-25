package store

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v4/internal/model"
	"github.com/charlesAcmen/livestream-danmaku/v4/internal/repo"
)

const (
	DefaultQueueSize     = 4096 // 最多存储 4096 条弹幕，超过则丢弃
	DefaultBatchSize     = 100  // 每次批量写入 MySQL 的弹幕数量
	DefaultFlushInterval = time.Second
	DefaultMaxRetries    = 3
	DefaultRetryBackoff  = 200 * time.Millisecond
)

type WriterMetrics struct {
	QueueLen      int    `json:"queue_len"`
	QueueCap      int    `json:"queue_cap"`
	Enqueued      uint64 `json:"enqueued"`
	Dropped       uint64 `json:"dropped"`
	Saved         uint64 `json:"saved"`
	Flushes       uint64 `json:"flushes"`
	FailedFlushes uint64 `json:"failed_flushes"`
}

type DBWriter struct {
	repo          *repo.MessageRepo
	input         chan *model.Danmaku // 弹幕写入队列
	batchSize     int
	flushInterval time.Duration
	maxRetries    int
	retryBackoff  time.Duration

	wg sync.WaitGroup

	enqueued      atomic.Uint64
	dropped       atomic.Uint64
	saved         atomic.Uint64
	flushes       atomic.Uint64
	failedFlushes atomic.Uint64
}

type WriterConfig struct {
	QueueSize     int
	BatchSize     int
	FlushInterval time.Duration
	MaxRetries    int
	RetryBackoff  time.Duration
}

func NewDBWriter(repo *repo.MessageRepo, config WriterConfig) *DBWriter {
	if config.QueueSize <= 0 {
		config.QueueSize = DefaultQueueSize
	}
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

	return &DBWriter{
		repo:          repo,
		input:         make(chan *model.Danmaku, config.QueueSize),
		batchSize:     config.BatchSize,
		flushInterval: config.FlushInterval,
		maxRetries:    config.MaxRetries,
		retryBackoff:  config.RetryBackoff,
	}
}

func (w *DBWriter) Start(ctx context.Context) {
	w.wg.Add(1)
	go w.loop(ctx)
}

func (w *DBWriter) Wait() {
	w.wg.Wait()
}

// Enqueue is intentionally non-blocking.
//
// If MySQL is slow and the persistence queue is full, V4 drops persistence for
// this danmaku instead of blocking the realtime WebSocket path.
func (w *DBWriter) Enqueue(message *model.Danmaku) bool {
	select {
	case w.input <- message:
		w.enqueued.Add(1)
		return true
	default:
		w.dropped.Add(1)
		return false
	}
}

func (w *DBWriter) Metrics() WriterMetrics {
	return WriterMetrics{
		QueueLen:      len(w.input),
		QueueCap:      cap(w.input),
		Enqueued:      w.enqueued.Load(),
		Dropped:       w.dropped.Load(),
		Saved:         w.saved.Load(),
		Flushes:       w.flushes.Load(),
		FailedFlushes: w.failedFlushes.Load(),
	}
}

func (w *DBWriter) loop(ctx context.Context) {
	defer w.wg.Done()

	buffer := make([]*model.Danmaku, 0, w.batchSize)
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	log.Printf("[db-writer] started queue=%d batch=%d flush=%s", cap(w.input), w.batchSize, w.flushInterval)

	for {
		select {
		case message := <-w.input:
			buffer = append(buffer, message)
			if len(buffer) >= w.batchSize {
				w.flush(ctx, buffer) // 写入 MySQL
				clearDanmakuBatch(buffer)
				buffer = buffer[:0]
			}
		case <-ticker.C: // 定时写入 MySQL
			if len(buffer) > 0 {
				w.flush(ctx, buffer)
				clearDanmakuBatch(buffer)
				buffer = buffer[:0]
			}
		case <-ctx.Done():
			for {
				select {
				case message := <-w.input:
					buffer = append(buffer, message)
					if len(buffer) >= w.batchSize {
						w.flush(context.Background(), buffer)
						clearDanmakuBatch(buffer)
						buffer = buffer[:0]
					}
				default:
					if len(buffer) > 0 {
						w.flush(context.Background(), buffer)
						clearDanmakuBatch(buffer)
					}
					log.Printf("[db-writer] stopped")
					return
				}
			}
		}
	}
}

func (w *DBWriter) flush(ctx context.Context, batch []*model.Danmaku) {
	if len(batch) == 0 {
		return
	}

	for attempt := 1; attempt <= w.maxRetries; attempt++ {
		err := w.repo.CreateInBatches(ctx, batch) // 实际写入 MySQL
		if err == nil {
			w.saved.Add(uint64(len(batch)))
			w.flushes.Add(1)
			return
		}

		log.Printf("[db-writer] flush failed attempt=%d count=%d err=%v", attempt, len(batch), err)

		select {
		case <-ctx.Done():
			w.failedFlushes.Add(1)
			return
		case <-time.After(w.retryBackoff):
		}
	}

	w.failedFlushes.Add(1)
	log.Printf("[db-writer] drop batch after retries count=%d", len(batch))
}

func clearDanmakuBatch(batch []*model.Danmaku) {
	for i := range batch {
		batch[i] = nil
	}
}
