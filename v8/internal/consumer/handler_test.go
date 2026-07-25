package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/v8/internal/model"
)

type fakeBatchRepository struct {
	mu       sync.Mutex
	events   *[]string
	inserted int64
	err      error
	calls    int
}

func (r *fakeBatchRepository) CreateIdempotent(context.Context, []*model.Danmaku) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.events != nil {
		*r.events = append(*r.events, "mysql")
	}
	return r.inserted, r.err
}

type fakeDeadLetterPublisher struct {
	events  *[]string
	records []model.DeadLetter
	err     error
}

func (p *fakeDeadLetterPublisher) Publish(_ context.Context, record model.DeadLetter) error {
	if p.events != nil {
		*p.events = append(*p.events, "dlq")
	}
	p.records = append(p.records, record)
	return p.err
}

type fakeSession struct {
	ctx    context.Context
	events *[]string
	marked []*sarama.ConsumerMessage
}

func (s *fakeSession) Claims() map[string][]int32 { return nil }
func (s *fakeSession) MemberID() string           { return "test-member" }
func (s *fakeSession) GenerationID() int32        { return 1 }
func (s *fakeSession) MarkOffset(string, int32, int64, string) {
}
func (s *fakeSession) Commit() {}
func (s *fakeSession) ResetOffset(string, int32, int64, string) {
}
func (s *fakeSession) MarkMessage(message *sarama.ConsumerMessage, _ string) {
	if s.events != nil {
		*s.events = append(*s.events, "mark")
	}
	s.marked = append(s.marked, message)
}
func (s *fakeSession) Context() context.Context { return s.ctx }

type fakeClaim struct {
	topic     string
	partition int32
	messages  chan *sarama.ConsumerMessage
}

func (c *fakeClaim) Topic() string                            { return c.topic }
func (c *fakeClaim) Partition() int32                         { return c.partition }
func (c *fakeClaim) InitialOffset() int64                     { return 0 }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

func testConfig() Config {
	return Config{
		BatchSize:     2,
		FlushInterval: time.Hour,
		FlushTimeout:  time.Second,
		MaxRetries:    1,
		RetryBackoff:  time.Millisecond,
	}
}

func TestFlushPendingWritesBeforeMarkingAndCountsDuplicates(t *testing.T) {
	events := make([]string, 0, 3)
	repository := &fakeBatchRepository{events: &events, inserted: 1}
	handler := NewHandler(repository, nil, testConfig())
	session := &fakeSession{ctx: context.Background(), events: &events}

	pending := []pendingMessage{
		{danmaku: &model.Danmaku{MessageID: "msg-1"}, message: &sarama.ConsumerMessage{Offset: 10}},
		{danmaku: &model.Danmaku{MessageID: "msg-2"}, message: &sarama.ConsumerMessage{Offset: 11}},
	}

	if err := handler.flushPending(context.Background(), session, pending); err != nil {
		t.Fatalf("flushPending() error = %v", err)
	}

	wantEvents := []string{"mysql", "mark", "mark"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}

	metrics := handler.Metrics()
	if metrics.Saved != 2 || metrics.Inserted != 1 || metrics.Duplicates != 1 || metrics.Batches != 1 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
}

func TestFlushPendingDoesNotMarkWhenMySQLFails(t *testing.T) {
	repository := &fakeBatchRepository{err: errors.New("mysql unavailable")}
	handler := NewHandler(repository, nil, testConfig())
	session := &fakeSession{ctx: context.Background()}
	pending := []pendingMessage{
		{danmaku: &model.Danmaku{MessageID: "msg-1"}, message: &sarama.ConsumerMessage{Offset: 10}},
	}

	if err := handler.flushPending(context.Background(), session, pending); err == nil {
		t.Fatal("flushPending() error = nil, want failure")
	}
	if len(session.marked) != 0 {
		t.Fatalf("marked %d messages after failed write", len(session.marked))
	}
	if handler.Metrics().FailedBatches != 1 {
		t.Fatalf("failed batches = %d, want 1", handler.Metrics().FailedBatches)
	}
}

func TestMalformedMessageGoesToDLQBeforeOffsetIsMarked(t *testing.T) {
	events := make([]string, 0, 2)
	dlq := &fakeDeadLetterPublisher{events: &events}
	handler := NewHandler(&fakeBatchRepository{}, dlq, testConfig())
	session := &fakeSession{ctx: context.Background(), events: &events}
	claim := &fakeClaim{
		topic:     "danmaku",
		partition: 3,
		messages:  make(chan *sarama.ConsumerMessage, 1),
	}
	claim.messages <- &sarama.ConsumerMessage{
		Topic:     "danmaku",
		Partition: 3,
		Offset:    42,
		Key:       []byte("room-1"),
		Value:     []byte("not-json"),
	}
	close(claim.messages)

	if err := handler.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("ConsumeClaim() error = %v", err)
	}

	wantEvents := []string{"dlq", "mark"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
	if len(dlq.records) != 1 {
		t.Fatalf("DLQ records = %d, want 1", len(dlq.records))
	}
	record := dlq.records[0]
	if record.SourceTopic != "danmaku" || record.SourcePartition != 3 || record.SourceOffset != 42 {
		t.Fatalf("unexpected DLQ record: %+v", record)
	}
	if handler.Metrics().DeadLetters != 1 {
		t.Fatalf("dead letters = %d, want 1", handler.Metrics().DeadLetters)
	}
}

func TestMalformedMessageIsNotMarkedWhenDLQFails(t *testing.T) {
	dlq := &fakeDeadLetterPublisher{err: errors.New("dlq unavailable")}
	handler := NewHandler(&fakeBatchRepository{}, dlq, testConfig())
	session := &fakeSession{ctx: context.Background()}
	claim := &fakeClaim{
		topic:    "danmaku",
		messages: make(chan *sarama.ConsumerMessage, 1),
	}
	claim.messages <- &sarama.ConsumerMessage{Topic: "danmaku", Offset: 1, Value: []byte("bad")}
	close(claim.messages)

	if err := handler.ConsumeClaim(session, claim); err == nil {
		t.Fatal("ConsumeClaim() error = nil, want DLQ failure")
	}
	if len(session.marked) != 0 {
		t.Fatalf("marked %d messages after DLQ failure", len(session.marked))
	}
	if handler.Metrics().DeadLetterFailures != 1 {
		t.Fatalf("dead letter failures = %d, want 1", handler.Metrics().DeadLetterFailures)
	}
}

func TestBufferedValidMessageIsSavedBeforeLaterPoisonMessageIsMarked(t *testing.T) {
	events := make([]string, 0, 4)
	repository := &fakeBatchRepository{events: &events, inserted: 1}
	dlq := &fakeDeadLetterPublisher{events: &events}
	config := testConfig()
	config.BatchSize = 10
	handler := NewHandler(repository, dlq, config)
	session := &fakeSession{ctx: context.Background(), events: &events}
	claim := &fakeClaim{
		topic:    "danmaku",
		messages: make(chan *sarama.ConsumerMessage, 2),
	}

	validDanmaku, err := json.Marshal(model.Danmaku{
		MessageID: "msg-1",
		RoomID:    "room-1",
		UserID:    "user-1",
		Content:   "hello",
		SendTime:  time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal danmaku: %v", err)
	}
	validPacket, err := json.Marshal(model.Packet{
		Type:   model.TypeDanmaku,
		RoomID: "room-1",
		Data:   validDanmaku,
	})
	if err != nil {
		t.Fatalf("marshal packet: %v", err)
	}

	claim.messages <- &sarama.ConsumerMessage{Topic: "danmaku", Offset: 1, Value: validPacket}
	claim.messages <- &sarama.ConsumerMessage{Topic: "danmaku", Offset: 2, Value: []byte("bad")}
	close(claim.messages)

	if err := handler.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("ConsumeClaim() error = %v", err)
	}

	wantEvents := []string{"mysql", "mark", "dlq", "mark"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
}
