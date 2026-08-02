package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/v10/internal/model"
)

type fakeBatchRepository struct {
	mu       sync.Mutex
	events   *[]string
	batches  [][]string
	inserted int64
	err      error
	calls    int
}

type recoveringBatchRepository struct {
	mu         sync.Mutex
	events     *[]string
	failures   int
	calls      int
	inserted   int64
	failureErr error
}

func (r *recoveringBatchRepository) CreateIdempotent(context.Context, []*model.Danmaku) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls++
	if r.calls <= r.failures {
		if r.events != nil {
			*r.events = append(*r.events, "mysql-failed")
		}
		return 0, r.failureErr
	}
	if r.events != nil {
		*r.events = append(*r.events, "mysql-succeeded")
	}
	return r.inserted, nil
}

func (r *fakeBatchRepository) CreateIdempotent(_ context.Context, messages []*model.Danmaku) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	messageIDs := make([]string, 0, len(messages))
	for _, message := range messages {
		messageIDs = append(messageIDs, message.MessageID)
	}
	r.batches = append(r.batches, messageIDs)
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

func TestFlushPendingDeduplicatesMessageIDsWithinBatch(t *testing.T) {
	repository := &fakeBatchRepository{inserted: 2}
	handler := NewHandler(repository, nil, testConfig())
	session := &fakeSession{ctx: context.Background()}
	pending := []pendingMessage{
		{danmaku: &model.Danmaku{MessageID: "msg-1"}, message: &sarama.ConsumerMessage{Offset: 10}},
		{danmaku: &model.Danmaku{MessageID: "msg-1"}, message: &sarama.ConsumerMessage{Offset: 11}},
		{danmaku: &model.Danmaku{MessageID: "msg-2"}, message: &sarama.ConsumerMessage{Offset: 12}},
	}

	if err := handler.flushPending(context.Background(), session, pending); err != nil {
		t.Fatalf("flushPending() error = %v", err)
	}

	wantBatch := []string{"msg-1", "msg-2"}
	if !reflect.DeepEqual(repository.batches, [][]string{wantBatch}) {
		t.Fatalf("repository batches = %v, want %v", repository.batches, [][]string{wantBatch})
	}
	if len(session.marked) != len(pending) {
		t.Fatalf("marked messages = %d, want %d", len(session.marked), len(pending))
	}
	metrics := handler.Metrics()
	if metrics.Saved != 3 || metrics.Inserted != 2 || metrics.Duplicates != 1 {
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

func TestConsumeClaimPausesPartitionAndRecoversAfterMySQLFailure(t *testing.T) {
	events := make([]string, 0, 3)
	repository := &recoveringBatchRepository{
		events:     &events,
		failures:   1,
		inserted:   1,
		failureErr: errors.New("mysql unavailable"),
	}
	config := testConfig()
	config.BatchSize = 1
	config.MaxRetries = 1
	config.RecoveryBackoffMin = time.Millisecond
	config.RecoveryBackoffMax = 2 * time.Millisecond
	handler := NewHandler(repository, nil, config)
	session := &fakeSession{ctx: context.Background(), events: &events}
	claim := &fakeClaim{
		topic:    "danmaku",
		messages: make(chan *sarama.ConsumerMessage, 1),
	}

	danmaku, err := json.Marshal(model.Danmaku{
		MessageID: "msg-recovery",
		RoomID:    "room-1",
		UserID:    "user-1",
		Content:   "hello after recovery",
		SendTime:  time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal danmaku: %v", err)
	}
	packet, err := json.Marshal(model.Packet{
		Type:   model.TypeDanmaku,
		RoomID: "room-1",
		Data:   danmaku,
	})
	if err != nil {
		t.Fatalf("marshal packet: %v", err)
	}
	claim.messages <- &sarama.ConsumerMessage{Topic: "danmaku", Offset: 10, Value: packet}
	close(claim.messages)

	if err := handler.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("ConsumeClaim() error = %v, want recovery", err)
	}
	wantEvents := []string{"mysql-failed", "mysql-succeeded", "mark"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}

	metrics := handler.Metrics()
	if metrics.PauseEvents != 1 || metrics.Recoveries != 1 || metrics.PausedPartitions != 0 {
		t.Fatalf("unexpected recovery metrics: %+v", metrics)
	}
}

func TestOversizedDanmakuGoesToDLQWithoutCallingMySQL(t *testing.T) {
	events := make([]string, 0, 2)
	repository := &fakeBatchRepository{events: &events}
	dlq := &fakeDeadLetterPublisher{events: &events}
	handler := NewHandler(repository, dlq, testConfig())
	session := &fakeSession{ctx: context.Background(), events: &events}
	claim := &fakeClaim{
		topic:    "danmaku",
		messages: make(chan *sarama.ConsumerMessage, 1),
	}

	danmaku, err := json.Marshal(model.Danmaku{
		MessageID: "msg-too-long",
		RoomID:    "room-1",
		UserID:    "user-1",
		Username:  "alice",
		Content:   strings.Repeat("x", 501),
		SendTime:  time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal danmaku: %v", err)
	}
	packet, err := json.Marshal(model.Packet{Type: model.TypeDanmaku, RoomID: "room-1", Data: danmaku})
	if err != nil {
		t.Fatalf("marshal packet: %v", err)
	}
	claim.messages <- &sarama.ConsumerMessage{Topic: "danmaku", Offset: 12, Value: packet}
	close(claim.messages)

	if err := handler.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("ConsumeClaim() error = %v", err)
	}
	if repository.calls != 0 {
		t.Fatalf("MySQL calls = %d, want 0 for invalid business data", repository.calls)
	}
	wantEvents := []string{"dlq", "mark"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
}
