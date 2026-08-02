package ws

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v10/internal/auth"
	"github.com/charlesAcmen/livestream-danmaku/v10/internal/model"
	"github.com/charlesAcmen/livestream-danmaku/v10/internal/ratelimit"
	"github.com/charlesAcmen/livestream-danmaku/v10/internal/resilience"
	"golang.org/x/crypto/bcrypt"
)

type fakeRoomPublisher struct {
	calls int
	err   error
}

type fakeAuthUserStore struct {
	user *model.User
}

func (s fakeAuthUserStore) FindByUsername(context.Context, string) (*model.User, error) {
	return s.user, nil
}

func (p *fakeRoomPublisher) Publish(context.Context, string, []byte) error {
	p.calls++
	return p.err
}

func TestServeWSWithAuthRejectsMissingTokenBeforeUpgrade(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	authService, err := auth.New(fakeAuthUserStore{user: &model.User{
		ID:           "user-1",
		Username:     "alice",
		PasswordHash: string(hash),
		Role:         "viewer",
		Status:       model.UserStatusActive,
	}}, auth.Config{Secret: strings.Repeat("s", 32)})
	if err != nil {
		t.Fatalf("New auth service: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "/ws?room=room-1", nil)
	response := httptest.NewRecorder()
	ServeWSWithAuth(NewManagerWithConfig(ManagerConfig{WorkerCount: 1}), authService, true, response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

type fakePersister struct {
	messages []*model.Danmaku
}

func (p *fakePersister) Enqueue(message *model.Danmaku) bool {
	p.messages = append(p.messages, message)
	return true
}

func TestRedisFailureFallsBackLocallyAndOpenCircuitSkipsDependency(t *testing.T) {
	publisher := &fakeRoomPublisher{err: errors.New("redis unavailable")}
	breaker := resilience.NewCircuitBreaker(resilience.Config{
		FailureThreshold: 1,
		OpenTimeout:      time.Hour,
	})
	manager := NewManagerWithConfig(ManagerConfig{
		WorkerCount:      1,
		RedisWorkerCount: 1,
		RedisPublisher:   publisher,
		RedisBreaker:     breaker,
	})
	client := NewClient("user-1", "alice", "room-1", nil)
	manager.handleRegister(client)

	firstPayload := []byte(`{"type":101,"room_id":"room-1"}`)
	manager.processRedisPublish(redisPublishJob{roomID: "room-1", payload: firstPayload})
	firstJob := <-manager.broadcastJobs
	if len(firstJob.Clients) != 1 || firstJob.Clients[0] != client || !reflect.DeepEqual(firstJob.Payload, firstPayload) {
		t.Fatalf("unexpected local fallback job: %+v", firstJob)
	}
	manager.releaseSnapshot(firstJob.Clients)

	secondPayload := []byte(`{"type":101,"room_id":"room-1","data":{}}`)
	manager.processRedisPublish(redisPublishJob{roomID: "room-1", payload: secondPayload})
	secondJob := <-manager.broadcastJobs
	manager.releaseSnapshot(secondJob.Clients)

	if publisher.calls != 1 {
		t.Fatalf("Redis calls = %d, want 1 because second call is short-circuited", publisher.calls)
	}
	metrics := manager.Metrics()
	if metrics.RedisPublishErrors != 1 || metrics.RedisCircuitFallbacks != 1 || metrics.RedisDegradedBroadcasts != 2 {
		t.Fatalf("unexpected Redis fallback metrics: %+v", metrics)
	}
}

func TestSubmitDanmakuRejectsWhenIngressQueueIsFullBeforePersistence(t *testing.T) {
	persister := &fakePersister{}
	manager := NewManagerWithConfig(ManagerConfig{WorkerCount: 1, Persister: persister})
	for i := 0; i < cap(manager.Broadcast); i++ {
		manager.Broadcast <- &model.Packet{Type: model.TypeDanmaku, RoomID: "room-1"}
	}

	accepted := manager.SubmitDanmaku(&model.Danmaku{
		MessageID: "msg-1",
		RoomID:    "room-1",
		UserID:    "user-1",
	}, []byte(`{"message_id":"msg-1"}`))
	if accepted {
		t.Fatal("SubmitDanmaku() = true with a full ingress queue")
	}
	if len(persister.messages) != 0 {
		t.Fatalf("persisted %d messages after ingress rejection", len(persister.messages))
	}
	if manager.Metrics().IngressDropped != 1 {
		t.Fatalf("ingress dropped = %d, want 1", manager.Metrics().IngressDropped)
	}
}

func TestServeWSRejectsConnectionBeforeUpgradeWhenAdmissionIsFull(t *testing.T) {
	traffic := ratelimit.New(ratelimit.Config{MaxConnections: 1, MaxConnectionsPerIP: 1})
	manager := NewManagerWithConfig(ManagerConfig{WorkerCount: 1, Traffic: traffic})
	release, ok := manager.AcquireConnection("192.0.2.1")
	if !ok {
		t.Fatal("failed to reserve first connection")
	}
	defer release()

	request := httptest.NewRequest(http.MethodGet, "/ws?uid=user-2&room=room-1", nil)
	request.RemoteAddr = "192.0.2.1:54321"
	response := httptest.NewRecorder()
	ServeWS(manager, response, request)

	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusTooManyRequests)
	}
	if response.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", response.Header().Get("Retry-After"))
	}
}

func TestSafeSendDisconnectsClientAfterConsecutiveSlowDrops(t *testing.T) {
	manager := NewManagerWithConfig(ManagerConfig{WorkerCount: 1})
	client := NewClient("slow-user", "slow", "room-1", nil)
	for i := 0; i < cap(client.Send); i++ {
		client.Send <- []byte("fill")
	}

	for i := 0; i < SlowClientDisconnectThreshold; i++ {
		manager.safeSend(client, []byte("danmaku"))
	}

	select {
	case <-client.done:
	default:
		t.Fatal("slow client was not closed at the consecutive-drop threshold")
	}
	metrics := manager.Metrics()
	if metrics.DroppedMessages != SlowClientDisconnectThreshold || metrics.SlowClientDisconnects != 1 {
		t.Fatalf("unexpected slow-client metrics: %+v", metrics)
	}
}

func TestServeWSRejectsOversizedIdentityBeforeConnectionAdmission(t *testing.T) {
	traffic := ratelimit.New(ratelimit.Config{MaxConnections: 1, MaxConnectionsPerIP: 1})
	manager := NewManagerWithConfig(ManagerConfig{WorkerCount: 1, Traffic: traffic})
	request := httptest.NewRequest(http.MethodGet,
		"/ws?uid="+strings.Repeat("u", 65)+"&room=room-1", nil)
	request.RemoteAddr = "192.0.2.10:12345"
	response := httptest.NewRecorder()

	ServeWS(manager, response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if accepted := manager.Metrics().Traffic.AcceptedConnections; accepted != 0 {
		t.Fatalf("accepted connections = %d, want 0 for invalid identity", accepted)
	}
}

func TestStatsBroadcastGuardSkipsOverlappingTick(t *testing.T) {
	manager := NewManagerWithConfig(ManagerConfig{WorkerCount: 1})
	manager.statsRunning.Store(true)

	if manager.startStatsBroadcast() {
		t.Fatal("overlapping stats broadcast was started")
	}
	if skipped := manager.Metrics().StatsTicksSkipped; skipped != 1 {
		t.Fatalf("skipped stats ticks = %d, want 1", skipped)
	}

	manager.statsRunning.Store(false)
	if !manager.startStatsBroadcast() {
		t.Fatal("idle stats broadcaster did not start")
	}
	deadline := time.Now().Add(time.Second)
	for manager.statsRunning.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if manager.statsRunning.Load() {
		t.Fatal("stats broadcaster did not release its guard")
	}
}
