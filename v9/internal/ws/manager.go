package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v9/internal/idgen"
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/infra"
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/model"
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/ratelimit"
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/resilience"
	"github.com/redis/go-redis/v9"
)

const (
	BroadcastBufferSize     = 512
	JobBufferSize           = 1024
	DefaultWorkerCount      = 16
	DefaultRedisWorkerCount = 4
	RedisPublishBufferSize  = 512
	SnapshotInitialCapacity = 256
	MaxSnapshotRetainCap    = 8192
	DefaultStatsInterval    = 3 * time.Second
)

type DanmakuPersister interface {
	Enqueue(message *model.Danmaku) bool
}

type RoomPublisher interface {
	Publish(ctx context.Context, roomID string, payload []byte) error
}

type roomPublisherFunc func(ctx context.Context, roomID string, payload []byte) error

func (f roomPublisherFunc) Publish(ctx context.Context, roomID string, payload []byte) error {
	return f(ctx, roomID, payload)
}

type ManagerConfig struct {
	WorkerCount      int
	RedisWorkerCount int
	Persister        DanmakuPersister
	RedisClient      *redis.Client
	RedisPublisher   RoomPublisher
	RedisBreaker     *resilience.CircuitBreaker
	Traffic          *ratelimit.Controller
}

type BroadcastJob struct {
	RoomID  string
	Clients []*Client
	Payload []byte
}

type redisPublishJob struct {
	roomID  string
	payload []byte
}

type Metrics struct {
	Rooms                   int                  `json:"rooms"`
	Clients                 int                  `json:"clients"`
	WorkerCount             int                  `json:"worker_count"`
	BroadcastQueueLen       int                  `json:"broadcast_queue_len"`
	BroadcastQueueCap       int                  `json:"broadcast_queue_cap"`
	IngressAccepted         uint64               `json:"ingress_accepted"`
	IngressDropped          uint64               `json:"ingress_dropped"`
	JobQueueLen             int                  `json:"job_queue_len"`
	JobQueueCap             int                  `json:"job_queue_cap"`
	LocalFanoutPackets      uint64               `json:"local_fanout_packets"`
	EnqueuedJobs            uint64               `json:"enqueued_jobs"`
	DroppedJobs             uint64               `json:"dropped_jobs"`
	DeliveredMessages       uint64               `json:"delivered_messages"`
	DroppedMessages         uint64               `json:"dropped_messages"`
	SlowClientDisconnects   uint64               `json:"slow_client_disconnects"`
	SnapshotPoolGets        uint64               `json:"snapshot_pool_gets"`
	SnapshotPoolPuts        uint64               `json:"snapshot_pool_puts"`
	SnapshotPoolNews        uint64               `json:"snapshot_pool_news"`
	SnapshotPoolDrops       uint64               `json:"snapshot_pool_drops"`
	PersistEnqueued         uint64               `json:"persist_enqueued"`
	PersistDropped          uint64               `json:"persist_dropped"`
	RedisEnabled            bool                 `json:"redis_enabled"`
	RedisPublished          uint64               `json:"redis_published"`
	RedisPublishErrors      uint64               `json:"redis_publish_errors"`
	RedisPublishQueued      uint64               `json:"redis_publish_queued"`
	RedisPublishQueueLen    int                  `json:"redis_publish_queue_len"`
	RedisPublishQueueCap    int                  `json:"redis_publish_queue_cap"`
	RedisWorkerCount        int                  `json:"redis_worker_count"`
	RedisQueueFallbacks     uint64               `json:"redis_queue_fallbacks"`
	RedisCircuitFallbacks   uint64               `json:"redis_circuit_fallbacks"`
	RedisDegradedBroadcasts uint64               `json:"redis_degraded_broadcasts"`
	RedisStatsErrors        uint64               `json:"redis_stats_errors"`
	RedisStatsFallbacks     uint64               `json:"redis_stats_fallbacks"`
	RedisReceived           uint64               `json:"redis_received"`
	RedisSubscriptions      uint64               `json:"redis_subscriptions"`
	RedisUnsubscriptions    uint64               `json:"redis_unsubscriptions"`
	StatsBroadcasts         uint64               `json:"stats_broadcasts"`
	StatsTicksSkipped       uint64               `json:"stats_ticks_skipped"`
	LikeEvents              uint64               `json:"like_events"`
	LikeDeltasFlushed       uint64               `json:"like_deltas_flushed"`
	OnlineReports           uint64               `json:"online_reports"`
	Goroutines              int                  `json:"goroutines"`
	AllocBytes              uint64               `json:"alloc_bytes"`
	TotalAllocBytes         uint64               `json:"total_alloc_bytes"`
	NumGC                   uint32               `json:"num_gc"`
	Traffic                 ratelimit.Metrics    `json:"traffic"`
	RedisCircuit            *resilience.Snapshot `json:"redis_circuit,omitempty"`
}

type Manager struct {
	ServerID string

	Register   chan *Client
	Unregister chan *Client
	Broadcast  chan *model.Packet

	broadcastJobs    chan *BroadcastJob
	workerCount      int
	persister        DanmakuPersister
	redisClient      *redis.Client
	redisPublisher   RoomPublisher
	redisBreaker     *resilience.CircuitBreaker
	redisPublishJobs chan redisPublishJob
	redisWorkerCount int
	traffic          *ratelimit.Controller
	messageIDs       *idgen.Generator

	mu        sync.RWMutex
	rooms     map[string]map[*Client]struct{}
	cancelSub map[string]context.CancelFunc

	// localLikes stores unflushed like deltas. localLikeTotals is only used when
	// Redis is disabled, so the no-middleware learning mode can still show stats.
	likesMu         sync.Mutex
	localLikes      map[string]uint64
	localLikeTotals map[string]uint64

	snapshotPool sync.Pool

	localFanoutPackets      atomic.Uint64
	ingressAccepted         atomic.Uint64
	ingressDropped          atomic.Uint64
	enqueuedJobs            atomic.Uint64
	droppedJobs             atomic.Uint64
	deliveredMessages       atomic.Uint64
	droppedMessages         atomic.Uint64
	slowClientDisconnects   atomic.Uint64
	snapshotPoolGets        atomic.Uint64
	snapshotPoolPuts        atomic.Uint64
	snapshotPoolNews        atomic.Uint64
	snapshotPoolDrops       atomic.Uint64
	persistEnqueued         atomic.Uint64
	persistDropped          atomic.Uint64
	redisPublished          atomic.Uint64
	redisPublishErrors      atomic.Uint64
	redisPublishQueued      atomic.Uint64
	redisQueueFallbacks     atomic.Uint64
	redisCircuitFallbacks   atomic.Uint64
	redisDegradedBroadcasts atomic.Uint64
	redisStatsErrors        atomic.Uint64
	redisStatsFallbacks     atomic.Uint64
	redisReceived           atomic.Uint64
	redisSubscriptions      atomic.Uint64
	redisUnsubscribes       atomic.Uint64
	statsBroadcasts         atomic.Uint64
	statsRunning            atomic.Bool
	statsTicksSkipped       atomic.Uint64
	likeEvents              atomic.Uint64
	likeDeltasFlushed       atomic.Uint64
	onlineReports           atomic.Uint64
}

func NewManager(workerCount int, persister DanmakuPersister, redisClient *redis.Client) *Manager {
	return NewManagerWithConfig(ManagerConfig{
		WorkerCount: workerCount,
		Persister:   persister,
		RedisClient: redisClient,
	})
}

func NewManagerWithConfig(config ManagerConfig) *Manager {
	if config.WorkerCount <= 0 {
		config.WorkerCount = DefaultWorkerCount
	}
	if config.RedisWorkerCount <= 0 {
		config.RedisWorkerCount = DefaultRedisWorkerCount
	}
	if config.RedisPublisher == nil && config.RedisClient != nil {
		config.RedisPublisher = roomPublisherFunc(func(ctx context.Context, roomID string, payload []byte) error {
			return infra.PublishToRoom(ctx, config.RedisClient, roomID, payload)
		})
	}
	if config.RedisPublisher != nil && config.RedisBreaker == nil {
		config.RedisBreaker = resilience.NewCircuitBreaker(resilience.DefaultConfig())
	}

	hostname, _ := os.Hostname()
	serverID := fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano())
	messageIDs, err := idgen.New()
	if err != nil {
		panic(fmt.Sprintf("initialize message id generator: %v", err))
	}

	m := &Manager{
		ServerID:         serverID,
		Register:         make(chan *Client),
		Unregister:       make(chan *Client),
		Broadcast:        make(chan *model.Packet, BroadcastBufferSize),
		broadcastJobs:    make(chan *BroadcastJob, JobBufferSize),
		workerCount:      config.WorkerCount,
		persister:        config.Persister,
		redisClient:      config.RedisClient,
		redisPublisher:   config.RedisPublisher,
		redisBreaker:     config.RedisBreaker,
		redisPublishJobs: make(chan redisPublishJob, RedisPublishBufferSize),
		redisWorkerCount: config.RedisWorkerCount,
		traffic:          config.Traffic,
		messageIDs:       messageIDs,
		rooms:            make(map[string]map[*Client]struct{}),
		cancelSub:        make(map[string]context.CancelFunc),
		localLikes:       make(map[string]uint64),
		localLikeTotals:  make(map[string]uint64),
	}
	m.snapshotPool.New = func() any {
		m.snapshotPoolNews.Add(1)
		return make([]*Client, 0, SnapshotInitialCapacity)
	}

	return m
}

func (m *Manager) NextMessageID() string {
	return m.messageIDs.Next()
}

func (m *Manager) AcquireConnection(ip string) (func(), bool) {
	if m.traffic == nil {
		return func() {}, true
	}
	return m.traffic.AcquireConnection(ip)
}

func (m *Manager) AllowDanmaku(roomID, userID string) (bool, ratelimit.Reason) {
	if m.traffic == nil {
		return true, ratelimit.ReasonNone
	}
	return m.traffic.AllowDanmaku(roomID, userID)
}

func (m *Manager) AllowLike(roomID, userID string) (bool, ratelimit.Reason) {
	if m.traffic == nil {
		return true, ratelimit.ReasonNone
	}
	return m.traffic.AllowLike(roomID, userID)
}

func (m *Manager) Run() {
	log.Printf("[manager] started server_id=%s workers=%d redis=%v", m.ServerID, m.workerCount, m.redisEnabled())
	m.startWorkers()
	statsTicker := time.NewTicker(DefaultStatsInterval)
	defer statsTicker.Stop()

	for {
		select {
		case client := <-m.Register:
			m.handleRegister(client)
		case client := <-m.Unregister:
			m.handleUnregister(client)
		case packet := <-m.Broadcast:
			m.handleBroadcast(packet)
		case <-statsTicker.C:
			m.startStatsBroadcast()
		}
	}
}

func (m *Manager) EnqueuePersistence(message *model.Danmaku) {
	if m.persister == nil {
		return
	}
	if m.persister.Enqueue(message) {
		m.persistEnqueued.Add(1)
		return
	}
	m.persistDropped.Add(1)
}

// SubmitDanmaku enqueues persistence only after the real-time ingress accepts
// the message. Downstream fan-out remains best effort, but an explicit ingress
// rejection cannot create a history-only record.
func (m *Manager) SubmitDanmaku(message *model.Danmaku, payload []byte) bool {
	packet := &model.Packet{
		Type:   model.TypeDanmaku,
		RoomID: message.RoomID,
		Data:   payload,
	}

	select {
	case m.Broadcast <- packet:
		m.ingressAccepted.Add(1)
		persistCopy := *message
		m.EnqueuePersistence(&persistCopy)
		return true
	default:
		m.ingressDropped.Add(1)
		return false
	}
}

func (m *Manager) AddLike(roomID string, count uint64) {
	if count == 0 {
		count = 1
	}

	// The read goroutine only does a cheap memory increment here. Redis writes
	// are batched by broadcastStats, which keeps high-frequency likes from
	// blocking WebSocket reads.
	m.likesMu.Lock()
	m.localLikes[roomID] += count
	m.localLikeTotals[roomID] += count
	m.likesMu.Unlock()

	m.likeEvents.Add(count)
}

func (m *Manager) startWorkers() {
	for id := 1; id <= m.workerCount; id++ {
		go m.broadcastWorker(id)
	}
	if m.redisPublisher != nil {
		for id := 1; id <= m.redisWorkerCount; id++ {
			go m.redisPublishWorker(id)
		}
	}
}

func (m *Manager) broadcastWorker(id int) {
	log.Printf("[worker:%d] started", id)

	for job := range m.broadcastJobs {
		for _, client := range job.Clients {
			m.safeSend(client, job.Payload)
		}
		m.releaseSnapshot(job.Clients)
	}
}

func (m *Manager) redisPublishWorker(id int) {
	log.Printf("[redis-worker:%d] started", id)
	for job := range m.redisPublishJobs {
		m.processRedisPublish(job)
	}
}

func (m *Manager) processRedisPublish(job redisPublishJob) {
	err := m.redisBreaker.Execute(func() error {
		return m.redisPublisher.Publish(context.Background(), job.roomID, job.payload)
	})
	if err == nil {
		m.redisPublished.Add(1)
		return
	}

	if errors.Is(err, resilience.ErrCircuitOpen) {
		m.redisCircuitFallbacks.Add(1)
	} else {
		m.redisPublishErrors.Add(1)
		log.Printf("[manager] redis publish failed room=%s err=%v, fallback to local broadcast", job.roomID, err)
	}
	m.redisDegradedBroadcasts.Add(1)
	m.enqueueLocalBroadcast(job.roomID, job.payload)
}

func (m *Manager) handleRegister(client *Client) {
	var subscribeRoom string
	var subscribeCtx context.Context

	m.mu.Lock()
	if _, ok := m.rooms[client.RoomID]; !ok {
		m.rooms[client.RoomID] = make(map[*Client]struct{})
		if m.redisClient != nil {
			ctx, cancel := context.WithCancel(context.Background())
			m.cancelSub[client.RoomID] = cancel
			subscribeRoom = client.RoomID
			subscribeCtx = ctx
		}
	}
	m.rooms[client.RoomID][client] = struct{}{}
	total := len(m.rooms[client.RoomID])
	m.mu.Unlock()

	if subscribeRoom != "" {
		go m.subscribeToRoom(subscribeCtx, subscribeRoom)
	}

	log.Printf("[manager] user=%s joined room=%s total=%d",
		client.UserID, client.RoomID, total)
}

func (m *Manager) handleUnregister(client *Client) {
	var cancel context.CancelFunc

	m.mu.Lock()
	clients, ok := m.rooms[client.RoomID]
	if !ok {
		m.mu.Unlock()
		return
	}

	if _, exists := clients[client]; !exists {
		m.mu.Unlock()
		return
	}

	delete(clients, client)

	remaining := len(clients)
	if remaining == 0 {
		delete(m.rooms, client.RoomID)
		cancel = m.cancelSub[client.RoomID]
		delete(m.cancelSub, client.RoomID)
	}
	m.mu.Unlock()
	client.Close()

	if cancel != nil {
		cancel()
		m.redisUnsubscribes.Add(1)
	}

	log.Printf("[manager] user=%s left room=%s remaining=%d",
		client.UserID, client.RoomID, remaining)
}

func (m *Manager) handleBroadcast(packet *model.Packet) {
	payload, err := json.Marshal(packet)
	if err != nil {
		log.Printf("[manager] marshal broadcast failed: %v", err)
		return
	}

	if m.redisPublisher != nil {
		job := redisPublishJob{roomID: packet.RoomID, payload: payload}
		select {
		case m.redisPublishJobs <- job:
			m.redisPublishQueued.Add(1)
		default:
			m.redisQueueFallbacks.Add(1)
			m.redisDegradedBroadcasts.Add(1)
			m.enqueueLocalBroadcast(packet.RoomID, payload)
		}
		return
	}

	m.enqueueLocalBroadcast(packet.RoomID, payload)
}

func (m *Manager) subscribeToRoom(ctx context.Context, roomID string) {
	channel := infra.RoomChannel(roomID)
	pubsub := m.redisClient.Subscribe(ctx, channel)
	defer pubsub.Close()

	m.redisSubscriptions.Add(1)
	log.Printf("[redis-sub] subscribed room=%s channel=%s", roomID, channel)

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			log.Printf("[redis-sub] unsubscribed room=%s channel=%s", roomID, channel)
			return
		case msg, ok := <-ch:
			if !ok {
				log.Printf("[redis-sub] channel closed room=%s", roomID)
				return
			}
			m.redisReceived.Add(1)
			m.enqueueLocalBroadcast(roomID, []byte(msg.Payload))
		}
	}
}

func (m *Manager) enqueueLocalBroadcast(roomID string, payload []byte) {
	clients := m.snapshotRoomClients(roomID)
	if len(clients) == 0 {
		m.releaseSnapshot(clients)
		return
	}

	m.localFanoutPackets.Add(1)
	job := &BroadcastJob{
		RoomID:  roomID,
		Clients: clients,
		Payload: payload,
	}

	select {
	case m.broadcastJobs <- job:
		m.enqueuedJobs.Add(1)
	default:
		m.droppedJobs.Add(1)
		m.releaseSnapshot(clients)
		log.Printf("[manager] broadcast job queue full, drop room=%s clients=%d", roomID, len(clients))
	}
}

func (m *Manager) snapshotRoomClients(roomID string) []*Client {
	m.mu.RLock()
	defer m.mu.RUnlock()

	roomClients, ok := m.rooms[roomID]
	if !ok || len(roomClients) == 0 {
		return nil
	}

	clients := m.snapshotPool.Get().([]*Client)
	m.snapshotPoolGets.Add(1)
	clients = clients[:0]

	for client := range roomClients {
		clients = append(clients, client)
	}
	return clients
}

func (m *Manager) releaseSnapshot(clients []*Client) {
	if clients == nil {
		return
	}

	if cap(clients) > MaxSnapshotRetainCap {
		m.snapshotPoolDrops.Add(1)
		return
	}

	for i := range clients {
		clients[i] = nil
	}

	clients = clients[:0]
	m.snapshotPool.Put(clients)
	m.snapshotPoolPuts.Add(1)
}

func (m *Manager) safeSend(client *Client, payload []byte) {
	select {
	case client.Send <- payload:
		client.slowDrops.Store(0)
		m.deliveredMessages.Add(1)
	default:
		m.droppedMessages.Add(1)
		if client.slowDrops.Add(1) == SlowClientDisconnectThreshold {
			m.slowClientDisconnects.Add(1)
			log.Printf("[manager] disconnect slow client user=%s room=%s consecutive_drops=%d",
				client.UserID, client.RoomID, SlowClientDisconnectThreshold)
			client.Close()
		}
	}
}

func (m *Manager) startStatsBroadcast() bool {
	if !m.statsRunning.CompareAndSwap(false, true) {
		m.statsTicksSkipped.Add(1)
		return false
	}
	go func() {
		defer m.statsRunning.Store(false)
		m.broadcastStats()
	}()
	return true
}

func (m *Manager) broadcastStats() {
	localRooms := m.snapshotRoomCounts()
	if len(localRooms) == 0 {
		return
	}

	// One stats tick does three jobs:
	// 1. report this server's local online count;
	// 2. flush locally aggregated like deltas;
	// 3. broadcast the room's latest online/like totals to local clients.
	likeDeltas := m.drainLocalLikeDeltas()
	reportTTL := DefaultStatsInterval*2 + time.Second

	for roomID, localOnline := range localRooms {
		var online uint64
		var likes uint64

		delta := likeDeltas[roomID]
		if m.redisClient != nil {
			var likeFlushed bool
			var err error
			online, likes, likeFlushed, err = m.loadDistributedStats(roomID, localOnline, delta, reportTTL)
			if err != nil {
				m.redisStatsFallbacks.Add(1)
				if !errors.Is(err, resilience.ErrCircuitOpen) {
					m.redisStatsErrors.Add(1)
					log.Printf("[stats] Redis unavailable room=%s err=%v, use local stats", roomID, err)
				}
				if delta > 0 && !likeFlushed {
					m.restoreLikeDelta(roomID, delta)
				}
				online = uint64(localOnline)
				likes = m.localLikeTotal(roomID)
			}
		} else {
			online = uint64(localOnline)
			likes = m.localLikeTotal(roomID)
			if delta > 0 {
				m.likeDeltasFlushed.Add(delta)
			}
		}

		stats := model.StatsData{
			Online: online,
			Likes:  likes,
		}
		data, err := json.Marshal(stats)
		if err != nil {
			log.Printf("[stats] marshal stats failed room=%s err=%v", roomID, err)
			continue
		}

		packet := model.Packet{
			Type:   model.TypeStats,
			RoomID: roomID,
			Data:   data,
		}
		payload, err := json.Marshal(packet)
		if err != nil {
			log.Printf("[stats] marshal packet failed room=%s err=%v", roomID, err)
			continue
		}

		m.enqueueLocalBroadcast(roomID, payload)
		m.statsBroadcasts.Add(1)
	}
}

func (m *Manager) loadDistributedStats(roomID string, localOnline int, delta uint64, reportTTL time.Duration) (online uint64, likes uint64, likeFlushed bool, err error) {
	err = m.redisBreaker.Execute(func() error {
		if updateErr := infra.UpdateServerOnline(context.Background(), m.redisClient, roomID, m.ServerID, localOnline, reportTTL); updateErr != nil {
			return updateErr
		}
		m.onlineReports.Add(1)

		if delta > 0 {
			if incrementErr := infra.IncrRoomLikes(context.Background(), m.redisClient, roomID, delta); incrementErr != nil {
				return incrementErr
			}
			likeFlushed = true
			m.likeDeltasFlushed.Add(delta)
		}

		var readErr error
		online, readErr = infra.GetTotalOnline(context.Background(), m.redisClient, roomID)
		if readErr != nil {
			return readErr
		}
		likes, readErr = infra.GetRoomLikes(context.Background(), m.redisClient, roomID)
		return readErr
	})
	return online, likes, likeFlushed, err
}

func (m *Manager) snapshotRoomCounts() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	counts := make(map[string]int, len(m.rooms))
	for roomID, clients := range m.rooms {
		counts[roomID] = len(clients)
	}
	return counts
}

func (m *Manager) drainLocalLikeDeltas() map[string]uint64 {
	m.likesMu.Lock()
	defer m.likesMu.Unlock()

	deltas := m.localLikes
	m.localLikes = make(map[string]uint64)
	return deltas
}

func (m *Manager) restoreLikeDelta(roomID string, delta uint64) {
	m.likesMu.Lock()
	defer m.likesMu.Unlock()
	m.localLikes[roomID] += delta
}

func (m *Manager) localLikeTotal(roomID string) uint64 {
	m.likesMu.Lock()
	defer m.likesMu.Unlock()
	return m.localLikeTotals[roomID]
}

func (m *Manager) Metrics() Metrics {
	m.mu.RLock()
	rooms := len(m.rooms)
	clients := 0
	for _, roomClients := range m.rooms {
		clients += len(roomClients)
	}
	m.mu.RUnlock()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	var trafficMetrics ratelimit.Metrics
	if m.traffic != nil {
		trafficMetrics = m.traffic.Metrics()
	}
	var circuitSnapshot *resilience.Snapshot
	if m.redisBreaker != nil {
		snapshot := m.redisBreaker.Snapshot()
		circuitSnapshot = &snapshot
	}

	return Metrics{
		Rooms:                   rooms,
		Clients:                 clients,
		WorkerCount:             m.workerCount,
		BroadcastQueueLen:       len(m.Broadcast),
		BroadcastQueueCap:       cap(m.Broadcast),
		IngressAccepted:         m.ingressAccepted.Load(),
		IngressDropped:          m.ingressDropped.Load(),
		JobQueueLen:             len(m.broadcastJobs),
		JobQueueCap:             cap(m.broadcastJobs),
		LocalFanoutPackets:      m.localFanoutPackets.Load(),
		EnqueuedJobs:            m.enqueuedJobs.Load(),
		DroppedJobs:             m.droppedJobs.Load(),
		DeliveredMessages:       m.deliveredMessages.Load(),
		DroppedMessages:         m.droppedMessages.Load(),
		SlowClientDisconnects:   m.slowClientDisconnects.Load(),
		SnapshotPoolGets:        m.snapshotPoolGets.Load(),
		SnapshotPoolPuts:        m.snapshotPoolPuts.Load(),
		SnapshotPoolNews:        m.snapshotPoolNews.Load(),
		SnapshotPoolDrops:       m.snapshotPoolDrops.Load(),
		PersistEnqueued:         m.persistEnqueued.Load(),
		PersistDropped:          m.persistDropped.Load(),
		RedisEnabled:            m.redisEnabled(),
		RedisPublished:          m.redisPublished.Load(),
		RedisPublishErrors:      m.redisPublishErrors.Load(),
		RedisPublishQueued:      m.redisPublishQueued.Load(),
		RedisPublishQueueLen:    len(m.redisPublishJobs),
		RedisPublishQueueCap:    cap(m.redisPublishJobs),
		RedisWorkerCount:        m.redisWorkerCount,
		RedisQueueFallbacks:     m.redisQueueFallbacks.Load(),
		RedisCircuitFallbacks:   m.redisCircuitFallbacks.Load(),
		RedisDegradedBroadcasts: m.redisDegradedBroadcasts.Load(),
		RedisStatsErrors:        m.redisStatsErrors.Load(),
		RedisStatsFallbacks:     m.redisStatsFallbacks.Load(),
		RedisReceived:           m.redisReceived.Load(),
		RedisSubscriptions:      m.redisSubscriptions.Load(),
		RedisUnsubscriptions:    m.redisUnsubscribes.Load(),
		StatsBroadcasts:         m.statsBroadcasts.Load(),
		StatsTicksSkipped:       m.statsTicksSkipped.Load(),
		LikeEvents:              m.likeEvents.Load(),
		LikeDeltasFlushed:       m.likeDeltasFlushed.Load(),
		OnlineReports:           m.onlineReports.Load(),
		Goroutines:              runtime.NumGoroutine(),
		AllocBytes:              mem.Alloc,
		TotalAllocBytes:         mem.TotalAlloc,
		NumGC:                   mem.NumGC,
		Traffic:                 trafficMetrics,
		RedisCircuit:            circuitSnapshot,
	}
}

func (m *Manager) redisEnabled() bool {
	return m.redisPublisher != nil || m.redisClient != nil
}
