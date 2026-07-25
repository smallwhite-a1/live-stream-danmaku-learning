package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v8/internal/idgen"
	"github.com/charlesAcmen/livestream-danmaku/v8/internal/infra"
	"github.com/charlesAcmen/livestream-danmaku/v8/internal/model"
	"github.com/redis/go-redis/v9"
)

const (
	BroadcastBufferSize     = 512
	JobBufferSize           = 1024
	DefaultWorkerCount      = 16
	SnapshotInitialCapacity = 256
	MaxSnapshotRetainCap    = 8192
	DefaultStatsInterval    = 3 * time.Second
)

type DanmakuPersister interface {
	Enqueue(message *model.Danmaku) bool
}

type BroadcastJob struct {
	RoomID  string
	Clients []*Client
	Payload []byte
}

type Metrics struct {
	Rooms                int    `json:"rooms"`
	Clients              int    `json:"clients"`
	WorkerCount          int    `json:"worker_count"`
	BroadcastQueueLen    int    `json:"broadcast_queue_len"`
	BroadcastQueueCap    int    `json:"broadcast_queue_cap"`
	JobQueueLen          int    `json:"job_queue_len"`
	JobQueueCap          int    `json:"job_queue_cap"`
	LocalFanoutPackets   uint64 `json:"local_fanout_packets"`
	EnqueuedJobs         uint64 `json:"enqueued_jobs"`
	DroppedJobs          uint64 `json:"dropped_jobs"`
	DeliveredMessages    uint64 `json:"delivered_messages"`
	DroppedMessages      uint64 `json:"dropped_messages"`
	SnapshotPoolGets     uint64 `json:"snapshot_pool_gets"`
	SnapshotPoolPuts     uint64 `json:"snapshot_pool_puts"`
	SnapshotPoolNews     uint64 `json:"snapshot_pool_news"`
	SnapshotPoolDrops    uint64 `json:"snapshot_pool_drops"`
	PersistEnqueued      uint64 `json:"persist_enqueued"`
	PersistDropped       uint64 `json:"persist_dropped"`
	RedisEnabled         bool   `json:"redis_enabled"`
	RedisPublished       uint64 `json:"redis_published"`
	RedisPublishErrors   uint64 `json:"redis_publish_errors"`
	RedisReceived        uint64 `json:"redis_received"`
	RedisSubscriptions   uint64 `json:"redis_subscriptions"`
	RedisUnsubscriptions uint64 `json:"redis_unsubscriptions"`
	StatsBroadcasts      uint64 `json:"stats_broadcasts"`
	LikeEvents           uint64 `json:"like_events"`
	LikeDeltasFlushed    uint64 `json:"like_deltas_flushed"`
	OnlineReports        uint64 `json:"online_reports"`
	Goroutines           int    `json:"goroutines"`
	AllocBytes           uint64 `json:"alloc_bytes"`
	TotalAllocBytes      uint64 `json:"total_alloc_bytes"`
	NumGC                uint32 `json:"num_gc"`
}

type Manager struct {
	ServerID string

	Register   chan *Client
	Unregister chan *Client
	Broadcast  chan *model.Packet

	broadcastJobs chan *BroadcastJob
	workerCount   int
	persister     DanmakuPersister
	redisClient   *redis.Client
	messageIDs    *idgen.Generator

	mu        sync.RWMutex
	rooms     map[string]map[*Client]struct{}
	cancelSub map[string]context.CancelFunc

	// localLikes stores unflushed like deltas. localLikeTotals is only used when
	// Redis is disabled, so the no-middleware learning mode can still show stats.
	likesMu         sync.Mutex
	localLikes      map[string]uint64
	localLikeTotals map[string]uint64

	snapshotPool sync.Pool

	localFanoutPackets atomic.Uint64
	enqueuedJobs       atomic.Uint64
	droppedJobs        atomic.Uint64
	deliveredMessages  atomic.Uint64
	droppedMessages    atomic.Uint64
	snapshotPoolGets   atomic.Uint64
	snapshotPoolPuts   atomic.Uint64
	snapshotPoolNews   atomic.Uint64
	snapshotPoolDrops  atomic.Uint64
	persistEnqueued    atomic.Uint64
	persistDropped     atomic.Uint64
	redisPublished     atomic.Uint64
	redisPublishErrors atomic.Uint64
	redisReceived      atomic.Uint64
	redisSubscriptions atomic.Uint64
	redisUnsubscribes  atomic.Uint64
	statsBroadcasts    atomic.Uint64
	likeEvents         atomic.Uint64
	likeDeltasFlushed  atomic.Uint64
	onlineReports      atomic.Uint64
}

func NewManager(workerCount int, persister DanmakuPersister, redisClient *redis.Client) *Manager {
	if workerCount <= 0 {
		workerCount = DefaultWorkerCount
	}

	hostname, _ := os.Hostname()
	serverID := fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano())
	messageIDs, err := idgen.New()
	if err != nil {
		panic(fmt.Sprintf("initialize message id generator: %v", err))
	}

	m := &Manager{
		ServerID:        serverID,
		Register:        make(chan *Client),
		Unregister:      make(chan *Client),
		Broadcast:       make(chan *model.Packet, BroadcastBufferSize),
		broadcastJobs:   make(chan *BroadcastJob, JobBufferSize),
		workerCount:     workerCount,
		persister:       persister,
		redisClient:     redisClient,
		messageIDs:      messageIDs,
		rooms:           make(map[string]map[*Client]struct{}),
		cancelSub:       make(map[string]context.CancelFunc),
		localLikes:      make(map[string]uint64),
		localLikeTotals: make(map[string]uint64),
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
			go m.broadcastStats()
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

func (m *Manager) handleRegister(client *Client) {
	var subscribeRoom string
	var subscribeCtx context.Context

	m.mu.Lock()
	if _, ok := m.rooms[client.RoomID]; !ok {
		m.rooms[client.RoomID] = make(map[*Client]struct{})
		if m.redisEnabled() {
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
	client.Close()

	remaining := len(clients)
	if remaining == 0 {
		delete(m.rooms, client.RoomID)
		cancel = m.cancelSub[client.RoomID]
		delete(m.cancelSub, client.RoomID)
	}
	m.mu.Unlock()

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

	if m.redisEnabled() {
		if err := infra.PublishToRoom(context.Background(), m.redisClient, packet.RoomID, payload); err != nil {
			m.redisPublishErrors.Add(1)
			log.Printf("[manager] redis publish failed room=%s err=%v, fallback to local broadcast", packet.RoomID, err)
			m.enqueueLocalBroadcast(packet.RoomID, payload)
			return
		}
		m.redisPublished.Add(1)
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
		m.deliveredMessages.Add(1)
	default:
		m.droppedMessages.Add(1)
		log.Printf("[manager] drop message for slow client user=%s room=%s",
			client.UserID, client.RoomID)
	}
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

		if m.redisEnabled() {
			if err := infra.UpdateServerOnline(context.Background(), m.redisClient, roomID, m.ServerID, localOnline, reportTTL); err != nil {
				log.Printf("[stats] update online failed room=%s err=%v", roomID, err)
			} else {
				m.onlineReports.Add(1)
			}

			if delta := likeDeltas[roomID]; delta > 0 {
				if err := infra.IncrRoomLikes(context.Background(), m.redisClient, roomID, delta); err != nil {
					log.Printf("[stats] incr likes failed room=%s delta=%d err=%v", roomID, delta, err)
					m.restoreLikeDelta(roomID, delta)
				} else {
					m.likeDeltasFlushed.Add(delta)
				}
			}

			totalOnline, err := infra.GetTotalOnline(context.Background(), m.redisClient, roomID)
			if err != nil {
				log.Printf("[stats] get total online failed room=%s err=%v", roomID, err)
			}
			totalLikes, err := infra.GetRoomLikes(context.Background(), m.redisClient, roomID)
			if err != nil {
				log.Printf("[stats] get likes failed room=%s err=%v", roomID, err)
			}
			online = totalOnline
			likes = totalLikes
		} else {
			online = uint64(localOnline)
			likes = m.localLikeTotal(roomID)
			if delta := likeDeltas[roomID]; delta > 0 {
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

	return Metrics{
		Rooms:                rooms,
		Clients:              clients,
		WorkerCount:          m.workerCount,
		BroadcastQueueLen:    len(m.Broadcast),
		BroadcastQueueCap:    cap(m.Broadcast),
		JobQueueLen:          len(m.broadcastJobs),
		JobQueueCap:          cap(m.broadcastJobs),
		LocalFanoutPackets:   m.localFanoutPackets.Load(),
		EnqueuedJobs:         m.enqueuedJobs.Load(),
		DroppedJobs:          m.droppedJobs.Load(),
		DeliveredMessages:    m.deliveredMessages.Load(),
		DroppedMessages:      m.droppedMessages.Load(),
		SnapshotPoolGets:     m.snapshotPoolGets.Load(),
		SnapshotPoolPuts:     m.snapshotPoolPuts.Load(),
		SnapshotPoolNews:     m.snapshotPoolNews.Load(),
		SnapshotPoolDrops:    m.snapshotPoolDrops.Load(),
		PersistEnqueued:      m.persistEnqueued.Load(),
		PersistDropped:       m.persistDropped.Load(),
		RedisEnabled:         m.redisEnabled(),
		RedisPublished:       m.redisPublished.Load(),
		RedisPublishErrors:   m.redisPublishErrors.Load(),
		RedisReceived:        m.redisReceived.Load(),
		RedisSubscriptions:   m.redisSubscriptions.Load(),
		RedisUnsubscriptions: m.redisUnsubscribes.Load(),
		StatsBroadcasts:      m.statsBroadcasts.Load(),
		LikeEvents:           m.likeEvents.Load(),
		LikeDeltasFlushed:    m.likeDeltasFlushed.Load(),
		OnlineReports:        m.onlineReports.Load(),
		Goroutines:           runtime.NumGoroutine(),
		AllocBytes:           mem.Alloc,
		TotalAllocBytes:      mem.TotalAlloc,
		NumGC:                mem.NumGC,
	}
}

func (m *Manager) redisEnabled() bool {
	return m.redisClient != nil
}
