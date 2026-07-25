package ws

import (
	"encoding/json"
	"log"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/charlesAcmen/livestream-danmaku/v5/internal/model"
)

const (
	BroadcastBufferSize     = 512
	JobBufferSize           = 1024
	DefaultWorkerCount      = 16
	SnapshotInitialCapacity = 256
	MaxSnapshotRetainCap    = 8192
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
	Rooms             int    `json:"rooms"`
	Clients           int    `json:"clients"`
	WorkerCount       int    `json:"worker_count"`
	BroadcastQueueLen int    `json:"broadcast_queue_len"`
	BroadcastQueueCap int    `json:"broadcast_queue_cap"`
	JobQueueLen       int    `json:"job_queue_len"`
	JobQueueCap       int    `json:"job_queue_cap"`
	BroadcastPackets  uint64 `json:"broadcast_packets"`
	EnqueuedJobs      uint64 `json:"enqueued_jobs"`
	DroppedJobs       uint64 `json:"dropped_jobs"`
	DeliveredMessages uint64 `json:"delivered_messages"`
	DroppedMessages   uint64 `json:"dropped_messages"`
	SnapshotPoolGets  uint64 `json:"snapshot_pool_gets"`
	SnapshotPoolPuts  uint64 `json:"snapshot_pool_puts"`
	SnapshotPoolNews  uint64 `json:"snapshot_pool_news"`
	SnapshotPoolDrops uint64 `json:"snapshot_pool_drops"`
	PersistEnqueued   uint64 `json:"persist_enqueued"`
	PersistDropped    uint64 `json:"persist_dropped"`
	Goroutines        int    `json:"goroutines"`
	AllocBytes        uint64 `json:"alloc_bytes"`
	TotalAllocBytes   uint64 `json:"total_alloc_bytes"`
	NumGC             uint32 `json:"num_gc"`
}

type Manager struct {
	Register   chan *Client
	Unregister chan *Client
	Broadcast  chan *model.Packet

	broadcastJobs chan *BroadcastJob
	workerCount   int
	persister     DanmakuPersister

	mu    sync.RWMutex
	rooms map[string]map[*Client]struct{}

	snapshotPool sync.Pool

	broadcastPackets  atomic.Uint64
	enqueuedJobs      atomic.Uint64
	droppedJobs       atomic.Uint64
	deliveredMessages atomic.Uint64
	droppedMessages   atomic.Uint64
	snapshotPoolGets  atomic.Uint64
	snapshotPoolPuts  atomic.Uint64
	snapshotPoolNews  atomic.Uint64
	snapshotPoolDrops atomic.Uint64
	persistEnqueued   atomic.Uint64
	persistDropped    atomic.Uint64
}

func NewManager(workerCount int, persister DanmakuPersister) *Manager {
	if workerCount <= 0 {
		workerCount = DefaultWorkerCount
	}

	m := &Manager{
		Register:      make(chan *Client),
		Unregister:    make(chan *Client),
		Broadcast:     make(chan *model.Packet, BroadcastBufferSize),
		broadcastJobs: make(chan *BroadcastJob, JobBufferSize),
		workerCount:   workerCount,
		persister:     persister,
		rooms:         make(map[string]map[*Client]struct{}),
	}
	m.snapshotPool.New = func() any {
		m.snapshotPoolNews.Add(1)
		return make([]*Client, 0, SnapshotInitialCapacity)
	}

	return m
}

func (m *Manager) Run() {
	log.Printf("[manager] started with %d broadcast workers", m.workerCount)
	m.startWorkers()

	for {
		select {
		case client := <-m.Register:
			m.handleRegister(client)
		case client := <-m.Unregister:
			m.handleUnregister(client)
		case packet := <-m.Broadcast:
			m.handleBroadcast(packet)
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
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.rooms[client.RoomID]; !ok {
		m.rooms[client.RoomID] = make(map[*Client]struct{})
	}
	m.rooms[client.RoomID][client] = struct{}{}

	log.Printf("[manager] user=%s joined room=%s total=%d",
		client.UserID, client.RoomID, len(m.rooms[client.RoomID]))
}

func (m *Manager) handleUnregister(client *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	clients, ok := m.rooms[client.RoomID]
	if !ok {
		return
	}

	if _, exists := clients[client]; !exists {
		return
	}

	delete(clients, client)
	client.Close()

	remaining := len(clients)
	if remaining == 0 {
		delete(m.rooms, client.RoomID)
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

	clients := m.snapshotRoomClients(packet.RoomID)
	if len(clients) == 0 {
		m.releaseSnapshot(clients)
		return
	}

	m.broadcastPackets.Add(1)
	job := &BroadcastJob{
		RoomID:  packet.RoomID,
		Clients: clients,
		Payload: payload,
	}

	select {
	case m.broadcastJobs <- job:
		m.enqueuedJobs.Add(1)
	default:
		m.droppedJobs.Add(1)
		m.releaseSnapshot(clients)
		log.Printf("[manager] broadcast job queue full, drop room=%s clients=%d",
			packet.RoomID, len(clients))
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
		Rooms:             rooms,
		Clients:           clients,
		WorkerCount:       m.workerCount,
		BroadcastQueueLen: len(m.Broadcast),
		BroadcastQueueCap: cap(m.Broadcast),
		JobQueueLen:       len(m.broadcastJobs),
		JobQueueCap:       cap(m.broadcastJobs),
		BroadcastPackets:  m.broadcastPackets.Load(),
		EnqueuedJobs:      m.enqueuedJobs.Load(),
		DroppedJobs:       m.droppedJobs.Load(),
		DeliveredMessages: m.deliveredMessages.Load(),
		DroppedMessages:   m.droppedMessages.Load(),
		SnapshotPoolGets:  m.snapshotPoolGets.Load(),
		SnapshotPoolPuts:  m.snapshotPoolPuts.Load(),
		SnapshotPoolNews:  m.snapshotPoolNews.Load(),
		SnapshotPoolDrops: m.snapshotPoolDrops.Load(),
		PersistEnqueued:   m.persistEnqueued.Load(),
		PersistDropped:    m.persistDropped.Load(),
		Goroutines:        runtime.NumGoroutine(),
		AllocBytes:        mem.Alloc,
		TotalAllocBytes:   mem.TotalAlloc,
		NumGC:             mem.NumGC,
	}
}
