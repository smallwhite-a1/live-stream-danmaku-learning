package ws

import (
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"

	"github.com/charlesAcmen/livestream-danmaku/v2/internal/model"
)

const (
	BroadcastBufferSize = 256
	JobBufferSize       = 512 // 每个worker的job队列大小
	DefaultWorkerCount  = 8   // 默认启用8个worker
)

// BroadcastJob is the unit of work consumed by broadcast workers.
//
// Manager creates a job after it has copied the current room client list. The
// worker then sends the same payload to every client in that snapshot.
type BroadcastJob struct {
	RoomID  string
	Clients []*Client
	Payload []byte
}

type Metrics struct {
	Rooms       int `json:"rooms"`
	Clients     int `json:"clients"`
	WorkerCount int `json:"worker_count"`
	// 收到多少弹幕
	BroadcastPackets uint64 `json:"broadcast_packets"`
	// 多少广播任务被放入队列
	EnqueuedJobs      uint64 `json:"enqueued_jobs"`
	DroppedJobs       uint64 `json:"dropped_jobs"`
	DeliveredMessages uint64 `json:"delivered_messages"`
	DroppedMessages   uint64 `json:"dropped_messages"`
}

type Manager struct {
	Register   chan *Client
	Unregister chan *Client
	Broadcast  chan *model.Packet // 弹幕进入通道

	broadcastJobs chan *BroadcastJob // 广播任务队列
	workerCount   int                // worker数量

	mu    sync.RWMutex
	rooms map[string]map[*Client]struct{}

	broadcastPackets  atomic.Uint64
	enqueuedJobs      atomic.Uint64
	droppedJobs       atomic.Uint64
	deliveredMessages atomic.Uint64
	droppedMessages   atomic.Uint64
}

func NewManager(workerCount int) *Manager {
	if workerCount <= 0 {
		workerCount = DefaultWorkerCount
	}

	return &Manager{
		Register:      make(chan *Client),
		Unregister:    make(chan *Client),
		Broadcast:     make(chan *model.Packet, BroadcastBufferSize),
		broadcastJobs: make(chan *BroadcastJob, JobBufferSize),
		workerCount:   workerCount,
		rooms:         make(map[string]map[*Client]struct{}),
	}
}

// Run is the central event loop.
//
// Compared with V1, Run no longer performs the whole room fan-out by itself.
// It converts a broadcast packet into a BroadcastJob and lets worker goroutines
// do the heavier per-client send work.
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
		return
	}

	m.broadcastPackets.Add(1)
	job := &BroadcastJob{
		RoomID:  packet.RoomID,
		Clients: clients,
		Payload: payload,
	}

	// Non-blocking job enqueue.
	//
	// If all workers are busy and the job queue is full, V2 drops the whole
	// broadcast job. This is a deliberate realtime-system tradeoff: protect the
	// server from unbounded memory growth and keep new events moving.
	select {
	case m.broadcastJobs <- job:
		m.enqueuedJobs.Add(1)
	default:
		m.droppedJobs.Add(1)
		log.Printf("[manager] broadcast job queue full, drop room=%s clients=%d",
			packet.RoomID, len(clients))
	}
}

func (m *Manager) snapshotRoomClients(roomID string) []*Client {
	m.mu.RLock()
	defer m.mu.RUnlock()

	roomClients, ok := m.rooms[roomID]
	if !ok {
		return nil
	}

	clients := make([]*Client, 0, len(roomClients))
	for client := range roomClients {
		clients = append(clients, client)
	}
	return clients
}

// safeSend is a non-blocking per-client send.
//
// Even after a job reaches a worker, one slow client must not block the whole
// worker forever. If that client's Send channel is full, only that client misses
// this message.
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

	return Metrics{
		Rooms:             rooms,
		Clients:           clients,
		WorkerCount:       m.workerCount,
		BroadcastPackets:  m.broadcastPackets.Load(),
		EnqueuedJobs:      m.enqueuedJobs.Load(),
		DroppedJobs:       m.droppedJobs.Load(),
		DeliveredMessages: m.deliveredMessages.Load(),
		DroppedMessages:   m.droppedMessages.Load(),
	}
}
