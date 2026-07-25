package ws

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/charlesAcmen/livestream-danmaku/v1/internal/model"
)

const BroadcastBufferSize = 128

type Manager struct {
	Register   chan *Client
	Unregister chan *Client
	Broadcast  chan *model.Packet

	mu    sync.RWMutex
	rooms map[string]map[*Client]struct{}
}

func NewManager() *Manager {
	return &Manager{
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan *model.Packet, BroadcastBufferSize),
		rooms:      make(map[string]map[*Client]struct{}),
	}
}

// Run is the central event loop of V1.
//
// Register, Unregister, and Broadcast are serialized here through channels.
// The rooms map is still protected by a mutex because broadcasting copies the
// room client list while other events may add or remove clients.
func (m *Manager) Run() {
	log.Println("[manager] started")

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

	if len(clients) == 0 {
		delete(m.rooms, client.RoomID)
	}

	log.Printf("[manager] user=%s left room=%s remaining=%d",
		client.UserID, client.RoomID, len(clients))
}

func (m *Manager) handleBroadcast(packet *model.Packet) {
	payload, err := json.Marshal(packet)
	if err != nil {
		log.Printf("[manager] marshal broadcast failed: %v", err)
		return
	}

	clients := m.snapshotRoomClients(packet.RoomID)
	for _, client := range clients {
		m.safeSend(client, payload)
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

// safeSend never blocks the whole room for one slow client.
//
// If client.Send is full, it means the client's WritePump is not keeping up.
// V1 chooses to drop this message for that client instead of blocking all
// broadcasts behind it.
func (m *Manager) safeSend(client *Client, payload []byte) {
	select {
	case client.Send <- payload:
	default:
		log.Printf("[manager] drop message for slow client user=%s room=%s",
			client.UserID, client.RoomID)
	}
}
