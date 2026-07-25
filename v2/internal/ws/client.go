package ws

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v2/internal/model"
	"github.com/gorilla/websocket"
)

const (
	SendBufferSize  = 64
	MaxMessageBytes = 4 * 1024
)

type Client struct {
	UserID   string
	Username string
	RoomID   string

	Conn *websocket.Conn

	// Send is this client's outbound queue.
	//
	// Worker goroutines put broadcast payloads into this channel. WritePump is
	// the only goroutine that reads from it and writes to the WebSocket.
	Send chan []byte

	done chan struct{}
	once sync.Once
}

func NewClient(userID, username, roomID string, conn *websocket.Conn) *Client {
	return &Client{
		UserID:   userID,
		Username: username,
		RoomID:   roomID,
		Conn:     conn,
		Send:     make(chan []byte, SendBufferSize),
		done:     make(chan struct{}),
	}
}

// ReadPump owns all reads from the WebSocket connection.
//
// High-concurrency rule: one goroutine should own WebSocket reads for a client.
// When it receives a valid danmaku, it does not broadcast by itself. It sends a
// packet into Manager.Broadcast and lets Manager decide how to fan it out.
func (c *Client) ReadPump(manager *Manager) {
	defer func() {
		manager.Unregister <- c
		c.Close()
	}()

	c.Conn.SetReadLimit(MaxMessageBytes)

	for {
		_, raw, err := c.Conn.ReadMessage()
		if err != nil {
			log.Printf("[client:%s] read stopped: %v", c.UserID, err)
			return
		}

		var packet model.Packet
		if err := json.Unmarshal(raw, &packet); err != nil {
			log.Printf("[client:%s] invalid packet: %v", c.UserID, err)
			continue
		}

		switch packet.Type {
		case model.TypeDanmaku:
			c.handleDanmaku(manager, packet.Data)
		default:
			log.Printf("[client:%s] unknown packet type: %d", c.UserID, packet.Type)
		}
	}
}

func (c *Client) handleDanmaku(manager *Manager, data []byte) {
	var input model.Danmaku
	if err := json.Unmarshal(data, &input); err != nil {
		log.Printf("[client:%s] invalid danmaku payload: %v", c.UserID, err)
		return
	}

	content := strings.TrimSpace(input.Content)
	if content == "" {
		return
	}

	full := model.Danmaku{
		RoomID:   c.RoomID,
		UserID:   c.UserID,
		Username: c.Username,
		Content:  content,
		SendTime: time.Now(),
	}

	payload, err := json.Marshal(full)
	if err != nil {
		log.Printf("[client:%s] marshal danmaku failed: %v", c.UserID, err)
		return
	}

	manager.Broadcast <- &model.Packet{
		Type:   model.TypeDanmaku,
		RoomID: c.RoomID,
		Data:   payload,
	}
}

// WritePump owns all writes to the WebSocket connection.
//
// High-concurrency rule: one goroutine should own WebSocket writes for a
// client. Workers never call Conn.WriteMessage directly; they only enqueue to
// c.Send through Manager.safeSend.
func (c *Client) WritePump() {
	for {
		select {
		case msg := <-c.Send:
			if err := c.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("[client:%s] write stopped: %v", c.UserID, err)
				c.Close()
				return
			}
		case <-c.done:
			return
		}
	}
}

// Close is idempotent.
//
// We close done and the socket, but we do not close Send. A worker may still
// hold a copied client pointer for a short moment. Sending to a closed channel
// would panic, so Manager removes the client from rooms and safeSend drops when
// the client's outbound buffer is full.
func (c *Client) Close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.Conn.Close()
	})
}
