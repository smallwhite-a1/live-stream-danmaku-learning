package ws

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v1/internal/model"
	"github.com/gorilla/websocket"
)

const SendBufferSize = 32

type Client struct {
	UserID   string
	Username string
	RoomID   string

	Conn *websocket.Conn
	Send chan []byte // 信息发送通道

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
// One client connection should have one read loop. When a message arrives, this
// goroutine parses it, enriches it with trusted server-side fields, and hands it
// to Manager through the Broadcast channel.
func (c *Client) ReadPump(manager *Manager) {
	defer func() {
		manager.Unregister <- c
		c.Close()
	}()

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
// Other goroutines do not call Conn.WriteMessage directly. They send bytes to
// c.Send, and this goroutine serializes the actual socket writes.
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

// Close is safe to call more than once.
//
// We intentionally do not close c.Send. A broadcast goroutine may still hold a
// copied client pointer and try to send to it. Sending to a closed channel would
// panic, while sending to an open buffered channel can be safely skipped by
// safeSend when the buffer becomes full.
func (c *Client) Close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.Conn.Close()
	})
}
