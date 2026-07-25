package ws

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v5/internal/model"
	"github.com/gorilla/websocket"
)

const (
	SendBufferSize  = 128
	MaxMessageBytes = 4 * 1024
)

type Client struct {
	UserID   string
	Username string
	RoomID   string

	Conn *websocket.Conn
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

	full := &model.Danmaku{
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

	kafkaCopy := *full
	manager.EnqueuePersistence(&kafkaCopy)

	manager.Broadcast <- &model.Packet{
		Type:   model.TypeDanmaku,
		RoomID: c.RoomID,
		Data:   payload,
	}
}

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

func (c *Client) Close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.Conn.Close()
	})
}
