package ws

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/charlesAcmen/livestream-danmaku/v10/internal/model"
	"github.com/gorilla/websocket"
)

const (
	SendBufferSize                = 128
	MaxMessageBytes               = 4 * 1024
	MaxDanmakuRunes               = 200
	MaxLikeCount                  = 20
	SlowClientDisconnectThreshold = 64
)

type Client struct {
	UserID   string
	Username string
	RoomID   string

	Conn *websocket.Conn
	Send chan []byte

	done      chan struct{}
	once      sync.Once
	slowDrops atomic.Uint32
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
			if allowed, reason := manager.AllowDanmaku(c.RoomID, c.UserID); !allowed {
				c.sendControl("rate_limited", "danmaku", string(reason), 1000)
				continue
			}
			c.handleDanmaku(manager, packet.Data)
		case model.ActionLike:
			if allowed, reason := manager.AllowLike(c.RoomID, c.UserID); !allowed {
				c.sendControl("rate_limited", "like", string(reason), 1000)
				continue
			}
			c.handleLike(manager, packet.Data)
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
	if utf8.RuneCountInString(content) > MaxDanmakuRunes {
		c.sendControl("content_too_long", "danmaku", "user", 0)
		return
	}

	full := &model.Danmaku{
		MessageID: manager.NextMessageID(),
		RoomID:    c.RoomID,
		UserID:    c.UserID,
		Username:  c.Username,
		Content:   content,
		SendTime:  time.Now(),
	}

	payload, err := json.Marshal(full)
	if err != nil {
		log.Printf("[client:%s] marshal danmaku failed: %v", c.UserID, err)
		return
	}

	if !manager.SubmitDanmaku(full, payload) {
		c.sendControl("server_overloaded", "danmaku", "server", 1000)
	}
}

func (c *Client) handleLike(manager *Manager, data []byte) {
	var like model.Like
	if len(data) > 0 {
		if err := json.Unmarshal(data, &like); err != nil {
			like.Count = 1
		}
	}
	if like.Count == 0 {
		like.Count = 1
	}
	if like.Count > MaxLikeCount {
		like.Count = MaxLikeCount
	}

	manager.AddLike(c.RoomID, like.Count)
}

func (c *Client) sendControl(code, action, scope string, retryAfterMillis int) {
	data, err := json.Marshal(model.ControlData{
		Code:             code,
		Action:           action,
		Scope:            scope,
		RetryAfterMillis: retryAfterMillis,
	})
	if err != nil {
		return
	}
	payload, err := json.Marshal(model.Packet{
		Type:   model.TypeControl,
		RoomID: c.RoomID,
		Data:   data,
	})
	if err != nil {
		return
	}

	select {
	case c.Send <- payload:
	default:
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
		if c.Conn != nil {
			_ = c.Conn.Close()
		}
	})
}
