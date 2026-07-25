package model

import (
	"encoding/json"
	"time"
)

const (
	TypeDanmaku = 101
)

// Packet is the outer WebSocket envelope.
//
// The server can route a message by Type and RoomID first, then parse Data.
// This mirrors the main project, but V1 only implements TypeDanmaku.
type Packet struct {
	Type   int             `json:"type"`
	RoomID string          `json:"room_id,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
}

// Danmaku is the real payload inside Packet.Data.
//
// In V1, the client only needs to send Content. The server fills RoomID,
// UserID, Username, and SendTime because those fields should be trusted by
// the backend, not by the client.
type Danmaku struct {
	RoomID   string    `json:"room_id"`
	UserID   string    `json:"user_id"`
	Username string    `json:"username"`
	Content  string    `json:"content"`
	SendTime time.Time `json:"send_time"`
}
