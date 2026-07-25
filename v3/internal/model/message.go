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
// The outer envelope gives the server a stable protocol shape. The server can
// route by Type and RoomID before parsing Data.
type Packet struct {
	Type   int             `json:"type"`
	RoomID string          `json:"room_id,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
}

// Danmaku is the chat payload inside Packet.Data.
//
// The client only sends Content. The server fills trusted fields such as RoomID,
// UserID, Username, and SendTime.
type Danmaku struct {
	RoomID   string    `json:"room_id"`
	UserID   string    `json:"user_id"`
	Username string    `json:"username"`
	Content  string    `json:"content"`
	SendTime time.Time `json:"send_time"`
}
