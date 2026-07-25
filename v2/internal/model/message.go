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
// V2 still keeps the protocol deliberately small. The outer packet lets the
// server route by Type and RoomID before parsing the inner Data payload.
type Packet struct {
	Type   int             `json:"type"`
	RoomID string          `json:"room_id,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
}

// Danmaku is the chat payload stored inside Packet.Data.
//
// The client only sends Content. The server fills identity and time fields
// after it trusts the connection query parameters.
type Danmaku struct {
	RoomID   string    `json:"room_id"`
	UserID   string    `json:"user_id"`
	Username string    `json:"username"`
	Content  string    `json:"content"`
	SendTime time.Time `json:"send_time"`
}
