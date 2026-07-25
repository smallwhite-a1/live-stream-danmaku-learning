package model

import (
	"encoding/json"
	"time"
)

const (
	TypeDanmaku = 101
	TypeStats   = 102
	ActionLike  = 103
)

type Packet struct {
	Type   int             `json:"type"`
	RoomID string          `json:"room_id,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
}

// Danmaku is the WebSocket payload, Redis payload body, Kafka payload body, and
// GORM model in V7.
type Danmaku struct {
	ID uint64 `gorm:"primaryKey;autoIncrement" json:"id"`

	RoomID   string    `gorm:"type:varchar(64);not null;index:idx_room_time" json:"room_id"`
	UserID   string    `gorm:"type:varchar(64);not null;index" json:"user_id"`
	Username string    `gorm:"type:varchar(64);not null" json:"username"`
	Content  string    `gorm:"type:varchar(500);not null" json:"content"`
	SendTime time.Time `gorm:"type:datetime(3);not null;index:idx_room_time" json:"send_time"`
}

func (Danmaku) TableName() string {
	return "v7_danmaku_messages"
}

type StatsData struct {
	Online uint64 `json:"online"`
	Likes  uint64 `json:"likes"`
}

type Like struct {
	Count uint64 `json:"count"`
}
