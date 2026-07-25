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

// Danmaku is the shared business message across WebSocket, Redis, Kafka, and
// MySQL. MessageID is generated once at ingress and survives every retry.
type Danmaku struct {
	ID uint64 `gorm:"primaryKey;autoIncrement" json:"id"`

	MessageID string    `gorm:"type:varchar(64);not null;uniqueIndex:uk_v8_message_id;index:idx_v8_room_cursor,priority:3" json:"message_id"`
	RoomID    string    `gorm:"type:varchar(64);not null;index:idx_v8_room_cursor,priority:1" json:"room_id"`
	UserID    string    `gorm:"type:varchar(64);not null;index" json:"user_id"`
	Username  string    `gorm:"type:varchar(64);not null" json:"username"`
	Content   string    `gorm:"type:varchar(500);not null" json:"content"`
	SendTime  time.Time `gorm:"type:datetime(3);not null;index:idx_v8_room_cursor,priority:2" json:"send_time"`
}

func (Danmaku) TableName() string {
	return "v8_danmaku_messages"
}

// DeadLetter preserves a poison Kafka message so operators can inspect and
// replay it instead of silently advancing the consumer offset.
type DeadLetter struct {
	SourceTopic     string    `json:"source_topic"`
	SourcePartition int32     `json:"source_partition"`
	SourceOffset    int64     `json:"source_offset"`
	OriginalKey     []byte    `json:"original_key,omitempty"`
	OriginalValue   []byte    `json:"original_value"`
	Reason          string    `json:"reason"`
	FailedAt        time.Time `json:"failed_at"`
}

type StatsData struct {
	Online uint64 `json:"online"`
	Likes  uint64 `json:"likes"`
}

type Like struct {
	Count uint64 `json:"count"`
}
