package model

import (
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"
)

const (
	TypeDanmaku = 101
	TypeStats   = 102
	ActionLike  = 103
	TypeControl = 104

	MaxMessageIDRunes = 64
	MaxRoomIDRunes    = 64
	MaxUserIDRunes    = 64
	MaxUsernameRunes  = 64
	MaxContentRunes   = 500
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

	MessageID string    `gorm:"type:varchar(64);not null;uniqueIndex:uk_v9_message_id;index:idx_v9_room_cursor,priority:3" json:"message_id"`
	RoomID    string    `gorm:"type:varchar(64);not null;index:idx_v9_room_cursor,priority:1" json:"room_id"`
	UserID    string    `gorm:"type:varchar(64);not null;index" json:"user_id"`
	Username  string    `gorm:"type:varchar(64);not null" json:"username"`
	Content   string    `gorm:"type:varchar(500);not null" json:"content"`
	SendTime  time.Time `gorm:"type:datetime(3);not null;index:idx_v9_room_cursor,priority:2" json:"send_time"`
}

func (Danmaku) TableName() string {
	return "v9_danmaku_messages"
}

// ValidateDanmakuStorage rejects values that cannot fit the MySQL schema.
// Invalid records should go to the Kafka DLQ instead of being mistaken for a
// database outage and pausing a partition forever.
func ValidateDanmakuStorage(danmaku *Danmaku) error {
	if danmaku == nil {
		return fmt.Errorf("nil danmaku")
	}
	fields := []struct {
		name  string
		value string
		max   int
	}{
		{name: "message_id", value: danmaku.MessageID, max: MaxMessageIDRunes},
		{name: "room_id", value: danmaku.RoomID, max: MaxRoomIDRunes},
		{name: "user_id", value: danmaku.UserID, max: MaxUserIDRunes},
		{name: "username", value: danmaku.Username, max: MaxUsernameRunes},
		{name: "content", value: danmaku.Content, max: MaxContentRunes},
	}
	for _, field := range fields {
		if utf8.RuneCountInString(field.value) > field.max {
			return fmt.Errorf("%s exceeds %d characters", field.name, field.max)
		}
	}
	if year := danmaku.SendTime.Year(); year < 1000 || year > 9999 {
		return fmt.Errorf("send_time year %d is outside MySQL DATETIME range", year)
	}
	return nil
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

type ControlData struct {
	Code             string `json:"code"`
	Action           string `json:"action,omitempty"`
	Scope            string `json:"scope,omitempty"`
	RetryAfterMillis int    `json:"retry_after_millis,omitempty"`
}
