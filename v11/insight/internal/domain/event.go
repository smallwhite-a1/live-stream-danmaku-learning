package domain

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

const maxContentRunes = 500

type MessageEvent struct {
	EventID       string    `json:"event_id"`
	RoomID        string    `json:"room_id"`
	UserID        string    `json:"user_id"`
	Username      string    `json:"username"`
	Content       string    `json:"content"`
	OccurredAt    time.Time `json:"occurred_at"`
	SchemaVersion string    `json:"schema_version"`
	Source        string    `json:"source"`
}

func (e MessageEvent) Validate() error {
	switch {
	case strings.TrimSpace(e.EventID) == "":
		return errors.New("event ID is required")
	case strings.TrimSpace(e.RoomID) == "":
		return errors.New("room ID is required")
	case strings.TrimSpace(e.UserID) == "":
		return errors.New("user ID is required")
	case strings.TrimSpace(e.Content) == "":
		return errors.New("content is required")
	case utf8.RuneCountInString(e.Content) > maxContentRunes:
		return errors.New("content exceeds 500 runes")
	case e.OccurredAt.IsZero():
		return errors.New("occurred at is required")
	default:
		return nil
	}
}
