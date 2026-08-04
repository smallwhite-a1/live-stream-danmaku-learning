package domain

import (
	"errors"
	"strings"
	"time"
)

type WindowRef struct {
	RoomID string    `json:"room_id"`
	Start  time.Time `json:"window_start"`
	End    time.Time `json:"window_end"`
}

func NewWindowRef(roomID string, at time.Time, size time.Duration) (WindowRef, error) {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return WindowRef{}, errors.New("room ID is required")
	}
	if size <= 0 {
		return WindowRef{}, errors.New("window size must be positive")
	}
	start := at.UTC().Truncate(size)
	return WindowRef{RoomID: roomID, Start: start, End: start.Add(size)}, nil
}

func (r WindowRef) Key() string {
	return r.RoomID + ":" + utcKeyTime(r.Start) + ":" + utcKeyTime(r.End)
}

type InsightWindow struct {
	Ref               WindowRef      `json:"ref"`
	Events            []MessageEvent `json:"events"`
	TotalMessages     int            `json:"total_messages"`
	DuplicateMessages int            `json:"duplicate_messages"`
	LateMessages      int            `json:"late_messages"`
}

func utcKeyTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}
