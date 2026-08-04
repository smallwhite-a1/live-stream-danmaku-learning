package domain

import (
	"strings"
	"testing"
	"time"
)

func TestMessageEventValidate(t *testing.T) {
	valid := MessageEvent{
		EventID: "event-1", RoomID: "room-1", UserID: "user-1",
		Content: "这个产品什么时候补货？", OccurredAt: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		name    string
		mutate  func(*MessageEvent)
		wantErr bool
	}{
		{name: "valid Chinese event"},
		{name: "blank event id", mutate: func(e *MessageEvent) { e.EventID = "  " }, wantErr: true},
		{name: "blank room id", mutate: func(e *MessageEvent) { e.RoomID = "" }, wantErr: true},
		{name: "blank user id", mutate: func(e *MessageEvent) { e.UserID = "\t" }, wantErr: true},
		{name: "blank content", mutate: func(e *MessageEvent) { e.Content = "  " }, wantErr: true},
		{name: "zero occurred at", mutate: func(e *MessageEvent) { e.OccurredAt = time.Time{} }, wantErr: true},
		{name: "content over 500 runes", mutate: func(e *MessageEvent) { e.Content = strings.Repeat("弹", 501) }, wantErr: true},
		{name: "malformed UTF-8 content", mutate: func(e *MessageEvent) { e.Content = string([]byte{0xff}) }, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := valid
			if tt.mutate != nil {
				tt.mutate(&event)
			}
			if err := event.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
