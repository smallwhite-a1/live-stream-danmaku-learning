package ws

import (
	"log"
	"net"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/charlesAcmen/livestream-danmaku/v9/internal/model"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func ServeWS(manager *Manager, w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(r.URL.Query().Get("uid"))
	username := strings.TrimSpace(r.URL.Query().Get("name"))
	roomID := strings.TrimSpace(r.URL.Query().Get("room"))

	if userID == "" || roomID == "" {
		http.Error(w, "missing uid or room", http.StatusBadRequest)
		return
	}
	if username == "" {
		username = userID
	}
	if utf8.RuneCountInString(userID) > model.MaxUserIDRunes ||
		utf8.RuneCountInString(username) > model.MaxUsernameRunes ||
		utf8.RuneCountInString(roomID) > model.MaxRoomIDRunes {
		http.Error(w, "uid, name, or room is too long", http.StatusBadRequest)
		return
	}

	release, ok := manager.AcquireConnection(remoteIP(r))
	if !ok {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "connection limit reached", http.StatusTooManyRequests)
		return
	}
	defer release()

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ws] upgrade failed: %v", err)
		return
	}

	client := NewClient(userID, username, roomID, conn)
	manager.Register <- client

	go client.WritePump()
	client.ReadPump(manager)
}

func remoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	return r.RemoteAddr
}
