package ws

import (
	"log"
	"net/http"
	"strings"

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
