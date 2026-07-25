package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/charlesAcmen/livestream-danmaku/v1/internal/ws"
)

func main() {
	port := flag.String("port", "8080", "server port")
	flag.Parse()
	// 调度器
	manager := ws.NewManager()
	go manager.Run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ws.ServeWS(manager, w, r)
	})
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})

	addr := ":" + *port
	log.Printf("[server] V1 listening on %s", addr)
	log.Printf("[server] WebSocket endpoint: ws://127.0.0.1:%s/ws?uid=1001&name=alice&room=room1", *port)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("[server] stopped: %v", err)
	}
}
