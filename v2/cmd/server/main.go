package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v2/internal/ws"
)

func main() {
	port := flag.String("port", "8080", "server port")
	workers := flag.Int("workers", ws.DefaultWorkerCount, "number of broadcast workers")
	flag.Parse()

	manager := ws.NewManager(*workers)
	go manager.Run()
	go logMetrics(manager)

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ws.ServeWS(manager, w, r)
	})
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(manager.Metrics())
	})

	addr := ":" + *port
	log.Printf("[server] V2 listening on %s workers=%d", addr, *workers)
	log.Printf("[server] WebSocket endpoint: ws://127.0.0.1:%s/ws?uid=1001&name=alice&room=room1", *port)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("[server] stopped: %v", err)
	}
}

func logMetrics(manager *ws.Manager) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		m := manager.Metrics()
		log.Printf("[metrics] rooms=%d clients=%d workers=%d packets=%d jobs=%d dropped_jobs=%d delivered=%d dropped_messages=%d",
			m.Rooms,
			m.Clients,
			m.WorkerCount,
			m.BroadcastPackets,
			m.EnqueuedJobs,
			m.DroppedJobs,
			m.DeliveredMessages,
			m.DroppedMessages,
		)
	}
}
