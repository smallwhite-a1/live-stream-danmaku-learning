package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v6/internal/model"
	"github.com/gorilla/websocket"
)

type counters struct {
	connected atomic.Int64
	sent      atomic.Int64
	received  atomic.Int64
	errors    atomic.Int64
}

func main() {
	port := flag.String("port", "8080", "server port")
	clients := flag.Int("clients", 200, "number of websocket clients")
	room := flag.String("room", "room1", "room id")
	activeRatio := flag.Float64("active", 0.1, "ratio of clients that send messages")
	interval := flag.Duration("interval", time.Second, "average send interval for active clients")
	duration := flag.Duration("duration", 30*time.Second, "benchmark duration")
	ramp := flag.Duration("ramp", 5*time.Millisecond, "delay between starting clients")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	var stats counters
	go report(ctx, &stats)

	host := "127.0.0.1:" + *port
	var wg sync.WaitGroup
	rampTimer := time.NewTicker(*ramp)
	defer rampTimer.Stop()

launchClients:
	for i := 0; i < *clients; i++ {
		select {
		case <-ctx.Done():
			break launchClients
		case <-rampTimer.C:
		}

		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runBot(ctx, host, *room, id, *activeRatio, *interval, &stats)
		}(i)
	}

	<-ctx.Done()
	wg.Wait()

	log.Printf("[benchmark] done connected=%d sent=%d received=%d errors=%d",
		stats.connected.Load(),
		stats.sent.Load(),
		stats.received.Load(),
		stats.errors.Load(),
	)
}

func runBot(ctx context.Context, host, room string, id int, activeRatio float64, avgInterval time.Duration, stats *counters) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	u := url.URL{Scheme: "ws", Host: host, Path: "/ws"}
	q := u.Query()
	q.Set("uid", fmt.Sprintf("bot-%d", id))
	q.Set("name", fmt.Sprintf("bot-%d", id))
	q.Set("room", room)
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		stats.errors.Add(1)
		return
	}
	stats.connected.Add(1)
	defer func() {
		stats.connected.Add(-1)
		_ = conn.Close()
	}()

	done := make(chan struct{})
	go readLoop(ctx, conn, done, stats)

	if rng.Float64() >= activeRatio {
		select {
		case <-ctx.Done():
		case <-done:
		}
		return
	}

	for {
		sleep := time.Duration(float64(avgInterval) * (0.5 + rng.Float64()))
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
			return
		case <-done:
			timer.Stop()
			return
		case <-timer.C:
			if err := sendDanmaku(conn, fmt.Sprintf("hello from bot-%d at %s", id, time.Now().Format("15:04:05.000"))); err != nil {
				stats.errors.Add(1)
				return
			}
			stats.sent.Add(1)
		}
	}
}

func readLoop(ctx context.Context, conn *websocket.Conn, done chan<- struct{}, stats *counters) {
	defer close(done)

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				stats.errors.Add(1)
			}
			return
		}

		var packet model.Packet
		if err := json.Unmarshal(raw, &packet); err != nil {
			continue
		}
		if packet.Type == model.TypeDanmaku {
			stats.received.Add(1)
		}
	}
}

func sendDanmaku(conn *websocket.Conn, content string) error {
	payload, err := json.Marshal(model.Danmaku{Content: content})
	if err != nil {
		return err
	}

	packet := model.Packet{
		Type: model.TypeDanmaku,
		Data: payload,
	}
	return conn.WriteJSON(packet)
}

func report(ctx context.Context, stats *counters) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastSent, lastReceived int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sent := stats.sent.Load()
			received := stats.received.Load()
			log.Printf("[benchmark] connected=%d sent/s=%d recv/s=%d total_sent=%d total_recv=%d errors=%d",
				stats.connected.Load(),
				sent-lastSent,
				received-lastReceived,
				sent,
				received,
				stats.errors.Load(),
			)
			lastSent = sent
			lastReceived = received
		}
	}
}
