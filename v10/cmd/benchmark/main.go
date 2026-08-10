package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v10/internal/model"
	"github.com/gorilla/websocket"
)

type counters struct {
	connected      atomic.Int64
	totalConnected atomic.Int64
	sent           atomic.Int64
	received       atomic.Int64
	rateLimited    atomic.Int64
	overloaded     atomic.Int64
	errors         atomic.Int64
}

const (
	latencyBucketWidth = 10 * time.Microsecond
	latencyMax         = 10 * time.Second
	latencyBucketCount = int(latencyMax/latencyBucketWidth) + 1
)

type latencyStats struct {
	count   atomic.Uint64
	buckets [latencyBucketCount]atomic.Uint64
}

type latencySummary struct {
	Count int
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
}

func (s *latencyStats) Record(latency time.Duration) {
	if latency < 0 {
		return
	}
	index := int((latency + latencyBucketWidth - 1) / latencyBucketWidth)
	if index >= latencyBucketCount {
		index = latencyBucketCount - 1
	}
	s.buckets[index].Add(1)
	s.count.Add(1)
}

func (s *latencyStats) Summary() latencySummary {
	count := s.count.Load()
	if count == 0 {
		return latencySummary{}
	}
	return latencySummary{
		Count: int(count),
		P50:   s.percentile(count, 0.50),
		P95:   s.percentile(count, 0.95),
		P99:   s.percentile(count, 0.99),
	}
}

func (s *latencyStats) percentile(count uint64, percentile float64) time.Duration {
	rank := uint64(math.Ceil(float64(count) * percentile))
	var seen uint64
	for index := range s.buckets {
		seen += s.buckets[index].Load()
		if seen >= rank {
			return time.Duration(index) * latencyBucketWidth
		}
	}
	return latencyMax
}

func main() {
	port := flag.String("port", "8080", "server port")
	clients := flag.Int("clients", 200, "number of websocket clients")
	room := flag.String("room", "room1", "room id")
	rooms := flag.Int("rooms", 1, "number of rooms; clients are spread across room-N when greater than 1")
	token := flag.String("token", "", "JWT access token for all benchmark clients")
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
	var latencies latencyStats
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
			roomID := *room
			if *rooms > 1 {
				roomID = fmt.Sprintf("%s-%d", *room, id%*rooms)
			}
			runBot(ctx, host, roomID, id, *token, *activeRatio, *interval, &stats, &latencies)
		}(i)
	}

	<-ctx.Done()
	wg.Wait()

	latency := latencies.Summary()
	log.Printf("[benchmark] done total_connected=%d sent=%d received=%d rate_limited=%d overloaded=%d errors=%d latency_samples=%d latency_p50=%s latency_p95=%s latency_p99=%s",
		stats.totalConnected.Load(),
		stats.sent.Load(),
		stats.received.Load(),
		stats.rateLimited.Load(),
		stats.overloaded.Load(),
		stats.errors.Load(),
		latency.Count,
		latency.P50,
		latency.P95,
		latency.P99,
	)
}

func runBot(ctx context.Context, host, room string, id int, token string, activeRatio float64, avgInterval time.Duration, stats *counters, latencies *latencyStats) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	u := url.URL{Scheme: "ws", Host: host, Path: "/ws"}
	q := u.Query()
	q.Set("uid", fmt.Sprintf("bot-%d", id))
	q.Set("name", fmt.Sprintf("bot-%d", id))
	q.Set("room", room)
	if token != "" {
		q.Set("token", token)
	}
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		stats.errors.Add(1)
		return
	}
	stats.connected.Add(1)
	stats.totalConnected.Add(1)
	defer func() {
		stats.connected.Add(-1)
		_ = conn.Close()
	}()

	done := make(chan struct{})
	go readLoop(ctx, conn, done, stats, latencies)

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

func readLoop(ctx context.Context, conn *websocket.Conn, done chan<- struct{}, stats *counters, latencies *latencyStats) {
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
		switch packet.Type {
		case model.TypeDanmaku:
			stats.received.Add(1)
		case model.TypeControl:
			var control model.ControlData
			if err := json.Unmarshal(packet.Data, &control); err != nil {
				continue
			}
			switch control.Code {
			case "rate_limited":
				stats.rateLimited.Add(1)
			case "server_overloaded":
				stats.overloaded.Add(1)
			}
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
			log.Printf("[benchmark] connected=%d sent/s=%d recv/s=%d total_sent=%d total_recv=%d rate_limited=%d overloaded=%d errors=%d",
				stats.connected.Load(),
				sent-lastSent,
				received-lastReceived,
				sent,
				received,
				stats.rateLimited.Load(),
				stats.overloaded.Load(),
				stats.errors.Load(),
			)
			lastSent = sent
			lastReceived = received
		}
	}
}
