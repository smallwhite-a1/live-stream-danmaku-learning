package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/v7/internal/infra"
	"github.com/charlesAcmen/livestream-danmaku/v7/internal/queue"
	"github.com/charlesAcmen/livestream-danmaku/v7/internal/ws"
	"github.com/redis/go-redis/v9"
)

type ServerMetrics struct {
	WebSocket ws.Metrics              `json:"websocket"`
	Kafka     *queue.PublisherMetrics `json:"kafka,omitempty"`
	Queue     map[string]string       `json:"queue"`
	Redis     map[string]string       `json:"redis"`
}

func main() {
	port := flag.String("port", "8080", "server port")
	workers := flag.Int("workers", ws.DefaultWorkerCount, "number of broadcast workers")
	kafkaEnabled := flag.Bool("kafka", true, "enable Kafka persistence")
	redisEnabled := flag.Bool("redis", true, "enable Redis Pub/Sub realtime distribution")
	brokersRaw := flag.String("brokers", getenv("V7_KAFKA_BROKERS", infra.DefaultKafkaBrokers), "Kafka broker list, comma separated")
	topic := flag.String("topic", getenv("V7_KAFKA_TOPIC", infra.DefaultKafkaTopic), "Kafka topic for danmaku persistence")
	redisAddr := flag.String("redis-addr", getenv("V7_REDIS_ADDR", infra.DefaultRedisAddr), "Redis address")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var (
		producer    sarama.AsyncProducer
		publisher   *queue.KafkaPublisher
		redisClient *redis.Client
	)

	if *kafkaEnabled {
		brokers := infra.ParseBrokers(*brokersRaw)
		var err error
		producer, err = infra.InitKafkaProducer(brokers)
		if err != nil {
			log.Fatalf("[server] init kafka producer failed: %v", err)
		}
		publisher = queue.NewKafkaPublisher(producer, *topic)
		log.Printf("[server] Kafka persistence enabled brokers=%v topic=%s", brokers, *topic)
	} else {
		log.Printf("[server] Kafka persistence disabled")
	}

	if *redisEnabled {
		redisClient = infra.InitRedisClient(*redisAddr)
		if err := infra.PingRedis(ctx, redisClient); err != nil {
			log.Fatalf("[server] init redis failed addr=%s err=%v", *redisAddr, err)
		}
		log.Printf("[server] Redis realtime enabled addr=%s", *redisAddr)
	} else {
		log.Printf("[server] Redis realtime disabled, falling back to local-only broadcast")
	}

	var persister ws.DanmakuPersister
	if publisher != nil {
		persister = publisher
	}

	manager := ws.NewManager(*workers, persister, redisClient)
	go manager.Run()
	go logMetrics(manager, publisher)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ws.ServeWS(manager, w, r)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(buildMetrics(manager, publisher, *kafkaEnabled, *redisEnabled))
	})

	server := &http.Server{
		Addr:              ":" + *port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("[server] V7 listening on :%s workers=%d kafka=%v redis=%v", *port, *workers, *kafkaEnabled, *redisEnabled)
		log.Printf("[server] WebSocket endpoint: ws://127.0.0.1:%s/ws?uid=1001&name=alice&room=room1", *port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[server] stopped: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("[server] shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[server] shutdown error: %v", err)
	}

	if publisher != nil {
		if err := publisher.Close(); err != nil {
			log.Printf("[server] close kafka publisher failed: %v", err)
		}
	} else if producer != nil {
		if err := producer.Close(); err != nil {
			log.Printf("[server] close kafka producer failed: %v", err)
		}
	}
	if redisClient != nil {
		if err := redisClient.Close(); err != nil {
			log.Printf("[server] close redis failed: %v", err)
		}
	}
}

func logMetrics(manager *ws.Manager, publisher *queue.KafkaPublisher) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		m := manager.Metrics()
		if publisher == nil {
			log.Printf("[metrics] rooms=%d clients=%d workers=%d local_fanout=%d delivered=%d stats=%d likes=%d online_reports=%d redis_pub=%d redis_recv=%d kafka=disabled goroutines=%d alloc=%dKB gc=%d",
				m.Rooms,
				m.Clients,
				m.WorkerCount,
				m.LocalFanoutPackets,
				m.DeliveredMessages,
				m.StatsBroadcasts,
				m.LikeEvents,
				m.OnlineReports,
				m.RedisPublished,
				m.RedisReceived,
				m.Goroutines,
				m.AllocBytes/1024,
				m.NumGC,
			)
			continue
		}

		km := publisher.Metrics()
		log.Printf("[metrics] rooms=%d clients=%d workers=%d local_fanout=%d delivered=%d stats=%d likes=%d online_reports=%d redis_pub=%d redis_recv=%d kafka_enqueued=%d kafka_dropped=%d kafka_errors=%d goroutines=%d alloc=%dKB gc=%d",
			m.Rooms,
			m.Clients,
			m.WorkerCount,
			m.LocalFanoutPackets,
			m.DeliveredMessages,
			m.StatsBroadcasts,
			m.LikeEvents,
			m.OnlineReports,
			m.RedisPublished,
			m.RedisReceived,
			km.Enqueued,
			km.Dropped,
			km.Errors,
			m.Goroutines,
			m.AllocBytes/1024,
			m.NumGC,
		)
	}
}

func buildMetrics(manager *ws.Manager, publisher *queue.KafkaPublisher, kafkaEnabled bool, redisEnabled bool) ServerMetrics {
	var kafkaMetrics *queue.PublisherMetrics
	if publisher != nil {
		metrics := publisher.Metrics()
		kafkaMetrics = &metrics
	}

	queueStatus := "disabled"
	if kafkaEnabled {
		queueStatus = "enabled"
	}

	redisStatus := "disabled"
	if redisEnabled {
		redisStatus = "enabled"
	}

	return ServerMetrics{
		WebSocket: manager.Metrics(),
		Kafka:     kafkaMetrics,
		Queue: map[string]string{
			"status": queueStatus,
		},
		Redis: map[string]string{
			"status": redisStatus,
		},
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
