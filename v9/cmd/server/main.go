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
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/infra"
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/queue"
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/ratelimit"
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/resilience"
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/ws"
	"github.com/redis/go-redis/v9"
)

type ServerMetrics struct {
	WebSocket ws.Metrics              `json:"websocket"`
	Kafka     *queue.PublisherMetrics `json:"kafka,omitempty"`
	Queue     map[string]string       `json:"queue"`
	Redis     map[string]string       `json:"redis"`
}

func main() {
	trafficDefaults := ratelimit.DefaultConfig()
	breakerDefaults := resilience.DefaultConfig()

	port := flag.String("port", "8080", "server port")
	workers := flag.Int("workers", ws.DefaultWorkerCount, "number of broadcast workers")
	redisWorkers := flag.Int("redis-workers", ws.DefaultRedisWorkerCount, "number of isolated Redis publish workers")
	kafkaEnabled := flag.Bool("kafka", true, "enable Kafka persistence")
	redisEnabled := flag.Bool("redis", true, "enable Redis Pub/Sub realtime distribution")
	brokersRaw := flag.String("brokers", getenv("V9_KAFKA_BROKERS", infra.DefaultKafkaBrokers), "Kafka broker list, comma separated")
	topic := flag.String("topic", getenv("V9_KAFKA_TOPIC", infra.DefaultKafkaTopic), "Kafka topic for danmaku persistence")
	redisAddr := flag.String("redis-addr", getenv("V9_REDIS_ADDR", infra.DefaultRedisAddr), "Redis address")
	maxConnections := flag.Int("max-connections", trafficDefaults.MaxConnections, "maximum WebSocket connections on this process")
	maxConnectionsPerIP := flag.Int("max-connections-per-ip", trafficDefaults.MaxConnectionsPerIP, "maximum WebSocket connections per remote IP")
	danmakuUserRate := flag.Float64("danmaku-user-rate", trafficDefaults.DanmakuPerUser.PerSecond, "danmaku tokens added per user per second")
	danmakuUserBurst := flag.Int("danmaku-user-burst", trafficDefaults.DanmakuPerUser.Burst, "maximum per-user danmaku burst")
	danmakuRoomRate := flag.Float64("danmaku-room-rate", trafficDefaults.DanmakuPerRoom.PerSecond, "danmaku tokens added per room per second")
	danmakuRoomBurst := flag.Int("danmaku-room-burst", trafficDefaults.DanmakuPerRoom.Burst, "maximum per-room danmaku burst")
	likeUserRate := flag.Float64("like-user-rate", trafficDefaults.LikePerUser.PerSecond, "like tokens added per user per second")
	likeUserBurst := flag.Int("like-user-burst", trafficDefaults.LikePerUser.Burst, "maximum per-user like burst")
	likeRoomRate := flag.Float64("like-room-rate", trafficDefaults.LikePerRoom.PerSecond, "like tokens added per room per second")
	likeRoomBurst := flag.Int("like-room-burst", trafficDefaults.LikePerRoom.Burst, "maximum per-room like burst")
	redisFailureThreshold := flag.Int("redis-breaker-failures", breakerDefaults.FailureThreshold, "consecutive Redis failures before opening the circuit")
	redisOpenTimeout := flag.Duration("redis-breaker-open", breakerDefaults.OpenTimeout, "Redis circuit open duration before one recovery probe")
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
			log.Printf("[server] Redis unavailable at startup addr=%s err=%v, start with local fallback", *redisAddr, err)
		} else {
			log.Printf("[server] Redis realtime enabled addr=%s", *redisAddr)
		}
	} else {
		log.Printf("[server] Redis realtime disabled, falling back to local-only broadcast")
	}

	var persister ws.DanmakuPersister
	if publisher != nil {
		persister = publisher
	}

	traffic := ratelimit.New(ratelimit.Config{
		MaxConnections:      *maxConnections,
		MaxConnectionsPerIP: *maxConnectionsPerIP,
		DanmakuPerUser:      ratelimit.Rate{PerSecond: *danmakuUserRate, Burst: *danmakuUserBurst},
		DanmakuPerRoom:      ratelimit.Rate{PerSecond: *danmakuRoomRate, Burst: *danmakuRoomBurst},
		LikePerUser:         ratelimit.Rate{PerSecond: *likeUserRate, Burst: *likeUserBurst},
		LikePerRoom:         ratelimit.Rate{PerSecond: *likeRoomRate, Burst: *likeRoomBurst},
	})
	var redisBreaker *resilience.CircuitBreaker
	if redisClient != nil {
		redisBreaker = resilience.NewCircuitBreaker(resilience.Config{
			FailureThreshold: *redisFailureThreshold,
			OpenTimeout:      *redisOpenTimeout,
		})
	}
	manager := ws.NewManagerWithConfig(ws.ManagerConfig{
		WorkerCount:      *workers,
		RedisWorkerCount: *redisWorkers,
		Persister:        persister,
		RedisClient:      redisClient,
		RedisBreaker:     redisBreaker,
		Traffic:          traffic,
	})
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
		log.Printf("[server] V9 listening on :%s workers=%d redis_workers=%d kafka=%v redis=%v", *port, *workers, *redisWorkers, *kafkaEnabled, *redisEnabled)
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
		redisCircuit := "disabled"
		if m.RedisCircuit != nil {
			redisCircuit = string(m.RedisCircuit.State)
		}
		if publisher == nil {
			log.Printf("[metrics] rooms=%d clients=%d ingress_drop=%d rate_user=%d rate_room=%d delivered=%d slow_drop=%d redis_circuit=%s redis_fallback=%d kafka=disabled goroutines=%d alloc=%dKB gc=%d",
				m.Rooms,
				m.Clients,
				m.IngressDropped,
				m.Traffic.DanmakuRejectedUser,
				m.Traffic.DanmakuRejectedRoom,
				m.DeliveredMessages,
				m.DroppedMessages,
				redisCircuit,
				m.RedisDegradedBroadcasts,
				m.Goroutines,
				m.AllocBytes/1024,
				m.NumGC,
			)
			continue
		}

		km := publisher.Metrics()
		log.Printf("[metrics] rooms=%d clients=%d ingress_drop=%d rate_user=%d rate_room=%d delivered=%d slow_drop=%d redis_circuit=%s redis_fallback=%d kafka_status=%s kafka_enqueued=%d kafka_acked=%d kafka_dropped=%d kafka_errors=%d goroutines=%d alloc=%dKB gc=%d",
			m.Rooms,
			m.Clients,
			m.IngressDropped,
			m.Traffic.DanmakuRejectedUser,
			m.Traffic.DanmakuRejectedRoom,
			m.DeliveredMessages,
			m.DroppedMessages,
			redisCircuit,
			m.RedisDegradedBroadcasts,
			km.Status,
			km.Enqueued,
			km.Acked,
			km.Dropped,
			km.Errors,
			m.Goroutines,
			m.AllocBytes/1024,
			m.NumGC,
		)
	}
}

func buildMetrics(manager *ws.Manager, publisher *queue.KafkaPublisher, kafkaEnabled bool, redisEnabled bool) ServerMetrics {
	managerMetrics := manager.Metrics()
	var kafkaMetrics *queue.PublisherMetrics
	if publisher != nil {
		metrics := publisher.Metrics()
		kafkaMetrics = &metrics
	}

	queueStatus := "disabled"
	if kafkaEnabled && kafkaMetrics == nil {
		queueStatus = "unavailable"
	} else if kafkaMetrics != nil {
		queueStatus = string(kafkaMetrics.Status)
	}

	redisStatus := "disabled"
	if redisEnabled {
		redisStatus = "healthy"
		if managerMetrics.RedisCircuit != nil && managerMetrics.RedisCircuit.State != resilience.StateClosed {
			redisStatus = "degraded"
		}
	}
	redisState := "disabled"
	if managerMetrics.RedisCircuit != nil {
		redisState = string(managerMetrics.RedisCircuit.State)
	}

	return ServerMetrics{
		WebSocket: managerMetrics,
		Kafka:     kafkaMetrics,
		Queue: map[string]string{
			"status": queueStatus,
		},
		Redis: map[string]string{
			"status":  redisStatus,
			"circuit": redisState,
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
