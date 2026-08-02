package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/v10/internal/auth"
	"github.com/charlesAcmen/livestream-danmaku/v10/internal/infra"
	"github.com/charlesAcmen/livestream-danmaku/v10/internal/queue"
	"github.com/charlesAcmen/livestream-danmaku/v10/internal/ratelimit"
	"github.com/charlesAcmen/livestream-danmaku/v10/internal/repo"
	"github.com/charlesAcmen/livestream-danmaku/v10/internal/resilience"
	"github.com/charlesAcmen/livestream-danmaku/v10/internal/webapp"
	"github.com/charlesAcmen/livestream-danmaku/v10/internal/ws"
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
	webDir := flag.String("web-dir", getenv("V10_WEB_DIR", ""), "optional directory containing the built web application")
	authEnabled := flag.Bool("auth", false, "enable database-backed JWT login")
	authRequired := flag.Bool("auth-required", false, "require JWT for WebSocket and metrics requests")
	mysqlDSN := flag.String("mysql-dsn", getenv("V10_MYSQL_DSN", infra.DefaultMySQLDSN), "MySQL DSN for users")
	jwtSecret := flag.String("jwt-secret", getenv("V10_JWT_SECRET", "v10-local-development-secret-change-me-32"), "HS256 signing secret")
	jwtIssuer := flag.String("jwt-issuer", getenv("V10_JWT_ISSUER", "v10"), "JWT issuer")
	jwtTTL := flag.Duration("jwt-ttl", 15*time.Minute, "JWT access token lifetime")
	secureCookie := flag.Bool("secure-cookie", false, "set Secure on the JWT cookie")
	workers := flag.Int("workers", ws.DefaultWorkerCount, "number of broadcast workers")
	redisWorkers := flag.Int("redis-workers", ws.DefaultRedisWorkerCount, "number of isolated Redis publish workers")
	kafkaEnabled := flag.Bool("kafka", true, "enable Kafka persistence")
	redisEnabled := flag.Bool("redis", true, "enable Redis Pub/Sub realtime distribution")
	brokersRaw := flag.String("brokers", getenv("V10_KAFKA_BROKERS", infra.DefaultKafkaBrokers), "Kafka broker list, comma separated")
	topic := flag.String("topic", getenv("V10_KAFKA_TOPIC", infra.DefaultKafkaTopic), "Kafka topic for danmaku persistence")
	redisAddr := flag.String("redis-addr", getenv("V10_REDIS_ADDR", infra.DefaultRedisAddr), "Redis address")
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

	var authService *auth.Service
	var sqlDB interface{ Close() error }
	if *authEnabled {
		dbCtx, cancelDB := context.WithTimeout(ctx, 5*time.Second)
		db, err := infra.OpenDB(dbCtx, *mysqlDSN)
		cancelDB()
		if err != nil {
			log.Fatalf("[server] init auth database failed: %v", err)
		}
		dbConn, err := db.DB()
		if err != nil {
			log.Fatalf("[server] get auth database connection failed: %v", err)
		}
		sqlDB = dbConn
		userRepo := repo.NewUserRepo(db)
		authService, err = auth.New(userRepo, auth.Config{
			Secret:       *jwtSecret,
			Issuer:       *jwtIssuer,
			AccessTTL:    *jwtTTL,
			CookieName:   "v10_access_token",
			SecureCookie: *secureCookie,
		})
		if err != nil {
			log.Fatalf("[server] init auth service failed: %v", err)
		}
		log.Printf("[server] JWT login enabled issuer=%s ttl=%s", *jwtIssuer, *jwtTTL)
	} else if *authRequired {
		log.Fatalf("[server] auth-required needs auth enabled")
	}
	if sqlDB != nil {
		defer sqlDB.Close()
	}

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
		ws.ServeWSWithAuth(manager, authService, *authRequired, w, r)
	})
	if authService != nil {
		mux.HandleFunc("/auth/login", authService.LoginHandler)
		mux.HandleFunc("/auth/logout", authService.LogoutHandler)
	} else {
		mux.HandleFunc("/auth/login", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "authentication disabled", http.StatusNotFound)
		})
	}
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	metricsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(buildMetrics(manager, publisher, *kafkaEnabled, *redisEnabled))
	})
	if authService != nil && *authRequired {
		mux.Handle("/metrics", authService.Require(metricsHandler))
	} else {
		mux.Handle("/metrics", metricsHandler)
	}
	if err := registerWebFrontend(mux, *webDir); err != nil {
		log.Printf("[server] web frontend unavailable dir=%s err=%v", *webDir, err)
	} else if strings.TrimSpace(*webDir) != "" {
		log.Printf("[server] web frontend enabled dir=%s", *webDir)
	}

	server := &http.Server{
		Addr:              ":" + *port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("[server] V10 listening on :%s workers=%d redis_workers=%d kafka=%v redis=%v", *port, *workers, *redisWorkers, *kafkaEnabled, *redisEnabled)
		log.Printf("[server] WebSocket endpoint: ws://127.0.0.1:%s/ws?room=room1&token=<access-token>", *port)
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

func registerWebFrontend(mux *http.ServeMux, webDir string) error {
	if strings.TrimSpace(webDir) == "" {
		return nil
	}

	handler, err := webapp.NewHandler(webDir)
	if err != nil {
		return err
	}
	mux.Handle("/", handler)
	return nil
}
