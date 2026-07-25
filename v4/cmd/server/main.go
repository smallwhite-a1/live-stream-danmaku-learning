package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v4/internal/infra"
	"github.com/charlesAcmen/livestream-danmaku/v4/internal/repo"
	"github.com/charlesAcmen/livestream-danmaku/v4/internal/store"
	"github.com/charlesAcmen/livestream-danmaku/v4/internal/ws"
	"gorm.io/gorm"
)

type ServerMetrics struct {
	WebSocket ws.Metrics           `json:"websocket"`
	DBWriter  *store.WriterMetrics `json:"db_writer,omitempty"`
	MySQL     map[string]string    `json:"mysql"`
}

func main() {
	port := flag.String("port", "8080", "server port")
	workers := flag.Int("workers", ws.DefaultWorkerCount, "number of broadcast workers")
	persist := flag.Bool("persist", true, "enable MySQL persistence")
	mysqlDSN := flag.String("mysql-dsn", getenv("V4_MYSQL_DSN", infra.DefaultMySQLDSN), "MySQL DSN")
	dbQueue := flag.Int("db-queue", store.DefaultQueueSize, "DB writer queue size")
	dbBatch := flag.Int("db-batch", store.DefaultBatchSize, "DB writer batch size")
	dbFlush := flag.Duration("db-flush", store.DefaultFlushInterval, "DB writer flush interval")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var (
		db       *gorm.DB
		msgRepo  *repo.MessageRepo
		dbWriter *store.DBWriter
	)

	if *persist {
		var err error
		db, err = infra.InitDB(*mysqlDSN)
		if err != nil {
			log.Fatalf("[server] init mysql failed: %v", err)
		}

		msgRepo = repo.NewMessageRepo(db)
		dbWriter = store.NewDBWriter(msgRepo, store.WriterConfig{
			QueueSize:     *dbQueue,
			BatchSize:     *dbBatch,
			FlushInterval: *dbFlush,
		})
		dbWriter.Start(ctx)
		log.Printf("[server] MySQL persistence enabled dsn=%s", maskDSN(*mysqlDSN))
	} else {
		log.Printf("[server] MySQL persistence disabled")
	}

	var persister ws.DanmakuPersister
	if dbWriter != nil {
		persister = dbWriter
	}

	manager := ws.NewManager(*workers, persister)
	go manager.Run()
	go logMetrics(manager, dbWriter)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ws.ServeWS(manager, w, r)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(buildMetrics(manager, dbWriter, *persist))
	})
	mux.HandleFunc("/history", func(w http.ResponseWriter, r *http.Request) {
		handleHistory(w, r, msgRepo)
	})

	server := &http.Server{
		Addr:              ":" + *port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("[server] V4 listening on :%s workers=%d persist=%v", *port, *workers, *persist)
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

	if dbWriter != nil {
		dbWriter.Wait()
	}
	if db != nil {
		sqlDB, err := db.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	}
}

func handleHistory(w http.ResponseWriter, r *http.Request, msgRepo *repo.MessageRepo) {
	if msgRepo == nil {
		http.Error(w, "persistence disabled", http.StatusServiceUnavailable)
		return
	}

	roomID := r.URL.Query().Get("room")
	if roomID == "" {
		http.Error(w, "missing room", http.StatusBadRequest)
		return
	}

	limit := 50
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
		limit = parsed
	}

	messages, err := msgRepo.ListRecentByRoom(r.Context(), roomID, limit)
	if err != nil {
		http.Error(w, "query history failed", http.StatusInternalServerError)
		return
	}

	count, err := msgRepo.CountByRoom(r.Context(), roomID)
	if err != nil {
		http.Error(w, "count history failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"room":     roomID,
		"count":    count,
		"messages": messages,
	})
}

func logMetrics(manager *ws.Manager, dbWriter *store.DBWriter) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		m := manager.Metrics()
		if dbWriter == nil {
			log.Printf("[metrics] rooms=%d clients=%d workers=%d packets=%d delivered=%d persist=disabled goroutines=%d alloc=%dKB gc=%d",
				m.Rooms,
				m.Clients,
				m.WorkerCount,
				m.BroadcastPackets,
				m.DeliveredMessages,
				m.Goroutines,
				m.AllocBytes/1024,
				m.NumGC,
			)
			continue
		}

		dbm := dbWriter.Metrics()
		log.Printf("[metrics] rooms=%d clients=%d workers=%d packets=%d delivered=%d persist_enqueued=%d persist_dropped=%d db_queue=%d/%d db_saved=%d db_flushes=%d db_failed=%d goroutines=%d alloc=%dKB gc=%d",
			m.Rooms,
			m.Clients,
			m.WorkerCount,
			m.BroadcastPackets,
			m.DeliveredMessages,
			m.PersistEnqueued,
			m.PersistDropped,
			dbm.QueueLen,
			dbm.QueueCap,
			dbm.Saved,
			dbm.Flushes,
			dbm.FailedFlushes,
			m.Goroutines,
			m.AllocBytes/1024,
			m.NumGC,
		)
	}
}

func buildMetrics(manager *ws.Manager, dbWriter *store.DBWriter, persist bool) ServerMetrics {
	var dbMetrics *store.WriterMetrics
	if dbWriter != nil {
		metrics := dbWriter.Metrics()
		dbMetrics = &metrics
	}

	status := "disabled"
	if persist {
		status = "enabled"
	}

	return ServerMetrics{
		WebSocket: manager.Metrics(),
		DBWriter:  dbMetrics,
		MySQL: map[string]string{
			"status": status,
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

func maskDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	return "***@tcp" + after(dsn, "@tcp")
}

func after(s, marker string) string {
	for i := 0; i+len(marker) <= len(s); i++ {
		if s[i:i+len(marker)] == marker {
			return s[i+len(marker):]
		}
	}
	return "(hidden)"
}
