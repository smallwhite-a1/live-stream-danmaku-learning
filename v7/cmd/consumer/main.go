package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v7/internal/consumer"
	"github.com/charlesAcmen/livestream-danmaku/v7/internal/infra"
	"github.com/charlesAcmen/livestream-danmaku/v7/internal/repo"
)

func main() {
	brokersRaw := flag.String("brokers", getenv("V7_KAFKA_BROKERS", infra.DefaultKafkaBrokers), "Kafka broker list, comma separated")
	topic := flag.String("topic", getenv("V7_KAFKA_TOPIC", infra.DefaultKafkaTopic), "Kafka topic")
	groupID := flag.String("group", getenv("V7_KAFKA_GROUP", infra.DefaultKafkaGroupID), "Kafka consumer group id")
	mysqlDSN := flag.String("mysql-dsn", getenv("V7_MYSQL_DSN", infra.DefaultMySQLDSN), "MySQL DSN")
	batchSize := flag.Int("batch", consumer.DefaultBatchSize, "MySQL insert batch size")
	flushInterval := flag.Duration("flush", consumer.DefaultFlushInterval, "batch flush interval")
	flag.Parse()

	db, err := infra.InitDB(*mysqlDSN)
	if err != nil {
		log.Fatalf("[consumer] init mysql failed: %v", err)
	}
	sqlDB, err := db.DB()
	if err == nil {
		defer sqlDB.Close()
	}

	messageRepo := repo.NewMessageRepo(db)
	handler := consumer.NewHandler(messageRepo, consumer.Config{
		BatchSize:     *batchSize,
		FlushInterval: *flushInterval,
	})

	brokers := infra.ParseBrokers(*brokersRaw)
	group, err := infra.InitKafkaConsumerGroup(brokers, *groupID)
	if err != nil {
		log.Fatalf("[consumer] init kafka group failed: %v", err)
	}
	defer group.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go logMetrics(ctx, handler)
	go drainGroupErrors(ctx, group)

	log.Printf("[consumer] started brokers=%v topic=%s group=%s batch=%d flush=%s", brokers, *topic, *groupID, *batchSize, *flushInterval)

	for {
		if err := group.Consume(ctx, []string{*topic}, handler); err != nil {
			log.Printf("[consumer] consume error: %v", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
		if ctx.Err() != nil {
			break
		}
	}

	log.Printf("[consumer] stopped")
}

func logMetrics(ctx context.Context, handler *consumer.Handler) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m := handler.Metrics()
			log.Printf("[consumer metrics] parsed=%d saved=%d batches=%d skipped=%d failed_batches=%d",
				m.Parsed,
				m.Saved,
				m.Batches,
				m.Skipped,
				m.FailedBatches,
			)
		}
	}
}

func drainGroupErrors(ctx context.Context, group interface{ Errors() <-chan error }) {
	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-group.Errors():
			if !ok {
				return
			}
			log.Printf("[consumer] group error: %v", err)
		}
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
