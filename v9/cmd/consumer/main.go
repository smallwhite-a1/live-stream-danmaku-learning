package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v9/internal/consumer"
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/infra"
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/queue"
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/repo"
)

func main() {
	brokersRaw := flag.String("brokers", getenv("V9_KAFKA_BROKERS", infra.DefaultKafkaBrokers), "Kafka broker list, comma separated")
	topic := flag.String("topic", getenv("V9_KAFKA_TOPIC", infra.DefaultKafkaTopic), "Kafka topic")
	dlqTopic := flag.String("dlq-topic", getenv("V9_KAFKA_DLQ_TOPIC", infra.DefaultKafkaDLQTopic), "Kafka dead letter topic")
	groupID := flag.String("group", getenv("V9_KAFKA_GROUP", infra.DefaultKafkaGroupID), "Kafka consumer group id")
	mysqlDSN := flag.String("mysql-dsn", getenv("V9_MYSQL_DSN", infra.DefaultMySQLDSN), "MySQL DSN")
	batchSize := flag.Int("batch", consumer.DefaultBatchSize, "MySQL insert batch size")
	flushInterval := flag.Duration("flush", consumer.DefaultFlushInterval, "batch flush interval")
	flushTimeout := flag.Duration("flush-timeout", consumer.DefaultFlushTimeout, "final batch flush timeout")
	maxRetries := flag.Int("max-retries", consumer.DefaultMaxRetries, "MySQL batch retry count")
	recoveryMin := flag.Duration("recovery-min", consumer.DefaultRecoveryMin, "minimum wait after MySQL retries are exhausted")
	recoveryMax := flag.Duration("recovery-max", consumer.DefaultRecoveryMax, "maximum MySQL recovery wait")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbCtx, cancelDB := context.WithTimeout(ctx, 5*time.Second)
	db, err := infra.OpenDB(dbCtx, *mysqlDSN)
	cancelDB()
	if err != nil {
		log.Fatalf("[consumer] init mysql failed: %v", err)
	}
	sqlDB, err := db.DB()
	if err == nil {
		defer sqlDB.Close()
	}

	brokers := infra.ParseBrokers(*brokersRaw)
	dlqProducer, err := infra.InitKafkaSyncProducer(brokers)
	if err != nil {
		log.Fatalf("[consumer] init DLQ producer failed: %v", err)
	}
	dlqPublisher := queue.NewKafkaDeadLetterPublisher(dlqProducer, *dlqTopic)
	defer func() {
		if err := dlqPublisher.Close(); err != nil {
			log.Printf("[consumer] close DLQ producer failed: %v", err)
		}
	}()

	messageRepo := repo.NewMessageRepo(db)
	handler := consumer.NewHandler(messageRepo, dlqPublisher, consumer.Config{
		BatchSize:          *batchSize,
		FlushInterval:      *flushInterval,
		FlushTimeout:       *flushTimeout,
		MaxRetries:         *maxRetries,
		RecoveryBackoffMin: *recoveryMin,
		RecoveryBackoffMax: *recoveryMax,
	})

	group, err := infra.InitKafkaConsumerGroup(brokers, *groupID)
	if err != nil {
		log.Fatalf("[consumer] init kafka group failed: %v", err)
	}
	defer group.Close()

	go logMetrics(ctx, handler)
	go drainGroupErrors(ctx, group)

	log.Printf("[consumer] started brokers=%v topic=%s dlq=%s group=%s batch=%d flush=%s recovery=%s..%s",
		brokers, *topic, *dlqTopic, *groupID, *batchSize, *flushInterval, *recoveryMin, *recoveryMax)

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
			log.Printf("[consumer metrics] parsed=%d saved=%d inserted=%d duplicates=%d batches=%d skipped=%d dlq=%d dlq_failures=%d retries=%d failed_batches=%d paused=%d pause_events=%d recoveries=%d recovery_wait_ms=%d last_batch_ms=%d",
				m.Parsed,
				m.Saved,
				m.Inserted,
				m.Duplicates,
				m.Batches,
				m.Skipped,
				m.DeadLetters,
				m.DeadLetterFailures,
				m.RetryAttempts,
				m.FailedBatches,
				m.PausedPartitions,
				m.PauseEvents,
				m.Recoveries,
				m.RecoveryWaitMillis,
				m.LastBatchMillis,
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
