package infra

import (
	"strings"
	"time"

	"github.com/IBM/sarama"
)

const (
	DefaultKafkaBrokers  = "127.0.0.1:9099"
	DefaultKafkaTopic    = "v10_danmaku_save_topic"
	DefaultKafkaGroupID  = "v10_danmaku_save_group"
	DefaultKafkaDLQTopic = "v10_danmaku_save_dlq"
)

func ParseBrokers(raw string) []string {
	if raw == "" {
		raw = DefaultKafkaBrokers
	}

	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			brokers = append(brokers, part)
		}
	}
	return brokers
}

func InitKafkaProducer(brokers []string) (sarama.AsyncProducer, error) {
	config := newReliableProducerConfig()
	// Keep the WebSocket/Redis path non-blocking while absorbing short Kafka bursts.
	// The bounded queue intentionally still drops under sustained overload.
	config.ChannelBufferSize = 32768
	config.Producer.Flush.Messages = 200
	config.Producer.Flush.MaxMessages = 500
	config.Producer.Flush.Bytes = 64 * 1024
	config.Producer.Flush.Frequency = 20 * time.Millisecond

	return sarama.NewAsyncProducer(brokers, config)
}

func InitKafkaSyncProducer(brokers []string) (sarama.SyncProducer, error) {
	config := newReliableProducerConfig()
	return sarama.NewSyncProducer(brokers, config)
}

func InitKafkaConsumerGroup(brokers []string, groupID string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = time.Second
	config.Consumer.Fetch.Default = 2 * 1024 * 1024
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange

	return sarama.NewConsumerGroup(brokers, groupID, config)
}

func newReliableProducerConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Net.MaxOpenRequests = 1
	config.Net.DialTimeout = 5 * time.Second
	config.Net.ReadTimeout = 5 * time.Second
	config.Net.WriteTimeout = 5 * time.Second

	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 200 * time.Millisecond
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewHashPartitioner
	return config
}
