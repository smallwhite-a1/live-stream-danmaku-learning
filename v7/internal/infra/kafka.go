package infra

import (
	"strings"
	"time"

	"github.com/IBM/sarama"
)

const (
	DefaultKafkaBrokers = "127.0.0.1:9096"
	DefaultKafkaTopic   = "v7_danmaku_save_topic"
	DefaultKafkaGroupID = "v7_danmaku_save_group"
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
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.ChannelBufferSize = 4096

	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Messages = 50
	config.Producer.Flush.MaxMessages = 200
	config.Producer.Flush.Bytes = 8 * 1024
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewHashPartitioner

	return sarama.NewAsyncProducer(brokers, config)
}

func InitKafkaConsumerGroup(brokers []string, groupID string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Fetch.Default = 2 * 1024 * 1024
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange

	return sarama.NewConsumerGroup(brokers, groupID, config)
}
