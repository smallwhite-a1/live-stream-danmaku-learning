package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/model"
)

// KafkaDeadLetterPublisher uses a synchronous producer because an invalid
// source message must not be marked consumed before Kafka acknowledges the DLQ.
type KafkaDeadLetterPublisher struct {
	producer sarama.SyncProducer
	topic    string
}

func NewKafkaDeadLetterPublisher(producer sarama.SyncProducer, topic string) *KafkaDeadLetterPublisher {
	return &KafkaDeadLetterPublisher{producer: producer, topic: topic}
}

func (p *KafkaDeadLetterPublisher) Publish(ctx context.Context, record model.DeadLetter) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal dead letter: %w", err)
	}

	key := fmt.Sprintf("%s:%d:%d", record.SourceTopic, record.SourcePartition, record.SourceOffset)
	_, _, err = p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(payload),
	})
	if err != nil {
		return fmt.Errorf("publish dead letter: %w", err)
	}
	return nil
}

func (p *KafkaDeadLetterPublisher) Close() error {
	return p.producer.Close()
}
