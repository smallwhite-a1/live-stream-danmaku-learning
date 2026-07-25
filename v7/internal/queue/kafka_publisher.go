package queue

import (
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/v7/internal/model"
)

type PublisherMetrics struct {
	Enqueued uint64 `json:"enqueued"`
	Dropped  uint64 `json:"dropped"`
	Errors   uint64 `json:"errors"`
}

type KafkaPublisher struct {
	producer sarama.AsyncProducer
	topic    string

	wg sync.WaitGroup

	enqueued atomic.Uint64
	dropped  atomic.Uint64
	errors   atomic.Uint64
}

func NewKafkaPublisher(producer sarama.AsyncProducer, topic string) *KafkaPublisher {
	p := &KafkaPublisher{
		producer: producer,
		topic:    topic,
	}

	p.wg.Add(1)
	go p.drainErrors()

	return p
}

func (p *KafkaPublisher) Enqueue(message *model.Danmaku) bool {
	data, err := json.Marshal(message)
	if err != nil {
		p.errors.Add(1)
		log.Printf("[kafka-publisher] marshal danmaku failed: %v", err)
		return false
	}

	packet := model.Packet{
		Type:   model.TypeDanmaku,
		RoomID: message.RoomID,
		Data:   data,
	}
	payload, err := json.Marshal(packet)
	if err != nil {
		p.errors.Add(1)
		log.Printf("[kafka-publisher] marshal packet failed: %v", err)
		return false
	}

	kafkaMsg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(message.RoomID),
		Value: sarama.ByteEncoder(payload),
	}

	select {
	case p.producer.Input() <- kafkaMsg:
		p.enqueued.Add(1)
		return true
	default:
		p.dropped.Add(1)
		return false
	}
}

func (p *KafkaPublisher) Metrics() PublisherMetrics {
	return PublisherMetrics{
		Enqueued: p.enqueued.Load(),
		Dropped:  p.dropped.Load(),
		Errors:   p.errors.Load(),
	}
}

func (p *KafkaPublisher) Close() error {
	err := p.producer.Close()
	p.wg.Wait()
	return err
}

func (p *KafkaPublisher) drainErrors() {
	defer p.wg.Done()

	for err := range p.producer.Errors() {
		p.errors.Add(1)
		log.Printf("[kafka-publisher] async write error: %v", err.Err)
	}
}
