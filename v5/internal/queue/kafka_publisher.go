package queue

import (
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"

	"github.com/IBM/sarama"
	"github.com/charlesAcmen/livestream-danmaku/v5/internal/model"
)

type PublisherMetrics struct {
	Enqueued uint64 `json:"enqueued"` // 成功放进 producer内部队列的次数
	Dropped  uint64 `json:"dropped"`  // 队列满，丢弃的次数
	Errors   uint64 `json:"errors"`   // producer内部队列写入失败的次数
}

type KafkaPublisher struct {
	// 往 producer.Input通道中塞信息，Sarma内部有后台goroutine会从这个通道中取出信息，写入到producer内部队列中
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
		Key:   sarama.StringEncoder(message.RoomID), // 相同Key的消息去往同一个分区，即同房间的弹幕进入同一分区
		Value: sarama.ByteEncoder(payload),
	}

	select {
	case p.producer.Input() <- kafkaMsg:
		p.enqueued.Add(1)
		return true
	default:
		p.dropped.Add(1) // 通道满了，丢弃消息
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
