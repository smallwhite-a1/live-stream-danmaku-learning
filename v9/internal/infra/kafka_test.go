package infra

import (
	"testing"

	"github.com/IBM/sarama"
)

func TestReliableProducerConfigIsValid(t *testing.T) {
	config := newReliableProducerConfig()

	if err := config.Validate(); err != nil {
		t.Fatalf("producer config is invalid: %v", err)
	}
	if !config.Producer.Idempotent {
		t.Fatal("idempotent producer is disabled")
	}
	if config.Producer.RequiredAcks != sarama.WaitForAll {
		t.Fatalf("required acks = %d, want WaitForAll", config.Producer.RequiredAcks)
	}
	if config.Net.MaxOpenRequests != 1 {
		t.Fatalf("max open requests = %d, want 1", config.Net.MaxOpenRequests)
	}
	if !config.Producer.Return.Successes || !config.Producer.Return.Errors {
		t.Fatal("producer result channels must both be enabled")
	}
}
