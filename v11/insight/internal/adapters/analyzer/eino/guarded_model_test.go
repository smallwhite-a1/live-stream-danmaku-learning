package eino

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestGuardedModelLimitsConcurrentCalls(t *testing.T) {
	delegate := &FakeModel{Delay: 30 * time.Millisecond}
	model, err := NewGuardedModel(delegate, GuardConfig{MaxInFlight: 2, Timeout: time.Second, FailureThreshold: 5, OpenFor: time.Second})
	if err != nil {
		t.Fatalf("NewGuardedModel() error = %v", err)
	}

	var workers sync.WaitGroup
	workers.Add(8)
	for range 8 {
		go func() {
			defer workers.Done()
			if _, err := model.Complete(context.Background(), CompletionRequest{}); err != nil {
				t.Errorf("Complete() error = %v", err)
			}
		}()
	}
	workers.Wait()
	if got := delegate.Snapshot().MaxInFlight; got > 2 {
		t.Fatalf("delegate max in-flight = %d, want at most 2", got)
	}
}

func TestGuardedModelAppliesTimeoutAndOpensCircuit(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		model, err := NewGuardedModel(&FakeModel{Delay: time.Hour}, GuardConfig{MaxInFlight: 1, Timeout: 10 * time.Millisecond, FailureThreshold: 5, OpenFor: time.Second})
		if err != nil {
			t.Fatalf("NewGuardedModel() error = %v", err)
		}
		_, err = model.Complete(context.Background(), CompletionRequest{})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Complete() error = %v, want deadline exceeded", err)
		}
	})

	t.Run("circuit", func(t *testing.T) {
		model, err := NewGuardedModel(&FakeModel{Err: errors.New("upstream unavailable")}, GuardConfig{MaxInFlight: 1, Timeout: time.Second, FailureThreshold: 2, OpenFor: time.Second})
		if err != nil {
			t.Fatalf("NewGuardedModel() error = %v", err)
		}
		for range 2 {
			if _, err := model.Complete(context.Background(), CompletionRequest{}); err == nil {
				t.Fatal("Complete() error = nil, want upstream failure")
			}
		}
		if _, err := model.Complete(context.Background(), CompletionRequest{}); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("Complete() error = %v, want ErrCircuitOpen", err)
		}
	})
}
