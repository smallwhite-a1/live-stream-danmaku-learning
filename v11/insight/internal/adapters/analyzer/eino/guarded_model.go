package eino

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("model circuit is open")

type GuardConfig struct {
	MaxInFlight      int
	Timeout          time.Duration
	FailureThreshold int
	OpenFor          time.Duration
}

type GuardedModel struct {
	delegate CompletionModel
	config   GuardConfig
	sem      chan struct{}

	mu              sync.Mutex
	consecutiveFail int
	openUntil       time.Time
}

func NewGuardedModel(delegate CompletionModel, config GuardConfig) (*GuardedModel, error) {
	if delegate == nil {
		return nil, errors.New("completion model is required")
	}
	if config.MaxInFlight <= 0 || config.Timeout <= 0 || config.FailureThreshold <= 0 || config.OpenFor <= 0 {
		return nil, errors.New("guard configuration must be positive")
	}
	return &GuardedModel{delegate: delegate, config: config, sem: make(chan struct{}, config.MaxInFlight)}, nil
}

func (m *GuardedModel) Complete(ctx context.Context, request CompletionRequest) (CompletionResponse, error) {
	if err := m.beforeCall(time.Now()); err != nil {
		return CompletionResponse{}, err
	}
	select {
	case m.sem <- struct{}{}:
	case <-ctx.Done():
		return CompletionResponse{}, ctx.Err()
	}
	defer func() { <-m.sem }()

	callCtx, cancel := context.WithTimeout(ctx, m.config.Timeout)
	defer cancel()
	response, err := m.delegate.Complete(callCtx, request)
	m.recordOutcome(time.Now(), err)
	return response, err
}

func (m *GuardedModel) beforeCall(now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if now.Before(m.openUntil) {
		return ErrCircuitOpen
	}
	return nil
}

func (m *GuardedModel) recordOutcome(now time.Time, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		m.consecutiveFail = 0
		return
	}
	m.consecutiveFail++
	if m.consecutiveFail >= m.config.FailureThreshold {
		m.openUntil = now.Add(m.config.OpenFor)
		m.consecutiveFail = 0
	}
}
