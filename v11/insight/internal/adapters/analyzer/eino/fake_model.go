package eino

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type FakeModel struct {
	Provider     string
	Model        string
	InputTokens  int
	OutputTokens int
	Latency      time.Duration
	Response     string
	Err          error
	Delay        time.Duration
	InvalidJSON  bool
	// Failure, DelayFor and InvalidJSONFor make load tests deterministic by call number.
	Failure        func(call int) error
	DelayFor       func(call int) time.Duration
	InvalidJSONFor func(call int) bool
	calls          atomic.Int64
	active         atomic.Int64
	maxActive      atomic.Int64
	latencyMu      sync.Mutex
	latencies      []time.Duration
}

type FakeModelSnapshot struct {
	Calls       int
	InFlight    int
	MaxInFlight int
	Latencies   []time.Duration
}

func (m *FakeModel) Complete(ctx context.Context, request CompletionRequest) (CompletionResponse, error) {
	started := time.Now()
	defer func() {
		m.latencyMu.Lock()
		m.latencies = append(m.latencies, time.Since(started))
		m.latencyMu.Unlock()
	}()
	if err := ctx.Err(); err != nil {
		return CompletionResponse{}, err
	}
	call := int(m.calls.Add(1))
	active := m.active.Add(1)
	for {
		current := m.maxActive.Load()
		if active <= current || m.maxActive.CompareAndSwap(current, active) {
			break
		}
	}
	defer m.active.Add(-1)

	delay := m.Delay
	if m.DelayFor != nil {
		delay = m.DelayFor(call)
	}
	if delay > 0 {
		select {
		case <-ctx.Done():
			return CompletionResponse{}, ctx.Err()
		case <-time.After(delay):
		}
	}
	if m.Failure != nil {
		if err := m.Failure(call); err != nil {
			return CompletionResponse{}, err
		}
	}
	if m.Err != nil {
		return CompletionResponse{}, m.Err
	}

	content := m.Response
	invalidJSON := m.InvalidJSON
	if m.InvalidJSONFor != nil {
		invalidJSON = m.InvalidJSONFor(call)
	}
	if invalidJSON {
		content = "{"
	} else if content == "" {
		content = fakeSemanticJSON(eventIDsFromPrompt(request.UserPrompt))
	}
	provider := m.Provider
	if provider == "" {
		provider = "fake"
	}
	model := m.Model
	if model == "" {
		model = "fake"
	}
	reportedLatency := m.Latency
	if reportedLatency == 0 {
		reportedLatency = time.Since(started)
	}
	return CompletionResponse{
		Content: content, Provider: provider, Model: model,
		InputTokens: m.InputTokens, OutputTokens: m.OutputTokens, Latency: reportedLatency,
	}, nil
}

func (m *FakeModel) Snapshot() FakeModelSnapshot {
	m.latencyMu.Lock()
	latencies := append([]time.Duration(nil), m.latencies...)
	m.latencyMu.Unlock()
	return FakeModelSnapshot{
		Calls:       int(m.calls.Load()),
		InFlight:    int(m.active.Load()),
		MaxInFlight: int(m.maxActive.Load()),
		Latencies:   latencies,
	}
}

func eventIDsFromPrompt(prompt string) []string {
	var ids []string
	for _, line := range strings.Split(prompt, "\n") {
		if !strings.HasPrefix(line, "[") {
			continue
		}
		end := strings.IndexByte(line, ']')
		if end > 1 {
			ids = append(ids, line[1:end])
		}
	}
	return ids
}

func fakeSemanticJSON(ids []string) string {
	evidence := "[]"
	if len(ids) > 0 {
		encoded, _ := json.Marshal([]string{ids[0]})
		evidence = string(encoded)
	}
	return `{"summary":"Deterministic analysis.","topics":[],"sentiment":{"label":"neutral","confidence":1,"evidence_event_ids":` + evidence + `},"questions":[],"alerts":[]}`
}
