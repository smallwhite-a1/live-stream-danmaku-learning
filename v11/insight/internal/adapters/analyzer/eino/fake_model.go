package eino

import (
	"context"
	"encoding/json"
	"strings"
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
}

func (m *FakeModel) Complete(ctx context.Context, request CompletionRequest) (CompletionResponse, error) {
	if err := ctx.Err(); err != nil {
		return CompletionResponse{}, err
	}
	if m.Delay > 0 {
		select {
		case <-ctx.Done():
			return CompletionResponse{}, ctx.Err()
		case <-time.After(m.Delay):
		}
	}
	if m.Err != nil {
		return CompletionResponse{}, m.Err
	}

	content := m.Response
	if m.InvalidJSON {
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
	return CompletionResponse{
		Content: content, Provider: provider, Model: model,
		InputTokens: m.InputTokens, OutputTokens: m.OutputTokens, Latency: m.Latency,
	}, nil
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
