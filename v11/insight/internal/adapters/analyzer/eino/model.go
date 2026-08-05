package eino

import (
	"context"
	"time"
)

type CompletionRequest struct {
	SystemPrompt string
	UserPrompt   string
	JSONMode     bool
}

type CompletionResponse struct {
	Content      string
	Provider     string
	Model        string
	InputTokens  int
	OutputTokens int
	Latency      time.Duration
}

type CompletionModel interface {
	Complete(context.Context, CompletionRequest) (CompletionResponse, error)
}
