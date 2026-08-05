package eino

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultDeepSeekEndpoint = "https://api.deepseek.com"
const defaultDeepSeekModel = "deepseek-v4-flash"

type DeepSeekConfig struct {
	Endpoint string
	APIKey   string
	Model    string
	Client   *http.Client
}

type DeepSeekModel struct {
	endpoint string
	apiKey   string
	model    string
	client   *http.Client
}

type ProviderError struct {
	StatusCode int
	Message    string
	Retryable  bool
}

func (e *ProviderError) Error() string {
	return fmt.Sprintf("deepseek request failed: status=%d message=%s", e.StatusCode, e.Message)
}

func NewDeepSeekModel(config DeepSeekConfig) (*DeepSeekModel, error) {
	if strings.TrimSpace(config.APIKey) == "" {
		return nil, errors.New("DeepSeek API key is required")
	}
	if strings.TrimSpace(config.Endpoint) == "" {
		config.Endpoint = defaultDeepSeekEndpoint
	}
	if strings.TrimSpace(config.Model) == "" {
		config.Model = defaultDeepSeekModel
	}
	if config.Client == nil {
		config.Client = &http.Client{}
	}
	return &DeepSeekModel{endpoint: strings.TrimRight(config.Endpoint, "/"), apiKey: config.APIKey, model: config.Model, client: config.Client}, nil
}

func (m *DeepSeekModel) Complete(ctx context.Context, request CompletionRequest) (CompletionResponse, error) {
	payload := deepSeekRequest{Model: m.model, Messages: []deepSeekMessage{{Role: "system", Content: request.SystemPrompt}, {Role: "user", Content: request.UserPrompt}}}
	if request.JSONMode {
		payload.ResponseFormat = &deepSeekResponseFormat{Type: "json_object"}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return CompletionResponse{}, fmt.Errorf("encode deepseek request: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, m.endpoint+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return CompletionResponse{}, fmt.Errorf("create deepseek request: %w", err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+m.apiKey)
	httpRequest.Header.Set("Content-Type", "application/json")

	started := time.Now()
	httpResponse, err := m.client.Do(httpRequest)
	latency := time.Since(started)
	if err != nil {
		return CompletionResponse{}, err
	}
	defer httpResponse.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(httpResponse.Body, 2<<20))
	if err != nil {
		return CompletionResponse{}, fmt.Errorf("read deepseek response: %w", err)
	}
	if httpResponse.StatusCode < http.StatusOK || httpResponse.StatusCode >= http.StatusMultipleChoices {
		return CompletionResponse{}, providerError(httpResponse.StatusCode, responseBody)
	}
	var decoded deepSeekResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return CompletionResponse{}, fmt.Errorf("decode deepseek response: %w", err)
	}
	if len(decoded.Choices) == 0 || strings.TrimSpace(decoded.Choices[0].Message.Content) == "" {
		return CompletionResponse{}, errors.New("deepseek response contains no completion content")
	}
	return CompletionResponse{Content: decoded.Choices[0].Message.Content, Provider: "deepseek", Model: m.model, InputTokens: decoded.Usage.PromptTokens, OutputTokens: decoded.Usage.CompletionTokens, Latency: latency}, nil
}

func providerError(status int, body []byte) error {
	var decoded struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &decoded)
	message := strings.TrimSpace(decoded.Error.Message)
	if message == "" {
		message = strings.TrimSpace(string(body))
	}
	return &ProviderError{StatusCode: status, Message: message, Retryable: status == http.StatusTooManyRequests || status >= http.StatusInternalServerError}
}

type deepSeekRequest struct {
	Model          string                  `json:"model"`
	Messages       []deepSeekMessage       `json:"messages"`
	ResponseFormat *deepSeekResponseFormat `json:"response_format,omitempty"`
}
type deepSeekMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}
type deepSeekResponseFormat struct {
	Type string `json:"type"`
}
type deepSeekResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}
