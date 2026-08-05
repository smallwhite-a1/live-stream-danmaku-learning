package eino

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDeepSeekModelCompletesJSONRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/chat/completions" || request.Header.Get("Authorization") != "Bearer test-key" {
			t.Fatalf("request = %s authorization=%q", request.URL.Path, request.Header.Get("Authorization"))
		}
		body, err := io.ReadAll(request.Body)
		if err != nil || !strings.Contains(string(body), `"response_format":{"type":"json_object"}`) {
			t.Fatalf("request body = %s, error = %v", body, err)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"choices":[{"message":{"content":"{\"summary\":\"ok\"}"}}],"usage":{"prompt_tokens":12,"completion_tokens":8}}`))
	}))
	defer server.Close()

	model, err := NewDeepSeekModel(DeepSeekConfig{Endpoint: server.URL, APIKey: "test-key", Model: "deepseek-v4-flash"})
	if err != nil {
		t.Fatalf("NewDeepSeekModel() error = %v", err)
	}
	response, err := model.Complete(context.Background(), CompletionRequest{SystemPrompt: "system", UserPrompt: "user", JSONMode: true})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if response.Provider != "deepseek" || response.Model != "deepseek-v4-flash" || response.Content != `{"summary":"ok"}` || response.InputTokens != 12 || response.OutputTokens != 8 {
		t.Fatalf("response = %+v", response)
	}
}

func TestDeepSeekModelClassifiesHTTPFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusTooManyRequests)
		_, _ = response.Write([]byte(`{"error":{"message":"slow down"}}`))
	}))
	defer server.Close()
	model, err := NewDeepSeekModel(DeepSeekConfig{Endpoint: server.URL, APIKey: "test-key"})
	if err != nil {
		t.Fatalf("NewDeepSeekModel() error = %v", err)
	}
	_, err = model.Complete(context.Background(), CompletionRequest{})
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.StatusCode != http.StatusTooManyRequests || !providerErr.Retryable {
		t.Fatalf("Complete() error = %v, want retryable 429 ProviderError", err)
	}
}
