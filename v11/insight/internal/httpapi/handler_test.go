package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	repositorymemory "github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/repository/memory"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
)

func TestHealthReturnsOKJSON(t *testing.T) {
	response := serve(t, New(repositorymemory.New()), http.MethodGet, "/health")

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", contentType)
	}
	var body map[string]string
	decodeJSON(t, response, &body)
	if body["status"] != "ok" {
		t.Fatalf("body = %#v, want status ok", body)
	}
}

func TestLatestReturnsNewestInsightForDecodedRoomID(t *testing.T) {
	repository := repositorymemory.New()
	mustSave(t, repository, insight("room one", 1))
	mustSave(t, repository, insight("room one", 2))

	response := serve(t, New(repository), http.MethodGet, "/api/v1/rooms/room%20one/insights/latest")

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	var got domain.RoomInsight
	decodeJSON(t, response, &got)
	if got.RoomID != "room one" || got.Status != domain.InsightStatusNormal || got.Semantic.Summary != "summary 2" || len(got.Semantic.Topics) != 1 || got.Semantic.Topics[0].EvidenceEventIDs[0] != "event-2" {
		t.Fatalf("latest insight = %+v, want newest semantic insight for decoded room", got)
	}
}

func TestLatestForUnknownRoomReturnsJSONNotFound(t *testing.T) {
	response := serve(t, New(repositorymemory.New()), http.MethodGet, "/api/v1/rooms/missing/insights/latest")

	assertJSONError(t, response, http.StatusNotFound)
}

func TestHistoryUsesNewestFirstOrderAndLimit(t *testing.T) {
	repository := repositorymemory.New()
	mustSave(t, repository, insight("room-1", 1))
	mustSave(t, repository, insight("room-1", 2))
	mustSave(t, repository, insight("room-1", 3))

	response := serve(t, New(repository), http.MethodGet, "/api/v1/rooms/room-1/insights/history?limit=2")

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	var got []domain.RoomInsight
	decodeJSON(t, response, &got)
	if len(got) != 2 || got[0].Semantic.Summary != "summary 3" || got[1].Semantic.Summary != "summary 2" {
		t.Fatalf("history = %+v, want newest two insights first", got)
	}
}

func TestHistoryDefaultsLimitAndRejectsInvalidLimits(t *testing.T) {
	repository := repositorymemory.New()
	for index := 1; index <= 25; index++ {
		mustSave(t, repository, insight("room-1", index))
	}
	handler := New(repository)

	response := serve(t, handler, http.MethodGet, "/api/v1/rooms/room-1/insights/history")
	if response.Code != http.StatusOK {
		t.Fatalf("default history status = %d, want %d", response.Code, http.StatusOK)
	}
	var history []domain.RoomInsight
	decodeJSON(t, response, &history)
	if len(history) != 20 {
		t.Fatalf("default history length = %d, want 20", len(history))
	}

	for _, rawLimit := range []string{"0", "101", "bad"} {
		response = serve(t, handler, http.MethodGet, "/api/v1/rooms/room-1/insights/history?limit="+rawLimit)
		assertJSONError(t, response, http.StatusBadRequest)
	}
}

func TestAPIMalformedPathsAndUnsupportedMethodsReturnJSONErrors(t *testing.T) {
	handler := New(repositorymemory.New())

	for _, request := range []struct {
		method string
		path   string
		status int
	}{
		{method: http.MethodPost, path: "/health", status: http.StatusMethodNotAllowed},
		{method: http.MethodPost, path: "/api/v1/rooms/room-1/insights/latest", status: http.StatusMethodNotAllowed},
		{method: http.MethodGet, path: "/api/v1/rooms/room-1/insights", status: http.StatusNotFound},
		{method: http.MethodGet, path: "/api/v1/rooms//insights/latest", status: http.StatusNotFound},
	} {
		response := serve(t, handler, request.method, request.path)
		assertJSONError(t, response, request.status)
	}
}

func serve(t *testing.T, handler http.Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, target, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeJSON(t *testing.T, response *httptest.ResponseRecorder, value any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(value); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
}

func assertJSONError(t *testing.T, response *httptest.ResponseRecorder, wantStatus int) {
	t.Helper()
	if response.Code != wantStatus {
		t.Fatalf("status = %d, want %d", response.Code, wantStatus)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", contentType)
	}
	var body map[string]string
	decodeJSON(t, response, &body)
	if body["error"] == "" {
		t.Fatalf("error body = %#v, want non-empty error", body)
	}
}

func mustSave(t *testing.T, repository *repositorymemory.Repository, value domain.RoomInsight) {
	t.Helper()
	if _, err := repository.Save(context.Background(), value); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

func insight(roomID string, index int) domain.RoomInsight {
	start := time.Date(2026, 8, 4, 12, index, 0, 0, time.UTC)
	return domain.RoomInsight{
		RoomID: roomID, WindowStart: start, WindowEnd: start.Add(time.Minute), Status: domain.InsightStatusNormal,
		Semantic: domain.SemanticInsight{
			Summary:   "summary " + string(rune('0'+index)),
			Topics:    []domain.Topic{{Name: "topic", EvidenceEventIDs: []string{"event-" + string(rune('0'+index))}}},
			Sentiment: domain.Sentiment{Label: "neutral"},
		},
		Model: domain.ModelMeta{PromptVersion: "test.v1"}, GeneratedAt: start.Add(time.Minute),
	}
}
