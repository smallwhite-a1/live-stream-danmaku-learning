package httpapi

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/ports"
)

const defaultHistoryLimit = 20

// New exposes the standalone insight JSON API.
func New(repository ports.InsightRepository) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("/health", methodNotAllowed)
	mux.HandleFunc("/", notFound)
	api := insights(repository)
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if strings.HasPrefix(request.URL.Path, "/api/") {
			api(response, request)
			return
		}
		mux.ServeHTTP(response, request)
	})
}

func health(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func insights(repository ports.InsightRepository) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			methodNotAllowed(response, request)
			return
		}

		roomID, action, ok := insightRoute(request.URL.EscapedPath())
		if !ok {
			notFound(response, request)
			return
		}

		switch action {
		case "latest":
			insight, found, err := repository.Latest(request.Context(), roomID)
			if err != nil {
				writeError(response, http.StatusInternalServerError, "load latest insight")
				return
			}
			if !found {
				writeError(response, http.StatusNotFound, "insight not found")
				return
			}
			writeJSON(response, http.StatusOK, insight)
		case "history":
			limit, err := historyLimit(request)
			if err != nil {
				writeError(response, http.StatusBadRequest, "limit must be an integer from 1 to 100")
				return
			}
			values, err := repository.List(request.Context(), roomID, limit)
			if err != nil {
				writeError(response, http.StatusInternalServerError, "load insight history")
				return
			}
			writeJSON(response, http.StatusOK, values)
		}
	}
}

func insightRoute(escapedPath string) (string, string, bool) {
	parts := strings.Split(escapedPath, "/")
	if len(parts) != 7 || parts[0] != "" || parts[1] != "api" || parts[2] != "v1" || parts[3] != "rooms" || parts[5] != "insights" || (parts[6] != "latest" && parts[6] != "history") {
		return "", "", false
	}
	roomID, err := url.PathUnescape(parts[4])
	if err != nil || strings.TrimSpace(roomID) == "" {
		return "", "", false
	}
	return roomID, parts[6], true
}

func historyLimit(request *http.Request) (int, error) {
	values, exists := request.URL.Query()["limit"]
	if !exists {
		return defaultHistoryLimit, nil
	}
	if len(values) != 1 {
		return 0, strconv.ErrSyntax
	}
	limit, err := strconv.Atoi(values[0])
	if err != nil || limit < 1 || limit > 100 {
		return 0, strconv.ErrSyntax
	}
	return limit, nil
}

func methodNotAllowed(response http.ResponseWriter, _ *http.Request) {
	writeError(response, http.StatusMethodNotAllowed, "method not allowed")
}

func notFound(response http.ResponseWriter, _ *http.Request) {
	writeError(response, http.StatusNotFound, "not found")
}

func writeError(response http.ResponseWriter, status int, message string) {
	writeJSON(response, status, map[string]string{"error": message})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
