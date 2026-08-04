package httpapi

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	repositorymemory "github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/repository/memory"
)

func TestWithStaticServesAssetsAndFallsBackToIndex(t *testing.T) {
	root := t.TempDir()
	writeWebFile(t, root, "index.html", "<html>app</html>")
	writeWebFile(t, root, "assets/app.js", "console.log('app')")
	handler, err := WithStatic(New(repositorymemory.New()), root)
	if err != nil {
		t.Fatalf("WithStatic() error = %v", err)
	}

	asset := serve(t, handler, http.MethodGet, "/assets/app.js")
	if asset.Code != http.StatusOK || asset.Body.String() != "console.log('app')" {
		t.Fatalf("asset response = (%d, %q), want JavaScript asset", asset.Code, asset.Body.String())
	}
	route := serve(t, handler, http.MethodGet, "/rooms/room-1")
	if route.Code != http.StatusOK || route.Body.String() != "<html>app</html>" {
		t.Fatalf("route response = (%d, %q), want index fallback", route.Code, route.Body.String())
	}
}

func TestWithStaticDoesNotSwallowAPIOrHealth(t *testing.T) {
	root := t.TempDir()
	writeWebFile(t, root, "index.html", "<html>app</html>")
	handler, err := WithStatic(New(repositorymemory.New()), root)
	if err != nil {
		t.Fatalf("WithStatic() error = %v", err)
	}

	api := serve(t, handler, http.MethodGet, "/api/v1/rooms/missing/insights/latest")
	assertJSONError(t, api, http.StatusNotFound)
	health := serve(t, handler, http.MethodGet, "/health")
	if health.Code != http.StatusOK || health.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("health response = (%d, %q), want API health JSON", health.Code, health.Header().Get("Content-Type"))
	}
}

func TestWithStaticWithoutWebDirectoryLeavesAPIWorking(t *testing.T) {
	handler, err := WithStatic(New(repositorymemory.New()), "")
	if err != nil {
		t.Fatalf("WithStatic() error = %v", err)
	}

	response := serve(t, handler, http.MethodGet, "/health")
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("health response = (%d, %q), want API-only health JSON", response.Code, response.Header().Get("Content-Type"))
	}
}

func TestWithStaticRejectsMissingDirectory(t *testing.T) {
	if _, err := WithStatic(New(repositorymemory.New()), filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("WithStatic() error = nil, want invalid web directory error")
	}
}

func TestWithStaticDoesNotServeTraversalOutsideRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "web")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	writeWebFile(t, root, "index.html", "<html>app</html>")
	writeWebFile(t, parent, "secret.txt", "private")
	handler, err := WithStatic(New(repositorymemory.New()), root)
	if err != nil {
		t.Fatalf("WithStatic() error = %v", err)
	}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/ignored", nil)
	request.URL.Path = "/../secret.txt"
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "<html>app</html>" {
		t.Fatalf("traversal response = (%d, %q), want index fallback", response.Code, response.Body.String())
	}
}

func writeWebFile(t *testing.T, root, name, contents string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
}
