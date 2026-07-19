package webapp

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewHandlerRequiresIndex(t *testing.T) {
	_, err := NewHandler(t.TempDir())
	if err == nil {
		t.Fatal("expected an error when index.html is missing")
	}
}

func TestHandlerServesExistingAsset(t *testing.T) {
	root := newWebFixture(t)
	assetPath := filepath.Join(root, "assets", "app.js")
	if err := os.MkdirAll(filepath.Dir(assetPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(assetPath, []byte("console.log('v10')"), 0o644); err != nil {
		t.Fatal(err)
	}

	response := serveRequest(t, root, http.MethodGet, "/assets/app.js")

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d", response.Code)
	}
	if got := strings.TrimSpace(response.Body.String()); got != "console.log('v10')" {
		t.Fatalf("body=%q", got)
	}
}

func TestHandlerFallsBackToIndexForClientRoute(t *testing.T) {
	root := newWebFixture(t)

	response := serveRequest(t, root, http.MethodGet, "/monitor")

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d", response.Code)
	}
	if !strings.Contains(response.Body.String(), `<div id="root"></div>`) {
		t.Fatalf("expected SPA index, body=%q", response.Body.String())
	}
}

func TestHandlerDoesNotTreatDirectoryAsAsset(t *testing.T) {
	root := newWebFixture(t)
	if err := os.Mkdir(filepath.Join(root, "monitor"), 0o755); err != nil {
		t.Fatal(err)
	}

	response := serveRequest(t, root, http.MethodGet, "/monitor")

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d", response.Code)
	}
	if !strings.Contains(response.Body.String(), `<div id="root"></div>`) {
		t.Fatalf("expected SPA index, body=%q", response.Body.String())
	}
}

func TestHandlerRejectsUnsupportedMethod(t *testing.T) {
	root := newWebFixture(t)

	response := serveRequest(t, root, http.MethodPost, "/monitor")

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d", response.Code)
	}
	if allow := response.Header().Get("Allow"); allow != "GET, HEAD" {
		t.Fatalf("Allow=%q", allow)
	}
}

func TestHandlerCannotEscapeRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "web")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(root, "index.html"),
		[]byte(`<div id="root"></div>`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "secret.txt"), []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}

	response := serveRequest(t, root, http.MethodGet, "/../secret.txt")

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d", response.Code)
	}
	if strings.Contains(response.Body.String(), "secret") {
		t.Fatal("handler served a file outside its root")
	}
	if !strings.Contains(response.Body.String(), `<div id="root"></div>`) {
		t.Fatalf("expected SPA fallback, body=%q", response.Body.String())
	}
}

func newWebFixture(t *testing.T) string {
	t.Helper()

	root := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(root, "index.html"),
		[]byte(`<div id="root"></div>`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	return root
}

func serveRequest(t *testing.T, root, method, target string) *httptest.ResponseRecorder {
	t.Helper()

	handler, err := NewHandler(root)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(method, target, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
