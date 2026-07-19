package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegisterWebFrontendIgnoresEmptyDirectory(t *testing.T) {
	mux := http.NewServeMux()

	if err := registerWebFrontend(mux, ""); err != nil {
		t.Fatalf("register empty web dir: %v", err)
	}
}

func TestRegisterWebFrontendReturnsErrorForMissingBuild(t *testing.T) {
	mux := http.NewServeMux()

	err := registerWebFrontend(mux, filepath.Join(t.TempDir(), "missing"))

	if err == nil {
		t.Fatal("expected missing web build to return an error")
	}
}

func TestRegisterWebFrontendServesClientRoutes(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(root, "index.html"),
		[]byte(`<div id="root"></div>`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	if err := registerWebFrontend(mux, root); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "/monitor", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d", response.Code)
	}
	if !strings.Contains(response.Body.String(), `<div id="root"></div>`) {
		t.Fatalf("body=%q", response.Body.String())
	}
}
