package webapp

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type handler struct {
	root  string
	index string
}

func NewHandler(root string) (http.Handler, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve web root: %w", err)
	}

	index := filepath.Join(absoluteRoot, "index.html")
	info, err := os.Stat(index)
	if err != nil {
		return nil, fmt.Errorf("stat web index: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("web index is not a regular file: %s", index)
	}

	return &handler{root: absoluteRoot, index: index}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	requested := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	candidate := filepath.Join(h.root, filepath.FromSlash(requested))
	if h.insideRoot(candidate) {
		if info, err := os.Stat(candidate); err == nil && info.Mode().IsRegular() {
			h.serveFile(w, r, candidate, info)
			return
		}
	}

	info, err := os.Stat(h.index)
	if err != nil {
		http.Error(w, "web application unavailable", http.StatusInternalServerError)
		return
	}
	h.serveFile(w, r, h.index, info)
}

func (h *handler) insideRoot(candidate string) bool {
	relative, err := filepath.Rel(h.root, candidate)
	if err != nil {
		return false
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func (h *handler) serveFile(
	w http.ResponseWriter,
	r *http.Request,
	filename string,
	info os.FileInfo,
) {
	file, err := os.Open(filename)
	if err != nil {
		http.Error(w, "web application unavailable", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	http.ServeContent(w, r, info.Name(), info.ModTime(), file)
}
