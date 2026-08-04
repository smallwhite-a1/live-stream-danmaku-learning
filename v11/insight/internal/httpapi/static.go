package httpapi

import (
	"errors"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func WithStatic(api http.Handler, webDir string) (http.Handler, error) {
	if webDir == "" {
		return api, nil
	}
	root, err := filepath.Abs(webDir)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("web directory must be a directory")
	}

	index := filepath.Join(root, "index.html")
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/health" || strings.HasPrefix(request.URL.Path, "/api/") {
			api.ServeHTTP(response, request)
			return
		}
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			methodNotAllowed(response, request)
			return
		}

		file, ok := staticFile(root, request.URL.Path)
		if ok {
			serveStaticFile(response, request, file)
			return
		}
		serveStaticFile(response, request, index)
	}), nil
}

func serveStaticFile(response http.ResponseWriter, request *http.Request, file string) {
	clone := request.Clone(request.Context())
	urlCopy := *request.URL
	urlCopy.Path = "/asset" + filepath.Ext(file)
	clone.URL = &urlCopy
	http.ServeFile(response, clone, file)
}

func staticFile(root, requestPath string) (string, bool) {
	clean := path.Clean("/" + requestPath)
	file := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(clean, "/")))
	relative, err := filepath.Rel(root, file)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", false
	}
	info, err := os.Stat(file)
	if err != nil || info.IsDir() {
		return "", false
	}
	return file, true
}
