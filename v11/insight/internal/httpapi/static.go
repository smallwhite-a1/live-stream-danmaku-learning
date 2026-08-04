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

	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/health" || request.URL.Path == "/api" || strings.HasPrefix(request.URL.Path, "/api/") {
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
		index, ok := staticFile(root, "/index.html")
		if !ok {
			http.NotFound(response, request)
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
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", false
	}
	clean := path.Clean("/" + requestPath)
	file := filepath.Join(resolvedRoot, filepath.FromSlash(strings.TrimPrefix(clean, "/")))
	resolvedFile, err := filepath.EvalSymlinks(file)
	if err != nil {
		return "", false
	}
	relative, err := filepath.Rel(resolvedRoot, resolvedFile)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", false
	}
	info, err := os.Stat(resolvedFile)
	if err != nil || info.IsDir() {
		return "", false
	}
	return resolvedFile, true
}
