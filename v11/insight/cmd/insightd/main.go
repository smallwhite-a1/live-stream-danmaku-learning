package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/analyzer/eino"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/analyzer/rule"
	repositorymemory "github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/repository/memory"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/source/jsonl"
	windowmemory "github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/window/memory"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/app"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/httpapi"
)

var listen = net.Listen

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("insightd", flag.ContinueOnError)
	flags.SetOutput(stderr)
	listenAddress := flags.String("listen", ":18120", "HTTP listen address")
	inputPath := flags.String("input", "./testdata/fixtures/demo.jsonl", "JSONL input file")
	webDir := flags.String("web-dir", "", "optional static web directory")
	windowSize := flags.Duration("window", time.Minute, "insight window duration")
	lateness := flags.Duration("lateness", 10*time.Second, "allowed event lateness")
	workers := flags.Int("workers", 2, "fixed processor worker count")
	jobCapacity := flags.Int("job-capacity", 128, "processor job queue capacity")
	model := flags.String("model", "fake", "analysis model")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *windowSize <= 0 {
		return errors.New("window must be positive")
	}
	if *lateness <= 0 {
		return errors.New("lateness must be positive")
	}
	if *workers <= 0 {
		return errors.New("workers must be positive")
	}
	if *jobCapacity < 0 {
		return errors.New("job capacity must not be negative")
	}
	if *model != "fake" {
		return fmt.Errorf("unsupported model %q", *model)
	}

	input, err := os.Open(*inputPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer input.Close()

	store := windowmemory.New(windowmemory.Config{WindowSize: *windowSize, Lateness: *lateness})
	repository := repositorymemory.New()
	primary, err := eino.NewAnalyzer(&eino.FakeModel{})
	if err != nil {
		return fmt.Errorf("create fake analyzer: %w", err)
	}
	processor, err := app.NewProcessor(store, primary, rule.NewAnalyzer(), repository, app.Config{Workers: *workers, JobCapacity: *jobCapacity})
	if err != nil {
		return fmt.Errorf("create processor: %w", err)
	}
	ingestor := app.NewIngestor(store, func() time.Time { return time.Time{} })
	if err := jsonl.New(input).Run(ctx, ingestor.Handle); err != nil {
		return fmt.Errorf("replay input: %w", err)
	}
	if _, err := processor.ProcessDue(ctx, time.Now().UTC()); err != nil {
		return fmt.Errorf("process due windows: %w", err)
	}

	handler, err := httpapi.WithStatic(httpapi.New(repository), *webDir)
	if err != nil {
		return fmt.Errorf("configure static web directory: %w", err)
	}
	listener, err := listen("tcp", *listenAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listenAddress, err)
	}
	defer listener.Close()

	_ = stdout
	return serve(ctx, &http.Server{Handler: handler}, listener)
}

func serve(ctx context.Context, server *http.Server, listener net.Listener) error {
	serveError := make(chan error, 1)
	go func() { serveError <- server.Serve(listener) }()

	select {
	case err := <-serveError:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown server: %w", err)
		}
		if err := <-serveError; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
