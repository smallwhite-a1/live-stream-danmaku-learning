package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunRejectsInvalidWorkerCount(t *testing.T) {
	err := run(context.Background(), []string{"-workers=0"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "workers") {
		t.Fatalf("run() error = %v, want worker count error", err)
	}
}

func TestRunRejectsAbsentInputFile(t *testing.T) {
	err := run(context.Background(), []string{"-input=missing.jsonl"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "open input") {
		t.Fatalf("run() error = %v, want input error", err)
	}
}

func TestRunRejectsInvalidWindowDuration(t *testing.T) {
	err := run(context.Background(), []string{"-window=0s"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "window") {
		t.Fatalf("run() error = %v, want window duration error", err)
	}
}

func TestRunRejectsNegativeJobCapacityAndUnsupportedModel(t *testing.T) {
	for _, args := range [][]string{{"-job-capacity=-1"}, {"-model=other"}} {
		err := run(context.Background(), args, io.Discard, io.Discard)
		if err == nil {
			t.Fatalf("run(%v) error = nil, want rejected configuration", args)
		}
	}
}

func TestRunServesReplayedFixtureUntilContextCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	oldListen := listen
	listen = func(_, _ string) (net.Listener, error) { return listener, nil }
	t.Cleanup(func() {
		listen = oldListen
		_ = listener.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{
			"-input=" + filepath.Join("..", "..", "testdata", "fixtures", "demo.jsonl"),
			"-job-capacity=1",
		}, io.Discard, &bytes.Buffer{})
	}()

	client := &http.Client{Timeout: 50 * time.Millisecond}
	url := "http://" + listener.Addr().String() + "/api/v1/rooms/room-alpha/insights/latest"
	var response *http.Response
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		response, err = client.Get(url)
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("GET replayed insight: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	var body struct {
		RoomID   string `json:"room_id"`
		Status   string `json:"status"`
		Semantic struct {
			Summary string `json:"summary"`
		} `json:"semantic"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.RoomID != "room-alpha" || body.Status != "normal" || body.Semantic.Summary == "" {
		t.Fatalf("response = %+v, want replayed normal semantic insight", body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("run() did not shut down after context cancellation")
	}
}
