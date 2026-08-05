package benchmark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/analyzer/eino"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/analyzer/rule"
	repositorymemory "github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/repository/memory"
	windowmemory "github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/window/memory"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/app"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/httpapi"
)

type Scenario string

const (
	ScenarioNormal      Scenario = "normal"
	ScenarioMixed       Scenario = "mixed"
	ScenarioTimeout     Scenario = "timeout"
	ScenarioMalformed   Scenario = "malformed"
	ScenarioUnavailable Scenario = "unavailable"
)

type Config struct {
	Rooms            int
	Workers          int
	ModelConcurrency int
	Timeout          time.Duration
	Scenario         Scenario
}

type Report struct {
	Rooms            int           `json:"rooms"`
	TotalWindows     int           `json:"total_windows"`
	Normal           int           `json:"normal"`
	Degraded         int           `json:"degraded"`
	Failed           int           `json:"failed"`
	HTTPVerified     int           `json:"http_verified"`
	ModelCalls       int           `json:"model_calls"`
	ModelMaxInFlight int           `json:"model_max_in_flight"`
	ModelP50         time.Duration `json:"model_p50"`
	ModelP95         time.Duration `json:"model_p95"`
	ModelP99         time.Duration `json:"model_p99"`
	ProcessDuration  time.Duration `json:"process_duration"`
	EndToEndDuration time.Duration `json:"end_to_end_duration"`
	Scenario         Scenario      `json:"scenario"`
}

func Run(ctx context.Context, config Config) (Report, error) {
	config = normalizeConfig(config)
	started := time.Now()
	repository := repositorymemory.New()
	store := windowmemory.New(windowmemory.Config{WindowSize: time.Minute, Lateness: time.Second, MaxEvents: 20})
	ingestor := app.NewIngestor(store, func() time.Time { return benchmarkEventTime })
	for room := 0; room < config.Rooms; room++ {
		for message := 0; message < 4; message++ {
			event := domain.MessageEvent{
				EventID: fmt.Sprintf("room-%03d-event-%d", room, message), RoomID: fmt.Sprintf("room-%03d", room),
				UserID: fmt.Sprintf("user-%d", message), Username: "viewer", Content: benchmarkMessage(message),
				OccurredAt: benchmarkEventTime.Add(time.Duration(message) * time.Second), SchemaVersion: "v1", Source: "benchmark",
			}
			if err := ingestor.Handle(ctx, event); err != nil {
				return Report{}, fmt.Errorf("ingest room %d: %w", room, err)
			}
		}
	}

	fake := newScenarioModel(config)
	guarded, err := eino.NewGuardedModel(fake, eino.GuardConfig{MaxInFlight: config.ModelConcurrency, Timeout: config.Timeout, FailureThreshold: 5, OpenFor: 200 * time.Millisecond})
	if err != nil {
		return Report{}, err
	}
	primary, err := eino.NewAnalyzer(guarded)
	if err != nil {
		return Report{}, err
	}
	processor, err := app.NewProcessor(store, primary, rule.NewAnalyzer(), repository, app.Config{Workers: config.Workers, JobCapacity: config.Rooms})
	if err != nil {
		return Report{}, err
	}

	processStarted := time.Now()
	var total app.Summary
	for {
		summary, err := processor.ProcessDue(ctx, benchmarkDueTime)
		if err != nil {
			return Report{}, err
		}
		total.Completed += summary.Completed
		total.Degraded += summary.Degraded
		total.Failed += summary.Failed
		if summary.Completed+summary.Degraded+summary.Failed == 0 {
			break
		}
	}
	processDuration := time.Since(processStarted)

	verified, err := verifyHTTP(ctx, repository, config.Rooms)
	if err != nil {
		return Report{}, err
	}
	snapshot := fake.Snapshot()
	return Report{Rooms: config.Rooms, TotalWindows: total.Completed + total.Degraded + total.Failed, Normal: total.Completed, Degraded: total.Degraded, Failed: total.Failed, HTTPVerified: verified, ModelCalls: snapshot.Calls, ModelMaxInFlight: snapshot.MaxInFlight, ModelP50: percentile(snapshot.Latencies, 50), ModelP95: percentile(snapshot.Latencies, 95), ModelP99: percentile(snapshot.Latencies, 99), ProcessDuration: processDuration, EndToEndDuration: time.Since(started), Scenario: config.Scenario}, nil
}

func percentile(values []time.Duration, value int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := (len(ordered)*value+99)/100 - 1
	if index < 0 {
		index = 0
	}
	return ordered[index]
}

var benchmarkEventTime = time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
var benchmarkDueTime = benchmarkEventTime.Add(2 * time.Minute)

func normalizeConfig(config Config) Config {
	if config.Rooms <= 0 {
		config.Rooms = 100
	}
	if config.Workers <= 0 {
		config.Workers = 16
	}
	if config.ModelConcurrency <= 0 {
		config.ModelConcurrency = 16
	}
	if config.Timeout <= 0 {
		config.Timeout = 50 * time.Millisecond
	}
	if config.Scenario == "" {
		config.Scenario = ScenarioNormal
	}
	return config
}

func newScenarioModel(config Config) *eino.FakeModel {
	model := &eino.FakeModel{DelayFor: func(call int) time.Duration { return time.Duration(5+call%4*5) * time.Millisecond }}
	switch config.Scenario {
	case ScenarioUnavailable:
		model.Failure = func(int) error { return errors.New("simulated upstream unavailable") }
	case ScenarioMalformed:
		model.InvalidJSONFor = func(int) bool { return true }
	case ScenarioTimeout:
		model.DelayFor = func(int) time.Duration { return config.Timeout * 2 }
	case ScenarioMixed:
		model.Failure = func(call int) error {
			if call%20 == 0 {
				return errors.New("simulated 429")
			}
			return nil
		}
		model.InvalidJSONFor = func(call int) bool { return call%25 == 0 }
		model.DelayFor = func(call int) time.Duration {
			if call%33 == 0 {
				return config.Timeout * 2
			}
			return time.Duration(5+call%4*5) * time.Millisecond
		}
	}
	return model
}

func benchmarkMessage(message int) string {
	if message == 3 {
		return "请问什么时候开始？"
	}
	return "直播内容不错"
}

func verifyHTTP(ctx context.Context, repository *repositorymemory.Repository, rooms int) (int, error) {
	server := httptest.NewServer(httpapi.New(repository))
	defer server.Close()
	client := &http.Client{Timeout: time.Second}
	jobs := make(chan int)
	errs := make(chan error, rooms)
	var verified int
	var mu sync.Mutex
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for room := range jobs {
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/api/v1/rooms/room-%03d/insights/latest", server.URL, room), nil)
				if err != nil {
					errs <- err
					continue
				}
				response, err := client.Do(request)
				if err != nil {
					errs <- err
					continue
				}
				var insight domain.RoomInsight
				err = json.NewDecoder(response.Body).Decode(&insight)
				_ = response.Body.Close()
				if err != nil || response.StatusCode != http.StatusOK || (insight.Status != domain.InsightStatusNormal && insight.Status != domain.InsightStatusDegraded) {
					errs <- fmt.Errorf("verify room %d: status=%d decode=%v", room, response.StatusCode, err)
					continue
				}
				mu.Lock()
				verified++
				mu.Unlock()
			}
		}()
	}
	for room := 0; room < rooms; room++ {
		jobs <- room
	}
	close(jobs)
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return verified, err
		}
	}
	return verified, nil
}
