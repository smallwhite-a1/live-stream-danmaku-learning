package benchmark

import (
	"context"
	"testing"
	"time"
)

func TestRunCompletesAllRoomsAndVerifiesHTTPResults(t *testing.T) {
	report, err := Run(context.Background(), Config{Rooms: 100, Scenario: ScenarioNormal, Workers: 8, ModelConcurrency: 8, Timeout: time.Second})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if report.TotalWindows != 100 || report.Normal != 100 || report.Degraded != 0 || report.Failed != 0 || report.HTTPVerified != 100 {
		t.Fatalf("report = %+v, want 100 normal verified windows", report)
	}
	if report.ModelMaxInFlight > 8 || report.ModelCalls != 100 {
		t.Fatalf("report = %+v, model concurrency or call count is invalid", report)
	}
	if report.ModelP50 <= 0 || report.ModelP95 < report.ModelP50 || report.ModelP99 < report.ModelP95 {
		t.Fatalf("report latency percentiles = p50:%s p95:%s p99:%s", report.ModelP50, report.ModelP95, report.ModelP99)
	}
}

func TestRunDegradesWhenModelIsUnavailable(t *testing.T) {
	report, err := Run(context.Background(), Config{Rooms: 100, Scenario: ScenarioUnavailable, Workers: 8, ModelConcurrency: 8, Timeout: time.Second})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if report.Normal != 0 || report.Degraded != 100 || report.Failed != 0 || report.HTTPVerified != 100 {
		t.Fatalf("report = %+v, want every room degraded and queryable", report)
	}
}
