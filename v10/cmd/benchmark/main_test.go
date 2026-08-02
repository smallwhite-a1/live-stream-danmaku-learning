package main

import (
	"testing"
	"time"
)

func TestLatencyStatsSummaryUsesNearestRankPercentiles(t *testing.T) {
	var stats latencyStats
	for _, latency := range []time.Duration{
		1 * time.Millisecond,
		2 * time.Millisecond,
		3 * time.Millisecond,
		4 * time.Millisecond,
		5 * time.Millisecond,
	} {
		stats.Record(latency)
	}

	summary := stats.Summary()
	if summary.Count != 5 {
		t.Fatalf("count = %d, want 5", summary.Count)
	}
	if summary.P50 != 3*time.Millisecond {
		t.Fatalf("p50 = %s, want 3ms", summary.P50)
	}
	if summary.P95 != 5*time.Millisecond {
		t.Fatalf("p95 = %s, want 5ms", summary.P95)
	}
	if summary.P99 != 5*time.Millisecond {
		t.Fatalf("p99 = %s, want 5ms", summary.P99)
	}
}

func TestLatencyStatsSummaryIsEmptyWithoutSamples(t *testing.T) {
	if summary := (&latencyStats{}).Summary(); summary.Count != 0 {
		t.Fatalf("count = %d, want 0", summary.Count)
	}
}

func TestLatencyStatsDoesNotDropSamplesAfterLegacyLimit(t *testing.T) {
	const sampleCount = 1_000_001
	var stats latencyStats
	for i := 0; i < sampleCount; i++ {
		stats.Record(time.Millisecond)
	}

	if summary := stats.Summary(); summary.Count != sampleCount {
		t.Fatalf("count = %d, want %d", summary.Count, sampleCount)
	}
}
