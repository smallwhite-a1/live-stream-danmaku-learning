package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/benchmark"
)

func main() {
	rooms := flag.String("rooms", "100,300,500", "comma-separated room counts")
	scenario := flag.String("scenario", string(benchmark.ScenarioNormal), "normal, mixed, timeout, malformed, or unavailable")
	workers := flag.Int("workers", 16, "processor workers")
	modelConcurrency := flag.Int("model-concurrency", 16, "maximum concurrent model calls")
	flag.Parse()

	selected, err := parseRooms(*rooms)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, roomCount := range selected {
		report, err := benchmark.Run(context.Background(), benchmark.Config{Rooms: roomCount, Scenario: benchmark.Scenario(*scenario), Workers: *workers, ModelConcurrency: *modelConcurrency})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func parseRooms(raw string) ([]int, error) {
	var values []int
	for _, item := range strings.Split(raw, ",") {
		var roomCount int
		if _, err := fmt.Sscanf(strings.TrimSpace(item), "%d", &roomCount); err != nil || roomCount <= 0 {
			return nil, fmt.Errorf("invalid room count %q", item)
		}
		values = append(values, roomCount)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("at least one room count is required")
	}
	return values, nil
}
