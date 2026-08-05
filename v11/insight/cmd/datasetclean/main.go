package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
)

type rawEvent struct {
	Time float64 `json:"time"`
	Text string  `json:"text"`
}

func main() {
	input := flag.String("input", "../../data/raw/DanmakuTPP/extracted", "extracted DanmakuTPP directory")
	output := flag.String("output", "../../data/processed/danmaku-tpp-10rooms-500.jsonl", "cleaned JSONL output")
	rooms := flag.Int("rooms", 10, "logical rooms")
	eventsPerRoom := flag.Int("events-per-room", 500, "events selected per room")
	flag.Parse()
	if err := clean(*input, *output, *rooms, *eventsPerRoom); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func clean(input, output string, rooms, eventsPerRoom int) error {
	if rooms <= 0 || eventsPerRoom <= 0 {
		return fmt.Errorf("rooms and events-per-room must be positive")
	}
	paths, err := jsonFiles(input)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o700); err != nil {
		return err
	}
	file, err := os.Create(output)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	defer writer.Flush()
	base := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	selectedRooms := 0
	for _, path := range paths {
		events, err := loadEvents(path)
		if err != nil || len(events) < eventsPerRoom {
			continue
		}
		roomID := fmt.Sprintf("dataset-room-%02d", selectedRooms+1)
		for index, event := range uniform(events, eventsPerRoom) {
			message := domain.MessageEvent{EventID: fmt.Sprintf("%s-event-%03d", roomID, index+1), RoomID: roomID, UserID: anonymousID(roomID, index), Username: "anonymous", Content: event.Text, OccurredAt: base.Add(time.Duration(event.Time * float64(time.Second))), SchemaVersion: "v1", Source: "danmaku-tpp"}
			if err := message.Validate(); err != nil {
				return fmt.Errorf("validate cleaned event: %w", err)
			}
			encoded, _ := json.Marshal(message)
			if _, err := writer.Write(append(encoded, '\n')); err != nil {
				return err
			}
		}
		selectedRooms++
		if selectedRooms == rooms {
			break
		}
	}
	if selectedRooms != rooms {
		return fmt.Errorf("only found %d source videos with %d clean events, want %d", selectedRooms, eventsPerRoom, rooms)
	}
	return nil
}

func jsonFiles(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".json") {
			paths = append(paths, path)
		}
		return nil
	})
	sort.Strings(paths)
	return paths, err
}

func loadEvents(path string) ([]rawEvent, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var source []rawEvent
	if err := json.NewDecoder(file).Decode(&source); err != nil {
		return nil, err
	}
	cleaned := make([]rawEvent, 0, len(source))
	for _, event := range source {
		text := strings.Join(strings.Fields(event.Text), " ")
		if event.Time < 0 || text == "" || !utf8.ValidString(text) || utf8.RuneCountInString(text) > 500 || looksSensitive(text) {
			continue
		}
		cleaned = append(cleaned, rawEvent{Time: event.Time, Text: text})
	}
	sort.Slice(cleaned, func(i, j int) bool { return cleaned[i].Time < cleaned[j].Time })
	return cleaned, nil
}

func uniform(events []rawEvent, limit int) []rawEvent {
	if len(events) == limit {
		return events
	}
	selected := make([]rawEvent, 0, limit)
	maxTime := events[len(events)-1].Time
	minTime := events[0].Time
	for i := 0; i < limit; i++ {
		index := i * (len(events) - 1) / (limit - 1)
		event := events[index]
		if maxTime > minTime {
			event.Time = (event.Time - minTime) / (maxTime - minTime) * 59
		}
		selected = append(selected, event)
	}
	return selected
}

func looksSensitive(text string) bool {
	return strings.Contains(text, "@") || strings.Contains(text, "http://") || strings.Contains(text, "https://")
}

func anonymousID(room string, index int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", room, index%100)))
	return "anon-" + hex.EncodeToString(sum[:4])
}
