package memory

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/ports"
)

const (
	defaultWindowSize = 60 * time.Second
	defaultLateness   = 10 * time.Second
	defaultMaxEvents  = 500
)

type Config struct {
	WindowSize time.Duration
	Lateness   time.Duration
	MaxEvents  int
}

type Store struct {
	mu        sync.Mutex
	config    Config
	windows   map[string]*windowState
	completed map[string]struct{}
}

type windowState struct {
	ref               domain.WindowRef
	events            []domain.MessageEvent
	eventIDs          map[string]struct{}
	totalMessages     int
	duplicateMessages int
	lateMessages      int
	dueAt             time.Time
	claimed           bool
}

var _ ports.WindowStore = (*Store)(nil)

func New(config Config) *Store {
	if config.WindowSize <= 0 {
		config.WindowSize = defaultWindowSize
	}
	if config.Lateness <= 0 {
		config.Lateness = defaultLateness
	}
	if config.MaxEvents <= 0 {
		config.MaxEvents = defaultMaxEvents
	}
	return &Store{
		config:    config,
		windows:   make(map[string]*windowState),
		completed: make(map[string]struct{}),
	}
}

func (s *Store) Add(_ context.Context, event domain.MessageEvent, now time.Time) (ports.AddResult, error) {
	if err := event.Validate(); err != nil {
		return ports.AddResult{}, err
	}
	ref, err := domain.NewWindowRef(event.RoomID, event.OccurredAt, s.config.WindowSize)
	if err != nil {
		return ports.AddResult{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := ref.Key()
	state, exists := s.windows[key]
	if !exists {
		state = &windowState{
			ref:      ref,
			eventIDs: make(map[string]struct{}),
			dueAt:    ref.End.Add(s.config.Lateness),
		}
		s.windows[key] = state
	}
	_, isCompleted := s.completed[key]
	result := ports.AddResult{Ref: ref, Completed: isCompleted}
	if _, duplicate := state.eventIDs[event.EventID]; duplicate {
		state.duplicateMessages++
		result.Duplicate = true
		return result, nil
	}

	state.eventIDs[event.EventID] = struct{}{}
	state.totalMessages++
	result.Added = true
	if !now.Before(ref.End.Add(s.config.Lateness)) {
		state.lateMessages++
		result.Late = true
		return result, nil
	}
	if len(state.events) < s.config.MaxEvents {
		state.events = append(state.events, event)
	}
	return result, nil
}

func (s *Store) ClaimDue(_ context.Context, now time.Time, limit int) ([]domain.WindowRef, error) {
	if limit <= 0 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	due := make([]*windowState, 0)
	for key, state := range s.windows {
		if _, done := s.completed[key]; done || state.claimed || state.dueAt.After(now) {
			continue
		}
		due = append(due, state)
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].ref.Start.Equal(due[j].ref.Start) {
			return due[i].ref.RoomID < due[j].ref.RoomID
		}
		return due[i].ref.Start.Before(due[j].ref.Start)
	})
	if len(due) > limit {
		due = due[:limit]
	}

	refs := make([]domain.WindowRef, len(due))
	for i, state := range due {
		state.claimed = true
		refs[i] = state.ref
	}
	return refs, nil
}

func (s *Store) Load(_ context.Context, ref domain.WindowRef) (domain.InsightWindow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.windows[ref.Key()]
	if !ok {
		return domain.InsightWindow{}, errors.New("window not found")
	}
	events := append([]domain.MessageEvent(nil), state.events...)
	sort.Slice(events, func(i, j int) bool {
		if events[i].OccurredAt.Equal(events[j].OccurredAt) {
			return events[i].EventID < events[j].EventID
		}
		return events[i].OccurredAt.Before(events[j].OccurredAt)
	})
	return domain.InsightWindow{
		Ref:               state.ref,
		Events:            events,
		TotalMessages:     state.totalMessages,
		DuplicateMessages: state.duplicateMessages,
		LateMessages:      state.lateMessages,
	}, nil
}

func (s *Store) Complete(_ context.Context, ref domain.WindowRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := ref.Key()
	state, ok := s.windows[key]
	if !ok {
		return errors.New("window not found")
	}
	s.completed[key] = struct{}{}
	state.claimed = false
	return nil
}

func (s *Store) Release(_ context.Context, ref domain.WindowRef, retryAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.windows[ref.Key()]
	if !ok {
		return errors.New("window not found")
	}
	state.claimed = false
	state.dueAt = retryAt
	return nil
}
