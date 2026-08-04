package jsonl

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/ports"
)

type Source struct {
	reader io.Reader
}

var _ ports.EventSource = (*Source)(nil)

func New(reader io.Reader) *Source {
	return &Source{reader: reader}
}

func (s *Source) Run(ctx context.Context, handle func(context.Context, domain.MessageEvent) error) error {
	scanner := bufio.NewScanner(s.reader)
	line := 0
	for scanner.Scan() {
		line++
		if err := ctx.Err(); err != nil {
			return err
		}
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		var event domain.MessageEvent
		if err := json.Unmarshal([]byte(text), &event); err != nil {
			return fmt.Errorf("decode line %d: %w", line, err)
		}
		if err := event.Validate(); err != nil {
			return fmt.Errorf("validate line %d: %w", line, err)
		}
		event.OccurredAt = event.OccurredAt.UTC()
		if err := handle(ctx, event); err != nil {
			return fmt.Errorf("handle line %d: %w", line, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan JSONL: %w", err)
	}
	return ctx.Err()
}
