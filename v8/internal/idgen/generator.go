package idgen

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

// Generator combines a process-random prefix with time and an atomic sequence.
// The ID is generated before Kafka, so retries keep the same business identity.
type Generator struct {
	prefix   string
	sequence atomic.Uint64
}

func New() (*Generator, error) {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return nil, fmt.Errorf("read random prefix: %w", err)
	}

	return &Generator{prefix: hex.EncodeToString(random)}, nil
}

func (g *Generator) Next() string {
	millis := time.Now().UnixMilli()
	sequence := g.sequence.Add(1)
	return fmt.Sprintf("%013x-%s-%016x", millis, g.prefix, sequence)
}
