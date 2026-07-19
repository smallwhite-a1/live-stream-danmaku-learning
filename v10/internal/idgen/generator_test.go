package idgen

import (
	"sync"
	"testing"
)

func TestGeneratorProducesUniqueIDsConcurrently(t *testing.T) {
	generator, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const total = 2000
	ids := make(chan string, total)
	var wg sync.WaitGroup

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids <- generator.Next()
		}()
	}

	wg.Wait()
	close(ids)

	seen := make(map[string]struct{}, total)
	for id := range ids {
		if id == "" {
			t.Fatal("Next() returned an empty id")
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate id generated: %s", id)
		}
		seen[id] = struct{}{}
	}

	if len(seen) != total {
		t.Fatalf("generated %d unique ids, want %d", len(seen), total)
	}
}
