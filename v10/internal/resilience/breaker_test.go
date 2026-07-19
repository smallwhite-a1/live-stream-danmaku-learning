package resilience

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type manualClock struct {
	nanos atomic.Int64
}

func newManualClock(now time.Time) *manualClock {
	c := &manualClock{}
	c.nanos.Store(now.UnixNano())
	return c
}

func (c *manualClock) Now() time.Time {
	return time.Unix(0, c.nanos.Load())
}

func (c *manualClock) Advance(d time.Duration) {
	c.nanos.Add(int64(d))
}

func TestCircuitBreakerOpensAndRejectsWithoutCallingDependency(t *testing.T) {
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	breaker := newCircuitBreaker(Config{
		FailureThreshold: 3,
		OpenTimeout:      5 * time.Second,
	}, clock.Now)

	dependencyErr := errors.New("redis unavailable")
	calls := 0
	for i := 0; i < 3; i++ {
		if err := breaker.Execute(func() error {
			calls++
			return dependencyErr
		}); !errors.Is(err, dependencyErr) {
			t.Fatalf("failure %d error = %v, want dependency error", i+1, err)
		}
	}

	if got := breaker.Snapshot().State; got != StateOpen {
		t.Fatalf("state = %q, want %q", got, StateOpen)
	}
	if err := breaker.Execute(func() error {
		calls++
		return nil
	}); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("open breaker error = %v, want ErrCircuitOpen", err)
	}
	if calls != 3 {
		t.Fatalf("dependency calls = %d, want 3", calls)
	}
	if breaker.Snapshot().Rejected != 1 {
		t.Fatalf("rejected = %d, want 1", breaker.Snapshot().Rejected)
	}
}

func TestHalfOpenProbeSuccessClosesCircuit(t *testing.T) {
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	breaker := newCircuitBreaker(Config{
		FailureThreshold: 1,
		OpenTimeout:      5 * time.Second,
	}, clock.Now)

	if err := breaker.Execute(func() error { return errors.New("down") }); err == nil {
		t.Fatal("initial dependency failure returned nil")
	}
	clock.Advance(5 * time.Second)

	if err := breaker.Execute(func() error { return nil }); err != nil {
		t.Fatalf("half-open probe error = %v", err)
	}
	snapshot := breaker.Snapshot()
	if snapshot.State != StateClosed {
		t.Fatalf("state = %q, want %q", snapshot.State, StateClosed)
	}
	if snapshot.Recoveries != 1 {
		t.Fatalf("recoveries = %d, want 1", snapshot.Recoveries)
	}
}

func TestHalfOpenAllowsOnlyOneConcurrentProbe(t *testing.T) {
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	breaker := newCircuitBreaker(Config{
		FailureThreshold: 1,
		OpenTimeout:      time.Second,
	}, clock.Now)

	_ = breaker.Execute(func() error { return errors.New("down") })
	clock.Advance(time.Second)

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- breaker.Execute(func() error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	if err := breaker.Execute(func() error {
		t.Fatal("second half-open probe called the dependency")
		return nil
	}); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("second half-open call error = %v, want ErrCircuitOpen", err)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("first half-open probe error = %v", err)
	}
}

func TestClosedSuccessResetsConsecutiveFailures(t *testing.T) {
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	breaker := newCircuitBreaker(Config{FailureThreshold: 2, OpenTimeout: time.Second}, clock.Now)

	_ = breaker.Execute(func() error { return errors.New("temporary") })
	if err := breaker.Execute(func() error { return nil }); err != nil {
		t.Fatalf("success error = %v", err)
	}
	_ = breaker.Execute(func() error { return errors.New("temporary") })

	snapshot := breaker.Snapshot()
	if snapshot.State != StateClosed || snapshot.ConsecutiveFailures != 1 {
		t.Fatalf("unexpected snapshot after reset: %+v", snapshot)
	}
}

func TestConcurrentFailuresCountOneClosedToOpenTransition(t *testing.T) {
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	breaker := newCircuitBreaker(Config{FailureThreshold: 1, OpenTimeout: time.Second}, clock.Now)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	done := make(chan error, 2)

	operation := func() error {
		started <- struct{}{}
		<-release
		return errors.New("down")
	}
	go func() { done <- breaker.Execute(operation) }()
	go func() { done <- breaker.Execute(operation) }()
	<-started
	<-started
	close(release)
	<-done
	<-done

	snapshot := breaker.Snapshot()
	if snapshot.State != StateOpen {
		t.Fatalf("state = %q, want open", snapshot.State)
	}
	if snapshot.Opened != 1 {
		t.Fatalf("opened transitions = %d, want 1", snapshot.Opened)
	}
}
