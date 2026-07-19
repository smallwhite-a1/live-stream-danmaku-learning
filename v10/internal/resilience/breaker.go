package resilience

import (
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("circuit breaker is open")

type State string

const (
	StateClosed   State = "closed"
	StateOpen     State = "open"
	StateHalfOpen State = "half_open"
)

type Config struct {
	FailureThreshold int
	OpenTimeout      time.Duration
}

func DefaultConfig() Config {
	return Config{
		FailureThreshold: 3,
		OpenTimeout:      5 * time.Second,
	}
}

type Snapshot struct {
	State               State  `json:"state"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
	Opened              uint64 `json:"opened"`
	Rejected            uint64 `json:"rejected"`
	Recoveries          uint64 `json:"recoveries"`
}

type CircuitBreaker struct {
	mu     sync.Mutex
	config Config
	now    func() time.Time

	state               State
	consecutiveFailures int
	openUntil           time.Time
	probeInFlight       bool
	opened              uint64
	rejected            uint64
	recoveries          uint64
	generation          uint64
}

type permit struct {
	state      State
	generation uint64
}

func NewCircuitBreaker(config Config) *CircuitBreaker {
	return newCircuitBreaker(config, time.Now)
}

func newCircuitBreaker(config Config, now func() time.Time) *CircuitBreaker {
	if config.FailureThreshold <= 0 {
		config.FailureThreshold = DefaultConfig().FailureThreshold
	}
	if config.OpenTimeout <= 0 {
		config.OpenTimeout = DefaultConfig().OpenTimeout
	}
	if now == nil {
		now = time.Now
	}

	return &CircuitBreaker{
		config: config,
		now:    now,
		state:  StateClosed,
	}
}

func (b *CircuitBreaker) Execute(operation func() error) (err error) {
	permit, allowed := b.allow()
	if !allowed {
		return ErrCircuitOpen
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			b.complete(permit, false)
			panic(recovered)
		}
	}()

	err = operation()
	b.complete(permit, err == nil)
	return err
}

func (b *CircuitBreaker) Snapshot() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()

	return Snapshot{
		State:               b.state,
		ConsecutiveFailures: b.consecutiveFailures,
		Opened:              b.opened,
		Rejected:            b.rejected,
		Recoveries:          b.recoveries,
	}
}

func (b *CircuitBreaker) allow() (permit, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return permit{state: StateClosed, generation: b.generation}, true
	case StateOpen:
		if b.now().Before(b.openUntil) {
			b.rejected++
			return permit{}, false
		}
		b.state = StateHalfOpen
		b.probeInFlight = true
		return permit{state: StateHalfOpen, generation: b.generation}, true
	case StateHalfOpen:
		if b.probeInFlight {
			b.rejected++
			return permit{}, false
		}
		b.probeInFlight = true
		return permit{state: StateHalfOpen, generation: b.generation}, true
	default:
		b.rejected++
		return permit{}, false
	}
}

func (b *CircuitBreaker) complete(permit permit, success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if permit.generation != b.generation || permit.state != b.state {
		return
	}

	if permit.state == StateHalfOpen {
		b.probeInFlight = false
		if success {
			b.state = StateClosed
			b.consecutiveFailures = 0
			b.recoveries++
			return
		}
		b.open()
		return
	}

	if success {
		b.consecutiveFailures = 0
		return
	}

	b.consecutiveFailures++
	if b.consecutiveFailures >= b.config.FailureThreshold {
		b.open()
	}
}

func (b *CircuitBreaker) open() {
	b.state = StateOpen
	b.probeInFlight = false
	b.openUntil = b.now().Add(b.config.OpenTimeout)
	b.opened++
	b.generation++
}
