package app

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/ports"
)

const (
	maxDueWindows      = 32
	defaultJobCapacity = 128
)

type Config struct {
	Workers     int
	JobCapacity int
}

type Summary struct {
	Completed int
	Degraded  int
	Failed    int
}

type workerResult struct {
	summary Summary
	err     error
}

type Processor struct {
	store      ports.WindowStore
	primary    ports.InsightAnalyzer
	fallback   ports.InsightAnalyzer
	repository ports.InsightRepository
	config     Config
}

func NewProcessor(store ports.WindowStore, primary, fallback ports.InsightAnalyzer, repository ports.InsightRepository, config Config) (*Processor, error) {
	if store == nil || primary == nil || fallback == nil || repository == nil {
		return nil, errors.New("processor dependencies are required")
	}
	if config.Workers <= 0 {
		return nil, errors.New("workers must be positive")
	}
	if config.JobCapacity < 0 {
		return nil, errors.New("job capacity must not be negative")
	}
	if config.JobCapacity == 0 {
		config.JobCapacity = defaultJobCapacity
	}
	return &Processor{store: store, primary: primary, fallback: fallback, repository: repository, config: config}, nil
}

func (p *Processor) ProcessDue(ctx context.Context, now time.Time) (Summary, error) {
	refs, err := p.store.ClaimDue(ctx, now, maxDueWindows)
	if err != nil {
		return Summary{}, err
	}
	jobs := make(chan domain.WindowRef, p.config.JobCapacity)
	results := make(chan workerResult, len(refs))
	var workers sync.WaitGroup
	workers.Add(p.config.Workers)
	for range p.config.Workers {
		go func() {
			defer workers.Done()
			for ref := range jobs {
				results <- p.processWindow(ctx, ref, now)
			}
		}()
	}
	for _, ref := range refs {
		jobs <- ref
	}
	close(jobs)
	workers.Wait()
	close(results)

	var summary Summary
	var processErr error
	for result := range results {
		summary.Completed += result.summary.Completed
		summary.Degraded += result.summary.Degraded
		summary.Failed += result.summary.Failed
		processErr = errors.Join(processErr, result.err)
	}
	return summary, processErr
}

func (p *Processor) processWindow(ctx context.Context, ref domain.WindowRef, now time.Time) workerResult {
	window, err := p.store.Load(ctx, ref)
	if err != nil {
		return p.failed(ctx, ref, now)
	}
	result, err := p.primary.Analyze(ctx, window)
	status := domain.InsightStatusNormal
	reason := ""
	if err != nil {
		reason = err.Error()
		result, err = p.fallback.Analyze(ctx, window)
		if err != nil {
			return p.failed(ctx, ref, now)
		}
		status = domain.InsightStatusDegraded
		result.Semantic = domain.SemanticInsight{
			Topics:    []domain.Topic{},
			Sentiment: domain.Sentiment{Label: "neutral", EvidenceEventIDs: []string{}},
			Questions: []domain.Question{},
			Alerts:    []domain.Alert{},
		}
		result.Model = domain.ModelMeta{Provider: "rule", Model: "rule", PromptVersion: "rule.v1"}
	}
	insight := domain.RoomInsight{
		RoomID: ref.RoomID, WindowStart: ref.Start, WindowEnd: ref.End, Status: status,
		Rules: result.Rules, Semantic: result.Semantic, Model: result.Model, GeneratedAt: now.UTC(), DegradedReason: reason,
	}
	if _, err := p.repository.Save(ctx, insight); err != nil {
		return p.failed(ctx, ref, now)
	}
	if err := p.store.Complete(ctx, ref); err != nil {
		return p.failed(ctx, ref, now)
	}
	if status == domain.InsightStatusDegraded {
		return workerResult{summary: Summary{Degraded: 1}}
	}
	return workerResult{summary: Summary{Completed: 1}}
}

func (p *Processor) failed(ctx context.Context, ref domain.WindowRef, now time.Time) workerResult {
	return workerResult{summary: Summary{Failed: 1}, err: p.store.Release(ctx, ref, now)}
}
