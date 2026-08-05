package rule

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
)

type Analyzer struct{}

func NewAnalyzer() Analyzer {
	return Analyzer{}
}

func (Analyzer) Analyze(ctx context.Context, window domain.InsightWindow) (domain.AnalysisResult, error) {
	if err := ctx.Err(); err != nil {
		return domain.AnalysisResult{}, err
	}

	counts := make(map[string]int, len(window.Events))
	users := make(map[string]struct{}, len(window.Events))
	perSecond := make(map[time.Time]int, len(window.Events))
	questionCount := 0

	for _, event := range window.Events {
		content := normalize(event.Content)
		counts[content]++
		users[event.UserID] = struct{}{}
		if isQuestion(content) {
			questionCount++
		}
		perSecond[event.OccurredAt.UTC().Truncate(time.Second)]++
	}

	stats := domain.RuleStats{
		MessageCount:  len(window.Events),
		UniqueUsers:   len(users),
		QuestionCount: questionCount,
	}
	for _, count := range perSecond {
		if count > stats.PeakMessagesPerSecond {
			stats.PeakMessagesPerSecond = count
		}
	}

	var repeatedMessages int
	var repeatedTexts []string
	for text, count := range counts {
		if count > 1 {
			repeatedMessages += count
			repeatedTexts = append(repeatedTexts, text)
		}
	}
	if stats.MessageCount > 0 {
		stats.RepeatedMessageRatio = float64(repeatedMessages) / float64(stats.MessageCount)
	}
	sort.Strings(repeatedTexts)
	for _, text := range repeatedTexts {
		if counts[text] > stats.TopRepeatedCount {
			stats.TopRepeatedText = text
			stats.TopRepeatedCount = counts[text]
		}
	}

	return domain.AnalysisResult{Rules: stats}, nil
}

func normalize(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func isQuestion(value string) bool {
	return strings.ContainsAny(value, "?？") || strings.Contains(value, "吗") || strings.Contains(value, "么") || strings.Contains(value, "什么")
}
