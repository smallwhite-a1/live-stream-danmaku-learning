package eino

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
)

const (
	maxSummaryRunes     = 1000
	maxTopicNameRunes   = 128
	maxQuestionRunes    = 500
	maxAlertRunes       = 500
	maxTopics           = 10
	maxQuestions        = 20
	maxAlerts           = 20
	maxEvidencePerField = 10
)

func parseAndValidate(content string, window domain.InsightWindow) (domain.SemanticInsight, error) {
	decoder := json.NewDecoder(strings.NewReader(content))
	decoder.DisallowUnknownFields()
	var semantic domain.SemanticInsight
	if err := decoder.Decode(&semantic); err != nil {
		return domain.SemanticInsight{}, fmt.Errorf("decode semantic insight: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return domain.SemanticInsight{}, errors.New("semantic insight must contain one JSON value")
	}

	evidence := make(map[string]struct{}, len(window.Events))
	for _, event := range window.Events {
		evidence[event.EventID] = struct{}{}
	}
	if err := validateSemantic(semantic, evidence); err != nil {
		return domain.SemanticInsight{}, err
	}
	return semantic, nil
}

func validateSemantic(semantic domain.SemanticInsight, allowed map[string]struct{}) error {
	if utf8.RuneCountInString(semantic.Summary) > maxSummaryRunes {
		return errors.New("summary exceeds 1000 runes")
	}
	if len(semantic.Topics) > maxTopics || len(semantic.Questions) > maxQuestions || len(semantic.Alerts) > maxAlerts {
		return errors.New("semantic collection exceeds limit")
	}
	if !validSentiment(semantic.Sentiment.Label) {
		return errors.New("unsupported sentiment label")
	}
	if err := validateConfidence(semantic.Sentiment.Confidence); err != nil {
		return fmt.Errorf("sentiment: %w", err)
	}
	if err := validateEvidence(semantic.Sentiment.EvidenceEventIDs, allowed); err != nil {
		return fmt.Errorf("sentiment: %w", err)
	}
	for _, topic := range semantic.Topics {
		if strings.TrimSpace(topic.Name) == "" || utf8.RuneCountInString(topic.Name) > maxTopicNameRunes {
			return errors.New("invalid topic name")
		}
		if err := validateConfidence(topic.Confidence); err != nil {
			return fmt.Errorf("topic: %w", err)
		}
		if err := validateEvidence(topic.EvidenceEventIDs, allowed); err != nil {
			return fmt.Errorf("topic: %w", err)
		}
	}
	for _, question := range semantic.Questions {
		if strings.TrimSpace(question.Text) == "" || utf8.RuneCountInString(question.Text) > maxQuestionRunes {
			return errors.New("invalid question text")
		}
		if err := validateEvidence(question.EvidenceEventIDs, allowed); err != nil {
			return fmt.Errorf("question: %w", err)
		}
	}
	for _, alert := range semantic.Alerts {
		if strings.TrimSpace(alert.Type) == "" || utf8.RuneCountInString(alert.Type) > 64 || utf8.RuneCountInString(alert.Description) > maxAlertRunes {
			return errors.New("invalid alert")
		}
		if !validSeverity(alert.Severity) {
			return errors.New("unsupported alert severity")
		}
		if err := validateEvidence(alert.EvidenceEventIDs, allowed); err != nil {
			return fmt.Errorf("alert: %w", err)
		}
	}
	return nil
}

func validateConfidence(value float64) error {
	if value < 0 || value > 1 {
		return errors.New("confidence must be between 0 and 1")
	}
	return nil
}

func validateEvidence(ids []string, allowed map[string]struct{}) error {
	if len(ids) > maxEvidencePerField {
		return errors.New("too many evidence event IDs")
	}
	for _, id := range ids {
		if _, ok := allowed[id]; !ok {
			return fmt.Errorf("unknown evidence event ID %q", id)
		}
	}
	return nil
}

func validSentiment(label string) bool {
	return label == "positive" || label == "neutral" || label == "negative" || label == "mixed"
}

func validSeverity(severity string) bool {
	return severity == "low" || severity == "medium" || severity == "high"
}
