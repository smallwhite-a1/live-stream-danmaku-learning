package domain

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

const maxAlertTypeRunes = 64

type RuleStats struct {
	MessageCount          int     `json:"message_count"`
	UniqueUsers           int     `json:"unique_users"`
	QuestionCount         int     `json:"question_count"`
	RepeatedMessageRatio  float64 `json:"repeated_message_ratio"`
	PeakMessagesPerSecond int     `json:"peak_messages_per_second"`
	TopRepeatedText       string  `json:"top_repeated_text,omitempty"`
	TopRepeatedCount      int     `json:"top_repeated_count"`
}

type Topic struct {
	Name             string   `json:"name"`
	Confidence       float64  `json:"confidence"`
	EvidenceEventIDs []string `json:"evidence_event_ids"`
}

type Sentiment struct {
	Label            string   `json:"label"`
	Confidence       float64  `json:"confidence"`
	EvidenceEventIDs []string `json:"evidence_event_ids"`
}

type Question struct {
	Text             string   `json:"text"`
	EvidenceEventIDs []string `json:"evidence_event_ids"`
}

type Alert struct {
	Type             string   `json:"type"`
	Severity         string   `json:"severity"`
	Description      string   `json:"description"`
	EvidenceEventIDs []string `json:"evidence_event_ids"`
}

type SemanticInsight struct {
	Summary   string     `json:"summary"`
	Topics    []Topic    `json:"topics"`
	Sentiment Sentiment  `json:"sentiment"`
	Questions []Question `json:"questions"`
	Alerts    []Alert    `json:"alerts"`
}

type ModelMeta struct {
	Provider      string `json:"provider"`
	Model         string `json:"model"`
	PromptVersion string `json:"prompt_version"`
	InputTokens   int    `json:"input_tokens"`
	OutputTokens  int    `json:"output_tokens"`
	LatencyMillis int64  `json:"latency_millis"`
}

type AnalysisResult struct {
	Rules    RuleStats       `json:"rules"`
	Semantic SemanticInsight `json:"semantic"`
	Model    ModelMeta       `json:"model"`
}

type InsightStatus string

const (
	InsightStatusNormal   InsightStatus = "normal"
	InsightStatusDegraded InsightStatus = "degraded"
)

type RoomInsight struct {
	RoomID         string          `json:"room_id"`
	WindowStart    time.Time       `json:"window_start"`
	WindowEnd      time.Time       `json:"window_end"`
	Status         InsightStatus   `json:"status"`
	Rules          RuleStats       `json:"rules"`
	Semantic       SemanticInsight `json:"semantic"`
	Model          ModelMeta       `json:"model"`
	GeneratedAt    time.Time       `json:"generated_at"`
	DegradedReason string          `json:"degraded_reason,omitempty"`
}

func (i RoomInsight) Validate() error {
	switch {
	case strings.TrimSpace(i.RoomID) == "":
		return errors.New("room ID is required")
	case i.WindowStart.IsZero():
		return errors.New("window start is required")
	case !i.WindowEnd.After(i.WindowStart):
		return errors.New("window end must be after window start")
	case i.Status != InsightStatusNormal && i.Status != InsightStatusDegraded:
		return errors.New("unsupported insight status")
	case strings.TrimSpace(i.Model.PromptVersion) == "":
		return errors.New("prompt version is required")
	case i.GeneratedAt.IsZero():
		return errors.New("generated at is required")
	case !validSentiment(i.Semantic.Sentiment.Label):
		return errors.New("unsupported sentiment label")
	}
	for _, alert := range i.Semantic.Alerts {
		if strings.TrimSpace(alert.Type) == "" {
			return errors.New("alert type is required")
		}
		if utf8.RuneCountInString(alert.Type) > maxAlertTypeRunes {
			return errors.New("alert type exceeds 64 runes")
		}
		if !validSeverity(alert.Severity) {
			return errors.New("unsupported alert severity")
		}
	}
	return nil
}

func (i RoomInsight) IdempotencyKey() string {
	return strings.TrimSpace(i.RoomID) + ":" + utcKeyTime(i.WindowStart) + ":" + utcKeyTime(i.WindowEnd) + ":" + strings.TrimSpace(i.Model.PromptVersion)
}

func validSentiment(label string) bool {
	switch label {
	case "positive", "neutral", "negative", "mixed":
		return true
	default:
		return false
	}
}

func validSeverity(severity string) bool {
	switch severity {
	case "low", "medium", "high":
		return true
	default:
		return false
	}
}
