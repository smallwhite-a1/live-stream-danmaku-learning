export type InsightStatus = 'normal' | 'degraded'
export type SentimentLabel = 'positive' | 'neutral' | 'negative' | 'mixed'
export type AlertSeverity = 'low' | 'medium' | 'high'

export interface RuleStats {
  message_count: number
  unique_users: number
  question_count: number
  repeated_message_ratio: number
  peak_messages_per_second: number
  top_repeated_text?: string
  top_repeated_count: number
}

export interface EvidenceClaim {
  evidence_event_ids: string[]
}

export interface Topic extends EvidenceClaim {
  name: string
  confidence: number
}

export interface Sentiment extends EvidenceClaim {
  label: SentimentLabel
  confidence: number
}

export interface Question extends EvidenceClaim {
  text: string
}

export interface Alert extends EvidenceClaim {
  type: string
  severity: AlertSeverity
  description: string
}

export interface SemanticInsight {
  summary: string
  topics: Topic[]
  sentiment: Sentiment
  questions: Question[]
  alerts: Alert[]
}

export interface ModelMeta {
  provider: string
  model: string
  prompt_version: string
  input_tokens: number
  output_tokens: number
  latency_millis: number
}

export interface RoomInsight {
  room_id: string
  window_start: string
  window_end: string
  status: InsightStatus
  rules: RuleStats
  semantic: SemanticInsight
  model: ModelMeta
  generated_at: string
  degraded_reason?: string
}

export interface EvidenceSelection {
  label: string
  eventIDs: string[]
}
