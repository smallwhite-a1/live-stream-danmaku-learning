import type { RoomInsight } from './types'

export class InsightApiError extends Error {
  constructor(message: string, readonly status?: number) {
    super(message)
    this.name = 'InsightApiError'
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isNumber(value: unknown): value is number {
  return typeof value === 'number'
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === 'string')
}

function isRules(value: unknown): boolean {
  if (!isRecord(value)) return false

  return isNumber(value.message_count)
    && isNumber(value.unique_users)
    && isNumber(value.question_count)
    && isNumber(value.repeated_message_ratio)
    && isNumber(value.peak_messages_per_second)
    && isNumber(value.top_repeated_count)
    && (value.top_repeated_text === undefined || typeof value.top_repeated_text === 'string')
}

function isSemantic(value: unknown): boolean {
  if (!isRecord(value) || typeof value.summary !== 'string' || !Array.isArray(value.topics) || !isRecord(value.sentiment) || !Array.isArray(value.questions) || !Array.isArray(value.alerts)) return false

  return value.topics.every((topic) => isRecord(topic)
    && typeof topic.name === 'string'
    && isNumber(topic.confidence)
    && isStringArray(topic.evidence_event_ids))
    && (typeof value.sentiment.label === 'string'
      && ['positive', 'neutral', 'negative', 'mixed'].includes(value.sentiment.label)
      && isNumber(value.sentiment.confidence)
      && isStringArray(value.sentiment.evidence_event_ids))
    && value.questions.every((question) => isRecord(question)
      && typeof question.text === 'string'
      && isStringArray(question.evidence_event_ids))
    && value.alerts.every((alert) => isRecord(alert)
      && typeof alert.type === 'string'
      && ['low', 'medium', 'high'].includes(alert.severity as string)
      && typeof alert.description === 'string'
      && isStringArray(alert.evidence_event_ids))
}

function isModel(value: unknown): boolean {
  if (!isRecord(value)) return false

  return typeof value.provider === 'string'
    && typeof value.model === 'string'
    && typeof value.prompt_version === 'string'
    && isNumber(value.input_tokens)
    && isNumber(value.output_tokens)
    && isNumber(value.latency_millis)
}

function isInsight(value: unknown): value is RoomInsight {
  if (!isRecord(value)) return false

  return typeof value.room_id === 'string'
    && typeof value.window_start === 'string'
    && typeof value.window_end === 'string'
    && (value.status === 'normal' || value.status === 'degraded')
    && isRules(value.rules)
    && isSemantic(value.semantic)
    && isModel(value.model)
    && typeof value.generated_at === 'string'
    && (value.degraded_reason === undefined || typeof value.degraded_reason === 'string')
}

export async function fetchLatestInsight(roomID: string, signal: AbortSignal): Promise<RoomInsight> {
  const response = await fetch(`/api/v1/rooms/${encodeURIComponent(roomID)}/insights/latest`, { signal })

  if (!response.ok) {
    throw new InsightApiError('The insight service returned an error.', response.status)
  }

  const payload: unknown = await response.json()
  if (!isInsight(payload)) {
    throw new InsightApiError('The insight response was incomplete.')
  }

  return payload
}
