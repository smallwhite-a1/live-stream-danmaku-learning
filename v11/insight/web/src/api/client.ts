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

function isInsight(value: unknown): value is RoomInsight {
  if (!isRecord(value)) return false

  return typeof value.room_id === 'string'
    && typeof value.window_start === 'string'
    && typeof value.window_end === 'string'
    && (value.status === 'normal' || value.status === 'degraded')
    && isRecord(value.rules)
    && isRecord(value.semantic)
    && isRecord(value.model)
    && typeof value.generated_at === 'string'
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
