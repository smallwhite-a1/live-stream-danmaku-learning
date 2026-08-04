import { Activity, Clock, MessageCircleMore } from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'
import { fetchLatestInsight, InsightApiError } from '../api/client'
import type { EvidenceSelection, RoomInsight } from '../api/types'
import { EvidencePanel } from '../components/EvidencePanel'
import { InsightHeader } from '../components/InsightHeader'
import { RuleMetrics } from '../components/RuleMetrics'
import { SemanticSections } from '../components/SemanticSections'
import '../styles/tokens.css'
import '../styles/global.css'

const DEFAULT_ROOM_ID = 'room-alpha'

function formatWindow(value: string) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleTimeString('en-GB', { hour12: false, timeZone: 'UTC' })
}

function errorMessage(error: unknown, roomID: string) {
  if (error instanceof InsightApiError && error.status === 404) return `No insight was found for ${roomID}.`
  if (error instanceof InsightApiError) return error.message
  return 'Unable to reach the insight service.'
}

function InsightSummary({ insight, stale }: { insight: RoomInsight; stale: boolean }) {
  const statusLabel = insight.status === 'normal' ? 'Normal' : 'Degraded'
  return (
    <>
      <section className="summary-band" aria-label="Insight summary">
        <div className="summary-status">
          <Activity aria-hidden="true" size={18} />
          <div><span className={`status status-${insight.status}`}>{statusLabel}</span>{stale && <span className="stale">Stale</span>}</div>
        </div>
        <div className="window-time"><Clock aria-hidden="true" size={18} /><span>{formatWindow(insight.window_start)} - {formatWindow(insight.window_end)} UTC</span></div>
        <div className="summary-copy"><MessageCircleMore aria-hidden="true" size={18} /><p>{insight.semantic.summary}</p></div>
      </section>
      {insight.status === 'degraded' && insight.degraded_reason && <p className="degraded-reason">{insight.degraded_reason}</p>}
      <RuleMetrics rules={insight.rules} />
    </>
  )
}

export function App() {
  const activeController = useRef<AbortController | null>(null)
  const [roomInput, setRoomInput] = useState(DEFAULT_ROOM_ID)
  const [roomID, setRoomID] = useState(DEFAULT_ROOM_ID)
  const [insight, setInsight] = useState<RoomInsight | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [stale, setStale] = useState(false)
  const [selection, setSelection] = useState<EvidenceSelection | null>(null)

  const loadRoom = useCallback(async (nextRoomID: string) => {
    const requestedRoomID = nextRoomID.trim()
    if (!requestedRoomID) {
      setError('Enter a room ID.')
      return
    }

    activeController.current?.abort()
    const controller = new AbortController()
    activeController.current = controller
    setRoomID(requestedRoomID)
    setRoomInput(requestedRoomID)
    setLoading(true)
    setError(null)

    try {
      const nextInsight = await fetchLatestInsight(requestedRoomID, controller.signal)
      if (!controller.signal.aborted) {
        setInsight(nextInsight)
        setSelection(null)
        setStale(false)
      }
    } catch (requestError) {
      if (!controller.signal.aborted) {
        setError(errorMessage(requestError, requestedRoomID))
        setStale(true)
      }
    } finally {
      if (!controller.signal.aborted) setLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadRoom(DEFAULT_ROOM_ID)
    return () => activeController.current?.abort()
  }, [loadRoom])

  return (
    <div className="app-shell">
      <InsightHeader roomID={roomInput} loading={loading} onRoomIDChange={setRoomInput} onSearch={() => void loadRoom(roomInput)} onRefresh={() => void loadRoom(roomID)} />
      <main className="workspace" aria-busy={loading}>
        {error && <div className="error-state" role="alert">{error}</div>}
        {insight ? (
          <div className="insight-workspace">
            <InsightSummary insight={insight} stale={stale} />
            <div className="semantic-layout">
              <SemanticSections insight={insight} onSelectEvidence={setSelection} />
              <EvidencePanel selection={selection} onClose={() => setSelection(null)} />
            </div>
          </div>
        ) : loading ? <div className="loading-state" role="status">Loading insight</div> : <div className="empty-state">No insight is available for this room.</div>}
      </main>
    </div>
  )
}
