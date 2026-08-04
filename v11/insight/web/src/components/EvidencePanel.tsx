import { Files, X } from 'lucide-react'
import type { EvidenceSelection } from '../api/types'

interface EvidencePanelProps {
  selection: EvidenceSelection | null
  onClose: () => void
}

export function EvidencePanel({ selection, onClose }: EvidencePanelProps) {
  return (
    <aside className="evidence-panel" aria-label="Evidence">
      <div className="evidence-heading">
        <div><Files aria-hidden="true" size={18} /><h2>Evidence</h2></div>
        {selection && <button className="icon-button" type="button" aria-label="Close evidence" title="Close evidence" onClick={onClose}><X aria-hidden="true" size={16} /></button>}
      </div>
      {selection ? (
        <>
          <p className="evidence-claim">{selection.label}</p>
          {selection.eventIDs.length > 0 ? <ul className="event-list">{selection.eventIDs.map((eventID) => <li key={eventID}><code>{eventID}</code></li>)}</ul> : <p className="empty-list">No EventIDs are available for this claim.</p>}
        </>
      ) : <p className="empty-list">No evidence selected.</p>}
    </aside>
  )
}
