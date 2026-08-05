import { Files } from 'lucide-react'
import type { EvidenceSelection, RoomInsight } from '../api/types'

interface SemanticSectionsProps {
  insight: RoomInsight
  onSelectEvidence: (selection: EvidenceSelection) => void
}

function displayLabel(value: string) {
  return value.charAt(0).toUpperCase() + value.slice(1)
}

interface EvidenceButtonProps {
  label: string
  eventIDs: string[]
  onSelectEvidence: (selection: EvidenceSelection) => void
}

function EvidenceButton({ label, eventIDs, onSelectEvidence }: EvidenceButtonProps) {
  return (
    <button
      className="evidence-button"
      type="button"
      aria-label={`View evidence for ${label}`}
      title={`View evidence for ${label}`}
      onClick={() => onSelectEvidence({ label, eventIDs })}
    >
      <Files aria-hidden="true" size={16} />
      <span>{eventIDs.length}</span>
    </button>
  )
}

function EmptyList({ text }: { text: string }) {
  return <p className="empty-list">{text}</p>
}

export function SemanticSections({ insight, onSelectEvidence }: SemanticSectionsProps) {
  const { semantic } = insight
  return (
    <section className="semantic-sections" aria-label="Semantic insight">
      <section className="semantic-section" aria-labelledby="topics-heading">
        <div className="section-heading"><h2 id="topics-heading">Topics</h2><span>{semantic.topics.length}</span></div>
        {semantic.topics.length === 0 ? <EmptyList text="No topics in this window." /> : (
          <ul className="semantic-list">
            {semantic.topics.map((topic, index) => (
              <li className="semantic-item" key={`${topic.name}-${index}`}>
                <div className="claim-text"><strong>{topic.name}</strong><span>{Math.round(topic.confidence * 100)}% confidence</span></div>
                <EvidenceButton label={topic.name} eventIDs={topic.evidence_event_ids} onSelectEvidence={onSelectEvidence} />
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="semantic-section" aria-labelledby="sentiment-heading">
        <div className="section-heading"><h2 id="sentiment-heading">Sentiment</h2></div>
        <div className="semantic-item">
          <div className="claim-text"><strong className={`sentiment-${semantic.sentiment.label}`}>{displayLabel(semantic.sentiment.label)}</strong><span>{Math.round(semantic.sentiment.confidence * 100)}% confidence</span></div>
          <EvidenceButton label={`${displayLabel(semantic.sentiment.label)} sentiment`} eventIDs={semantic.sentiment.evidence_event_ids} onSelectEvidence={onSelectEvidence} />
        </div>
      </section>

      <section className="semantic-section" aria-labelledby="questions-heading">
        <div className="section-heading"><h2 id="questions-heading">Questions</h2><span>{semantic.questions.length}</span></div>
        {semantic.questions.length === 0 ? <EmptyList text="No questions in this window." /> : (
          <ul className="semantic-list">
            {semantic.questions.map((question, index) => (
              <li className="semantic-item" key={`${question.text}-${index}`}>
                <div className="claim-text"><strong>{question.text}</strong></div>
                <EvidenceButton label={question.text} eventIDs={question.evidence_event_ids} onSelectEvidence={onSelectEvidence} />
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="semantic-section" aria-labelledby="alerts-heading">
        <div className="section-heading"><h2 id="alerts-heading">Alerts</h2><span>{semantic.alerts.length}</span></div>
        {semantic.alerts.length === 0 ? <EmptyList text="No alerts in this window." /> : (
          <ul className="semantic-list">
            {semantic.alerts.map((alert, index) => (
              <li className="semantic-item" key={`${alert.type}-${index}`}>
                <div className="claim-text"><strong className={`severity-${alert.severity}`}>{alert.type}</strong><span>{alert.description}</span></div>
                <EvidenceButton label={alert.type} eventIDs={alert.evidence_event_ids} onSelectEvidence={onSelectEvidence} />
              </li>
            ))}
          </ul>
        )}
      </section>
    </section>
  )
}
