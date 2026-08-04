import type { RuleStats } from '../api/types'

interface RuleMetricsProps {
  rules: RuleStats
}

const metrics: Array<{ key: keyof RuleStats; label: string; format?: (value: number) => string }> = [
  { key: 'message_count', label: 'Messages' },
  { key: 'unique_users', label: 'Unique users' },
  { key: 'question_count', label: 'Questions' },
  { key: 'repeated_message_ratio', label: 'Repeated ratio', format: (value) => `${(value * 100).toFixed(1)}%` },
  { key: 'peak_messages_per_second', label: 'Peak msg/s' },
]

export function RuleMetrics({ rules }: RuleMetricsProps) {
  return (
    <section className="metrics" aria-label="Rule metrics">
      {metrics.map(({ key, label, format }) => {
        const value = rules[key]
        return (
          <div className="metric" key={key}>
            <dt>{label}</dt>
            <dd>{typeof value === 'number' ? (format ? format(value) : value.toLocaleString()) : '0'}</dd>
          </div>
        )
      })}
    </section>
  )
}
