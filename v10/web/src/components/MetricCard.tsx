import type { LucideIcon } from "lucide-react";

interface MetricCardProps {
  detail: string;
  icon: LucideIcon;
  label: string;
  testId: string;
  tone?: "default" | "warning" | "error";
  value: string;
}

export function MetricCard({
  detail,
  icon: Icon,
  label,
  testId,
  tone = "default",
  value,
}: MetricCardProps) {
  return (
    <article
      className={`metric-card metric-card--${tone}`}
      data-testid={testId}
    >
      <div className="metric-card__heading">
        <span>{label}</span>
        <Icon aria-hidden="true" size={18} />
      </div>
      <strong>{value}</strong>
      <small>{detail}</small>
    </article>
  );
}
