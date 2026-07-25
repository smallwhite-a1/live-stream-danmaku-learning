import { AlertTriangle, CheckCircle2, CircleAlert, Info } from "lucide-react";

import type { MetricEvent } from "../metrics/types";

interface GovernanceEventsProps {
  events: readonly MetricEvent[];
}

const eventText: Record<MetricEvent["code"], string> = {
  danmaku_limited: "弹幕请求被限流",
  like_limited: "点赞请求被限流",
  ingress_dropped: "入口消息被丢弃",
  slow_client_disconnected: "慢客户端已断开",
  redis_state_changed: "Redis 状态发生变化",
  kafka_state_changed: "Kafka 状态发生变化",
};

const levelIcon = {
  error: CircleAlert,
  info: Info,
  recovery: CheckCircle2,
  warning: AlertTriangle,
};

function formatTime(timestamp: number): string {
  return new Intl.DateTimeFormat("zh-CN", {
    hour: "2-digit",
    hour12: false,
    minute: "2-digit",
    second: "2-digit",
  }).format(timestamp);
}

export function GovernanceEvents({ events }: GovernanceEventsProps) {
  if (events.length === 0) {
    return (
      <div className="governance-events__empty">
        当前会话尚未推导出治理事件
      </div>
    );
  }

  return (
    <div className="governance-events">
      {[...events].reverse().map((event) => {
        const Icon = levelIcon[event.level];

        return (
          <article
            className={`governance-event governance-event--${event.level}`}
            data-testid="governance-event"
            key={event.id}
          >
            <Icon aria-hidden="true" size={16} />
            <div>
              <strong>{eventText[event.code]}</strong>
              <span>
                {formatTime(event.observedAt)}
                {" · "}
                变化 +{event.delta}
              </span>
            </div>
          </article>
        );
      })}
    </div>
  );
}
