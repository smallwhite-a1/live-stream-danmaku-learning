import type { LucideIcon } from "lucide-react";

import type { ServerMetrics } from "../metrics/types";

export type DependencyTone = "healthy" | "warning" | "error" | "neutral";

export interface DependencyView {
  detail: string;
  label: string;
  tone: DependencyTone;
}

interface DependencyStatusProps extends DependencyView {
  icon: LucideIcon;
  name: string;
}

export function redisView(metrics: ServerMetrics | null): DependencyView {
  if (!metrics) {
    return { detail: "等待服务端指标", label: "等待指标", tone: "neutral" };
  }
  if (metrics.redis.status === "disabled" || metrics.redis.circuit === "disabled") {
    return { detail: "当前使用本机房间广播", label: "未启用", tone: "neutral" };
  }
  if (metrics.redis.circuit === "open") {
    return { detail: "熔断器已打开，广播已回退本机", label: "熔断", tone: "error" };
  }
  if (metrics.redis.circuit === "half_open") {
    return { detail: "正在进行恢复探测", label: "探测恢复", tone: "warning" };
  }
  if (metrics.redis.status === "degraded") {
    return { detail: "跨实例广播异常，当前允许本机降级", label: "降级", tone: "warning" };
  }

  return { detail: "跨实例房间广播可用", label: "正常", tone: "healthy" };
}

export function kafkaView(metrics: ServerMetrics | null): DependencyView {
  if (!metrics) {
    return { detail: "等待服务端指标", label: "等待指标", tone: "neutral" };
  }

  switch (metrics.queue.status) {
    case "disabled":
      return { detail: "当前不执行异步持久化", label: "未启用", tone: "neutral" };
    case "unavailable":
      return { detail: "Producer 未能建立可用链路", label: "不可用", tone: "error" };
    case "degraded":
      return { detail: "发送错误或积压已触发降级状态", label: "降级", tone: "warning" };
    case "healthy":
      return { detail: "Producer 投递状态正常", label: "正常", tone: "healthy" };
  }
}

export const mysqlView: DependencyView = {
  detail: "数据库位于独立 Consumer 进程之后",
  label: "当前接口不可观测",
  tone: "neutral",
};

export function DependencyStatus({
  detail,
  icon: Icon,
  label,
  name,
  tone,
}: DependencyStatusProps) {
  return (
    <div className="dependency-status">
      <div className="dependency-status__identity">
        <span className="dependency-status__icon">
          <Icon aria-hidden="true" size={17} />
        </span>
        <div>
          <strong>{name}</strong>
          <small>{detail}</small>
        </div>
      </div>
      <span className={`status-label status-label--${tone}`}>
        <span aria-hidden="true" />
        {label}
      </span>
    </div>
  );
}
