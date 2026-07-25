import {
  Activity,
  CircleGauge,
  Database,
  Gauge,
  HardDrive,
  MemoryStick,
  MessageSquareWarning,
  RadioTower,
  ServerCog,
  Users,
} from "lucide-react";

import {
  DependencyStatus,
  kafkaView,
  mysqlView,
  redisView,
} from "../components/DependencyStatus";
import { GovernanceEvents } from "../components/GovernanceEvents";
import { MetricCard } from "../components/MetricCard";
import { MetricChart } from "../components/MetricChart";
import { useMetrics } from "../metrics/useMetrics";

const rateFormatter = new Intl.NumberFormat("zh-CN", {
  maximumFractionDigits: 2,
});

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`;
  }

  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

function formatTimestamp(timestamp: number | null): string {
  if (timestamp === null) {
    return "尚无成功采样";
  }

  return new Intl.DateTimeFormat("zh-CN", {
    hour: "2-digit",
    hour12: false,
    minute: "2-digit",
    second: "2-digit",
  }).format(timestamp);
}

export function MonitorPage() {
  const metrics = useMetrics();
  const latest = metrics.latest;
  const currentSample = metrics.samples.at(-1);
  const traffic = latest?.websocket.traffic;
  const limitedTotal = traffic
    ? traffic.danmaku_rejected_user
      + traffic.danmaku_rejected_room
      + traffic.like_rejected_user
      + traffic.like_rejected_room
    : 0;
  const redis = redisView(latest);
  const kafka = kafkaView(latest);

  return (
    <main className="operations-page">
      <header className="page-heading">
        <div>
          <span className="section-kicker">RUNTIME / GOVERNANCE</span>
          <h1>运行监控</h1>
          <p>指标来自当前 Go 进程，每 2 秒采样一次。</p>
        </div>
        <div className="freshness">
          <span
            className={`freshness__state freshness__state--${metrics.freshness}`}
          >
            {metrics.freshness === "fresh" && "数据正常"}
            {metrics.freshness === "stale" && "数据已过期"}
            {metrics.freshness === "loading" && "正在连接"}
          </span>
          <time dateTime={metrics.lastSuccessAt ? new Date(metrics.lastSuccessAt).toISOString() : undefined}>
            最近成功 {formatTimestamp(metrics.lastSuccessAt)}
          </time>
        </div>
      </header>

      <section aria-label="运行摘要" className="metric-grid">
        <MetricCard
          detail={`${latest?.websocket.rooms ?? 0} 个活跃房间`}
          icon={Users}
          label="当前连接"
          testId="metric-current-connections"
          value={String(latest?.websocket.clients ?? "--")}
        />
        <MetricCard
          detail="由累计投递量差分计算"
          icon={Activity}
          label="每秒投递"
          testId="metric-delivered-rate"
          value={currentSample
            ? rateFormatter.format(currentSample.deliveredPerSecond)
            : "--"}
        />
        <MetricCard
          detail="广播入口队列未接收"
          icon={MessageSquareWarning}
          label="入口丢弃"
          testId="metric-ingress-dropped"
          tone={(latest?.websocket.ingress_dropped ?? 0) > 0 ? "warning" : "default"}
          value={String(latest?.websocket.ingress_dropped ?? "--")}
        />
        <MetricCard
          detail="弹幕与点赞，用户与房间合计"
          icon={Gauge}
          label="限流累计"
          testId="metric-limited-total"
          tone={limitedTotal > 0 ? "warning" : "default"}
          value={latest ? String(limitedTotal) : "--"}
        />
      </section>

      <div className="monitor-layout">
        <div className="monitor-layout__primary">
          <section className="monitor-section">
            <div className="monitor-section__heading">
              <div>
                <span className="section-kicker">LAST 60 SECONDS</span>
                <h2>投递与治理趋势</h2>
              </div>
              <CircleGauge aria-hidden="true" size={19} />
            </div>
            <MetricChart samples={metrics.samples} />
          </section>

          <section className="monitor-section">
            <div className="monitor-section__heading">
              <div>
                <span className="section-kicker">GO RUNTIME</span>
                <h2>进程资源</h2>
              </div>
              <ServerCog aria-hidden="true" size={19} />
            </div>
            <div className="runtime-grid">
              <div>
                <Activity aria-hidden="true" size={18} />
                <span>goroutine</span>
                <strong>{latest?.websocket.goroutines ?? "--"}</strong>
              </div>
              <div>
                <MemoryStick aria-hidden="true" size={18} />
                <span>内存占用</span>
                <strong>{latest ? formatBytes(latest.websocket.alloc_bytes) : "--"}</strong>
              </div>
              <div>
                <RadioTower aria-hidden="true" size={18} />
                <span>已投递累计</span>
                <strong>{latest?.websocket.delivered_messages ?? "--"}</strong>
              </div>
              <div>
                <HardDrive aria-hidden="true" size={18} />
                <span>慢客户端断开</span>
                <strong>{latest?.websocket.slow_client_disconnects ?? "--"}</strong>
              </div>
            </div>
          </section>
        </div>

        <aside className="monitor-layout__side">
          <section className="monitor-section">
            <div className="monitor-section__heading">
              <div>
                <span className="section-kicker">DEPENDENCIES</span>
                <h2>基础依赖</h2>
              </div>
              <Database aria-hidden="true" size={19} />
            </div>
            <div className="dependency-list">
              <DependencyStatus icon={RadioTower} name="Redis" {...redis} />
              <DependencyStatus icon={HardDrive} name="Kafka" {...kafka} />
              <DependencyStatus icon={Database} name="MySQL" {...mysqlView} />
            </div>
          </section>

          <section className="monitor-section monitor-section--events">
            <div className="monitor-section__heading">
              <div>
                <span className="section-kicker">SESSION EVENTS</span>
                <h2>最近治理事件</h2>
              </div>
              <span className="monitor-section__count">{metrics.events.length}</span>
            </div>
            <GovernanceEvents events={metrics.events} />
          </section>
        </aside>
      </div>
    </main>
  );
}
