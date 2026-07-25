import {
  Bot,
  Database,
  GitBranch,
  Inbox,
  Network,
  PanelsTopLeft,
  RadioTower,
  Send,
  ShieldCheck,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";

import {
  kafkaView,
  mysqlView,
  redisView,
  type DependencyTone,
  type DependencyView,
} from "../components/DependencyStatus";
import { useMetrics } from "../metrics/useMetrics";

interface ChainNodeProps {
  detail: string;
  icon: LucideIcon;
  name: string;
  status: string;
  testId?: string;
  tone: DependencyTone;
}

function ChainNode({
  detail,
  icon: Icon,
  name,
  status,
  testId,
  tone,
}: ChainNodeProps) {
  return (
    <article className="chain-node" data-testid={testId}>
      <div className="chain-node__icon">
        <Icon aria-hidden="true" size={19} />
      </div>
      <div className="chain-node__body">
        <strong>{name}</strong>
        <span>{detail}</span>
      </div>
      <span className={`status-label status-label--${tone}`}>
        <span aria-hidden="true" />
        {status}
      </span>
    </article>
  );
}

function runtimeView(
  freshness: "loading" | "fresh" | "stale",
  healthyDetail: string,
): DependencyView {
  if (freshness === "loading") {
    return { detail: "等待服务端指标", label: "等待指标", tone: "neutral" };
  }
  if (freshness === "stale") {
    return { detail: "保留上次状态，当前采样已过期", label: "数据过期", tone: "warning" };
  }

  return { detail: healthyDetail, label: "运行中", tone: "healthy" };
}

function consumerView(queue: DependencyView): DependencyView {
  if (queue.label === "未启用") {
    return { detail: "Kafka 未启用时无需启动", label: "未启用", tone: "neutral" };
  }

  return {
    detail: "独立进程，当前服务未聚合其健康状态",
    label: "独立进程",
    tone: "neutral",
  };
}

export function ChainPage() {
  const metrics = useMetrics();
  const websocket = runtimeView(metrics.freshness, "完成协议校验、身份校验与流量限制");
  const manager = runtimeView(metrics.freshness, "单线程管理房间，工作池执行扇出");
  const redis = redisView(metrics.latest);
  const kafka = kafkaView(metrics.latest);
  const consumer = consumerView(kafka);

  return (
    <main className="operations-page chain-page">
      <header className="page-heading">
        <div>
          <span className="section-kicker">MESSAGE PATH / OWNERSHIP</span>
          <h1>链路说明</h1>
          <p>实时广播与异步持久化在 Manager 接收成功后分开推进。</p>
        </div>
        <div className="chain-legend" aria-label="状态图例">
          <span><i className="legend-dot legend-dot--healthy" />正常</span>
          <span><i className="legend-dot legend-dot--warning" />降级</span>
          <span><i className="legend-dot legend-dot--neutral" />不可观测</span>
        </div>
      </header>

      <section aria-label="弹幕处理链路" className="chain-console">
        <div className="chain-ingress">
          <ChainNode
            detail="建立 WebSocket，发送 101 / 103 消息"
            icon={PanelsTopLeft}
            name="浏览器"
            status="已接入"
            tone="healthy"
          />
          <span className="chain-arrow" aria-hidden="true">↓</span>
          <ChainNode
            detail={websocket.detail}
            icon={ShieldCheck}
            name="WebSocket 校验与限流"
            status={websocket.label}
            tone={websocket.tone}
          />
          <span className="chain-arrow" aria-hidden="true">↓</span>
          <ChainNode
            detail={manager.detail}
            icon={Network}
            name="Manager 房间广播"
            status={manager.label}
            tone={manager.tone}
          />
        </div>

        <div className="chain-split">
          <GitBranch aria-hidden="true" size={22} />
          <span>接收成功后分支</span>
        </div>

        <div className="chain-branches">
          <section
            aria-labelledby="realtime-branch-title"
            className="chain-branch"
            data-testid="realtime-branch"
          >
            <header>
              <span className="section-kicker">LOW LATENCY</span>
              <h2 id="realtime-branch-title">实时广播分支</h2>
            </header>
            <ChainNode
              detail={redis.detail}
              icon={RadioTower}
              name="Redis / 本机降级"
              status={redis.label}
              testId="chain-node-redis"
              tone={redis.tone}
            />
            <div className="chain-outcome">
              <Send aria-hidden="true" size={17} />
              <span>safeSend 写入房间客户端发送队列</span>
            </div>
          </section>

          <section
            aria-labelledby="persistence-branch-title"
            className="chain-branch"
            data-testid="persistence-branch"
          >
            <header>
              <span className="section-kicker">DURABLE ASYNC</span>
              <h2 id="persistence-branch-title">异步持久化分支</h2>
            </header>
            <ChainNode
              detail={kafka.detail}
              icon={Send}
              name="Kafka Producer"
              status={kafka.label}
              testId="chain-node-kafka"
              tone={kafka.tone}
            />
            <div className="consumer-fork">
              <aside className="future-branch" data-testid="future-ai-branch">
                <Bot aria-hidden="true" size={20} />
                <div>
                  <span className="section-kicker">FUTURE / OFFLINE</span>
                  <strong>V11 独立异步消费者</strong>
                  <small>从消息队列旁路消费，不进入实时广播关键路径</small>
                </div>
              </aside>

              <div className="current-consumer-path">
                <span className="chain-arrow" aria-hidden="true">↓</span>
                <ChainNode
                  detail={consumer.detail}
                  icon={Inbox}
                  name="Consumer"
                  status={consumer.label}
                  tone={consumer.tone}
                />
                <span className="chain-arrow" aria-hidden="true">↓</span>
                <ChainNode
                  detail={mysqlView.detail}
                  icon={Database}
                  name="MySQL"
                  status={mysqlView.label}
                  testId="chain-node-mysql"
                  tone={mysqlView.tone}
                />
              </div>
            </div>
          </section>
        </div>
      </section>
    </main>
  );
}
