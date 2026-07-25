import type { MetricEvent, MetricSample, ServerMetrics } from "./types";

function counterDelta(previous: number, current: number): number {
  return current >= previous ? current - previous : 0;
}

function limitedTotal(metrics: ServerMetrics): number {
  const { traffic } = metrics.websocket;

  return traffic.danmaku_rejected_user
    + traffic.danmaku_rejected_room
    + traffic.like_rejected_user
    + traffic.like_rejected_room;
}

function droppedTotal(metrics: ServerMetrics): number {
  return metrics.websocket.ingress_dropped + metrics.websocket.dropped_messages;
}

function rate(previous: number, current: number, seconds: number): number {
  return counterDelta(previous, current) / seconds;
}

export function deriveSample(
  previous: MetricSample | null,
  current: ServerMetrics,
  sampledAt: number,
): MetricSample {
  if (!previous) {
    return {
      sampledAt,
      raw: current,
      deliveredPerSecond: 0,
      limitedPerSecond: 0,
      droppedPerSecond: 0,
    };
  }

  const seconds = Math.max((sampledAt - previous.sampledAt) / 1_000, 0.001);

  return {
    sampledAt,
    raw: current,
    deliveredPerSecond: rate(
      previous.raw.websocket.delivered_messages,
      current.websocket.delivered_messages,
      seconds,
    ),
    limitedPerSecond: rate(limitedTotal(previous.raw), limitedTotal(current), seconds),
    droppedPerSecond: rate(droppedTotal(previous.raw), droppedTotal(current), seconds),
  };
}

type EventDetails = Omit<MetricEvent, "id" | "delta" | "observedAt">;

function stateLevel(
  previous: string,
  current: string,
  failedState: string,
): MetricEvent["level"] {
  if (current === failedState) {
    return current === "open" ? "error" : "warning";
  }

  return previous === failedState ? "recovery" : "info";
}

export function deriveEvents(
  previous: ServerMetrics | null,
  current: ServerMetrics,
  sampledAt: number,
): MetricEvent[] {
  if (!previous) {
    return [];
  }

  const events = new Map<MetricEvent["code"], MetricEvent>();
  const add = (details: EventDetails, delta: number) => {
    if (delta <= 0) {
      return;
    }

    const existing = events.get(details.code);
    if (existing) {
      existing.delta += delta;
      return;
    }

    events.set(details.code, {
      ...details,
      id: `${details.code}:${sampledAt}`,
      delta,
      observedAt: sampledAt,
    });
  };

  const previousTraffic = previous.websocket.traffic;
  const currentTraffic = current.websocket.traffic;
  add({
    code: "danmaku_limited",
    level: "warning",
    message: "Danmaku requests were rate limited",
  }, counterDelta(previousTraffic.danmaku_rejected_user, currentTraffic.danmaku_rejected_user));
  add({
    code: "danmaku_limited",
    level: "warning",
    message: "Danmaku requests were rate limited",
  }, counterDelta(previousTraffic.danmaku_rejected_room, currentTraffic.danmaku_rejected_room));
  add({
    code: "like_limited",
    level: "warning",
    message: "Like requests were rate limited",
  }, counterDelta(previousTraffic.like_rejected_user, currentTraffic.like_rejected_user));
  add({
    code: "like_limited",
    level: "warning",
    message: "Like requests were rate limited",
  }, counterDelta(previousTraffic.like_rejected_room, currentTraffic.like_rejected_room));
  add({
    code: "ingress_dropped",
    level: "warning",
    message: "Ingress messages were dropped",
  }, counterDelta(previous.websocket.ingress_dropped, current.websocket.ingress_dropped));
  add({
    code: "slow_client_disconnected",
    level: "error",
    message: "Slow clients were disconnected",
  }, counterDelta(
    previous.websocket.slow_client_disconnects,
    current.websocket.slow_client_disconnects,
  ));

  if (previous.redis.circuit !== current.redis.circuit) {
    add({
      code: "redis_state_changed",
      level: stateLevel(previous.redis.circuit, current.redis.circuit, "open"),
      message: `Redis circuit changed from ${previous.redis.circuit} to ${current.redis.circuit}`,
    }, 1);
  }

  if (previous.kafka?.status !== current.kafka?.status) {
    add({
      code: "kafka_state_changed",
      level: stateLevel(previous.kafka?.status ?? "disabled", current.kafka?.status ?? "disabled", "degraded"),
      message: `Kafka status changed from ${previous.kafka?.status ?? "disabled"} to ${current.kafka?.status ?? "disabled"}`,
    }, 1);
  }

  return [...events.values()];
}
