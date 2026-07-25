import type { MetricEvent, MetricSample, ServerMetrics } from "./types";

function counterDelta(previous: number, current: number): number {
  return current >= previous ? current - previous : 0;
}

function limitedDelta(previous: ServerMetrics, current: ServerMetrics): number {
  const previousTraffic = previous.websocket.traffic;
  const currentTraffic = current.websocket.traffic;

  return counterDelta(previousTraffic.danmaku_rejected_user, currentTraffic.danmaku_rejected_user)
    + counterDelta(previousTraffic.danmaku_rejected_room, currentTraffic.danmaku_rejected_room)
    + counterDelta(previousTraffic.like_rejected_user, currentTraffic.like_rejected_user)
    + counterDelta(previousTraffic.like_rejected_room, currentTraffic.like_rejected_room);
}

function droppedDelta(previous: ServerMetrics, current: ServerMetrics): number {
  return counterDelta(previous.websocket.ingress_dropped, current.websocket.ingress_dropped)
    + counterDelta(previous.websocket.dropped_messages, current.websocket.dropped_messages);
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
    limitedPerSecond: limitedDelta(previous.raw, current) / seconds,
    droppedPerSecond: droppedDelta(previous.raw, current) / seconds,
  };
}

type EventDetails = Omit<MetricEvent, "id" | "delta" | "observedAt">;

function redisLevel(
  previous: ServerMetrics["redis"],
  current: ServerMetrics["redis"],
): MetricEvent["level"] {
  if (current.status === "disabled" || current.circuit === "disabled") {
    return "info";
  }

  if (current.circuit === "open") {
    return "error";
  }

  if (current.circuit === "half_open" || current.status === "degraded") {
    return "warning";
  }

  if (
    current.status === "healthy"
    && current.circuit === "closed"
    && (previous.status === "degraded" || previous.circuit === "open" || previous.circuit === "half_open")
  ) {
    return "recovery";
  }

  return "info";
}

function kafkaLevel(
  previous: ServerMetrics["queue"]["status"],
  current: ServerMetrics["queue"]["status"],
): MetricEvent["level"] {
  if (current === "unavailable") {
    return "error";
  }

  if (current === "degraded") {
    return "warning";
  }

  if (current === "disabled") {
    return "info";
  }

  return previous === "degraded" || previous === "unavailable" ? "recovery" : "info";
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

  if (
    previous.redis.status !== current.redis.status
    || previous.redis.circuit !== current.redis.circuit
  ) {
    add({
      code: "redis_state_changed",
      level: redisLevel(previous.redis, current.redis),
      message: `Redis changed from ${previous.redis.status}/${previous.redis.circuit} to ${current.redis.status}/${current.redis.circuit}`,
    }, 1);
  }

  if (previous.queue.status !== current.queue.status) {
    add({
      code: "kafka_state_changed",
      level: kafkaLevel(previous.queue.status, current.queue.status),
      message: `Kafka queue changed from ${previous.queue.status} to ${current.queue.status}`,
    }, 1);
  }

  return [...events.values()];
}
