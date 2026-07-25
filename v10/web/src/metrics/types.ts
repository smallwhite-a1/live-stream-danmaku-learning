export interface ServerMetrics {
  websocket: {
    rooms: number;
    clients: number;
    ingress_accepted: number;
    ingress_dropped: number;
    delivered_messages: number;
    dropped_messages: number;
    slow_client_disconnects: number;
    goroutines: number;
    alloc_bytes: number;
    traffic: {
      current_connections: number;
      danmaku_rejected_user: number;
      danmaku_rejected_room: number;
      like_rejected_user: number;
      like_rejected_room: number;
    };
    redis_circuit?: {
      state: "closed" | "open" | "half_open";
      opened: number;
      rejected: number;
      recoveries: number;
    };
  };
  kafka?: {
    enqueued: number;
    acked: number;
    dropped: number;
    errors: number;
    status: "healthy" | "degraded";
  };
  queue: { status: "healthy" | "degraded" | "disabled" | "unavailable" };
  redis: {
    status: "healthy" | "degraded" | "disabled";
    circuit: "closed" | "open" | "half_open" | "disabled";
  };
}

export interface MetricSample {
  sampledAt: number;
  raw: ServerMetrics;
  deliveredPerSecond: number;
  limitedPerSecond: number;
  droppedPerSecond: number;
}

export interface MetricEvent {
  id: string;
  code:
    | "danmaku_limited"
    | "like_limited"
    | "ingress_dropped"
    | "slow_client_disconnected"
    | "redis_state_changed"
    | "kafka_state_changed";
  level: "info" | "warning" | "error" | "recovery";
  message: string;
  delta: number;
  observedAt: number;
}
