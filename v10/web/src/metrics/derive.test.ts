import { describe, expect, it } from "vitest";

import { deriveEvents, deriveSample } from "./derive";
import type { ServerMetrics } from "./types";

function metrics(overrides: Partial<ServerMetrics> = {}): ServerMetrics {
  const base: ServerMetrics = {
    websocket: {
      rooms: 1,
      clients: 2,
      ingress_accepted: 0,
      ingress_dropped: 0,
      delivered_messages: 0,
      dropped_messages: 0,
      slow_client_disconnects: 0,
      goroutines: 10,
      alloc_bytes: 100,
      traffic: {
        current_connections: 2,
        danmaku_rejected_user: 0,
        danmaku_rejected_room: 0,
        like_rejected_user: 0,
        like_rejected_room: 0,
      },
    },
    queue: { status: "healthy" },
    redis: { status: "healthy", circuit: "closed" },
  };

  return {
    ...base,
    ...overrides,
    websocket: {
      ...base.websocket,
      ...overrides.websocket,
      traffic: {
        ...base.websocket.traffic,
        ...overrides.websocket?.traffic,
      },
    },
  };
}

describe("deriveSample", () => {
  it("computes delivered, limited, and dropped rates from cumulative values", () => {
    const previous = deriveSample(null, metrics({
      websocket: {
        ...metrics().websocket,
        delivered_messages: 10,
        ingress_dropped: 2,
        dropped_messages: 3,
        traffic: {
          ...metrics().websocket.traffic,
          danmaku_rejected_user: 4,
          danmaku_rejected_room: 1,
          like_rejected_user: 2,
          like_rejected_room: 1,
        },
      },
    }), 1_000);
    const current = metrics({
      websocket: {
        ...metrics().websocket,
        delivered_messages: 22,
        ingress_dropped: 5,
        dropped_messages: 7,
        traffic: {
          ...metrics().websocket.traffic,
          danmaku_rejected_user: 6,
          danmaku_rejected_room: 2,
          like_rejected_user: 5,
          like_rejected_room: 3,
        },
      },
    });

    expect(deriveSample(previous, current, 3_000)).toMatchObject({
      deliveredPerSecond: 6,
      limitedPerSecond: 4,
      droppedPerSecond: 3.5,
    });
  });

  it("returns zero rate for the first sample", () => {
    expect(deriveSample(null, metrics({
      websocket: { ...metrics().websocket, delivered_messages: 20 },
    }), 1_000)).toMatchObject({
      deliveredPerSecond: 0,
      limitedPerSecond: 0,
      droppedPerSecond: 0,
    });
  });

  it("treats a counter reset as a zero delta", () => {
    const previous = deriveSample(null, metrics({
      websocket: {
        ...metrics().websocket,
        delivered_messages: 20,
        traffic: { ...metrics().websocket.traffic, danmaku_rejected_user: 5 },
      },
    }), 1_000);

    expect(deriveSample(previous, metrics({
      websocket: {
        ...metrics().websocket,
        delivered_messages: 2,
        traffic: { ...metrics().websocket.traffic, danmaku_rejected_user: 1 },
      },
    }), 2_000)).toMatchObject({
      deliveredPerSecond: 0,
      limitedPerSecond: 0,
      droppedPerSecond: 0,
    });
  });

  it("sums reset-safe limited deltas independently before calculating the rate", () => {
    const previous = deriveSample(null, metrics({
      websocket: {
        ...metrics().websocket,
        traffic: {
          ...metrics().websocket.traffic,
          danmaku_rejected_user: 8,
          danmaku_rejected_room: 3,
          like_rejected_user: 4,
          like_rejected_room: 1,
        },
      },
    }), 1_000);

    expect(deriveSample(previous, metrics({
      websocket: {
        ...metrics().websocket,
        traffic: {
          ...metrics().websocket.traffic,
          danmaku_rejected_user: 1,
          danmaku_rejected_room: 5,
          like_rejected_user: 6,
          like_rejected_room: 1,
        },
      },
    }), 2_000)).toMatchObject({
      limitedPerSecond: 4,
    });
  });

  it("sums reset-safe dropped deltas independently before calculating the rate", () => {
    const previous = deriveSample(null, metrics({
      websocket: {
        ...metrics().websocket,
        ingress_dropped: 9,
        dropped_messages: 4,
      },
    }), 1_000);

    expect(deriveSample(previous, metrics({
      websocket: {
        ...metrics().websocket,
        ingress_dropped: 2,
        dropped_messages: 7,
      },
    }), 2_000)).toMatchObject({
      droppedPerSecond: 3,
    });
  });
});

describe("deriveEvents", () => {
  it("generates merged events for limit increments in one polling cycle", () => {
    const previous = metrics();
    const current = metrics({
      websocket: {
        ...metrics().websocket,
        traffic: {
          ...metrics().websocket.traffic,
          danmaku_rejected_user: 2,
          danmaku_rejected_room: 3,
          like_rejected_user: 4,
          like_rejected_room: 1,
        },
      },
    });

    expect(deriveEvents(previous, current, 5_000)).toMatchObject([
      { code: "danmaku_limited", delta: 5, observedAt: 5_000 },
      { code: "like_limited", delta: 5, observedAt: 5_000 },
    ]);
  });

  it("generates one event for a Redis state transition", () => {
    const events = deriveEvents(
      metrics(),
      metrics({ redis: { status: "degraded", circuit: "open" } }),
      5_000,
    );

    expect(events).toEqual([expect.objectContaining({
      code: "redis_state_changed",
      level: "error",
      delta: 1,
      observedAt: 5_000,
    })]);
  });

  it("keeps Redis open to half-open transitions at warning severity", () => {
    const events = deriveEvents(
      metrics({ redis: { status: "degraded", circuit: "open" } }),
      metrics({ redis: { status: "degraded", circuit: "half_open" } }),
      5_000,
    );

    expect(events).toEqual([expect.objectContaining({
      code: "redis_state_changed",
      level: "warning",
      delta: 1,
      observedAt: 5_000,
    })]);
  });

  it("marks Redis half-open to healthy closed as recovery", () => {
    const events = deriveEvents(
      metrics({ redis: { status: "degraded", circuit: "half_open" } }),
      metrics({ redis: { status: "healthy", circuit: "closed" } }),
      5_000,
    );

    expect(events).toEqual([expect.objectContaining({
      code: "redis_state_changed",
      level: "recovery",
      delta: 1,
      observedAt: 5_000,
    })]);
  });

  it("treats Redis disabled transitions as informational", () => {
    const events = deriveEvents(
      metrics({ redis: { status: "healthy", circuit: "closed" } }),
      metrics({ redis: { status: "disabled", circuit: "disabled" } }),
      5_000,
    );

    expect(events).toEqual([expect.objectContaining({
      code: "redis_state_changed",
      level: "info",
      delta: 1,
      observedAt: 5_000,
    })]);
  });

  it("emits a Redis governance event when only the Redis status changes", () => {
    const events = deriveEvents(
      metrics({ redis: { status: "degraded", circuit: "closed" } }),
      metrics({ redis: { status: "healthy", circuit: "closed" } }),
      5_000,
    );

    expect(events).toEqual([expect.objectContaining({
      code: "redis_state_changed",
      level: "recovery",
      delta: 1,
      observedAt: 5_000,
    })]);
  });

  it("generates one event for a Kafka state transition", () => {
    const events = deriveEvents(
      metrics({ queue: { status: "healthy" } }),
      metrics({ queue: { status: "degraded" } }),
      5_000,
    );

    expect(events).toEqual([expect.objectContaining({
      code: "kafka_state_changed",
      level: "warning",
      delta: 1,
      observedAt: 5_000,
    })]);
  });

  it("treats queue-only unavailable Kafka governance as an error", () => {
    const events = deriveEvents(
      metrics({ queue: { status: "healthy" } }),
      metrics({ queue: { status: "unavailable" } }),
      5_000,
    );

    expect(events).toEqual([expect.objectContaining({
      code: "kafka_state_changed",
      level: "error",
      delta: 1,
      observedAt: 5_000,
    })]);
  });

  it("treats queue-only degraded Kafka governance as a warning", () => {
    const events = deriveEvents(
      metrics({ queue: { status: "healthy" } }),
      metrics({ queue: { status: "degraded" } }),
      5_000,
    );

    expect(events).toEqual([expect.objectContaining({
      code: "kafka_state_changed",
      level: "warning",
      delta: 1,
      observedAt: 5_000,
    })]);
  });

  it("treats queue-only recovery to healthy as recovery", () => {
    const events = deriveEvents(
      metrics({ queue: { status: "unavailable" } }),
      metrics({ queue: { status: "healthy" } }),
      5_000,
    );

    expect(events).toEqual([expect.objectContaining({
      code: "kafka_state_changed",
      level: "recovery",
      delta: 1,
      observedAt: 5_000,
    })]);
  });

  it("treats queue-only disabled Kafka governance as informational", () => {
    const events = deriveEvents(
      metrics({ queue: { status: "healthy" } }),
      metrics({ queue: { status: "disabled" } }),
      5_000,
    );

    expect(events).toEqual([expect.objectContaining({
      code: "kafka_state_changed",
      level: "info",
      delta: 1,
      observedAt: 5_000,
    })]);
  });

  it("treats Kafka enablement from disabled as informational", () => {
    const events = deriveEvents(
      metrics({ queue: { status: "disabled" } }),
      metrics({ queue: { status: "healthy" } }),
      5_000,
    );

    expect(events).toEqual([expect.objectContaining({
      code: "kafka_state_changed",
      level: "info",
      delta: 1,
      observedAt: 5_000,
    })]);
  });

  it("does not generate events when values do not change", () => {
    const current = metrics({
      kafka: { enqueued: 1, acked: 1, dropped: 0, errors: 0, status: "healthy" },
    });

    expect(deriveEvents(current, current, 5_000)).toEqual([]);
  });
});
