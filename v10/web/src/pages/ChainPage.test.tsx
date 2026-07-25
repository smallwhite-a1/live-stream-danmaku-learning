import { cleanup, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { ServerMetrics } from "../metrics/types";
import type { MetricsState } from "../metrics/useMetrics";
import { useMetrics } from "../metrics/useMetrics";
import { ChainPage } from "./ChainPage";

vi.mock("../metrics/useMetrics", () => ({
  useMetrics: vi.fn(),
}));

function metrics(): ServerMetrics {
  return {
    websocket: {
      rooms: 1,
      clients: 2,
      ingress_accepted: 10,
      ingress_dropped: 0,
      delivered_messages: 20,
      dropped_messages: 0,
      slow_client_disconnects: 0,
      goroutines: 12,
      alloc_bytes: 1024,
      traffic: {
        current_connections: 2,
        danmaku_rejected_user: 0,
        danmaku_rejected_room: 0,
        like_rejected_user: 0,
        like_rejected_room: 0,
      },
    },
    kafka: {
      enqueued: 10,
      acked: 8,
      dropped: 1,
      errors: 1,
      status: "degraded",
    },
    queue: { status: "degraded" },
    redis: { status: "disabled", circuit: "disabled" },
  };
}

function state(): MetricsState {
  return {
    latest: metrics(),
    samples: [],
    events: [],
    freshness: "fresh",
    lastSuccessAt: Date.now(),
    refresh: vi.fn(async () => {}),
  };
}

beforeEach(() => {
  vi.mocked(useMetrics).mockReturnValue(state());
});

afterEach(cleanup);

describe("ChainPage", () => {
  it("renders every real processing node and both backend branches", () => {
    render(<ChainPage />);

    [
      "浏览器",
      "WebSocket 校验与限流",
      "Manager 房间广播",
      "Redis / 本机降级",
      "Kafka Producer",
      "Consumer",
      "MySQL",
    ].forEach((node) => {
      expect(screen.getByText(node)).toBeInTheDocument();
    });
    expect(screen.getByText("实时广播分支")).toBeInTheDocument();
    expect(screen.getByText("异步持久化分支")).toBeInTheDocument();
  });

  it("uses real Redis and Kafka states while keeping MySQL unobservable", () => {
    render(<ChainPage />);

    expect(within(screen.getByTestId("chain-node-redis")).getByText("未启用")).toBeInTheDocument();
    expect(within(screen.getByTestId("chain-node-kafka")).getByText("降级")).toBeInTheDocument();
    expect(
      within(screen.getByTestId("chain-node-mysql")).getByText("当前接口不可观测"),
    ).toBeInTheDocument();
  });

  it("keeps the future AI consumer outside the realtime branch", () => {
    render(<ChainPage />);

    expect(screen.getByText("V11 独立异步消费者")).toBeInTheDocument();
    expect(screen.queryByText("AI 模型调用")).not.toBeInTheDocument();
    const futureBranch = screen.getByTestId("future-ai-branch");
    const currentConsumer = screen.getByText("Consumer").closest(".chain-node");

    expect(futureBranch).not.toBe(screen.getByTestId("realtime-branch"));
    expect(
      futureBranch.compareDocumentPosition(currentConsumer!)
      & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });
});
