import { cleanup, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { MetricEvent, MetricSample, ServerMetrics } from "../metrics/types";
import type { MetricsState } from "../metrics/useMetrics";
import { useMetrics } from "../metrics/useMetrics";
import { MonitorPage } from "./MonitorPage";

vi.mock("../metrics/useMetrics", () => ({
  useMetrics: vi.fn(),
}));

function serverMetrics(overrides: Partial<ServerMetrics> = {}): ServerMetrics {
  const base: ServerMetrics = {
    websocket: {
      rooms: 3,
      clients: 14,
      ingress_accepted: 120,
      ingress_dropped: 4,
      delivered_messages: 800,
      dropped_messages: 2,
      slow_client_disconnects: 1,
      goroutines: 32,
      alloc_bytes: 12 * 1024 * 1024,
      traffic: {
        current_connections: 14,
        danmaku_rejected_user: 5,
        danmaku_rejected_room: 2,
        like_rejected_user: 1,
        like_rejected_room: 0,
      },
    },
    kafka: {
      enqueued: 100,
      acked: 98,
      dropped: 1,
      errors: 1,
      status: "healthy",
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

function sample(
  sampledAt: number,
  deliveredPerSecond: number,
  raw = serverMetrics(),
): MetricSample {
  return {
    sampledAt,
    raw,
    deliveredPerSecond,
    limitedPerSecond: 1.5,
    droppedPerSecond: 0.5,
  };
}

function state(overrides: Partial<MetricsState> = {}): MetricsState {
  const now = new Date("2026-07-25T12:00:00Z").getTime();
  return {
    latest: serverMetrics(),
    samples: [sample(now - 2_000, 6), sample(now, 7.5)],
    events: [],
    freshness: "fresh",
    lastSuccessAt: now,
    refresh: vi.fn(async () => {}),
    ...overrides,
  };
}

beforeEach(() => {
  vi.mocked(useMetrics).mockReturnValue(state());
});

afterEach(cleanup);

describe("MonitorPage", () => {
  it("renders current connections, delivered rate, and runtime values", () => {
    render(<MonitorPage />);

    expect(screen.getByRole("heading", { name: "运行监控" })).toBeInTheDocument();
    expect(screen.getByTestId("metric-current-connections")).toHaveTextContent("14");
    expect(screen.getByTestId("metric-delivered-rate")).toHaveTextContent("7.5");
    expect(screen.getByText("32")).toBeInTheDocument();
    expect(screen.getByText("12.0 MB")).toBeInTheDocument();
  });

  it("shows disabled dependencies as 未启用 and never infers MySQL health", () => {
    vi.mocked(useMetrics).mockReturnValue(state({
      latest: serverMetrics({
        kafka: undefined,
        queue: { status: "disabled" },
        redis: { status: "disabled", circuit: "disabled" },
      }),
    }));

    render(<MonitorPage />);

    expect(screen.getAllByText("未启用")).toHaveLength(2);
    expect(screen.getByText("当前接口不可观测")).toBeInTheDocument();
    expect(screen.queryByText("MySQL 正常")).not.toBeInTheDocument();
  });

  it("marks data stale while retaining the last successful values", () => {
    vi.mocked(useMetrics).mockReturnValue(state({
      freshness: "stale",
      latest: serverMetrics({
        websocket: {
          ...serverMetrics().websocket,
          clients: 17,
        },
      }),
      samples: [sample(Date.now(), 9.25)],
    }));

    render(<MonitorPage />);

    expect(screen.getByText("数据已过期")).toBeInTheDocument();
    expect(screen.getByTestId("metric-current-connections")).toHaveTextContent("17");
    expect(screen.getByTestId("metric-delivered-rate")).toHaveTextContent("9.25");
    expect(screen.getAllByText("数据过期")).toHaveLength(2);
  });

  it("renders governance events newest first", () => {
    const events: MetricEvent[] = [
      {
        id: "old",
        code: "danmaku_limited",
        level: "warning",
        message: "old",
        delta: 2,
        observedAt: 1_000,
      },
      {
        id: "new",
        code: "slow_client_disconnected",
        level: "error",
        message: "new",
        delta: 1,
        observedAt: 2_000,
      },
    ];
    vi.mocked(useMetrics).mockReturnValue(state({ events }));

    render(<MonitorPage />);

    const renderedEvents = screen.getAllByTestId("governance-event");
    expect(within(renderedEvents[0]!).getByText("慢客户端已断开")).toBeInTheDocument();
    expect(within(renderedEvents[1]!).getByText("弹幕请求被限流")).toBeInTheDocument();
  });

  it("waits for two samples before rendering chart axes", () => {
    vi.mocked(useMetrics).mockReturnValue(state({
      samples: [sample(Date.now(), 0)],
    }));

    render(<MonitorPage />);

    expect(screen.getByText("正在收集趋势数据")).toBeInTheDocument();
  });

  it("keeps unknown room counts unknown before the first successful sample", () => {
    vi.mocked(useMetrics).mockReturnValue(state({
      latest: null,
      samples: [],
      freshness: "stale",
      lastSuccessAt: null,
    }));

    render(<MonitorPage />);

    expect(screen.getByTestId("metric-current-connections")).toHaveTextContent("--");
    expect(screen.getByText("活跃房间未知")).toBeInTheDocument();
    expect(screen.queryByText("0 个活跃房间")).not.toBeInTheDocument();
  });

  it("shows slow-client queue drops separately from disconnects", () => {
    render(<MonitorPage />);

    expect(screen.getByTestId("runtime-dropped-messages")).toHaveTextContent("发送队列丢弃");
    expect(screen.getByTestId("runtime-dropped-messages")).toHaveTextContent("2");
    expect(screen.getByText("慢客户端断开")).toBeInTheDocument();
    expect(screen.getByText("入口丢弃累计")).toBeInTheDocument();
  });

  it("provides an accessible data table for a rendered trend", () => {
    render(<MonitorPage />);

    const table = screen.getByRole("table", { name: "最近 60 秒趋势数据" });
    expect(table).toBeInTheDocument();
    expect(within(table).getByRole("columnheader", { name: "投递/秒" })).toBeInTheDocument();
    expect(within(table).getAllByRole("row")).toHaveLength(3);
  });
});
