import { MemoryRouter } from "react-router-dom";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { MetricsState } from "../metrics/useMetrics";
import { useMetrics } from "../metrics/useMetrics";
import type { DanmakuSocketState } from "../realtime/useDanmakuSocket";
import { useDanmakuSocket } from "../realtime/useDanmakuSocket";
import { App } from "./App";

vi.mock("../metrics/useMetrics", () => ({
  useMetrics: vi.fn(),
}));

vi.mock("../realtime/useDanmakuSocket", () => ({
  useDanmakuSocket: vi.fn(),
}));

const baseSocketState: DanmakuSocketState = {
  status: "connected",
  messages: [],
  stats: { online: 7, likes: 23 },
  lastControl: null,
  retryUntil: { danmaku: 0, like: 0 },
  sendDanmaku: vi.fn(() => true),
  sendLike: vi.fn(() => true),
  reconnect: vi.fn(),
};

const baseMetricsState: MetricsState = {
  latest: null,
  samples: [],
  events: [],
  freshness: "loading",
  lastSuccessAt: null,
  refresh: vi.fn(async () => {}),
};

function renderApp(initialEntry = "/"): void {
  render(
    <MemoryRouter initialEntries={[initialEntry]}>
      <App />
    </MemoryRouter>,
  );
}

beforeEach(() => {
  const values = new Map<string, string>();
  Object.defineProperty(window, "localStorage", {
    configurable: true,
    value: {
      getItem: vi.fn((key: string) => values.get(key) ?? null),
      setItem: vi.fn((key: string, value: string) => {
        values.set(key, value);
      }),
    },
  });
  window.history.replaceState({}, "", "/");
  vi.mocked(useDanmakuSocket).mockReturnValue(baseSocketState);
  vi.mocked(useMetrics).mockReturnValue(baseMetricsState);
});

afterEach(cleanup);

describe("App", () => {
  it("shows the first-viewport brand, route tabs, and live room summary", () => {
    renderApp();

    expect(screen.getByText("DANMAKU LAB")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "直播间" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "运行监控" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "链路说明" })).toBeInTheDocument();
    expect(screen.getByText("7")).toBeInTheDocument();
    expect(screen.getByText("23")).toBeInTheDocument();
  });

  it("uses the approved accessible labels when switching rooms", async () => {
    const user = userEvent.setup();
    renderApp();

    await user.click(screen.getByRole("button", { name: "更换房间" }));

    expect(screen.getByLabelText("昵称")).toBeInTheDocument();
    expect(screen.getByLabelText("房间")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "连接房间" })).toBeInTheDocument();
  });

  it.each([
    ["rate_limited", "发送过快，请稍后重试"],
    ["server_overloaded", "服务当前繁忙，这条弹幕没有被接收"],
    ["content_too_long", "弹幕不能超过 200 个字符"],
    ["unknown_code", "服务端拒绝了本次操作"],
  ])("shows control code %s as exact feedback", (code, feedback) => {
    vi.mocked(useDanmakuSocket).mockReturnValue({
      ...baseSocketState,
      lastControl: { code },
    });

    renderApp();

    expect(screen.getByText(feedback)).toBeInTheDocument();
  });

  it.each([
    ["/monitor", "运行监控"],
    ["/chain", "链路说明"],
  ])("renders route %s", async (route, heading) => {
    renderApp(route);

    expect(await screen.findByRole("heading", { name: heading })).toBeInTheDocument();
  });

  it("redirects unknown routes to the live room", () => {
    renderApp("/not-a-route");

    expect(screen.getByRole("heading", { name: "实时弹幕" })).toBeInTheDocument();
  });
});
