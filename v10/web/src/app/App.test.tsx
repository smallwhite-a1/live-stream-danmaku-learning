import { MemoryRouter } from "react-router-dom";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { DanmakuSocketState } from "../realtime/useDanmakuSocket";
import { useDanmakuSocket } from "../realtime/useDanmakuSocket";
import { App } from "./App";

vi.mock("../realtime/useDanmakuSocket", () => ({
  useDanmakuSocket: vi.fn(),
}));

const baseSocketState: DanmakuSocketState = {
  status: "connected",
  messages: [],
  stats: { online: 7, likes: 23 },
  lastControl: null,
  retryUntil: 0,
  sendDanmaku: vi.fn(() => true),
  sendLike: vi.fn(() => true),
  reconnect: vi.fn(),
};

function renderApp(): void {
  render(
    <MemoryRouter initialEntries={["/"]}>
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
});
