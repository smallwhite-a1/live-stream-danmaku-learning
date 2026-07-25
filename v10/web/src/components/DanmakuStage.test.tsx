import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { DanmakuMessage } from "../protocol/types";
import { assignLane, DanmakuStage } from "./DanmakuStage";

function message(id: number): DanmakuMessage {
  return {
    id,
    message_id: `message-${id}`,
    room_id: "room-01",
    user_id: `user-${id}`,
    username: `User ${id}`,
    content: `Danmaku ${id}`,
    send_time: "2026-07-25T12:00:00Z",
  };
}

function setMobileViewport(matches: boolean): void {
  vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({
    matches,
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
  }));
}

beforeEach(() => {
  setMobileViewport(false);
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("assignLane", () => {
  it("selects the earliest available lane", () => {
    expect(assignLane([800, 200, 500], 100, 3)).toBe(1);
  });

  it("never selects a lane outside the configured lane count", () => {
    expect(assignLane([800, 700, 0], 100, 2)).toBeLessThanOrEqual(1);
    expect(assignLane([], 100, 4)).toBe(0);
  });
});

describe("DanmakuStage", () => {
  it("shows the local live stage and room status", () => {
    render(
      <DanmakuStage
        messages={[]}
        online={12}
        roomId="room-01"
      />,
    );

    expect(screen.getByAltText("直播间演示舞台")).toBeInTheDocument();
    expect(screen.getByText("LIVE")).toBeInTheDocument();
    expect(screen.getByText("room-01")).toBeInTheDocument();
    expect(screen.getByText("12 人在线")).toBeInTheDocument();
  });

  it("keeps only 40 active danmaku within eight desktop lanes", () => {
    render(
      <DanmakuStage
        messages={Array.from({ length: 45 }, (_, index) => message(index + 1))}
        online={12}
        roomId="room-01"
      />,
    );

    const activeDanmaku = screen.getAllByTestId("active-danmaku");
    expect(activeDanmaku).toHaveLength(40);
    expect(activeDanmaku[0]).toHaveTextContent("Danmaku 6");
    expect(activeDanmaku.every((item) => Number(item.dataset.lane) <= 7)).toBe(true);
  });

  it("uses at most four lanes on mobile", () => {
    setMobileViewport(true);

    render(
      <DanmakuStage
        messages={Array.from({ length: 12 }, (_, index) => message(index + 1))}
        online={2}
        roomId="room-01"
      />,
    );

    expect(
      screen.getAllByTestId("active-danmaku")
        .every((item) => Number(item.dataset.lane) <= 3),
    ).toBe(true);
  });

  it("does not reschedule existing danmaku when the bounded window advances", () => {
    const { rerender } = render(
      <DanmakuStage
        messages={Array.from({ length: 40 }, (_, index) => message(index + 1))}
        online={2}
        roomId="room-01"
      />,
    );
    const existing = screen.getByText("Danmaku 2").closest("[data-testid='active-danmaku']");
    const originalLane = existing?.getAttribute("data-lane");
    const originalDelay = (existing as HTMLElement | null)?.style.animationDelay;
    const originalTop = (existing as HTMLElement | null)?.style.top;

    rerender(
      <DanmakuStage
        messages={Array.from({ length: 41 }, (_, index) => message(index + 1))}
        online={2}
        roomId="room-01"
      />,
    );

    const stable = screen.getByText("Danmaku 2").closest("[data-testid='active-danmaku']");
    expect(stable).toBe(existing);
    expect(stable).toHaveAttribute("data-lane", originalLane);
    expect((stable as HTMLElement).style.animationDelay).toBe(originalDelay);
    expect((stable as HTMLElement).style.top).toBe(originalTop);
    expect(screen.getByText("Danmaku 41")).toBeInTheDocument();
  });

  it("removes finished danmaku without replaying it on later updates", () => {
    const { rerender } = render(
      <DanmakuStage messages={[message(1)]} online={1} roomId="room-01" />,
    );
    const finished = screen.getByText("Danmaku 1").closest("[data-testid='active-danmaku']");

    fireEvent.animationEnd(finished!);
    expect(screen.queryByText("Danmaku 1")).not.toBeInTheDocument();

    rerender(
      <DanmakuStage
        messages={[message(1), message(2)]}
        online={1}
        roomId="room-01"
      />,
    );

    expect(screen.queryByText("Danmaku 1")).not.toBeInTheDocument();
    expect(screen.getByText("Danmaku 2")).toBeInTheDocument();
  });
});
