import { StrictMode } from "react";
import { act, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { useDanmakuSocket, type Identity } from "./useDanmakuSocket";

class MockWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  static instances: MockWebSocket[] = [];

  readonly sent: string[] = [];
  readonly url: string;
  closeCalls = 0;
  readyState = MockWebSocket.CONNECTING;
  onclose: ((event: CloseEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent<string>) => void) | null = null;
  onopen: ((event: Event) => void) | null = null;

  constructor(url: string) {
    this.url = url;
    MockWebSocket.instances.push(this);
  }

  static reset(): void {
    MockWebSocket.instances = [];
  }

  close(): void {
    this.closeCalls += 1;
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.(new CloseEvent("close"));
  }

  open(): void {
    this.readyState = MockWebSocket.OPEN;
    this.onopen?.(new Event("open"));
  }

  receive(data: string): void {
    this.onmessage?.(new MessageEvent("message", { data }));
  }

  send(data: string): void {
    this.sent.push(data);
  }
}

const identity: Identity = {
  userId: "u 1",
  username: "林",
  roomId: "room/1",
};

function danmakuPacket(id: number, roomId = identity.roomId): string {
  return JSON.stringify({
    type: 101,
    room_id: roomId,
    data: {
      id,
      message_id: `message-${id}`,
      room_id: roomId,
      user_id: "other-user",
      username: "Grace",
      content: `message ${id}`,
      send_time: "2026-07-25T12:00:00Z",
    },
  });
}

function currentSocket(): MockWebSocket {
  const socket = MockWebSocket.instances.at(-1);

  if (!socket) {
    throw new Error("Expected a WebSocket instance");
  }

  return socket;
}

function installMockWebSocket(): void {
  vi.stubGlobal("WebSocket", MockWebSocket);
}

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  MockWebSocket.reset();
});

describe("useDanmakuSocket", () => {
  it("connects with the encoded identity", () => {
    installMockWebSocket();

    renderHook(() => useDanmakuSocket(identity));

    const socketURL = new URL(currentSocket().url);
    expect(socketURL.protocol).toBe("ws:");
    expect(socketURL.pathname).toBe("/ws");
    expect(socketURL.searchParams).toEqual(new URLSearchParams({
      uid: "u 1",
      name: "林",
      room: "room/1",
    }));
  });

  it("stores incoming danmaku and stats", () => {
    installMockWebSocket();

    const { result } = renderHook(() => useDanmakuSocket(identity));

    act(() => {
      currentSocket().receive(danmakuPacket(1));
      currentSocket().receive(JSON.stringify({
        type: 102,
        room_id: identity.roomId,
        data: { online: 12, likes: 34 },
      }));
    });

    expect(result.current.messages.map((message) => message.content)).toEqual(["message 1"]);
    expect(result.current.stats).toEqual({ online: 12, likes: 34 });
  });

  it("keeps only the newest 300 messages", () => {
    installMockWebSocket();

    const { result } = renderHook(() => useDanmakuSocket(identity));

    act(() => {
      for (let id = 1; id <= 301; id += 1) {
        currentSocket().receive(danmakuPacket(id));
      }
    });

    expect(result.current.messages).toHaveLength(300);
    expect(result.current.messages[0]?.content).toBe("message 2");
    expect(result.current.messages.at(-1)?.content).toBe("message 301");
  });

  it("sends danmaku and likes without optimistically appending a message", () => {
    installMockWebSocket();

    const { result } = renderHook(() => useDanmakuSocket(identity));

    act(() => {
      currentSocket().open();
    });

    expect(result.current.sendDanmaku("  Hello  ")).toBe(true);
    expect(result.current.sendLike(3)).toBe(true);
    expect(currentSocket().sent.map((packet) => JSON.parse(packet))).toEqual([
      { type: 101, data: { content: "Hello" } },
      { type: 103, data: { count: 3 } },
    ]);
    expect(result.current.messages).toEqual([]);
  });

  it("does not report sends as successful while the socket is not open", () => {
    installMockWebSocket();

    const { result } = renderHook(() => useDanmakuSocket(identity));

    expect(result.current.sendDanmaku("Hello")).toBe(false);
    expect(result.current.sendLike()).toBe(false);
    expect(currentSocket().sent).toEqual([]);
  });

  it("reconnects after 500ms and increases the retry delay", () => {
    vi.useFakeTimers();
    installMockWebSocket();

    const { result } = renderHook(() => useDanmakuSocket(identity));

    act(() => {
      currentSocket().close();
    });
    expect(result.current.status).toBe("reconnecting");

    act(() => {
      vi.advanceTimersByTime(499);
    });
    expect(MockWebSocket.instances).toHaveLength(1);

    act(() => {
      vi.advanceTimersByTime(1);
    });
    expect(MockWebSocket.instances).toHaveLength(2);

    act(() => {
      currentSocket().close();
      vi.advanceTimersByTime(999);
    });
    expect(MockWebSocket.instances).toHaveLength(2);

    act(() => {
      vi.advanceTimersByTime(1);
    });
    expect(MockWebSocket.instances).toHaveLength(3);
  });

  it("caps the reconnect delay at ten seconds", () => {
    vi.useFakeTimers();
    installMockWebSocket();

    renderHook(() => useDanmakuSocket(identity));

    for (const delay of [500, 1_000, 2_000, 4_000, 8_000]) {
      act(() => {
        currentSocket().close();
        vi.advanceTimersByTime(delay);
      });
    }

    const socketCountBeforeCap = MockWebSocket.instances.length;

    act(() => {
      currentSocket().close();
      vi.advanceTimersByTime(9_999);
    });
    expect(MockWebSocket.instances).toHaveLength(socketCountBeforeCap);

    act(() => {
      vi.advanceTimersByTime(1);
    });
    expect(MockWebSocket.instances).toHaveLength(socketCountBeforeCap + 1);
  });

  it("does not reconnect after component unmount", () => {
    vi.useFakeTimers();
    installMockWebSocket();

    const { unmount } = renderHook(() => useDanmakuSocket(identity));

    unmount();
    act(() => {
      vi.advanceTimersByTime(10_000);
    });

    expect(MockWebSocket.instances).toHaveLength(1);
  });

  it("closes the old socket when identity changes", () => {
    installMockWebSocket();

    const { rerender } = renderHook(({ currentIdentity }) => useDanmakuSocket(currentIdentity), {
      initialProps: { currentIdentity: identity },
    });
    const oldSocket = currentSocket();

    rerender({
      currentIdentity: { ...identity, roomId: "room-2" },
    });

    expect(oldSocket.closeCalls).toBe(1);
    expect(MockWebSocket.instances).toHaveLength(2);
    expect(new URL(currentSocket().url).searchParams.get("room")).toBe("room-2");
  });

  it("clears room and user state when the identity changes", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-07-25T12:00:00Z"));
    installMockWebSocket();

    const { result, rerender } = renderHook(({ currentIdentity }) => useDanmakuSocket(currentIdentity), {
      initialProps: { currentIdentity: identity },
    });

    act(() => {
      currentSocket().receive(danmakuPacket(1));
      currentSocket().receive(JSON.stringify({
        type: 102,
        room_id: identity.roomId,
        data: { online: 12, likes: 34 },
      }));
      currentSocket().receive(JSON.stringify({
        type: 104,
        room_id: identity.roomId,
        data: { code: "rate_limited", retry_after_millis: 5_000 },
      }));
    });

    rerender({
      currentIdentity: { ...identity, roomId: "room-2", userId: "user-2", username: "Ada" },
    });

    expect(result.current.messages).toEqual([]);
    expect(result.current.stats).toEqual({ online: 0, likes: 0 });
    expect(result.current.lastControl).toBeNull();
    expect(result.current.retryUntil).toBe(0);
  });

  it("does not let a queued retry create an extra socket after an identity change", () => {
    vi.useFakeTimers();
    installMockWebSocket();

    const { rerender } = renderHook(({ currentIdentity }) => useDanmakuSocket(currentIdentity), {
      initialProps: { currentIdentity: identity },
    });

    act(() => {
      currentSocket().close();
    });

    rerender({
      currentIdentity: { ...identity, roomId: "room-2" },
    });

    act(() => {
      vi.advanceTimersByTime(10_000);
    });

    expect(MockWebSocket.instances).toHaveLength(2);
    expect(new URL(currentSocket().url).searchParams.get("room")).toBe("room-2");
  });

  it("keeps StrictMode retries and connections scoped to the active lifecycle", () => {
    vi.useFakeTimers();
    installMockWebSocket();

    const { rerender } = renderHook(({ currentIdentity }) => useDanmakuSocket(currentIdentity), {
      initialProps: { currentIdentity: identity },
      wrapper: StrictMode,
    });

    const activeSocket = currentSocket();
    act(() => {
      activeSocket.close();
    });

    rerender({
      currentIdentity: { ...identity, roomId: "room-2" },
    });

    act(() => {
      vi.advanceTimersByTime(10_000);
    });

    expect(MockWebSocket.instances).toHaveLength(3);
    expect(activeSocket.closeCalls).toBe(1);
    expect(new URL(currentSocket().url).searchParams.get("room")).toBe("room-2");
  });

  it("keeps the socket when the identity values are unchanged", () => {
    installMockWebSocket();

    const { rerender } = renderHook(({ currentIdentity }) => useDanmakuSocket(currentIdentity), {
      initialProps: { currentIdentity: identity },
    });
    const socket = currentSocket();

    rerender({ currentIdentity: { ...identity } });

    expect(socket.closeCalls).toBe(0);
    expect(MockWebSocket.instances).toHaveLength(1);
  });

  it("ignores callbacks from a stale socket generation", () => {
    installMockWebSocket();

    const { result, rerender } = renderHook(({ currentIdentity }) => useDanmakuSocket(currentIdentity), {
      initialProps: { currentIdentity: identity },
    });
    const oldSocket = currentSocket();

    rerender({
      currentIdentity: { ...identity, roomId: "room-2" },
    });
    act(() => {
      oldSocket.receive(danmakuPacket(1, "room-2"));
    });

    expect(result.current.messages).toEqual([]);
  });

  it("reconnects manually without scheduling an automatic retry", () => {
    vi.useFakeTimers();
    installMockWebSocket();

    const { result } = renderHook(() => useDanmakuSocket(identity));
    const oldSocket = currentSocket();

    act(() => {
      result.current.reconnect();
      vi.runAllTimers();
    });

    expect(oldSocket.closeCalls).toBe(1);
    expect(MockWebSocket.instances).toHaveLength(2);
    expect(result.current.status).toBe("connecting");
  });

  it("maps a control retry time to retryUntil", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-07-25T12:00:00Z"));
    installMockWebSocket();

    const { result } = renderHook(() => useDanmakuSocket(identity));

    act(() => {
      currentSocket().receive(JSON.stringify({
        type: 104,
        room_id: identity.roomId,
        data: {
          code: "rate_limited",
          retry_after_millis: 5_000,
        },
      }));
    });

    expect(result.current.lastControl).toEqual({
      code: "rate_limited",
      retry_after_millis: 5_000,
    });
    expect(result.current.retryUntil).toBe(Date.now() + 5_000);
  });
});
