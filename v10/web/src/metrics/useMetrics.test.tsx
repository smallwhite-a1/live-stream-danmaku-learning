import { StrictMode } from "react";
import { act, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { ServerMetrics } from "./types";
import { useMetrics } from "./useMetrics";

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

function deferred<T>(): {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((nextResolve, nextReject) => {
    resolve = nextResolve;
    reject = nextReject;
  });

  return { promise, resolve, reject };
}

async function flushMicrotasks(): Promise<void> {
  await act(async () => {
    await Promise.resolve();
  });
}

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("useMetrics", () => {
  it("polls immediately and then every 2 seconds without overlapping refreshes", async () => {
    vi.useFakeTimers();

    const first = deferred<{ json(): Promise<ServerMetrics> }>();
    const second = deferred<{ json(): Promise<ServerMetrics> }>();
    const fetchMock = vi.fn()
      .mockImplementationOnce(() => first.promise)
      .mockImplementationOnce(() => second.promise);
    vi.stubGlobal("fetch", fetchMock);

    renderHook(() => useMetrics());

    expect(fetchMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      vi.advanceTimersByTime(2_000);
      await Promise.resolve();
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      first.resolve({ json: async () => metrics() });
      await Promise.resolve();
    });

    await act(async () => {
      vi.advanceTimersByTime(2_000);
      await Promise.resolve();
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);

    await act(async () => {
      second.resolve({ json: async () => metrics() });
      await Promise.resolve();
    });
  });

  it("keeps the newest 30 samples", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-07-25T00:00:00Z"));

    const fetchMock = vi.fn(() => Promise.resolve({
      json: async () => metrics({
        websocket: { ...metrics().websocket, delivered_messages: fetchMock.mock.calls.length },
      }),
    }));
    vi.stubGlobal("fetch", fetchMock);

    const { result } = renderHook(() => useMetrics());
    await flushMicrotasks();

    for (let cycle = 0; cycle < 30; cycle += 1) {
      await act(async () => {
        vi.advanceTimersByTime(2_000);
        await Promise.resolve();
      });
      await flushMicrotasks();
    }

    expect(result.current.samples).toHaveLength(30);
    expect(result.current.samples[0]?.raw.websocket.delivered_messages).toBe(2);
    expect(result.current.samples.at(-1)?.raw.websocket.delivered_messages).toBe(31);
  });

  it("keeps the newest 50 governance events", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-07-25T00:00:00Z"));

    let delivered = 0;
    let limited = 0;
    const fetchMock = vi.fn(() => {
      delivered += 1;
      limited += 1;

      return Promise.resolve({
        json: async () => metrics({
          websocket: {
            ...metrics().websocket,
            delivered_messages: delivered,
            traffic: {
              ...metrics().websocket.traffic,
              danmaku_rejected_user: limited,
            },
          },
        }),
      });
    });
    vi.stubGlobal("fetch", fetchMock);

    const { result } = renderHook(() => useMetrics());
    await flushMicrotasks();

    for (let cycle = 0; cycle < 55; cycle += 1) {
      await act(async () => {
        vi.advanceTimersByTime(2_000);
        await Promise.resolve();
      });
      await flushMicrotasks();
    }

    expect(result.current.events).toHaveLength(50);
    expect(result.current.events.every((event) => event.code === "danmaku_limited")).toBe(true);
    expect(result.current.events[0]?.observedAt).toBe(new Date("2026-07-25T00:00:12Z").getTime());
  });

  it("sets stale after a failed refresh while preserving data, then returns to fresh on recovery", async () => {
    vi.useFakeTimers();

    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        json: async () => metrics({
          websocket: { ...metrics().websocket, delivered_messages: 10 },
        }),
      })
      .mockRejectedValueOnce(new Error("boom"))
      .mockResolvedValueOnce({
        json: async () => metrics({
          websocket: { ...metrics().websocket, delivered_messages: 12 },
        }),
      });
    vi.stubGlobal("fetch", fetchMock);

    const { result } = renderHook(() => useMetrics());
    await flushMicrotasks();

    const firstSnapshot = result.current.samples;
    expect(result.current.freshness).toBe("fresh");

    await act(async () => {
      vi.advanceTimersByTime(2_000);
      await Promise.resolve();
    });
    await flushMicrotasks();

    expect(result.current.freshness).toBe("stale");
    expect(result.current.samples).toBe(firstSnapshot);
    expect(result.current.latest?.websocket.delivered_messages).toBe(10);

    await act(async () => {
      await result.current.refresh();
    });

    expect(result.current.freshness).toBe("fresh");
    expect(result.current.latest?.websocket.delivered_messages).toBe(12);
    expect(result.current.lastSuccessAt).not.toBeNull();
  });

  it("aborts the active request on unmount and supports manual refresh", async () => {
    vi.useFakeTimers();

    const second = deferred<{ json(): Promise<ServerMetrics> }>();
    let secondSignal: AbortSignal | undefined;
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        json: async () => metrics({
          websocket: { ...metrics().websocket, delivered_messages: 1 },
        }),
      })
      .mockImplementationOnce((_: RequestInfo | URL, init?: RequestInit) => {
        secondSignal = init?.signal as AbortSignal | undefined;
        secondSignal?.addEventListener("abort", () => {
          second.reject(new DOMException("The operation was aborted.", "AbortError"));
        }, { once: true });
        return second.promise;
      });
    vi.stubGlobal("fetch", fetchMock);

    const { result, unmount } = renderHook(() => useMetrics());
    await flushMicrotasks();

    let refreshPromise!: Promise<void>;
    await act(async () => {
      refreshPromise = result.current.refresh();
      await Promise.resolve();
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);

    unmount();

    expect(secondSignal?.aborted).toBe(true);
    await expect(refreshPromise).resolves.toBeUndefined();
  });

  it("starts a replacement request immediately after StrictMode cleanup aborts the first lifecycle request", async () => {
    vi.useFakeTimers();

    const first = deferred<{ json(): Promise<ServerMetrics> }>();
    const second = deferred<{ json(): Promise<ServerMetrics> }>();
    const signals: AbortSignal[] = [];
    const fetchMock = vi.fn()
      .mockImplementationOnce((_: RequestInfo | URL, init?: RequestInit) => {
        const signal = init?.signal as AbortSignal;
        signals.push(signal);
        signal.addEventListener("abort", () => {
          first.reject(new DOMException("The operation was aborted.", "AbortError"));
        }, { once: true });
        return first.promise;
      })
      .mockImplementationOnce((_: RequestInfo | URL, init?: RequestInit) => {
        signals.push(init?.signal as AbortSignal);
        return second.promise;
      });
    vi.stubGlobal("fetch", fetchMock);

    const { result } = renderHook(() => useMetrics(), { wrapper: StrictMode });

    await flushMicrotasks();

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(signals[0]?.aborted).toBe(true);

    await act(async () => {
      second.resolve({
        json: async () => metrics({
          websocket: { ...metrics().websocket, delivered_messages: 1 },
        }),
      });
      await Promise.resolve();
    });

    expect(result.current.freshness).toBe("fresh");
    expect(result.current.latest?.websocket.delivered_messages).toBe(1);
  });

  it("keeps sampledAt monotonic so same-millisecond refresh event ids stay unique", async () => {
    vi.useFakeTimers();

    const now = 1_700_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);

    let limited = 0;
    const fetchMock = vi.fn(() => Promise.resolve({
      json: async () => metrics({
        websocket: {
          ...metrics().websocket,
          traffic: {
            ...metrics().websocket.traffic,
            danmaku_rejected_user: limited,
          },
        },
      }),
    }));
    vi.stubGlobal("fetch", fetchMock);

    const { result } = renderHook(() => useMetrics());
    await flushMicrotasks();

    limited = 1;
    await act(async () => {
      await result.current.refresh();
    });

    limited = 2;
    await act(async () => {
      await result.current.refresh();
    });

    expect(result.current.events).toHaveLength(2);
    expect(result.current.events[0]?.id).not.toBe(result.current.events[1]?.id);
    expect(result.current.samples[1]?.sampledAt).toBeGreaterThan(result.current.samples[0]?.sampledAt ?? 0);
    expect(result.current.samples[2]?.sampledAt).toBeGreaterThan(result.current.samples[1]?.sampledAt ?? 0);
  });

  it("recovers from a synchronous fetch throw on the next refresh", async () => {
    vi.useFakeTimers();

    const fetchMock = vi.fn()
      .mockImplementationOnce(() => {
        throw new Error("sync boom");
      })
      .mockResolvedValueOnce({
        json: async () => metrics({
          websocket: { ...metrics().websocket, delivered_messages: 3 },
        }),
      });
    vi.stubGlobal("fetch", fetchMock);

    const { result } = renderHook(() => useMetrics());
    await flushMicrotasks();

    expect(result.current.freshness).toBe("stale");

    await act(async () => {
      await result.current.refresh();
    });

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(result.current.freshness).toBe("fresh");
    expect(result.current.latest?.websocket.delivered_messages).toBe(3);
  });
});
