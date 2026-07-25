import { useCallback, useEffect, useRef, useState } from "react";

import { deriveEvents, deriveSample } from "./derive";
import type { MetricEvent, MetricSample, ServerMetrics } from "./types";

export interface MetricsState {
  latest: ServerMetrics | null;
  samples: MetricSample[];
  events: MetricEvent[];
  freshness: "loading" | "fresh" | "stale";
  lastSuccessAt: number | null;
  refresh(): Promise<void>;
}

function keepNewest<T>(items: T[], limit: number): T[] {
  return items.slice(-limit);
}

export function useMetrics(): MetricsState {
  const [latest, setLatest] = useState<ServerMetrics | null>(null);
  const [samples, setSamples] = useState<MetricSample[]>([]);
  const [events, setEvents] = useState<MetricEvent[]>([]);
  const [freshness, setFreshness] = useState<MetricsState["freshness"]>("loading");
  const [lastSuccessAt, setLastSuccessAt] = useState<number | null>(null);

  const latestRef = useRef<ServerMetrics | null>(null);
  const samplesRef = useRef<MetricSample[]>([]);
  const eventsRef = useRef<MetricEvent[]>([]);
  const sampledAtRef = useRef(0);
  const mountedRef = useRef(true);
  const inFlightRef = useRef<Promise<void> | null>(null);
  const controllerRef = useRef<AbortController | null>(null);

  const refresh = useCallback((): Promise<void> => {
    const inFlight = inFlightRef.current;
    if (inFlight) {
      return inFlight;
    }

    const controller = new AbortController();
    controllerRef.current = controller;

    let resolveRequest!: () => void;
    const request = new Promise<void>((resolve) => {
      resolveRequest = resolve;
    });
    inFlightRef.current = request;

    void (async () => {
      try {
        const response = await fetch("/metrics", {
          signal: controller.signal,
        });
        if ("ok" in response && response.ok === false) {
          throw new Error(`Metrics request failed with status ${response.status}`);
        }

        const current = await response.json() as ServerMetrics;
        if (!mountedRef.current || controller.signal.aborted) {
          return;
        }

        const sampledAt = Math.max(Date.now(), sampledAtRef.current + 1);
        sampledAtRef.current = sampledAt;
        const sample = deriveSample(samplesRef.current.at(-1) ?? null, current, sampledAt);
        const nextEvents = keepNewest(
          [...eventsRef.current, ...deriveEvents(latestRef.current, current, sampledAt)],
          50,
        );
        const nextSamples = keepNewest([...samplesRef.current, sample], 30);

        latestRef.current = current;
        samplesRef.current = nextSamples;
        eventsRef.current = nextEvents;

        setLatest(current);
        setSamples(nextSamples);
        setEvents(nextEvents);
        setFreshness("fresh");
        setLastSuccessAt(sampledAt);
      } catch {
        if (!mountedRef.current || controller.signal.aborted) {
          return;
        }

        setFreshness("stale");
      } finally {
        if (controllerRef.current === controller) {
          controllerRef.current = null;
        }
        if (inFlightRef.current === request) {
          inFlightRef.current = null;
        }
        resolveRequest();
      }
    })();

    return request;
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    void refresh();

    const intervalId = window.setInterval(() => {
      void refresh();
    }, 2_000);

    return () => {
      mountedRef.current = false;
      window.clearInterval(intervalId);
      controllerRef.current?.abort();
      controllerRef.current = null;
      inFlightRef.current = null;
    };
  }, [refresh]);

  return {
    latest,
    samples,
    events,
    freshness,
    lastSuccessAt,
    refresh,
  };
}
