import { useEffect, useMemo, useState } from "react";

import stageImage from "../assets/live-stage.webp";
import type { DanmakuMessage } from "../protocol/types";

interface DanmakuStageProps {
  messages: readonly DanmakuMessage[];
  online: number;
  roomId: string;
}

const MOBILE_QUERY = "(max-width: 720px)";
const ACTIVE_LIMIT = 40;
const LANE_GAP_MILLIS = 2_200;

export function assignLane(
  lanes: readonly number[],
  now: number,
  laneCount: number,
): number {
  const boundedLaneCount = Number.isFinite(laneCount)
    ? Math.max(1, Math.trunc(laneCount))
    : 1;
  let selectedLane = 0;
  let earliestAvailability = lanes[0] ?? now;

  for (let lane = 1; lane < boundedLaneCount; lane += 1) {
    const availability = lanes[lane] ?? now;
    if (availability < earliestAvailability) {
      selectedLane = lane;
      earliestAvailability = availability;
    }
  }

  return selectedLane;
}

function useLaneCount(): number {
  const [isMobile, setIsMobile] = useState(
    () => window.matchMedia?.(MOBILE_QUERY).matches ?? false,
  );

  useEffect(() => {
    const mediaQuery = window.matchMedia?.(MOBILE_QUERY);
    if (!mediaQuery) {
      return;
    }

    const updateViewport = (event: MediaQueryListEvent) => {
      setIsMobile(event.matches);
    };
    setIsMobile(mediaQuery.matches);
    mediaQuery.addEventListener("change", updateViewport);

    return () => {
      mediaQuery.removeEventListener("change", updateViewport);
    };
  }, []);

  return isMobile ? 4 : 8;
}

export function DanmakuStage({ messages, online, roomId }: DanmakuStageProps) {
  const laneCount = useLaneCount();
  const activeDanmaku = useMemo(() => {
    const laneReadyAt = Array.from({ length: laneCount }, () => 0);

    return messages.slice(-ACTIVE_LIMIT).map((message) => {
      const lane = assignLane(laneReadyAt, 0, laneCount);
      const delayMillis = laneReadyAt[lane] ?? 0;
      laneReadyAt[lane] = delayMillis + LANE_GAP_MILLIS;

      return { delayMillis, lane, message };
    });
  }, [laneCount, messages]);

  return (
    <section aria-label="直播舞台" className="danmaku-stage">
      <img alt="直播间演示舞台" className="danmaku-stage__image" src={stageImage} />
      <div className="danmaku-stage__shade" />

      <div className="danmaku-stage__status">
        <div className="danmaku-stage__room">
          <span className="live-label">LIVE</span>
          <strong>{roomId}</strong>
        </div>
        <span>{online} 人在线</span>
      </div>

      <div className="danmaku-stage__overlay" aria-live="polite">
        {activeDanmaku.map(({ delayMillis, lane, message }, index) => (
          <div
            className="active-danmaku"
            data-lane={lane}
            data-testid="active-danmaku"
            key={message.message_id}
            style={{
              animationDelay: `${delayMillis / 1_000}s`,
              top: `calc(${((lane + 0.5) / laneCount) * 100}% - 12px)`,
              zIndex: index + 1,
            }}
          >
            <strong>{message.username}</strong>
            <span>{message.content}</span>
          </div>
        ))}
      </div>
    </section>
  );
}
