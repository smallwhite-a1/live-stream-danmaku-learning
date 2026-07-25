import { useEffect, useRef, useState } from "react";

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

interface ActiveDanmaku {
  delayMillis: number;
  lane: number;
  message: DanmakuMessage;
}

export function assignLane(
  lanes: readonly number[],
  now: number,
  laneCount: number,
): number {
  const boundedLaneCount = Number.isFinite(laneCount)
    ? Math.max(1, Math.trunc(laneCount))
    : 1;
  let selectedLane = 0;
  let earliestAvailability = Math.max(lanes[0] ?? now, now);

  for (let lane = 1; lane < boundedLaneCount; lane += 1) {
    const availability = Math.max(lanes[lane] ?? now, now);
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
  const [activeDanmaku, setActiveDanmaku] = useState<ActiveDanmaku[]>([]);
  const laneReadyAtRef = useRef<number[]>([]);
  const seenMessageIdsRef = useRef(new Set<string>());
  const stageScopeRef = useRef(`${roomId}:${laneCount}`);

  useEffect(() => {
    const stageScope = `${roomId}:${laneCount}`;
    if (stageScopeRef.current !== stageScope) {
      stageScopeRef.current = stageScope;
      laneReadyAtRef.current = [];
      seenMessageIdsRef.current.clear();
      setActiveDanmaku([]);
    }

    const now = Date.now();
    const nextDanmaku: ActiveDanmaku[] = [];
    for (const message of messages) {
      if (seenMessageIdsRef.current.has(message.message_id)) {
        continue;
      }

      seenMessageIdsRef.current.add(message.message_id);
      const lane = assignLane(laneReadyAtRef.current, now, laneCount);
      const startsAt = Math.max(laneReadyAtRef.current[lane] ?? now, now);
      laneReadyAtRef.current[lane] = startsAt + LANE_GAP_MILLIS;
      nextDanmaku.push({
        delayMillis: startsAt - now,
        lane,
        message,
      });
    }

    const retainedMessageIds = new Set(messages.map((message) => message.message_id));
    for (const messageId of seenMessageIdsRef.current) {
      if (!retainedMessageIds.has(messageId)) {
        seenMessageIdsRef.current.delete(messageId);
      }
    }

    if (nextDanmaku.length > 0) {
      setActiveDanmaku((currentDanmaku) => (
        [...currentDanmaku, ...nextDanmaku].slice(-ACTIVE_LIMIT)
      ));
    }
  }, [laneCount, messages, roomId]);

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
            onAnimationEnd={() => {
              setActiveDanmaku((currentDanmaku) => (
                currentDanmaku.filter((item) => item.message.message_id !== message.message_id)
              ));
            }}
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
