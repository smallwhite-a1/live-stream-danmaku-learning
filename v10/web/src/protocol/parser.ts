import { PacketType, type ControlMessage, type DanmakuMessage, type RoomStats, type ServerEvent } from "./types";

type UnknownRecord = Record<string, unknown>;

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonNegativeFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

function isDanmakuMessage(value: unknown): value is DanmakuMessage {
  return isRecord(value)
    && isNonNegativeFiniteNumber(value.id)
    && typeof value.message_id === "string"
    && typeof value.room_id === "string"
    && typeof value.user_id === "string"
    && typeof value.username === "string"
    && typeof value.content === "string"
    && typeof value.send_time === "string";
}

function isRoomStats(value: unknown): value is RoomStats {
  return isRecord(value)
    && isNonNegativeFiniteNumber(value.online)
    && isNonNegativeFiniteNumber(value.likes);
}

function isControlMessage(value: unknown): value is ControlMessage {
  return isRecord(value)
    && typeof value.code === "string"
    && (value.action === undefined || typeof value.action === "string")
    && (value.scope === undefined || typeof value.scope === "string")
    && (value.retry_after_millis === undefined
      || isNonNegativeFiniteNumber(value.retry_after_millis));
}

export function parseServerPacket(raw: string): ServerEvent | null {
  let parsed: unknown;

  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }

  if (!isRecord(parsed) || typeof parsed.room_id !== "string") {
    return null;
  }

  switch (parsed.type) {
    case PacketType.Danmaku:
      if (!isDanmakuMessage(parsed.data)) {
        return null;
      }

      return {
        kind: "danmaku",
        roomId: parsed.room_id,
        data: parsed.data,
      };
    case PacketType.Stats:
      if (!isRoomStats(parsed.data)) {
        return null;
      }

      return {
        kind: "stats",
        roomId: parsed.room_id,
        data: parsed.data,
      };
    case PacketType.Control:
      if (!isControlMessage(parsed.data)) {
        return null;
      }

      return {
        kind: "control",
        roomId: parsed.room_id,
        data: parsed.data,
      };
    default:
      return null;
  }
}

export function encodeDanmaku(content: string): string {
  const trimmedContent = content.trim();

  if (!trimmedContent) {
    throw new Error("Danmaku content cannot be empty");
  }

  return JSON.stringify({
    type: PacketType.Danmaku,
    data: { content: trimmedContent },
  });
}

export function encodeLike(count = 1): string {
  const boundedCount = Number.isFinite(count)
    ? Math.min(Math.max(Math.trunc(count), 1), 20)
    : 1;

  return JSON.stringify({
    type: PacketType.Like,
    data: { count: boundedCount },
  });
}
