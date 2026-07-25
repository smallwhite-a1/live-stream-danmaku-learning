import { PacketType, type ControlMessage, type DanmakuMessage, type RoomStats, type ServerEvent } from "./types";

interface RawPacket {
  type: number;
  room_id: string;
  data: unknown;
}

export function parseServerPacket(raw: string): ServerEvent | null {
  let parsed: unknown;

  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }

  if (!parsed || typeof parsed !== "object") {
    return null;
  }

  const packet = parsed as RawPacket;

  switch (packet.type) {
    case PacketType.Danmaku:
      return {
        kind: "danmaku",
        roomId: packet.room_id,
        data: packet.data as DanmakuMessage,
      };
    case PacketType.Stats:
      return {
        kind: "stats",
        roomId: packet.room_id,
        data: packet.data as RoomStats,
      };
    case PacketType.Control:
      return {
        kind: "control",
        roomId: packet.room_id,
        data: packet.data as ControlMessage,
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
  const boundedCount = Math.min(Math.max(count, 1), 20);

  return JSON.stringify({
    type: PacketType.Like,
    data: { count: boundedCount },
  });
}
