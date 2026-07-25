export const PacketType = {
  Danmaku: 101,
  Stats: 102,
  Like: 103,
  Control: 104,
} as const;

export interface DanmakuMessage {
  id: number;
  message_id: string;
  room_id: string;
  user_id: string;
  username: string;
  content: string;
  send_time: string;
}

export interface RoomStats {
  online: number;
  likes: number;
}

export interface ControlMessage {
  code: "rate_limited" | "server_overloaded" | "content_too_long" | string;
  action?: string;
  scope?: string;
  retry_after_millis?: number;
}

export type ServerEvent =
  | { kind: "danmaku"; roomId: string; data: DanmakuMessage }
  | { kind: "stats"; roomId: string; data: RoomStats }
  | { kind: "control"; roomId: string; data: ControlMessage };
