import { describe, expect, it } from "vitest";

import { encodeDanmaku, encodeLike, parseServerPacket } from "./parser";

describe("protocol parser", () => {
  it("parses a danmaku packet", () => {
    expect(
      parseServerPacket(
        JSON.stringify({
          type: 101,
          room_id: "room-01",
          data: {
            id: 1,
            message_id: "message-01",
            room_id: "room-01",
            user_id: "user-01",
            username: "Ada",
            content: "Hello",
            send_time: "2026-07-19T12:00:00Z",
          },
        }),
      ),
    ).toEqual({
      kind: "danmaku",
      roomId: "room-01",
      data: {
        id: 1,
        message_id: "message-01",
        room_id: "room-01",
        user_id: "user-01",
        username: "Ada",
        content: "Hello",
        send_time: "2026-07-19T12:00:00Z",
      },
    });
  });

  it("parses a room stats packet", () => {
    expect(
      parseServerPacket(
        JSON.stringify({
          type: 102,
          room_id: "room-01",
          data: { online: 12, likes: 34 },
        }),
      ),
    ).toEqual({
      kind: "stats",
      roomId: "room-01",
      data: { online: 12, likes: 34 },
    });
  });

  it("parses a control packet", () => {
    expect(
      parseServerPacket(
        JSON.stringify({
          type: 104,
          room_id: "room-01",
          data: {
            code: "rate_limited",
            action: "send",
            scope: "user",
            retry_after_millis: 500,
          },
        }),
      ),
    ).toEqual({
      kind: "control",
      roomId: "room-01",
      data: {
        code: "rate_limited",
        action: "send",
        scope: "user",
        retry_after_millis: 500,
      },
    });
  });

  it("returns null for malformed JSON", () => {
    expect(parseServerPacket("not-json")).toBeNull();
  });

  it("returns null for a non-packet JSON value", () => {
    expect(parseServerPacket("null")).toBeNull();
  });

  it("returns null for an unknown packet type", () => {
    expect(
      parseServerPacket(
        JSON.stringify({ type: 999, room_id: "room-01", data: {} }),
      ),
    ).toBeNull();
  });

  it("encodes trimmed danmaku content", () => {
    expect(JSON.parse(encodeDanmaku("  Hello  "))).toEqual({
      type: 101,
      data: { content: "Hello" },
    });
  });

  it("rejects empty danmaku content", () => {
    expect(() => encodeDanmaku("   ")).toThrow("Danmaku content cannot be empty");
  });

  it("encodes a bounded like count", () => {
    expect(JSON.parse(encodeLike(0))).toEqual({
      type: 103,
      data: { count: 1 },
    });
    expect(JSON.parse(encodeLike(99))).toEqual({
      type: 103,
      data: { count: 20 },
    });
  });
});
