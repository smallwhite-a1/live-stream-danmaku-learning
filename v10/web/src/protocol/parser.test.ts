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

  it("returns null when a known packet has a non-string room ID", () => {
    expect(
      parseServerPacket(
        JSON.stringify({ type: 102, room_id: 7, data: { online: 12, likes: 34 } }),
      ),
    ).toBeNull();
  });

  it.each([
    ["a missing required string", { id: 1, message_id: "message-01", room_id: "room-01", user_id: "user-01", username: "Ada", send_time: "2026-07-19T12:00:00Z" }],
    ["a negative message ID", { id: -1, message_id: "message-01", room_id: "room-01", user_id: "user-01", username: "Ada", content: "Hello", send_time: "2026-07-19T12:00:00Z" }],
  ])("returns null for danmaku data with %s", (_description, data) => {
    expect(
      parseServerPacket(JSON.stringify({ type: 101, room_id: "room-01", data })),
    ).toBeNull();
  });

  it("returns null for danmaku data with a non-finite message ID", () => {
    expect(
      parseServerPacket('{"type":101,"room_id":"room-01","data":{"id":1e999,"message_id":"message-01","room_id":"room-01","user_id":"user-01","username":"Ada","content":"Hello","send_time":"2026-07-19T12:00:00Z"}}'),
    ).toBeNull();
  });

  it.each([
    ["a negative counter", { online: -1, likes: 34 }],
    ["a non-numeric counter", { online: "12", likes: 34 }],
  ])("returns null for stats data with %s", (_description, data) => {
    expect(
      parseServerPacket(JSON.stringify({ type: 102, room_id: "room-01", data })),
    ).toBeNull();
  });

  it("returns null for stats data with a non-finite counter", () => {
    expect(
      parseServerPacket('{"type":102,"room_id":"room-01","data":{"online":12,"likes":1e999}}'),
    ).toBeNull();
  });

  it.each([
    ["a non-string code", { code: 400 }],
    ["a non-string optional action", { code: "rate_limited", action: 1 }],
    ["a non-string optional scope", { code: "rate_limited", scope: false }],
    ["a negative retry delay", { code: "rate_limited", retry_after_millis: -1 }],
  ])("returns null for control data with %s", (_description, data) => {
    expect(
      parseServerPacket(JSON.stringify({ type: 104, room_id: "room-01", data })),
    ).toBeNull();
  });

  it("returns null for control data with a non-finite retry delay", () => {
    expect(
      parseServerPacket('{"type":104,"room_id":"room-01","data":{"code":"rate_limited","retry_after_millis":1e999}}'),
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

  it.each([
    [Number.NaN, 1],
    [Number.POSITIVE_INFINITY, 1],
    [Number.NEGATIVE_INFINITY, 1],
    [1.9, 1],
    [20.9, 20],
    [0, 1],
    [-3.7, 1],
  ])("encodes an integer like count for %s", (input, expectedCount) => {
    expect(JSON.parse(encodeLike(input))).toEqual({
      type: 103,
      data: { count: expectedCount },
    });
  });
});
