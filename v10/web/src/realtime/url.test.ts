import { describe, expect, it } from "vitest";

import { buildSocketURL } from "./url";

describe("buildSocketURL", () => {
  it("encodes the identity in a websocket URL", () => {
    expect(
      buildSocketURL(
        new URL("http://localhost:5173/"),
        { userId: "u 1", username: "林", roomId: "room/1" },
      ),
    ).toBe("ws://localhost:5173/ws?uid=u+1&name=%E6%9E%97&room=room%2F1");
  });

  it("uses wss for an HTTPS page", () => {
    expect(
      buildSocketURL(
        new URL("https://example.test/app/"),
        { userId: "u1", username: "Ada", roomId: "room-1" },
      ),
    ).toBe("wss://example.test/ws?uid=u1&name=Ada&room=room-1");
  });
});
