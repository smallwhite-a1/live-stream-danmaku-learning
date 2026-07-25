import { describe, expect, it, vi } from "vitest";

import { resolveIdentity } from "./identity";

const STORAGE_KEY = "danmaku-lab.identity.v10";

function createStorage(initialValue: string | null = null): Pick<Storage, "getItem" | "setItem"> {
  let value = initialValue;

  return {
    getItem: vi.fn(() => value),
    setItem: vi.fn((_key: string, nextValue: string) => {
      value = nextValue;
    }),
  };
}

describe("resolveIdentity", () => {
  it("lets query uid, name, and room override stored identity", () => {
    const storage = createStorage(JSON.stringify({
      userId: "stored-user",
      username: "Stored user",
      roomId: "stored-room",
    }));

    const identity = resolveIdentity(
      "?uid=query-user&name=%E6%9E%97&room=query-room",
      storage,
    );

    expect(identity).toEqual({
      userId: "query-user",
      username: "林",
      roomId: "query-room",
    });
    expect(storage.setItem).toHaveBeenCalledWith(STORAGE_KEY, JSON.stringify(identity));
  });

  it("reuses a stored identity", () => {
    const storedIdentity = {
      userId: "web-1234abcd",
      username: "访客-abcd",
      roomId: "room-02",
    };
    const storage = createStorage(JSON.stringify(storedIdentity));

    expect(resolveIdentity("", storage)).toEqual(storedIdentity);
  });

  it("creates and stores a stable local identity when none exists", () => {
    const storage = createStorage();

    const firstIdentity = resolveIdentity("", storage);
    const secondIdentity = resolveIdentity("", storage);

    expect(firstIdentity).toEqual(secondIdentity);
    expect(firstIdentity).toEqual({
      userId: expect.stringMatching(/^web-[0-9a-f]{8}$/),
      username: expect.stringMatching(/^访客-[0-9a-f]{4}$/),
      roomId: "room-01",
    });
    expect(firstIdentity.username.slice(-4)).toBe(firstIdentity.userId.slice(-4));
  });

  it("falls back from blank values to a readable nickname and room-01", () => {
    const storage = createStorage(JSON.stringify({
      userId: "web-a1b2c3d4",
      username: "   ",
      roomId: "",
    }));

    expect(resolveIdentity("?uid=%20&name=%20%20&room=%20", storage)).toEqual({
      userId: "web-a1b2c3d4",
      username: "访客-c3d4",
      roomId: "room-01",
    });
  });
});
