import type { Identity } from "../realtime/url";

export type { Identity } from "../realtime/url";

const STORAGE_KEY = "danmaku-lab.identity.v10";
const DEFAULT_ROOM = "room-01";
const MAX_IDENTITY_RUNES = 64;

const memoryIdentities = new WeakMap<object, Identity>();

function readStoredIdentity(
  storage: Pick<Storage, "getItem" | "setItem">,
): Partial<Identity> {
  try {
    const rawIdentity = storage.getItem(STORAGE_KEY);
    if (!rawIdentity) {
      return memoryIdentities.get(storage) ?? {};
    }

    const parsedIdentity: unknown = JSON.parse(rawIdentity);
    if (typeof parsedIdentity !== "object" || parsedIdentity === null) {
      return {};
    }

    const candidate = parsedIdentity as Record<string, unknown>;
    return {
      userId: typeof candidate.userId === "string" ? candidate.userId : undefined,
      username: typeof candidate.username === "string" ? candidate.username : undefined,
      roomId: typeof candidate.roomId === "string" ? candidate.roomId : undefined,
    };
  } catch {
    return memoryIdentities.get(storage) ?? {};
  }
}

function createUserId(): string {
  const bytes = new Uint8Array(4);
  crypto.getRandomValues(bytes);
  const suffix = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `web-${suffix}`;
}

function fallbackName(userId: string): string {
  return `访客-${userId.slice(-4)}`;
}

function validIdentityField(value: string | null | undefined): string | undefined {
  const trimmedValue = value?.trim();
  if (!trimmedValue || Array.from(trimmedValue).length > MAX_IDENTITY_RUNES) {
    return undefined;
  }

  return trimmedValue;
}

export function resolveIdentity(
  search: string,
  storage: Pick<Storage, "getItem" | "setItem">,
): Identity {
  const query = new URLSearchParams(search);
  const storedIdentity = readStoredIdentity(storage);
  const userId = validIdentityField(query.get("uid"))
    ?? validIdentityField(storedIdentity.userId)
    ?? createUserId();
  const identity = {
    userId,
    username: validIdentityField(query.get("name"))
      ?? validIdentityField(storedIdentity.username)
      ?? fallbackName(userId),
    roomId: validIdentityField(query.get("room"))
      ?? validIdentityField(storedIdentity.roomId)
      ?? DEFAULT_ROOM,
  };

  memoryIdentities.set(storage, identity);
  try {
    storage.setItem(STORAGE_KEY, JSON.stringify(identity));
  } catch {
    // In-memory identity keeps the current page stable when storage is unavailable.
  }

  return identity;
}
