export interface Identity {
  userId: string;
  username: string;
  roomId: string;
}

export function buildSocketURL(location: URL, identity: Identity): string {
  const socketURL = new URL("/ws", location);

  socketURL.protocol = location.protocol === "https:" ? "wss:" : "ws:";
  socketURL.search = new URLSearchParams({
    uid: identity.userId,
    name: identity.username,
    room: identity.roomId,
  }).toString();

  return socketURL.toString();
}
