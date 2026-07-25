import { useState } from "react";
import { Routes, Route } from "react-router-dom";
import { X } from "lucide-react";

import { AppHeader } from "../components/AppHeader";
import { LiveRoomPage } from "../pages/LiveRoomPage";
import { useDanmakuSocket } from "../realtime/useDanmakuSocket";
import { resolveIdentity, type Identity } from "./identity";

export function App() {
  const [identity, setIdentity] = useState<Identity>(() => (
    resolveIdentity(window.location.search, window.localStorage)
  ));
  const [roomSwitcherOpen, setRoomSwitcherOpen] = useState(false);
  const socket = useDanmakuSocket(identity);

  const connectRoom = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const formData = new FormData(event.currentTarget);
    const fallbackName = `访客-${identity.userId.slice(-4)}`;
    const username = String(formData.get("username") ?? "").trim() || fallbackName;
    const roomId = String(formData.get("roomId") ?? "").trim() || "room-01";
    const search = new URLSearchParams({
      uid: identity.userId,
      name: username,
      room: roomId,
    });
    const nextIdentity = resolveIdentity(`?${search}`, window.localStorage);

    window.history.replaceState({}, "", `${window.location.pathname}?${search}`);
    setIdentity(nextIdentity);
    setRoomSwitcherOpen(false);
  };

  return (
    <div className="app-shell">
      <AppHeader
        identity={identity}
        onOpenRoomSwitcher={() => {
          setRoomSwitcherOpen(true);
        }}
        onReconnect={socket.reconnect}
        status={socket.status}
      />

      <Routes>
        <Route
          path="/"
          element={<LiveRoomPage identity={identity} socket={socket} />}
        />
      </Routes>

      {roomSwitcherOpen && (
        <div className="room-switcher-backdrop" role="presentation">
          <section
            aria-labelledby="room-switcher-title"
            aria-modal="true"
            className="room-switcher"
            role="dialog"
          >
            <div className="room-switcher__heading">
              <div>
                <span className="section-kicker">IDENTITY / ROOM</span>
                <h2 id="room-switcher-title">连接设置</h2>
              </div>
              <button
                aria-label="关闭房间设置"
                className="icon-button"
                onClick={() => {
                  setRoomSwitcherOpen(false);
                }}
                title="关闭"
                type="button"
              >
                <X aria-hidden="true" size={18} />
              </button>
            </div>

            <form className="room-switcher__form" onSubmit={connectRoom}>
              <label>
                <span>昵称</span>
                <input defaultValue={identity.username} name="username" />
              </label>
              <label>
                <span>房间</span>
                <input defaultValue={identity.roomId} name="roomId" />
              </label>
              <button className="command-button room-switcher__submit" type="submit">
                连接房间
              </button>
            </form>
          </section>
        </div>
      )}
    </div>
  );
}
