import { NavLink } from "react-router-dom";
import { Settings } from "lucide-react";

import type { Identity } from "../app/identity";
import type { SocketStatus } from "../realtime/useDanmakuSocket";
import { ConnectionBadge } from "./ConnectionBadge";

interface AppHeaderProps {
  identity: Identity;
  onOpenRoomSwitcher: () => void;
  onReconnect: () => void;
  status: SocketStatus;
}

const routes = [
  { label: "直播间", to: "/" },
  { label: "运行监控", to: "/monitor" },
  { label: "链路说明", to: "/chain" },
];

export function AppHeader({
  identity,
  onOpenRoomSwitcher,
  onReconnect,
  status,
}: AppHeaderProps) {
  return (
    <header className="app-header">
      <div className="app-header__bar">
        <NavLink className="app-brand" to="/">
          <span className="app-brand__signal" aria-hidden="true" />
          DANMAKU LAB
        </NavLink>

        <div className="app-header__session">
          <ConnectionBadge onReconnect={onReconnect} status={status} />
          <div className="app-header__identity">
            <strong>{identity.username}</strong>
            <span>{identity.roomId}</span>
          </div>
          <button
            aria-label="更换房间"
            className="icon-button"
            onClick={onOpenRoomSwitcher}
            title="更换房间"
            type="button"
          >
            <Settings aria-hidden="true" size={18} />
          </button>
        </div>
      </div>

      <nav aria-label="主导航" className="route-tabs">
        <div className="route-tabs__inner">
          {routes.map((route) => (
            <NavLink
              className={({ isActive }) => `route-tab${isActive ? " route-tab--active" : ""}`}
              end={route.to === "/"}
              key={route.to}
              to={route.to}
            >
              {route.label}
            </NavLink>
          ))}
        </div>
      </nav>
    </header>
  );
}
