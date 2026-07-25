import { CircleOff, Radio, RefreshCw, Wifi } from "lucide-react";

import type { SocketStatus } from "../realtime/useDanmakuSocket";

interface ConnectionBadgeProps {
  onReconnect: () => void;
  status: SocketStatus;
}

const statusText: Record<SocketStatus, string> = {
  connected: "已连接",
  connecting: "连接中",
  reconnecting: "重连中",
  offline: "离线",
};

export function ConnectionBadge({ onReconnect, status }: ConnectionBadgeProps) {
  const StatusIcon = status === "connected"
    ? Wifi
    : status === "offline"
      ? CircleOff
      : Radio;

  return (
    <div className={`connection-badge connection-badge--${status}`} aria-live="polite">
      <StatusIcon aria-hidden="true" size={16} />
      <span>{statusText[status]}</span>
      {status === "offline" && (
        <button
          aria-label="重新连接"
          className="icon-button icon-button--compact"
          onClick={onReconnect}
          title="重新连接"
          type="button"
        >
          <RefreshCw aria-hidden="true" size={15} />
        </button>
      )}
    </div>
  );
}
