import { Activity, Heart, MessageSquareText, Users } from "lucide-react";

import type { Identity } from "../app/identity";
import { DanmakuStage } from "../components/DanmakuStage";
import { MessageComposer } from "../components/MessageComposer";
import { MessageList } from "../components/MessageList";
import type { DanmakuSocketState } from "../realtime/useDanmakuSocket";

interface LiveRoomPageProps {
  identity: Identity;
  socket: DanmakuSocketState;
}

const connectionText = {
  connected: "正常",
  connecting: "连接中",
  reconnecting: "重连中",
  offline: "离线",
};

export function LiveRoomPage({ identity, socket }: LiveRoomPageProps) {
  const metrics = [
    { icon: Users, label: "在线", value: socket.stats.online },
    { icon: Heart, label: "点赞", value: socket.stats.likes },
    { icon: Activity, label: "连接状态", value: connectionText[socket.status] },
    { icon: MessageSquareText, label: "本次消息", value: socket.messages.length },
  ];

  return (
    <main className="live-room">
      <div className="live-room__stage-column">
        <DanmakuStage
          messages={socket.messages}
          online={socket.stats.online}
          roomId={identity.roomId}
        />

        <section aria-label="房间摘要" className="room-summary">
          {metrics.map(({ icon: Icon, label, value }) => (
            <div className="summary-metric" key={label}>
              <Icon aria-hidden="true" size={18} />
              <div>
                <span>{label}</span>
                <strong>{value}</strong>
              </div>
            </div>
          ))}
        </section>
      </div>

      <aside aria-label="实时弹幕" className="chat-panel">
        <div className="chat-panel__heading">
          <div>
            <span className="section-kicker">REALTIME FEED</span>
            <h1>实时弹幕</h1>
          </div>
          <span className="chat-panel__count">{socket.messages.length}</span>
        </div>

        <MessageList
          lastControl={socket.lastControl}
          messages={socket.messages}
        />
        <MessageComposer
          retryUntil={socket.retryUntil}
          sendDanmaku={socket.sendDanmaku}
          sendLike={socket.sendLike}
        />
      </aside>
    </main>
  );
}
