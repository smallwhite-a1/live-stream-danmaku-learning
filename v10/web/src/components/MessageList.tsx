import { useEffect, useRef } from "react";

import type { ControlMessage, DanmakuMessage } from "../protocol/types";

interface MessageListProps {
  lastControl: ControlMessage | null;
  messages: readonly DanmakuMessage[];
}

const controlText: Record<string, string> = {
  rate_limited: "发送过快，请稍后重试",
  server_overloaded: "服务当前繁忙，这条弹幕没有被接收",
  content_too_long: "弹幕不能超过 200 个字符",
};

function formatTime(sendTime: string): string {
  const parsedTime = new Date(sendTime);
  if (Number.isNaN(parsedTime.getTime())) {
    return "--:--:--";
  }

  return new Intl.DateTimeFormat("zh-CN", {
    hour: "2-digit",
    hour12: false,
    minute: "2-digit",
    second: "2-digit",
  }).format(parsedTime);
}

export function MessageList({ lastControl, messages }: MessageListProps) {
  const listRef = useRef<HTMLDivElement>(null);
  const visibleMessages = messages.slice(-300);

  useEffect(() => {
    const list = listRef.current;
    if (list) {
      list.scrollTop = list.scrollHeight;
    }
  }, [lastControl, messages]);

  return (
    <div className="message-list" ref={listRef}>
      {visibleMessages.length === 0 && !lastControl && (
        <div className="message-list__empty">
          <span className="signal-pulse" aria-hidden="true" />
          等待直播信号
        </div>
      )}

      {visibleMessages.map((message) => (
        <article className="message-item" key={message.message_id}>
          <div className="message-item__meta">
            <strong>{message.username}</strong>
            <time dateTime={message.send_time}>{formatTime(message.send_time)}</time>
          </div>
          <p>{message.content}</p>
        </article>
      ))}

      {lastControl && (
        <div className="control-notice" role="status">
          {controlText[lastControl.code] ?? "服务端拒绝了本次操作"}
        </div>
      )}
    </div>
  );
}
