import { useEffect, useReducer, useState } from "react";
import { Heart, Send } from "lucide-react";

import type { ActionRetryUntil } from "../realtime/useDanmakuSocket";

interface MessageComposerProps {
  retryUntil: ActionRetryUntil;
  sendDanmaku: (content: string) => boolean;
  sendLike: (count?: number) => boolean;
}

const CONTENT_LIMIT = 200;

export function MessageComposer({
  retryUntil,
  sendDanmaku,
  sendLike,
}: MessageComposerProps) {
  const [content, setContent] = useState("");
  const [retryRevision, refreshRetryState] = useReducer((revision: number) => revision + 1, 0);
  const characterCount = Array.from(content).length;
  const now = Date.now();
  const isDanmakuRetrying = retryUntil.danmaku > now;
  const isLikeRetrying = retryUntil.like > now;

  useEffect(() => {
    const currentTime = Date.now();
    const nextDeadline = [retryUntil.danmaku, retryUntil.like]
      .filter((deadline) => deadline > currentTime)
      .sort((left, right) => left - right)[0];
    if (nextDeadline === undefined) {
      return;
    }

    const timer = window.setTimeout(() => {
      refreshRetryState();
    }, nextDeadline - currentTime);

    return () => {
      window.clearTimeout(timer);
    };
  }, [retryRevision, retryUntil.danmaku, retryUntil.like]);

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmedContent = content.trim();
    if (!trimmedContent || isDanmakuRetrying) {
      return;
    }

    if (sendDanmaku(trimmedContent)) {
      setContent("");
    }
  };

  return (
    <form className="message-composer" onSubmit={handleSubmit}>
      <div className="message-composer__input">
        <textarea
          aria-label="弹幕内容"
          onChange={(event) => {
            setContent(Array.from(event.target.value).slice(0, CONTENT_LIMIT).join(""));
          }}
          placeholder="发送一条实时弹幕"
          rows={3}
          value={content}
        />
        <span aria-live="polite" className="message-composer__count">
          {characterCount}/{CONTENT_LIMIT}
        </span>
      </div>

      <div className="message-composer__actions">
        <button
          aria-label="点赞"
          className="icon-button icon-button--like"
          disabled={isLikeRetrying}
          onClick={() => {
            sendLike();
          }}
          title="点赞"
          type="button"
        >
          <Heart aria-hidden="true" size={18} />
        </button>
        <button
          aria-label="发送弹幕"
          className="command-button"
          disabled={isDanmakuRetrying}
          title="发送弹幕"
          type="submit"
        >
          <Send aria-hidden="true" size={17} />
          <span>发送</span>
        </button>
      </div>
    </form>
  );
}
