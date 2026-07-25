import { useEffect, useState } from "react";
import { Heart, Send } from "lucide-react";

interface MessageComposerProps {
  retryUntil: number;
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
  const [, setRetryRevision] = useState(0);
  const characterCount = Array.from(content).length;
  const isRetrying = retryUntil > Date.now();

  useEffect(() => {
    const remainingMillis = retryUntil - Date.now();
    if (remainingMillis <= 0) {
      return;
    }

    const timer = window.setTimeout(() => {
      setRetryRevision((revision) => revision + 1);
    }, remainingMillis);

    return () => {
      window.clearTimeout(timer);
    };
  }, [retryUntil]);

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmedContent = content.trim();
    if (!trimmedContent || isRetrying) {
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
          disabled={isRetrying}
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
          disabled={isRetrying}
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
