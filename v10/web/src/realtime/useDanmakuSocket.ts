import { useCallback, useEffect, useRef, useState } from "react";

import { encodeDanmaku, encodeLike, parseServerPacket } from "../protocol/parser";
import type { ControlMessage, DanmakuMessage, RoomStats } from "../protocol/types";
import { buildSocketURL, type Identity } from "./url";

export type { Identity } from "./url";

export type SocketStatus = "connecting" | "connected" | "reconnecting" | "offline";

export interface DanmakuSocketState {
  status: SocketStatus;
  messages: DanmakuMessage[];
  stats: RoomStats;
  lastControl: ControlMessage | null;
  retryUntil: number;
  sendDanmaku(content: string): boolean;
  sendLike(count?: number): boolean;
  reconnect(): void;
}

const EMPTY_STATS: RoomStats = { online: 0, likes: 0 };
const MESSAGE_LIMIT = 300;
const INITIAL_RETRY_DELAY = 500;
const MAX_RETRY_DELAY = 10_000;

export function useDanmakuSocket(identity: Identity): DanmakuSocketState {
  const { roomId, userId, username } = identity;
  const [status, setStatus] = useState<SocketStatus>("connecting");
  const [messages, setMessages] = useState<DanmakuMessage[]>([]);
  const [stats, setStats] = useState<RoomStats>(EMPTY_STATS);
  const [lastControl, setLastControl] = useState<ControlMessage | null>(null);
  const [retryUntil, setRetryUntil] = useState(0);
  const socketRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const attemptRef = useRef(0);
  const generationRef = useRef(0);
  const connectRef = useRef<(nextStatus: SocketStatus) => void>(() => {});

  const clearReconnectTimer = useCallback(() => {
    if (reconnectTimerRef.current !== null) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
  }, []);

  const scheduleReconnect = useCallback((generation: number) => {
    if (generation !== generationRef.current || reconnectTimerRef.current !== null) {
      return;
    }

    const delay = Math.min(
      INITIAL_RETRY_DELAY * (2 ** attemptRef.current),
      MAX_RETRY_DELAY,
    );
    attemptRef.current += 1;
    setStatus("reconnecting");
    reconnectTimerRef.current = setTimeout(() => {
      reconnectTimerRef.current = null;

      if (generation === generationRef.current) {
        connectRef.current("reconnecting");
      }
    }, delay);
  }, []);

  const connect = useCallback((nextStatus: SocketStatus) => {
    clearReconnectTimer();
    const generation = generationRef.current + 1;
    generationRef.current = generation;
    setStatus(nextStatus);

    let socket: WebSocket;
    try {
      socket = new WebSocket(buildSocketURL(new URL(window.location.href), {
        userId,
        username,
        roomId,
      }));
    } catch {
      scheduleReconnect(generation);
      return;
    }

    socketRef.current = socket;

    socket.onopen = () => {
      if (generation !== generationRef.current || socketRef.current !== socket) {
        return;
      }

      attemptRef.current = 0;
      setStatus("connected");
    };

    socket.onmessage = (event) => {
      if (generation !== generationRef.current || socketRef.current !== socket) {
        return;
      }

      const packet = parseServerPacket(String(event.data));
      if (!packet || packet.roomId !== roomId) {
        return;
      }

      switch (packet.kind) {
        case "danmaku":
          setMessages((currentMessages) => [
            ...currentMessages.slice(-(MESSAGE_LIMIT - 1)),
            packet.data,
          ]);
          break;
        case "stats":
          setStats(packet.data);
          break;
        case "control":
          setLastControl(packet.data);
          setRetryUntil(
            packet.data.retry_after_millis === undefined
              ? 0
              : Date.now() + packet.data.retry_after_millis,
          );
          break;
      }
    };

    socket.onclose = () => {
      if (generation !== generationRef.current || socketRef.current !== socket) {
        return;
      }

      socketRef.current = null;
      scheduleReconnect(generation);
    };
  }, [clearReconnectTimer, roomId, scheduleReconnect, userId, username]);

  connectRef.current = connect;

  useEffect(() => {
    connect("connecting");

    return () => {
      clearReconnectTimer();
      attemptRef.current = 0;
      generationRef.current += 1;
      const socket = socketRef.current;
      socketRef.current = null;
      socket?.close();
    };
  }, [clearReconnectTimer, connect]);

  const sendDanmaku = useCallback((content: string) => {
    const socket = socketRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return false;
    }

    try {
      socket.send(encodeDanmaku(content));
      return true;
    } catch {
      return false;
    }
  }, []);

  const sendLike = useCallback((count?: number) => {
    const socket = socketRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return false;
    }

    try {
      socket.send(encodeLike(count));
      return true;
    } catch {
      return false;
    }
  }, []);

  const reconnect = useCallback(() => {
    clearReconnectTimer();
    attemptRef.current = 0;
    generationRef.current += 1;
    const socket = socketRef.current;
    socketRef.current = null;
    socket?.close();
    connectRef.current("connecting");
  }, [clearReconnectTimer]);

  return {
    status,
    messages,
    stats,
    lastControl,
    retryUntil,
    sendDanmaku,
    sendLike,
    reconnect,
  };
}
