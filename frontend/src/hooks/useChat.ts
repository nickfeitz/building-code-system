import { useCallback, useRef, useState } from "react";
import { streamChat } from "../api/client";

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

interface SendOptions {
  model?: string;
  useClaude?: boolean;
}

export function useChat() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [streaming, setStreaming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const send = useCallback(async (text: string, opts?: SendOptions) => {
    if (!text.trim() || streaming) return;
    setError(null);
    setStreaming(true);
    setMessages((m) => [...m, { role: "user", content: text }, { role: "assistant", content: "" }]);

    const controller = new AbortController();
    abortRef.current = controller;

    try {
      for await (const chunk of streamChat(text, opts, controller.signal)) {
        setMessages((m) => {
          const next = [...m];
          next[next.length - 1] = {
            role: "assistant",
            content: next[next.length - 1].content + chunk,
          };
          return next;
        });
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (controller.signal.aborted) {
        // user cancelled — leave messages as-is
      } else {
        setError(msg);
        setMessages((m) => {
          const next = [...m];
          const last = next[next.length - 1];
          if (last && last.role === "assistant" && !last.content) {
            next[next.length - 1] = { role: "assistant", content: `⚠️ ${msg}` };
          }
          return next;
        });
      }
    } finally {
      setStreaming(false);
      abortRef.current = null;
    }
  }, [streaming]);

  const stop = useCallback(() => {
    abortRef.current?.abort();
  }, []);

  const reset = useCallback(() => {
    abortRef.current?.abort();
    setMessages([]);
    setError(null);
  }, []);

  return { messages, streaming, error, send, stop, reset };
}
