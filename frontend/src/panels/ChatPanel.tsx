import { useEffect, useRef, useState } from "react";
import { useChat } from "../hooks/useChat";
import { useLLMStatus } from "../hooks/useStats";

export function ChatPanel() {
  const { messages, streaming, error, send, stop, reset } = useChat();
  const llm = useLLMStatus();
  const [text, setText] = useState("");
  const [model, setModel] = useState<string>("");
  const scrollRef = useRef<HTMLDivElement>(null);

  // Pre-fill model select with the backend default
  useEffect(() => {
    if (!model && llm.data?.model) setModel(llm.data.model);
  }, [llm.data?.model, model]);

  // Auto-scroll on new content
  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: "smooth" });
  }, [messages]);

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const t = text.trim();
    if (!t) return;
    setText("");
    void send(t, { model: model || undefined });
  };

  return (
    <div className="h-full flex flex-col">
      {/* Toolbar */}
      <div className="flex items-center gap-3 px-6 py-2 border-b border-surface-400 bg-surface-800">
        <label className="text-xs text-surface-100">Model</label>
        <select
          className="input !py-1 !text-xs max-w-xs"
          value={model}
          onChange={(e) => setModel(e.target.value)}
          disabled={streaming}
        >
          {(llm.data?.available_models ?? []).map((m) => (
            <option key={m} value={m}>
              {m}
            </option>
          ))}
          {!llm.data && <option value="">loading…</option>}
        </select>
        <div className="flex-1" />
        <button
          type="button"
          onClick={reset}
          disabled={streaming || messages.length === 0}
          className="btn-ghost"
        >
          Clear
        </button>
      </div>

      {/* Messages */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
        {messages.length === 0 && (
          <div className="text-center text-sm text-surface-100 mt-10">
            Ask a question about the building codes, e.g. <em>"What are the egress requirements for residential corridors?"</em>
          </div>
        )}
        {messages.map((m, i) => (
          <div
            key={i}
            className={`flex ${m.role === "user" ? "justify-end" : "justify-start"}`}
          >
            <div
              className={`max-w-[80%] rounded-lg px-4 py-3 text-sm whitespace-pre-wrap break-words ${
                m.role === "user"
                  ? "bg-accent text-white"
                  : "bg-surface-800 border border-surface-400 text-surface-50"
              }`}
            >
              {m.content || (streaming ? "…" : "")}
            </div>
          </div>
        ))}
        {error && (
          <div className="text-xs text-danger text-center">{error}</div>
        )}
      </div>

      {/* Input */}
      <form onSubmit={onSubmit} className="px-6 py-4 border-t border-surface-400 bg-surface-800">
        <div className="flex gap-2">
          <textarea
            className="input resize-none flex-1"
            rows={2}
            placeholder="Ask about building codes…"
            value={text}
            onChange={(e) => setText(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                onSubmit(e);
              }
            }}
            disabled={streaming}
          />
          {streaming ? (
            <button type="button" onClick={stop} className="btn-ghost">
              Stop
            </button>
          ) : (
            <button type="submit" className="btn-primary" disabled={!text.trim()}>
              Send
            </button>
          )}
        </div>
        <div className="mt-1 text-xs text-surface-100">
          Enter to send · Shift+Enter for newline
        </div>
      </form>
    </div>
  );
}
