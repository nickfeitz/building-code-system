import { useHealth } from "../hooks/useHealth";
import { useStats, useLLMStatus } from "../hooks/useStats";
import { useImports } from "../hooks/useImports";
import { ImportsTable } from "../components/ImportsTable";

function StatCard({ label, value, hint }: { label: string; value: React.ReactNode; hint?: string }) {
  return (
    <div className="card">
      <div className="text-xs uppercase tracking-wider text-surface-100">{label}</div>
      <div className="mt-2 text-3xl font-semibold text-white tabular-nums">{value}</div>
      {hint && <div className="mt-1 text-xs text-surface-100">{hint}</div>}
    </div>
  );
}

function StatusRow({
  label,
  ok,
  detail,
}: {
  label: string;
  ok: boolean | undefined;
  detail?: string;
}) {
  const dot =
    ok === true ? "bg-success" : ok === false ? "bg-danger" : "bg-surface-200";
  return (
    <div className="flex items-center justify-between py-2 border-b border-surface-500 last:border-0">
      <div className="flex items-center gap-3">
        <span className={`w-2 h-2 rounded-full ${dot}`} />
        <span className="text-sm text-surface-50">{label}</span>
      </div>
      <span className="text-xs text-surface-100">{detail ?? (ok ? "ok" : ok === false ? "down" : "—")}</span>
    </div>
  );
}

export function DashboardPanel() {
  const health = useHealth();
  const stats = useStats();
  const llm = useLLMStatus();
  const imports = useImports(8);
  const activePhases = new Set(["queued", "parsing", "indexing"]);
  const activeCount = (imports.data ?? []).filter((r) => activePhases.has(r.phase)).length;

  const dbOk = health.data?.database === "ok";
  const embedOk = health.data?.embedding_service === "ok";
  const ollamaOk = health.data?.ollama?.available;
  const overall = health.data?.status;

  return (
    <div className="h-full overflow-y-auto p-6 space-y-6">
      {/* Top row: counters */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          label="Code Sections"
          value={stats.data?.total_sections ?? "—"}
          hint="indexed and embedded"
        />
        <StatCard
          label="References"
          value={stats.data?.total_references ?? "—"}
        />
        <StatCard
          label="Code Books"
          value={stats.data?.code_books ?? "—"}
        />
        <StatCard
          label="Pending Quarantine"
          value={stats.data?.pending_quarantine ?? "—"}
          hint={stats.data?.pending_quarantine ? "needs review" : "none"}
        />
      </div>

      {/* Middle row: services + LLM */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <h2 className="text-sm font-semibold text-white mb-2">System Health</h2>
          <div className="text-xs text-surface-100 mb-2">
            Overall:{" "}
            <span
              className={
                overall === "healthy"
                  ? "text-success"
                  : overall === "degraded"
                    ? "text-warn"
                    : "text-danger"
              }
            >
              {overall ?? "checking…"}
            </span>
          </div>
          <StatusRow label="PostgreSQL (building_code)" ok={dbOk} detail={health.data?.database} />
          <StatusRow label="Embedding service" ok={embedOk} detail={health.data?.embedding_service} />
          <StatusRow label="Ollama" ok={ollamaOk} detail={health.data?.ollama?.url} />
          <StatusRow
            label="Claude API"
            ok={health.data?.claude_api === "configured"}
            detail={health.data?.claude_api}
          />
        </div>

        <div className="card">
          <h2 className="text-sm font-semibold text-white mb-2">LLM</h2>
          <div className="text-sm text-surface-50">
            Provider: <span className="text-white">{llm.data?.provider ?? "—"}</span>
          </div>
          <div className="text-sm text-surface-50">
            Default model: <span className="text-white">{llm.data?.model ?? "—"}</span>
          </div>
          <div className="mt-3">
            <div className="text-xs uppercase tracking-wider text-surface-100 mb-2">
              Available Ollama models
            </div>
            <div className="flex flex-wrap gap-1.5">
              {(llm.data?.available_models ?? []).map((m) => (
                <span
                  key={m}
                  className={`badge ${m === llm.data?.model ? "bg-accent text-white" : "bg-surface-500 text-surface-50"}`}
                >
                  {m}
                </span>
              ))}
              {!llm.data && <span className="text-xs text-surface-100">loading…</span>}
            </div>
          </div>
        </div>
      </div>

      {/* Imports */}
      <div className="card">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-sm font-semibold text-white">
            Imports
            {activeCount > 0 && (
              <span className="ml-2 badge bg-accent/30 text-accent animate-pulse">
                {activeCount} active
              </span>
            )}
          </h2>
          <span className="text-xs text-surface-100">
            latest 8 · refresh 2 s while active, 10 s idle
          </span>
        </div>
        <ImportsTable rows={imports.data} compact emptyMessage="No uploads or scans yet. Use the Import or Catalog panel to get started." />
      </div>

      {/* Footer: refresh hint */}
      <div className="text-xs text-surface-100 text-center">
        Auto-refreshing: health 5s, stats 10s, LLM 15s, imports 2–10s
      </div>
    </div>
  );
}
