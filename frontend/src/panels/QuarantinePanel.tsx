import { useQuery, useQueryClient } from "@tanstack/react-query";
import { apiGet, apiPost } from "../api/client";
import type { QuarantineItem } from "../api/types";

export function QuarantinePanel() {
  const qc = useQueryClient();

  const list = useQuery({
    queryKey: ["quarantine"],
    queryFn: () => apiGet<QuarantineItem[]>("/quarantine"),
    refetchInterval: 10_000,
  });

  const act = async (id: number, action: "approve" | "reject") => {
    await apiPost(`/quarantine/${id}/${action}`);
    qc.invalidateQueries({ queryKey: ["quarantine"] });
    qc.invalidateQueries({ queryKey: ["stats"] });
  };

  return (
    <div className="h-full overflow-y-auto p-6">
      <div className="card">
        <h2 className="text-sm font-semibold text-surface-50 mb-3">
          Pending Quarantine ({list.data?.length ?? 0})
        </h2>
        {list.isLoading && <div className="text-xs text-surface-100">loading…</div>}
        {list.data && list.data.length === 0 && (
          <div className="text-xs text-surface-100">Nothing to review.</div>
        )}
        <div className="space-y-3">
          {(list.data ?? []).map((it) => (
            <div
              key={it.id}
              className="border border-surface-400 rounded p-3 bg-surface-700"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0">
                  <div className="text-xs text-surface-100">{it.source}</div>
                  <div className="text-sm text-surface-50 truncate">{it.reason}</div>
                  {it.content && (
                    <div className="text-xs text-surface-100 mt-2 line-clamp-3">
                      {it.content}
                    </div>
                  )}
                </div>
                <div className="flex gap-2 shrink-0">
                  <button onClick={() => act(it.id, "approve")} className="btn-primary">
                    Approve
                  </button>
                  <button onClick={() => act(it.id, "reject")} className="btn-ghost">
                    Reject
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
