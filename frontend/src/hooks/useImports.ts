import { useQuery } from "@tanstack/react-query";
import { apiGet } from "../api/client";
import type { ImportJob } from "../api/types";

const ACTIVE_PHASES = new Set(["queued", "parsing", "indexing"]);

function hasActive(rows: ImportJob[] | undefined) {
  return !!rows?.some((r) => ACTIVE_PHASES.has(r.phase));
}

/**
 * Poll /api/imports. Ticks every 2 s when at least one job is active,
 * otherwise every 10 s. Keeps cost low when nothing is happening.
 */
export function useImports(limit = 20) {
  return useQuery({
    queryKey: ["imports", limit],
    queryFn: () => apiGet<ImportJob[]>(`/imports?limit=${limit}`),
    refetchInterval: (q) => (hasActive(q.state.data) ? 2_000 : 10_000),
    refetchIntervalInBackground: false,
    staleTime: 1_000,
  });
}

/**
 * Poll a single import by id — used after an upload to show live phase
 * progression. Stops polling once the job is terminal.
 */
export function useImportJob(id: number | null) {
  return useQuery({
    queryKey: ["import", id],
    queryFn: () => apiGet<ImportJob>(`/imports/${id}`),
    enabled: id != null,
    refetchInterval: (q) => {
      const j = q.state.data;
      if (!j) return 2_000;
      return ACTIVE_PHASES.has(j.phase) ? 2_000 : false;
    },
    staleTime: 1_000,
  });
}
