import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiGet, apiPost } from "../api/client";
import type { CatalogResponse, CatalogScanResponse } from "../api/types";

export function useCatalog() {
  return useQuery({
    queryKey: ["catalog"],
    queryFn: () => apiGet<CatalogResponse>("/catalog"),
    refetchInterval: 15_000,
    staleTime: 5_000,
  });
}

export function useCatalogScan() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (codeBookIds: number[]) =>
      apiPost<CatalogScanResponse>("/catalog/scan", { code_book_ids: codeBookIds }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["catalog"] });
      qc.invalidateQueries({ queryKey: ["import-status"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}
