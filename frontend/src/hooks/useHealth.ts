import { useQuery } from "@tanstack/react-query";
import { apiGet } from "../api/client";
import type { HealthResponse } from "../api/types";

export function useHealth() {
  return useQuery({
    queryKey: ["health"],
    queryFn: () => apiGet<HealthResponse>("/health"),
    refetchInterval: 5_000,
    refetchIntervalInBackground: false,
  });
}
