import { useQuery } from "@tanstack/react-query";
import { apiGet } from "../api/client";
import type { StatsResponse, LLMStatusResponse } from "../api/types";

export function useStats() {
  return useQuery({
    queryKey: ["stats"],
    queryFn: () => apiGet<StatsResponse>("/stats"),
    refetchInterval: 10_000,
  });
}

export function useLLMStatus() {
  return useQuery({
    queryKey: ["llm-status"],
    queryFn: () => apiGet<LLMStatusResponse>("/llm/status"),
    refetchInterval: 15_000,
  });
}
