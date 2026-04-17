import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiGet, apiPost } from "../api/client";
import type {
  FlagRequest,
  FlagResponse,
  PageSection,
  PageText,
  PdfPageMeta,
} from "../api/types";

/** One-shot metadata (page count, dims, filename) for a stored PDF. */
export function usePdfMeta(pdfId: number | null) {
  return useQuery({
    queryKey: ["pdf-meta", pdfId],
    queryFn: () => apiGet<PdfPageMeta>(`/code-book-pdfs/${pdfId}/pages`),
    enabled: pdfId != null,
    staleTime: 60_000,
  });
}

/** Raw PyMuPDF text for a single page (what the parser sees). */
export function usePageText(pdfId: number | null, page: number) {
  return useQuery({
    queryKey: ["pdf-page-text", pdfId, page],
    queryFn: () =>
      apiGet<PageText>(`/code-book-pdfs/${pdfId}/pages/${page}/text`),
    enabled: pdfId != null && page > 0,
    staleTime: 60_000,
  });
}

/**
 * Sections whose page_number matches. Returns [] for old imports that
 * predate page-number tracking; the reviewer UI handles the empty state.
 */
export function usePageSections(bookId: number | null, page: number) {
  return useQuery({
    queryKey: ["book-page-sections", bookId, page],
    queryFn: () =>
      apiGet<PageSection[]>(`/code-books/${bookId}/sections?page=${page}`),
    enabled: bookId != null && page > 0,
    staleTime: 30_000,
  });
}

export function useFlagPage() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: FlagRequest) =>
      apiPost<FlagResponse>("/review/flag", req),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["quarantine"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}
