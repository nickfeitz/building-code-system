import { useQuery } from "@tanstack/react-query";
import { apiGet } from "../api/client";

/** Outline row returned by /api/code-books/{id}/sections?outline=true. */
export interface OutlineRow {
  id: number;
  section_number: string;
  section_title: string | null;
  full_text: string | null;
  depth: number;
  page_number: number | null;
  has_ca_amendment: boolean;
  amendment_agency: string | null;
  section_type: string | null;
}

/** Full section detail from /api/sections/{id}. */
export interface SectionDetail {
  id: number;
  code_book_id: number;
  chapter: string | null;
  section_number: string;
  section_title: string | null;
  full_text: string;
  depth: number;
  path: string | null;
  effective_date: string | null;
  superseded_date: string | null;
  amended: boolean;
}

/** The PDF associated with a book — id + filename. */
export interface BookPdf {
  id: number;
  filename: string;
  size_bytes: number;
  uploaded_at: string | null;
  sha256: string;
}

/**
 * Flat list of every live section in the book, WITH full body text.
 * For ASCE 7-22 this is ~2 MB — big but cached forever in react-query
 * and served gzipped by nginx, so subsequent navigation inside the book
 * is instant. This lets the Browser panel render a selected section AND
 * all its descendants inline (UpCodes-style "read the whole chapter" view)
 * without a second round-trip per subsection.
 */
export function useBookOutline(bookId: number | null) {
  return useQuery({
    queryKey: ["book-outline", bookId],
    queryFn: () =>
      apiGet<OutlineRow[]>(`/code-books/${bookId}/sections?limit=5000`),
    enabled: bookId != null,
    staleTime: 60_000,
  });
}

export function useSectionDetail(sectionId: number | null) {
  return useQuery({
    queryKey: ["section-detail", sectionId],
    queryFn: () => apiGet<SectionDetail>(`/sections/${sectionId}`),
    enabled: sectionId != null,
    staleTime: 60_000,
  });
}

export function useBookPdfs(bookId: number | null) {
  return useQuery({
    queryKey: ["book-pdfs", bookId],
    queryFn: () => apiGet<BookPdf[]>(`/code-books/${bookId}/pdfs`),
    enabled: bookId != null,
    staleTime: 60_000,
  });
}
