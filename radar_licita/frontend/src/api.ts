import type { DashboardSummary, Opportunity, OpportunityDetail, Supplier, Recommendations, PartnerCandidate, PartnerSearch, FinancialResult, TechnologySearchResult, HistoricalTechnologyExhibit } from './types'

const BASE = import.meta.env.VITE_API_URL || '/api/v1'

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${BASE}${path}`, {
    ...init,
    headers: init?.body instanceof FormData ? init.headers : { 'Content-Type': 'application/json', ...init?.headers },
  })
  if (!response.ok) {
    let message = `Erro ${response.status}`
    try { const detail = (await response.json()).detail; message = typeof detail === 'string' ? detail : detail ? JSON.stringify(detail) : message } catch { /* response is not JSON */ }
    throw new Error(message)
  }
  return response.json()
}

export const api = {
  summary: () => request<DashboardSummary>('/dashboard/summary'),
  opportunities: (params: URLSearchParams) => request<{ data: Opportunity[]; total: number; total_pages: number }>(`/opportunities?${params}`),
  opportunity: (id: number) => request<OpportunityDetail>(`/opportunities/${id}`),
  sync: () => request('/sync/pncp', { method: 'POST', body: JSON.stringify({ mode: 'open', max_pages: 2, hydrate_details: true }) }),
  processDocuments: (id: number) => request(`/opportunities/${id}/documents/process`, { method: 'POST' }),
  updateStatus: (id: number, status: string) => request(`/opportunities/${id}/status`, { method: 'PATCH', body: JSON.stringify({ status }) }),
  suppliers: () => request<Supplier[]>('/suppliers'),
  createSupplier: (data: Record<string, unknown>) => request<Supplier>('/suppliers', { method: 'POST', body: JSON.stringify(data) }),
  createProduct: (supplierId: number, data: Record<string, unknown>) => request(`/suppliers/${supplierId}/products`, { method: 'POST', body: JSON.stringify(data) }),
  createQuote: (productId: number, data: Record<string, unknown>) => request(`/supplier-products/${productId}/quotes`, { method: 'POST', body: JSON.stringify(data) }),
  importCsv: (file: File) => { const body = new FormData(); body.append('file', file); return request('/suppliers/import-csv', { method: 'POST', body }) },
  settings: () => request<Record<string, unknown>>('/settings'),
  saveSettings: (data: Record<string, unknown>) => request('/settings', { method: 'PUT', body: JSON.stringify(data) }),
  simulate: (itemId: number, data: Record<string, unknown>) => request<FinancialResult>(`/items/${itemId}/financial-scenarios`, { method: 'POST', body: JSON.stringify(data) }),
  recommendations: (data: Record<string, unknown>, signal?: AbortSignal) => request<Recommendations>('/recommendations/search', { method: 'POST', body: JSON.stringify(data), signal }),
  partnerCandidates: (itemId: number) => request<PartnerCandidate[]>(`/items/${itemId}/partner-candidates`),
  partnerSearch: (itemId: number, data: Record<string, unknown>) => request<PartnerSearch>(`/items/${itemId}/partner-searches`, { method: 'POST', body: JSON.stringify(data) }),
  promoteCandidate: (id: number) => request<PartnerCandidate>(`/partner-candidates/${id}/promote`, { method: 'POST' }),
  matchProduct: (productId: number, itemId: number) => request(`/supplier-products/${productId}/match/${itemId}`, { method: 'POST' }),
  technologySearch: (data: Record<string, unknown>) => request<TechnologySearchResult>('/technology/search', { method: 'POST', body: JSON.stringify(data) }),
  historicalTechnology: () => request<HistoricalTechnologyExhibit>('/technology/historical-exhibit'),
  refreshHistoricalTechnology: () => request<HistoricalTechnologyExhibit>('/technology/historical-exhibit/refresh', { method: 'POST' }),
}

