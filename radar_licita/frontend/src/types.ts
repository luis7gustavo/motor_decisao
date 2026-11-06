export type DeliveryClass = 'A' | 'B' | 'C' | 'D'
export type AnalysisStatus = 'new' | 'in_analysis' | 'awaiting_quote' | 'viable' | 'discarded' | 'preparing_proposal' | 'proposal_sent' | 'won' | 'lost' | 'contracted' | 'completed'

export interface DashboardSummary {
  open_opportunities: number
  new_opportunities: number
  analyzed_opportunities: number
  financially_viable: number
  remote_favorable: number
  closing_soon: number
  last_sync_at: string | null
  last_sync_status: string | null
}

export interface Opportunity {
  id: number
  official_control_number: string
  object_description: string
  organization_name: string
  municipality: string | null
  uf: string | null
  modality_name: string | null
  estimated_total_value: string | null
  is_price_confidential: boolean
  proposal_close_at: string | null
  pncp_url: string
  source_system_url: string | null
  analysis_status: AnalysisStatus
  is_open: boolean
  score: string | null
  score_provisional: boolean
  delivery_classification: DeliveryClass | null
  remote_delivery_score: number | null
  lowest_quote: string | null
  simulated_margin: string | null
  required_capital: string | null
  item_count: number
}

export interface Evidence {
  id: number
  field_name: string
  normalized_value: string | null
  page_number: number | null
  excerpt: string
  confidence: string
  extraction_method: string
}

export interface ProcurementItem {
  id: number
  item_number: number
  description: string
  complementary_description: string | null
  material_or_service: string | null
  quantity: string | null
  unit: string | null
  unit_estimated_value: string | null
  total_estimated_value: string | null
  price_is_confidential: boolean
  judgment_criterion: string | null
  status: string | null
}

export interface OpportunityDetail extends Opportunity {
  organization_cnpj: string
  unit_name: string | null
  year: number
  sequence: number
  purchase_number: string | null
  process_number: string | null
  additional_information: string | null
  dispute_mode_name: string | null
  judgment_criterion_name: string | null
  status_name: string | null
  proposal_open_at: string | null
  published_at: string | null
  source_updated_at: string | null
  items: ProcurementItem[]
  documents: Array<{
    id: number
    sequence: number
    title: string | null
    document_type: string | null
    source_url: string
    processing_status: string
    processing_error: string | null
    extractions: Evidence[]
  }>
  delivery_requirement: null | {
    remote_delivery_score: number
    classification: DeliveryClass
    confidence: string
    direct_ship_score: number
    requires_installation: boolean | null
    requires_assembly: boolean | null
    requires_site_visit: boolean | null
    delivery_locations_count: number | null
    delivery_deadline_days: number | null
    delivery_address: string | null
    justifications: string[]
    pending_checks: string[]
    evidence_ids: number[]
  }
  latest_score: null | {
    item_id: number | null
    eligibility: 'viable' | 'pending' | 'blocked'
    score: string | null
    provisional: boolean
    eligible: boolean
    components: Record<string, number>
    blockers: string[]
    pending_checks: string[]
    explanation: string
    calculated_at: string
  }
  item_analyses: Array<{
    item_id: number | null; eligibility: 'viable' | 'pending' | 'blocked'
    score: string | null; components: Record<string, number>; blockers: string[]
    pending_checks: string[]; explanation: string
  }>
}

export interface Supplier {
  id: number
  name: string
  tax_id: string | null
  origin_uf: string | null
  contact_name: string | null
  email: string | null
  phone: string | null
  active: boolean
  products: Array<{
    id: number
    name: string
    brand: string | null
    model: string | null
    sku: string | null
    technical_description: string | null
    direct_shipping: boolean | null
    quotes: Array<{ id: number; procurement_item_id: number | null; unit_price: string; valid_until: string | null; estimated_freight: string | null; freight_known: boolean; freight_scope: Record<string, unknown> | null; source: string }>
  }>
}

export interface RecommendationItem {
  procurement_id: number; item_id: number; eligibility: string; score: string | null
  required_capital: string | null; budget_remaining: string | null; estimated_result: string | null
  margin_rate: string | null; proposal_close_at: string | null; pending_checks: string[]
  blockers: string[]; explanation: string; cost_basis: string; origin: Record<string, unknown>
}
export interface Recommendations {
  recommendations: RecommendationItem[]; needs_analysis: RecommendationItem[]
  outside_budget: RecommendationItem[]; blocked: RecommendationItem[]
  totals: Record<string, number>; evaluated_at: string
}
export interface PartnerCandidate {
  id: number; legal_name: string; tax_id: string; discovery_status: string
  comparability: string; reasons: string[]; matched_terms: string[]
  supplier_id: number | null; evidence_count: number; last_observed_at: string | null
  observations?: Array<{source_url: string; description?: string; result_date?: string; price_nature?: string; unit_price?: string}>
}
export interface PartnerSearch {
  id: number; status: string; complete: boolean; request_count: number
  queried_at: string; consulted_window: Record<string, unknown>; limitations: string[]
  candidates: PartnerCandidate[]
}
export interface FinancialResult {
  revenue: string; estimated_result: string; margin_rate: string; required_capital: string
  break_even_unit_price: string; assumptions_complete: boolean; sensitivity: Record<string, unknown>
}

export interface TechnologySearchResult {
  status: string
  searched_at: string
  coverage: {
    open_rows: number; open_purchases_checked: number; historical_rows: number
    historical_purchases_checked: number; historical_result_items_checked: number
    request_count: number; history_from: string; history_to: string; open_until: string
  }
  limitations: string[]
  opportunities: Array<{
    procurement_id: number; item_id: number; control_number: string; object_description: string
    item_number: number; description: string; organization: string; uf: string | null
    proposal_close_at: string | null; pncp_url: string; categories: string[]
    public_estimate: string | null; estimate_basis: string
    budgets: Array<{ amount: string; estimate_fits: boolean | null }>
    companies: Array<{
      tax_id: string; legal_name: string; technical_match: 'pending'
      matched_terms: string[]; similarity: number
      evidence: Array<{ description: string; result_date: string | null; unit_price: string | null; source_url: string; control_number: string }>
    }>
  }>
}

export interface HistoricalTechnologyExhibit {
  mode: 'snapshot' | 'live'
  captured_at: string
  request_count: number
  selection: string
  totals: { procurements: number; items: number; results: number; distinct_companies: number }
  limitations: string[]
  cases: Array<{
    control_number: string; object_description: string; organization: string; uf: string | null
    published_at: string | null; proposal_close_at: string; pncp_url: string
    purchase_url: string; purchase_hash: string
    items: Array<{
      item_number: number; description: string; quantity: string | null; unit: string | null
      public_estimate: string | null; item_url: string; item_hash: string
      companies: Array<{
        tax_id: string; legal_name: string; result_date: string | null
        unit_price: string | null; status: string | null; source_url: string
        source_hash: string; technical_match: 'pending'
      }>
    }>
  }>
}

