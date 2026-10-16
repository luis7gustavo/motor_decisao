import { useEffect, useState } from 'react'
import { ArrowUpRight, Building2, ChevronRight, ExternalLink, FileSearch, RefreshCw, ShieldCheck } from 'lucide-react'
import { api } from './api'
import type { HistoricalTechnologyExhibit } from './types'

const money = new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' })
const formatMoney = (value: string | null) => value == null ? 'Não informada' : money.format(Number(value))
const formatDate = (value: string | null) => value ? value.slice(0, 10).split('-').reverse().join('/') : 'Não informada'
const failure = (error: unknown) => error instanceof Error ? error.message : 'Operação não concluída'

export function HistoricalTechnologyPanel() {
  const [data, setData] = useState<HistoricalTechnologyExhibit | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState('')
  const [selected, setSelected] = useState<string | null>(null)
  const [query, setQuery] = useState('')

  useEffect(() => {
    api.historicalTechnology().then(setData).catch(e => setError(failure(e))).finally(() => setLoading(false))
  }, [])

  const refresh = async () => {
    setRefreshing(true)
    setError('')
    try { setData(await api.refreshHistoricalTechnology()) }
    catch (e) { setError(`Consulta ao vivo indisponível: ${failure(e)}. O acervo capturado permanece na tela.`) }
    finally { setRefreshing(false) }
  }

  const cases = data?.cases ?? []
  const visibleCases = cases.filter(item => `${item.organization} ${item.object_description} ${item.control_number}`.toLocaleLowerCase('pt-BR').includes(query.toLocaleLowerCase('pt-BR')))
  const active = visibleCases.find(item => item.control_number === selected) ?? visibleCases[0]

  return <section className="historical-workspace space-y-5" aria-label="Acervo histórico de tecnologia">
    <div className="historical-hero">
      <div className="historical-hero-copy">
        <span className="eyebrow">Acervo público / tecnologia</span>
        <h2>Da contratação ao <em>possível parceiro.</em></h2>
        <p>Explore pregões eletrônicos encerrados. Cada empresa exibida está ligada a um resultado público do mesmo item e tem sua fonte no PNCP.</p>
        <div className="historical-hero-actions">
          <span className="historical-live-tag"><span />{data?.mode === 'live' ? 'Reconsulta ao vivo' : 'Captura oficial registrada'}</span>
          <button type="button" onClick={refresh} disabled={refreshing} className="historical-refresh focus-ring"><RefreshCw size={15} className={refreshing ? 'animate-spin' : ''}/>{refreshing ? 'Consultando…' : 'Reconsultar PNCP'}</button>
        </div>
      </div>
      <div className="historical-hero-art" aria-hidden="true"><div className="historical-orbit orbit-one"/><div className="historical-orbit orbit-two"/><div className="historical-orbit orbit-three"/><div className="historical-orbit-core"><span className="sillo-logo-mark" /></div></div>
    </div>

    {loading && <div role="status" className="panel rounded-2xl p-6 text-sm">Carregando acervo oficial registrado…</div>}
    {error && <div role="alert" className="historical-alert">{error}</div>}

    {data && <>
      <div className="historical-stats" aria-label="Resumo do acervo">
        <div><strong>{data.totals.procurements}</strong><span>Contratações</span></div>
        <div><strong>{data.totals.items}</strong><span>Itens analisados</span></div>
        <div><strong>{data.totals.results}</strong><span>Resultados</span></div>
        <div><strong>{data.totals.distinct_companies}</strong><span>Empresas distintas</span></div>
        <div className="historical-capture"><span>Última captura</span><strong>{new Date(data.captured_at).toLocaleString('pt-BR')}</strong></div>
      </div>

      <div className="historical-note"><ShieldCheck size={18}/><p>Estes pregões estão encerrados. Um resultado público identifica uma empresa para investigação, mas não comprova contrato executado, disponibilidade, interesse comercial ou cotação atual.</p></div>

      {cases.length > 0 ? <div className="historical-layout">
        <aside className="historical-index panel" aria-label="Lista de contratações">
          <div className="historical-index-head"><div><span className="eyebrow">Navegar pelo acervo</span><h3>Contratações</h3></div><span className="historical-count">{visibleCases.length}</span></div>
          <label className="historical-search"><FileSearch size={16}/><input aria-label="Filtrar contratações" value={query} onChange={e => setQuery(e.target.value)} placeholder="Buscar órgão ou objeto" /></label>
          <div className="historical-index-list">
            {visibleCases.map((contract, index) => <button type="button" key={contract.control_number} onClick={() => setSelected(contract.control_number)} className={`historical-case-button focus-ring ${active?.control_number === contract.control_number ? 'is-selected' : ''}`} aria-pressed={active?.control_number === contract.control_number}>
              <span className="historical-case-top"><span>{String(index + 1).padStart(2, '0')} / {contract.uf || 'UF —'}</span><ChevronRight size={16}/></span>
              <strong>{contract.organization}</strong>
              <span className="historical-case-object">{contract.object_description}</span>
              <span className="historical-case-meta">{contract.items.length} {contract.items.length === 1 ? 'item' : 'itens'} no recorte · {formatDate(contract.proposal_close_at)}</span>
            </button>)}
            {!visibleCases.length && <p className="p-5 text-sm text-[#536874]">Nenhuma contratação corresponde à busca.</p>}
          </div>
        </aside>

        {active ? <article className="historical-detail panel" aria-live="polite">
          <div className="historical-detail-head"><div><span className="eyebrow">Pregão eletrônico histórico / {active.uf || 'UF não informada'}</span><h3>{active.organization}</h3><p className="mono">{active.control_number}</p></div><a href={active.purchase_url} target="_blank" rel="noreferrer" className="historical-source-link focus-ring">Contratação no PNCP <ArrowUpRight size={15}/></a></div>
          <p className="historical-object">{active.object_description}</p>
          <div className="historical-detail-meta"><span>Propostas encerradas <strong>{formatDate(active.proposal_close_at)}</strong></span><span>{active.items.length} {active.items.length === 1 ? 'item selecionado' : 'itens selecionados'}</span></div>
          <div className="historical-items-heading"><span className="eyebrow">Evidência por item</span><h4>Itens e empresas encontradas</h4></div>
          <div className="historical-items">{active.items.map(item => <section key={item.item_number} className="historical-item">
            <div className="historical-item-head"><div><span className="historical-item-number">ITEM {String(item.item_number).padStart(2, '0')}</span><h5>{item.description}</h5></div><a href={item.item_url} target="_blank" rel="noreferrer" className="historical-text-link focus-ring">Item oficial <ExternalLink size={13}/></a></div>
            <div className="historical-item-facts"><div><span>Estimativa pública total</span><strong>{formatMoney(item.public_estimate)}</strong></div><div><span>Quantidade</span><strong>{item.quantity || 'Não informada'} {item.unit || ''}</strong></div><div><span>Empresas no resultado</span><strong>{item.companies.length}</strong></div></div>
            {item.companies.length > 0 && <div className="historical-companies">{item.companies.map(company => <div key={`${company.tax_id}-${company.source_hash}`} className="historical-company"><div className="historical-company-icon"><Building2 size={18}/></div><div className="historical-company-main"><div className="historical-company-name"><strong>{company.legal_name}</strong><span>Possível parceira</span></div><p>CNPJ {company.tax_id} · Resultado {formatDate(company.result_date)} · {company.status || 'Situação não informada'}</p><div className="historical-company-footer"><span>Valor unitário informado como homologado: <strong>{formatMoney(company.unit_price)}</strong></span><a href={company.source_url} target="_blank" rel="noreferrer" className="historical-text-link focus-ring">Ver resultado no PNCP <ExternalLink size={13}/></a></div><p className="historical-pending">Compatibilidade técnica e condições comerciais pendentes de validação.</p></div></div>)}</div>}
          </section>)}</div>
          <div className="historical-detail-footer"><span>Fontes oficiais vinculadas por número PNCP e número do item.</span><span>Origem: PNCP <ArrowUpRight size={13}/></span></div>
        </article> : <div className="panel historical-detail p-6 text-sm text-[#536874]">Ajuste a busca para ver os detalhes da contratação.</div>}
      </div> : <div className="panel rounded-2xl p-6 text-sm text-[#536874]">Nenhuma contratação disponível nesta captura.</div>}
    </>}
  </section>
}
