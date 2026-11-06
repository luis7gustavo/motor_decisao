# 17 — Matriz de rastreabilidade

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: rascunho para revisão do operador; [PROPOSTO], sem implementação nesta missão.

## 1. Convenções e autoridade

Cada linha percorre CP → RF → RN → RT → componente → entidade → UC → teste → fase; diagramas complementam a relação. CP/RT reservados aqui são definidos integralmente no [PRD](00-prd.md) e [TRD](00-trd.md) desta consolidação. Regras e aceites detalhados permanecem em 04/05/06 e testes em 14. Todos os vínculos são [PROPOSTO], não prova de implementação. As entidades são as de 08; schemas não significam novos bancos.

## 2. Cobertura funcional

| CP | RF / prioridade | RN | RT | Componente | Entidades principais | UC | Teste | Fase | Diagramas |
|---|---|---|---|---|---|---|---|---|---|
| CP-01 | RF-001 / Must | RN-026, RN-027 | RT-001 | Ingestão oficial / Contratações | procurement.procurements; procurement.procurement_versions; control.ingestion_jobs | UC-01 | TST-001 | 1 | D01, D02, D08, D11 |
| CP-01 | RF-002 / Must | RN-001, RN-003, RN-022 a RN-025 | RT-001 | Ingestão oficial / Contratações | procurement.procurements; procurement.procurement_versions; control.ingestion_jobs | UC-01 | TST-002 | 1 | D01, D02, D08, D11 |
| CP-01 | RF-003 / Must | RN-026, RN-028 | RT-001 | Ingestão oficial / Contratações | procurement.procurements; procurement.procurement_versions; control.ingestion_jobs | UC-01 | TST-003 | 1 | D01, D02, D08, D11 |
| CP-01 | RF-004 / Must | RN-003, RN-028 | RT-001 | Ingestão oficial / Contratações | procurement.procurements; procurement.procurement_versions; control.ingestion_jobs | UC-01 | TST-004 | 1 | D01, D02, D08, D11 |
| CP-01 | RF-005 / Must | RN-026 a RN-029 | RT-001 | Ingestão oficial / Contratações | procurement.procurements; procurement.procurement_versions; control.ingestion_jobs | UC-01 | TST-005 | 1 | D01, D02, D08, D11 |
| CP-02 | RF-006 / Must | RN-026, RN-028 | RT-002 | Documentos / Contratações | procurement.procurement_documents; procurement.evidences; procurement.requirements | UC-02 | TST-006 | 2 | D02, D07, D09, D12 |
| CP-02 | RF-007 / Must | RN-026, RN-027 | RT-002 | Documentos / Contratações | procurement.procurement_documents; procurement.evidences; procurement.requirements | UC-02 | TST-007 | 2 | D02, D07, D09, D12 |
| CP-02 | RF-008 / Must | RN-002, RN-007, RN-010, RN-022 a RN-027 | RT-002 | Documentos / Contratações | procurement.procurement_documents; procurement.evidences; procurement.requirements | UC-02 | TST-008 | 2 | D02, D07, D09, D12 |
| CP-02 | RF-009 / Must | RN-023, RN-024, RN-027, RN-030 | RT-002 | Documentos / Contratações | procurement.procurement_documents; procurement.evidences; procurement.requirements | UC-02 | TST-009 | 2 | D02, D07, D09, D12 |
| CP-03 | RF-010 / Must | RN-010, RN-011, RN-013, RN-014 | RT-003 | Logística | procurement.delivery_requirements; commercial.opportunity_analyses | UC-03 | TST-010 | 2 | D03, D13 |
| CP-03 | RF-011 / Must | RN-012, RN-013, RN-027 | RT-003 | Logística | procurement.delivery_requirements; commercial.opportunity_analyses | UC-03 | TST-011 | 2 | D03, D13 |
| CP-03 | RF-012 / Must | RN-010, RN-013, RN-014 | RT-003 | Logística | procurement.delivery_requirements; commercial.opportunity_analyses | UC-03 | TST-012 | 2 | D03, D13 |
| CP-04 | RF-013 / Must | RN-006, RN-014, RN-026 | RT-004 | Fornecimento | sourcing.suppliers; sourcing.supplier_products; sourcing.supplier_quotes | UC-04 | TST-013 | 3 | D05, D06 |
| CP-04 | RF-014 / Must | RN-015, RN-026 | RT-004 | Fornecimento | sourcing.suppliers; sourcing.supplier_products; sourcing.supplier_quotes | UC-04 | TST-014 | 3 | D05, D06 |
| CP-04 | RF-015 / Must | RN-006, RN-014, RN-026 | RT-004 | Fornecimento | sourcing.suppliers; sourcing.supplier_products; sourcing.supplier_quotes | UC-04 | TST-015 | 3 | D05, D06 |
| CP-04 | RF-016 / Must | RN-005, RN-006, RN-015 | RT-004 | Fornecimento | sourcing.suppliers; sourcing.supplier_products; sourcing.supplier_quotes | UC-04 | TST-016 | 3 | D05, D06 |
| CP-05 | RF-017 / Must | RN-007, RN-015 | RT-005 | Matching | sourcing.product_matches; procurement.procurement_items | UC-05 | TST-017 | 3 | D03, D05, D09 |
| CP-05 | RF-018 / Must | RN-015, RN-024, RN-027 | RT-005 | Matching | sourcing.product_matches; procurement.procurement_items | UC-05 | TST-018 | 3 | D03, D05, D09 |
| CP-05 | RF-019 / Must | RN-015, RN-028, RN-030 | RT-005 | Matching | sourcing.product_matches; procurement.procurement_items | UC-05 | TST-019 | 3 | D03, D05, D09 |
| CP-06 | RF-020 / Must | RN-004, RN-005, RN-019, RN-020 | RT-006 | Finanças | commercial.financial_scenarios; procurement.judgment_units | UC-06 | TST-020 | 4 | D06, D14 |
| CP-06 | RF-021 / Must | RN-004, RN-005, RN-019 a RN-021 | RT-006 | Finanças | commercial.financial_scenarios; procurement.judgment_units | UC-06 | TST-021 | 4 | D06, D14 |
| CP-06 | RF-022 / Must | RN-016 a RN-018, RN-021 | RT-006 | Finanças | commercial.financial_scenarios; procurement.judgment_units | UC-06 | TST-022 | 4 | D06, D14 |
| CP-06 | RF-023 / Must | RN-007 a RN-009, RN-019, RN-020 | RT-006 | Finanças | commercial.financial_scenarios; procurement.judgment_units | UC-06 | TST-023 | 4 | D06, D14 |
| CP-07 | RF-024 / Should | RN-005, RN-016 a RN-021 | RT-007 | Finanças | commercial.financial_scenarios | UC-06 | TST-024 | 4 | D14 |
| CP-08 | RF-025 / Must | RN-001 a RN-003, RN-006 a RN-011, RN-015, RN-018, RN-022 a RN-029 | RT-008 | Decisão | commercial.opportunity_analyses; commercial.opportunity_scores; commercial.system_settings | UC-07 | TST-025 | 4 | D03, D09, D13, D14 |
| CP-08 | RF-026 / Must | RN-027, RN-029 | RT-008 | Decisão | commercial.opportunity_analyses; commercial.opportunity_scores; commercial.system_settings | UC-07 | TST-026 | 4 | D03, D09, D13, D14 |
| CP-08 | RF-027 / Must | RN-020, RN-026 a RN-030 | RT-008 | Decisão | commercial.opportunity_analyses; commercial.opportunity_scores; commercial.system_settings | UC-07 | TST-027 | 4 | D03, D09, D13, D14 |
| CP-09 | RF-028 / Should | RN-004, RN-005, RN-015, RN-026, RN-035 | RT-009 | Contexto histórico / Consulta | commercial.historical_prices; historical_comparisons | UC-09; UC-07 quando cenário | TST-028/061–064 | 2-H básico; 6 ampliado | D01, D11, D15 |
| CP-10 | RF-029 / Must | RN-024, RN-029 a RN-032 | RT-010 | Revisão/comercial / Consulta/UI | commercial.opportunities; commercial.opportunity_reviews; commercial.opportunity_status_history; commercial.analysis_invalidations | UC-08 | TST-029 | 5 | D04, D07, D09, D10 |
| CP-10 | RF-030 / Must | RN-028, RN-030 a RN-032 | RT-010 | Revisão/comercial / Consulta/UI | commercial.opportunities; commercial.opportunity_reviews; commercial.opportunity_status_history; commercial.analysis_invalidations | UC-08 | TST-030 | 5 | D04, D07, D09, D10 |
| CP-10 | RF-031 / Must | RN-030 a RN-032 | RT-010 | Revisão/comercial / Consulta/UI | commercial.opportunities; commercial.opportunity_reviews; commercial.opportunity_status_history; commercial.analysis_invalidations | UC-08 | TST-031 | 5 | D04, D07, D09, D10 |
| CP-10 | RF-032 / Must | RN-002, RN-007 a RN-009, RN-016, RN-030 a RN-032 | RT-010 | Revisão/comercial / Consulta/UI | commercial.opportunities; commercial.opportunity_reviews; commercial.opportunity_status_history; commercial.analysis_invalidations | UC-08 | TST-032 | 5 | D04, D07, D09, D10 |
| CP-10 | RF-033 / Must | RN-004, RN-005, RN-026, RN-027, RN-032 | RT-010 | Revisão/comercial / Consulta/UI | commercial.opportunities; commercial.opportunity_reviews; commercial.opportunity_status_history; commercial.analysis_invalidations | UC-08 | TST-033 | 5 | D04, D07, D09, D10 |
| CP-10 | RF-034 / Must | RN-028, RN-030 a RN-032 | RT-010 | Revisão/comercial / Consulta/UI | commercial.opportunities; commercial.opportunity_reviews; commercial.opportunity_status_history; commercial.analysis_invalidations | UC-08 | TST-034 | 5 | D04, D07, D09, D10 |

## 3. Cobertura não funcional

RT abaixo é o vínculo principal, sem excluir aplicação transversal. RNF-003/004 também vinculam RT-012; RNF-005/008 vinculam RT-014; RNF-020 vincula RT-006; RNF-019 vincula RT-015.

| RNF | RT principal | Teste transversal | Fonte dos limites e critério |
|---|---|---|---|
| RNF-001 | RT-012 | TST-035 | 05, RNF-001; entrada/saída em 14 |
| RNF-002 | RT-012 | TST-036 | 05, RNF-002; entrada/saída em 14 |
| RNF-003 | RT-002 | TST-037 | 05, RNF-003; entrada/saída em 14 |
| RNF-004 | RT-002 | TST-038 | 05, RNF-004; entrada/saída em 14 |
| RNF-005 | RT-010 | TST-039 | 05, RNF-005; entrada/saída em 14 |
| RNF-006 | RT-013 | TST-040 | 05, RNF-006; entrada/saída em 14 |
| RNF-007 | RT-001 | TST-041 | 05, RNF-007; entrada/saída em 14 |
| RNF-008 | RT-001 | TST-042 | 05, RNF-008; entrada/saída em 14 |
| RNF-009 | RT-016 | TST-043 | 05, RNF-009; entrada/saída em 14 |
| RNF-010 | RT-006 | TST-044 | 05, RNF-010; entrada/saída em 14 |
| RNF-011 | RT-015 | TST-045 | 05, RNF-011; entrada/saída em 14 |
| RNF-012 | RT-015 | TST-046 | 05, RNF-012; entrada/saída em 14 |
| RNF-013 | RT-013 | TST-047 | 05, RNF-013; entrada/saída em 14 |
| RNF-014 | RT-014 | TST-048 | 05, RNF-014; entrada/saída em 14 |
| RNF-015 | RT-010 | TST-049 | 05, RNF-015; entrada/saída em 14 |
| RNF-016 | RT-008 | TST-050 | 05, RNF-016; entrada/saída em 14 |
| RNF-017 | RT-014 | TST-051 | 05, RNF-017; entrada/saída em 14 |
| RNF-018 | RT-001 | TST-052 | 05, RNF-018; entrada/saída em 14 |
| RNF-019 | RT-011 | TST-053 | 05, RNF-019; entrada/saída em 14 |
| RNF-020 | RT-008 | TST-054 | 05, RNF-020; entrada/saída em 14 |
| RNF-021 | RT-016 | TST-055 | 05, RNF-021; entrada/saída em 14 |

## 4. Diagrama, decisão e fase

| Diagramas | Decisões / uso | Fases |
|---|---|---|
| D01, D04, D07 | ADR-001/002/011; contexto, capacidades e jornada | 0–6 |
| D02, D12 | ADR-003/004/005/013; processos, redes e persistência | 1–5 |
| D03, D09 | ADR-006/015/017; motor e publicação coerente | 2–5 |
| D05, D06, D11 | ADR-008/014/016; versões, entidades e autoridade | 1–6 |
| D08 | ADR-010/014; coleta, retry e idempotência | 1 |
| D10 | ADR-008; RN-032 e fatos externos | 5 |
| D13 | ADR-006/012; classe independente de aptidão | 2–4 |
| D14 | ADR-007/015; Decimal, custo e capital | 4 |

## 5. Critérios de consistência

Conferir 38 RF (32 Must/6 Should), 21 RNF, 37 RN, 10 CP, 19 RT e 71 testes planejados (revisado em R1). Todo Must aparece em CP e RT; todo RT possui origem explícita no TRD; todos os RNF têm teste e RT; referências não criam requisito novo por omissão de origem. RF-024 e RF-028 permanecem Should e não são pré-requisitos obrigatórios do ranking básico/carteira. TST são planejados, enquanto a validação documental efetivamente executada fica no README e progresso.

Convenção de contagem de estados: os 45 arcos de RN-032/D10 incluem a entrada inicial Início→E01; são 44 transições entre os 16 estados. Eventos sem avanço não acrescentam transições.


## 6. Rastreabilidade adicional R1 — revisado em R1

As linhas abaixo estendem, sem substituir IDs da seção 2. Famílias CP/EP reutilizadas; nenhum Must novo, todos os Must revistos explicitamente cobertos. Fonte de entidades e cardinalidades: 08 §9. UC-09 = investigar candidato e registrar jornada comercial; UC-04 mantém autoridade do cadastro/cotação. RF-028 básico é apresentação, RF-035/036/037/038 têm donos distintos; fase 6 só ampliação.

| CP | RF / prioridade | RN | RT | Componente | Entidade | UC | Teste | Fase |
|---|---|---|---|---|---|---|---|---|
| CP-09 | RF-035 / Should | RN-005/026/028/033/034 | RT-017 | Coleta/identidade | ingestion_jobs; pncp_responses; public_events; event_links; historical_prices | UC-09 | TST-056–060/064/065/069 | 2-H |
| CP-09 | RF-036 / Should | RN-005/015/024/026/028/035 | RT-018 | Comparabilidade | historical_comparisons; historical_prices; system_settings | UC-09 | TST-061–063/065 | 2-H |
| CP-09 | RF-037 / Should | RN-026/027/033–037 | RT-018 | Candidatos | suppliers; supplier_candidates; historical_comparisons | UC-09 | TST-059/063/066/068/071 | 2-H |
| CP-04 | RF-038 / Should | RN-006/024/026/030/031/033/036/037 | RT-019 | Relacionamento | supplier_candidates; candidate_interactions; supplier_quotes; evidences | UC-09/04 | TST-059/066/067/071 | 3 |
| CP-01 | RF-003 / Must estendido | RN-026/028/033/034 | RT-001/017 | Identidade/reconciliação | procurements; suppliers; public_events; event_links | UC-01/09 | TST-003/056/057/059/060 | 1; histórico 2-H |
| CP-01 | RF-004 / Must estendido | RN-003/028 | RT-001/017 | Atualização/invalidação | public_events; historical_comparisons; analysis_invalidations | UC-01/09 | TST-004/065/069 | 1; histórico 2-H |
| CP-01 | RF-005 / Must estendido | RN-026–029/036 | RT-001/017 | Consulta/falha externa | ingestion_jobs; procurement_versions; supplier_quotes | UC-01/07/09 | TST-005/068/069 | 1; histórico 2-H |
| CP-04 | RF-013 / Must estendido | RN-006/014/026/033/037 | RT-004/019 | Cadastro independente | suppliers; evidences | UC-04/09 | TST-013/059/060 | 3 |
| CP-04 | RF-015 / Must estendido | RN-006/014/026/037 | RT-004/019 | Cotação por escopo | supplier_products; supplier_quotes; candidate_interactions | UC-04/09 | TST-015/067 | 3 |
| CP-06 | RF-020/021 / Must preservado com caso adicional | RN-005/006/019/020/036 | RT-006 | Finanças | financial_scenarios; supplier_quotes; historical_prices opcional | UC-06 | TST-020/021/068 | 4 |
| CP-08 | RF-025/026/027 / Must preservado com caso adicional | RN-027–029/036 | RT-008 | Decisão | opportunity_analyses; opportunity_scores; system_settings | UC-07 | TST-025–027/054/068 | 4 |

RN-034 também cobre RF-035/028 sem somar etapas; RN-035 cobre suficiência/outliers/comparabilidade; RN-036 impede penalidade/bloqueio por ausência de histórico; RN-037 separa dimensões/escopo. RF-012/013 sem ciclo: cadastro básico independente; RF-028 básico não exige RF-017/027. RF-035 requer fundação e contrato; RF-036 usa item descrito (inclusive manual evidenciado), RF-037 não exige agregado suficiente, RF-038 contato precede cotação. Nenhuma aresta reversa cadastro→histórico é criada.

### RNF estendidos → RT e testes adicionais

| RNF | RT | Teste / aceitação em 14 e perfil em 05 |
|---|---|---|
| RNF-002 | RT-012/019 | TST-071/036; zero sentinela sensível em logs |
| RNF-005 | RT-017/018/019 | TST-056/065/071/039; evidências/versões/escopos completos |
| RNF-007 | RT-001/017 | TST-069/041; teto de tentativas e Retry-After |
| RNF-008 | RT-017 | TST-056/057/059/065/069/042; replay e etapas |
| RNF-009 | RT-016/018 | TST-043 no R1-P1; p95 proposto, não medido |
| RNF-010 | RT-006/009/017 | TST-060/062/064/044; precisão e natureza |
| RNF-011 | RT-015/017 | TST-045/069; contrato versionado antes de ativar |
| RNF-012 | RT-015 | TST-070/071/046; isolamento antes de escrita |
| RNF-013 | RT-013/017 | TST-063/069/071/047; contagens e qualidade |
| RNF-016 | RT-008/009/018/019 | TST-063/066/068; limites de inferência |
| RNF-018 | RT-001/017 | TST-065/069/052; atualidade/checkpoint |
| RNF-019 | RT-011/015 | TST-070/071/053; reprodução em container |
| RNF-020 | RT-008/018/019 | TST-071/054; políticas/ranking reproduzíveis |
| RNF-021 | RT-016/017 | TST-069/055; saturação sem perda |

Demais RNF conservam vínculos da seção 3 e controles transversais; RNF-014/017 têm extensão de fixtures de restore/preservação em TST-048/051. Nenhuma aprovação operacional por validação documental.

MN-11–18: CP-09/04 → RF-028/035–038 → RT-017–019 → eventos/comparações/candidatos/interações/cotações e ledger de pesquisa → UC-09 → TST-063/066/067/071 → 2-H/3. Definições normativas em 23 §8, DAN as referencia sem duplicar denominadores. D15 sintetiza a jornada; ADR-018/019/020 e D01–09/11/12 atualizados conforme checklist do índice de diagramas.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: linhas adicionais de Should, extensões Must, RNF→RT/testes e métricas; RF-028 repartido por responsabilidade/fase sem duplicação.

