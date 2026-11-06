# Contrato da demonstração por item

Versão 0.3 congelada em 15/10/2026. Estado CONGELADO PARA IMPLEMENTAÇÃO. Este arquivo define contratos de dados e propriedade de arquivos; não declara endpoints novos implementados.

## Base existente

Usar /api/v1. Preservar GET /health, GET /dashboard/summary, GET /opportunities, GET /opportunities/{id}, PATCH /opportunities/{id}/status, GET /suppliers, POST /suppliers, POST /suppliers/{id}/products, POST /supplier-products/{id}/quotes, POST /items/{item_id}/financial-scenarios e POST /opportunities/{id}/rank. Preservar as rotas restantes.

GET /opportunities retorna data, page, page_size, total e total_pages. page_size válido de 10 a 100. Decimais em respostas são strings. null significa indisponível; 0 é um valor conhecido. Taxas são frações.

## Acréscimos congelados

1. QuoteOut expõe procurement_item_id e os dados necessários à seleção: fornecedor/produto identificados, valor, data, validade e frete conhecido. O vínculo já existe no modelo de cotação.
2. FinancialScenarioCreate e FinancialScenarioOut incluem supplier_quote_id opcional; o modelo FinancialScenario persiste o vínculo com FK por migração aditiva. Ausência permite simular, porém mantém análise pendente. Verificar item, produto, cotação, quantidade e validade antes de promover análise. Aquisição deve usar o preço unitário normalizado da cotação selecionada; guardar snapshot do preço e condições. Frete requer evidência para o destino, quantidade e prazo do caso. Override divergente sem confirmação rastreável fica pending.
3. ScoreOut expõe item_id, eligibility com valores viable/pending/blocked e pending_checks como lista de texto. Manter score, eligible, provisional, components, blockers, explanation e calculated_at existentes.
4. Detalhe da contratação oferece análise identificada por item, por exemplo item_analyses como lista de ScoreOut. latest_score pode permanecer por compatibilidade, mas nunca representar todos os itens. A UI usa a análise do item selecionado.
5. FinancialScenarioOut devolve capital efetivamente usado e identificação do cenário, além de assumptions_complete e sensitivity já existentes. Documentar de onde veio o capital.
6. Origem e ambiente de demonstração ficam visíveis na UI e disponíveis em metadados aditivos. S01 define um único formato simples compartilhado, sem inserir dados de produto em JSON oficial bruto.

Adicionar campos opcionais e migrações aditivas; não apagar tabelas, registros ou campos antigos. A revisão 0001 usa Base.metadata.create_all com modelos atuais: a revisão nova deve inspecionar estruturas, adicionar só o ausente e validar tipo/FK das existentes. Testar upgrade de cópia do schema antigo e upgrade head de SQLite vazio. Não reescrever a 0001 silenciosamente. Se a proposta exigir redução para caber no prazo, congelar a alternativa antes da implementação e conservar análise pendente onde faltar rastreabilidade.

## Elegibilidade e estado comercial

blocked se há impeditivo conhecido; pending se faltam dados críticos e não existe impeditivo conhecido; viable somente com evidência e premissas suficientes. Impeditivo prevalece sobre pendência. eligible somente true para viable. provisional true se há incompletude; pode coexistir com blocked. Score numérico somente para viable; nos outros casos score=null, com componentes e motivos consultáveis.

Produto possibly_compatible ou insufficient_information não satisfaz compatibilidade confirmada. Validade de cotação ausente não prova vigência. Logística D é pendência; local obrigatório sem solução suportada é bloqueio. Frete e tributos não confirmados mantêm pendência. Lote/grupo não pode ser recomendado pelo item isolado.

Status comercial usa AnalysisStatus existente, como new, in_analysis, awaiting_quote e discarded, com mudança manual via PATCH. Não transformar uma alteração manual para viable em elegibilidade calculada nem apresentar estados de resultado comercial como fatos sem registro.

## Capital e vigência

Na criação do cenário: capital explícito do payload, senão configuração global, senão pendência. Persistir o valor efetivo. Análises salvas do mesmo cenário usam o snapshot persistido. Na busca por orçamento, reavaliar capital com o valor explícito da requisição, sem gravar essa substituição no cenário ou nas configurações; o detalhe conserva o contexto da busca. Mudança persistente de premissa requer novo cálculo identificado. Margem mínima e pesos também precisam de origem explícita no resultado.

Selecionar a versão mais recente por item com desempate determinístico por data e id. Se item, cotação, validade ou premissas mudarem, invalidar a conclusão anterior e recalcular ou retornar pending. Nunca manter indicação positiva só porque houve um score positivo histórico.

Abertura exige status de fonte compatível e prazo futuro conhecido, conforme semântica atual verificada. Prazo ausente não confirma abertura. Instantes persistidos devem ser comparados após normalização documentada; não assumir que toda data PNCP sem offset é UTC sem verificar o contrato.

## Exemplo conceitual da análise

~~~json
{
  "item_id": 101,
  "eligibility": "pending",
  "eligible": false,
  "provisional": true,
  "score": null,
  "pending_checks": ["Cotação atual não selecionada"],
  "blockers": [],
  "explanation": "Simulação calculada; falta cotação atual para concluir a análise."
}
~~~

O exemplo é proposto e sintético. S01 registra os tipos finais, exemplos dos três estados, migrações e propriedade dos arquivos. S03 S04 S05 e S06 não alteram o contrato unilateralmente.

## Tipos finais e exemplos de elegibilidade

`eligibility` é um enum textual com `viable`, `pending` e `blocked`. `eligible` é verdadeiro somente para `viable`; `score` é `null` nos demais estados. `pending_checks` e `blockers` são listas de textos estáveis para a UI. Todos os valores monetários e taxas são transportados como strings decimais; ausência é `null`.

Exemplos sintéticos congelados:

~~~json
{
  "item_id": 101,
  "eligibility": "viable",
  "eligible": true,
  "provisional": false,
  "score": "0.82",
  "pending_checks": [],
  "blockers": [],
  "capital_used": "1500.00",
  "capital_source": "request",
  "explanation": "Cotação atual, frete, tributos e premissas confirmados para o item."
}
~~~

~~~json
{
  "item_id": 102,
  "eligibility": "pending",
  "eligible": false,
  "provisional": true,
  "score": null,
  "pending_checks": ["Cotação atual não selecionada", "Frete para o destino não confirmado"],
  "blockers": [],
  "capital_used": null,
  "capital_source": null,
  "explanation": "Há simulação, mas faltam evidências críticas para concluir a análise."
}
~~~

~~~json
{
  "item_id": 103,
  "eligibility": "blocked",
  "eligible": false,
  "provisional": false,
  "score": null,
  "pending_checks": [],
  "blockers": ["Capital disponível inferior ao capital necessário"],
  "capital_used": "1500.00",
  "capital_source": "request",
  "explanation": "O item exige R$ 1.500,00 e o orçamento aplicado foi R$ 500,00."
}
~~~

## Migração e compatibilidade congeladas

A implementação deve ser aditiva e idempotente. Antes de cada alteração, inspecionar tabelas e colunas existentes; adicionar somente estruturas ausentes; validar os tipos/FKs que já existirem; não apagar dados, reescrever a revisão 0001 ou usar `create_all` para esconder divergências. O aceite exige dois ensaios isolados: (a) cópia do schema antigo atualizada sem duplicar colunas e (b) SQLite vazio atualizado até o head. O banco original `backend/radar_licita.db` não é destino de migração ou carga da demonstração.

## Propriedade dos arquivos e integração

S03 é proprietária da fundação compartilhada de persistência e migrações em `backend/app/models.py`, `backend/app/schemas.py` e `backend/alembic/`; S04 é proprietária de `backend/app/recommendations/` e dos testes de orçamento; S05 é proprietária de `backend/app/partners/`, adapter/cache e seus testes; S06 é proprietária da jornada em `frontend/src/`; S07 é a única etapa que conecta routers em `main.py`/`routes.py` compartilhados e atualiza os testes de integração. S01 congela o contrato e não implementa essas rotas.

Metadados de origem seguem o formato aditivo `source: {"kind": "official|synthetic|manual", "label": "...", "observed_at": "...", "reference": "..."}`. Dados oficiais mantêm texto, datas e URLs originais; sintéticos carregam `kind=synthetic` visível. Nenhum JSON oficial bruto recebe campos de produto.

## Contratos obrigatórios de orçamento e parceiros

ESCOPO_ORCAMENTO_E_PARCEIROS.md define as rotas e schemas adicionais: POST /recommendations/search, POST /items/{item_id}/partner-searches, GET /items/{item_id}/partner-candidates e POST /partner-candidates/{candidate_id}/promote, sob /api/v1. Congelar em S01 antes da implementação.

S03 cria a fundação compartilhada de persistência para observações de resultados, candidatos e registros de busca, com chaves de evento/empresa, origem, versões e migração aditiva. Campos propostos em ESCOPO_ORCAMENTO_E_PARCEIROS.md. Orçamento é contexto de leitura por requisição, sem necessidade de nova tabela por digitação.

S04 e S05 usam módulos e schemas próprios dentro de recommendations/ e partners/. Não alteram models.py, schemas.py, main.py ou routes.py compartilhados em paralelo. S07 conecta routers ao produto e atualiza os testes de integração. Manter uma única aplicação, um único banco de demo e o mesmo motor de elegibilidade.

Preço público histórico não preenche SupplierQuote nem acquisition_unit_cost. O cadastro da empresa descoberta não confirma parceria. O prazo exige recorte pequeno, mas mantém estes dois recursos como obrigatórios.
