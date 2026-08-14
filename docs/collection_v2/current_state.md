# SILLO Collection Platform V2 - Estado atual

Data do inventario: 2026-08-12

Este documento registra o estado do checkout antes da modernizacao da coleta.
Ele e a referencia da Fase 0 e separa fatos observados no repositorio de
validacoes que dependem do ambiente Docker.

## Baseline e escopo do inventario

- Checkout Git: `C:\Users\luisg\revenda assistida\motor_decisao`.
- Branch ativa: `main`.
- Commit: `d3cb7c17cf83c305a6615288263b8ea6c1092143`.
- Marcador preservado: `baseline/heuristic-engine-20260601`.
- Estado inicial: arvore limpa e `main` um commit a frente de `origin/main`.
- O diretorio pai `revenda assistida` nao e um repositorio Git.
- O inventario cobriu os 117 arquivos rastreados do checkout, os entrypoints,
  migrations, configuracoes, dependencias e documentacao operacional.
- A mudanca desta missao fica restrita a coleta, validacao e persistencia
  Bronze. Silver, Gold, heuristica, ML e Power BI so podem receber ajustes
  estritamente necessarios para aplicar corretamente o teto de candidato.

## Arquitetura atual

```text
CLI / scripts / FastAPI / daemon local
                 |
                 v
       pipelines especificos por fonte
          |          |          |
          v          v          v
     API HTTP    HTTP/BS4    Playwright
          |          |          |
          +----------+----------+
                     |
         control.pipeline_runs
          control.source_runs
     control.data_quality_checks
                     |
                     v
                  Bronze
                     |
              Silver e Gold
```

Nao existe atualmente um `Collector Contract`, um registro central tipado de
fontes nem uma CLI unica. A orquestracao e dividida entre subprocessos,
threads, um daemon com `sleep` e um endpoint FastAPI.

## Stack e infraestrutura existentes

| Componente | Estado atual | Decisao V2 |
| --- | --- | --- |
| PostgreSQL 15 + pgvector | `postgres` no Compose, volume `motor_postgres_data` | Preservar como banco central SILLO |
| Redis 7 | `redis` no Compose, persistencia AOF | Preservar e reutilizar para Prefect; nao criar Redis por PC |
| FastAPI | servico `api`, porta local 8010 | Preservar; retirar dele a responsabilidade de orquestrar threads de coleta gradualmente |
| Alembic | revisions `20260430_0001` a `20260526_0007` | Preservar e evoluir incrementalmente |
| Playwright sincrono | helper local e dois grupos de scraper | Migrar os scrapers web para Crawlee |
| Selenium Grid | container, helper e script de validacao | Legado; nenhum coletor ativo o usa. Retirar da rota padrao depois da validacao Crawlee |
| httpx | Mercado Livre, comparadores, fornecedor e OAuth | Preservar para APIs estruturadas; scrapers HTTP passam a usar Crawlee quando aplicavel |
| BeautifulSoup | parser generico de fornecedor | Separar parser e reutilizar fixtures; fetch passa a Crawlee |
| structlog | dependencia declarada, sem padronizacao efetiva | Ativar logging estruturado na plataforma V2 |
| Docker Compose | `postgres`, `redis`, `selenium`, `api`, `daemon` | Evoluir com profiles de control node e worker |
| Prefect | ausente | Adicionar self-hosted apenas para coleta |
| Crawlee | ausente | Adicionar como base dos coletores web |

O Docker Desktop estava desligado durante o inventario. Portanto nao foi
possivel confirmar o schema vivo, a revision Alembic aplicada nem as contagens
atuais do banco. A documentacao historica aponta `20260526_0007`, mas isso deve
ser revalidado quando o daemon Docker estiver disponivel.

## Schemas e tabelas declarados pelas migrations

### `control`

- `control.pipeline_runs`: run de alto nivel, status, config, metadata e erro.
- `control.source_runs`: run por fonte, contadores, metadata e erro.
- `control.data_quality_checks`: checks e metricas simples.

As tabelas ja representam parte importante do contrato desejado e serao
evoluidas; nao devem ser duplicadas.

### `bronze`

- `bronze.mercado_livre_items_raw`.
- `bronze.mercado_livre_products_raw`.
- `bronze.market_web_listings_raw`.
- `bronze.price_history_raw`.
- `bronze.supplier_products_raw`.

As tabelas preservam payload, hash, data/hora e `source_run_id`. A idempotencia
atual e por constraints diarias, por exemplo fonte + identidade/hash +
`fetched_date`.

### `silver`

- `silver.mercado_livre_product_prices`.
- `silver.supplier_products_normalized`.

### `gold`

- `gold.decision_opportunities`.
- `gold.decision_engine_runs`.
- `gold.decision_opportunity_snapshots`.

## Fontes e integracoes atuais

| Fonte | Implementacao atual | Acesso/estrategia | Estado no config ativo | Destino |
| --- | --- | --- | --- | --- |
| Mercado Livre | `MercadoLivreClient` e tres scripts especializados | API JSON oficial, OAuth opcional | habilitada | Bronze de itens/produtos e Silver de resumo de precos |
| Amazon | `PlaywrightMarketSource` generico | browser Playwright | habilitada | `bronze.market_web_listings_raw` |
| Kabum | `PlaywrightMarketSource` generico | browser Playwright | habilitada | `bronze.market_web_listings_raw` |
| Terabyte | `PlaywrightMarketSource` generico | browser Playwright | habilitada | `bronze.market_web_listings_raw` |
| Shopee | `PlaywrightMarketSource` com enriquecimento | browser Playwright | desabilitada | `bronze.market_web_listings_raw` |
| Magalu | `PlaywrightMarketSource` generico | browser Playwright | desabilitada | `bronze.market_web_listings_raw` |
| AliExpress | `PlaywrightMarketSource` generico | browser Playwright | desabilitada/experimental | `bronze.market_web_listings_raw` |
| Pichau | `PlaywrightMarketSource` generico | browser Playwright | desabilitada | `bronze.market_web_listings_raw` |
| Buscape | dois scrapers | HTTP/Next.js e Playwright de detalhe | habilitada | `bronze.price_history_raw` |
| Zoom | dois scrapers | HTTP/Next.js e Playwright de detalhe | habilitada | `bronze.price_history_raw` |
| Mirao | `GenericHtmlSupplierScraper` | HTTP + BeautifulSoup, paginacao por URL | habilitada | `bronze.supplier_products_raw` |
| MegaMix | extracao de PDF e importacao JSON manual | arquivo estruturado local | regra de portfolio habilitada | `bronze.supplier_products_raw` |
| Coletek | extracao/importacao de catalogo manual | JSON local | regra de portfolio habilitada | `bronze.supplier_products_raw` |

Nao ha implementacoes atuais de Fujioka, Agis, Bluevix, Mega Market, Grupo TEK,
Martins, Allied, TD SYNNEX, Band, OMNI, Sol, FCInfo, Netshop, Cia da
Informatica ou GamerStar. Essas fontes pertencem a Fase 9 e exigem
reconnaissance antes de qualquer coletor.

## Scrapers e browsers encontrados

### Browser ativo

- `pipelines/market_web/sources.py`: uma classe Playwright sincrona com regras
  de sete fontes no mesmo arquivo, parsing parcialmente acoplado ao browser,
  retries e `time.sleep` locais.
- `pipelines/price_history/comparison_web_scraper.py`: Playwright sincrono para
  busca e paginas de detalhe de Buscape/Zoom, com parsing e navegacao na mesma
  classe.
- `pipelines/common/playwright_browser.py`: cria um browser Chromium novo por
  contexto/coleta.

### HTTP scraping ativo

- `pipelines/suppliers/generic_html.py`: `httpx` + BeautifulSoup para Mirao,
  retries, paginacao e parsing na mesma classe.
- `pipelines/price_history/comparison_scraper.py`: `httpx` e parsing de
  `__NEXT_DATA__` de Buscape/Zoom.

### Selenium

- `pipelines/common/selenium_browser.py` cria sessao remota.
- `scripts/validate_selenium.py` valida o Grid.
- O Compose inicia `selenium/standalone-chrome`.
- Nenhum pipeline de coleta importa `remote_chrome`; Selenium e infraestrutura
  legada, nao uma dependencia funcional da coleta ativa.

## Entry points e orquestracao atuais

- `scripts/collect_all.py`: dispara ML, web e fornecedores com threads e
  subprocessos; depois tambem executa ETL, motor e Power BI.
- `app/api/operations.py`: duplica orquestracao de Bronze em `ThreadPoolExecutor`
  e thread de background no processo FastAPI.
- `scripts/daemon.py`: loop de quatro horas baseado em `time.sleep`.
- `scripts/daemon.ps1`: integra o daemon ao Task Scheduler do Windows.
- `scripts/collect_market_web.py`: CLI especifica de market web.
- `scripts/collect_price_comparison.py`: CLI especifica de comparador HTTP.
- `scripts/collect_price_history_web.py`: CLI especifica de comparador browser.
- `scripts/collect_supplier.py` e `collect_suppliers.py`: CLIs sobrepostas.
- `scripts/collect_mercado_livre*.py`: tres caminhos parcialmente duplicados
  para a mesma API.
- `scripts/collect_status.py`: consulta operacional por janela, sem consulta
  dedicada por `run_id`.
- `ColetaMotor.cmd` e `ColetaMotorHTTP.cmd`: atalhos Windows a preservar como
  wrappers de compatibilidade.

## Concorrencia, retries e rate limit atuais

- Concorrencia em tres lugares: `collect_all.py`, `app/api/operations.py` e
  `pipelines/market_web/ingest.py`/scripts especificos.
- Browser atual pode abrir um Chromium por fonte e por tarefa, sem limite
  global de RAM alem do numero de threads configurado.
- Mercado Livre possui retry simples e backoff exponencial sem jitter.
- Mirao distingue status retryable e terminal, mas usa `time.sleep` local.
- Market web e price history repetem retries de bloqueio e timeout.
- Os limites estao espalhados entre YAML e defaults Python.
- Nao ha classificacao comum de erro, Request Queue, Session Pool ou metrica
  uniforme de requests HTTP/browser.

## Limites de preco localizados

Foram encontrados dois tetos efetivos de `1500.00`, ambos aplicados a
evidencias de mercado:

1. `market_sources.market_web.max_price` em `config/config.yaml` e
   `config/config.example.yaml`, consumido por
   `pipelines/market_web/ingest.py` com fallback Python `1500.0`.
2. `market_sources.price_history.max_price` nos mesmos YAMLs, consumido por
   `pipelines/price_history/ingest.py` com fallback Python `1500.0`.

O minimo `100.00` tambem e aplicado nesses dois caminhos.

Nao existe hoje teto de compra em `supplier_price`. O motor seleciona todos os
produtos normalizados com `supplier_price > 0`. A evidencia do Mercado Livre
tambem nao recebe esse teto.

### Correcao semantica planejada

- Criar uma unica fonte de verdade: `Settings.max_product_price_brl`, com
  alias `SILLO_MAX_PRODUCT_PRICE_BRL` e default `3000.00`.
- Aplicar o teto a elegibilidade de produto fornecedor/candidato de compra.
- Preservar no Bronze e no conjunto de evidencias precos de mercado acima de
  R$ 3.000 quando uteis para comparacao.
- Remover o uso do teto de compra como filtro destrutivo de market web e price
  history. Validacoes de preco ausente/invalido permanecem.
- Testar `2999.99` e `3000.00` como elegiveis e `3000.01` como nao elegivel.

## Acoplamentos, duplicacoes e legado

- O registro de fontes esta dividido entre `SOURCE_CONFIGS`, YAMLs e listas
  default de scripts.
- Modelos de snapshot usam `dataclass`/`Protocol` independentes e nao formam um
  contrato unico.
- Fetch, parse, normalize, validate e persist estao misturados em varios
  coletores.
- Persistencia e lifecycle de run se repetem em cada ingest e nos imports
  manuais.
- `app/api/operations.py` importa constantes de scripts, invertendo a camada de
  dependencia.
- `collect_all.py` mistura coleta com ETL, heuristica e Power BI, fora do escopo
  Prefect desta etapa.
- `src/motor_decisao` e `tests` contem somente caches ignorados do checkout
  deslocado; o codigo rastreado ativo esta em `app/`, `pipelines/` e `scripts/`.
- Redis e declarado e iniciado, mas nao participa da coordenacao de coleta.
- `structlog` e dependencia declarada, mas a operacao usa principalmente
  `print()` e logs ad hoc.

## Componentes preservados

- Schemas Bronze/Silver/Gold e dados historicos.
- `control.pipeline_runs`, `control.source_runs` e
  `control.data_quality_checks`, evoluidos por migration.
- API oficial do Mercado Livre e seu OAuth.
- PostgreSQL, Redis, FastAPI, Alembic e Docker Compose.
- Parsers e regras de extracao que passarem nos contract tests.
- Payloads, hashes e constraints de idempotencia existentes.
- Scripts `.cmd`/PowerShell como wrappers de compatibilidade.
- Heuristica, ML e Power BI fora da orquestracao Prefect desta fase.

## Componentes a migrar ou substituir gradualmente

- Playwright direto de market web -> Crawlee, preferindo AdaptivePlaywright e
  usando Playwright somente quando necessario.
- Playwright direto de Buscape/Zoom -> Crawlee com parser isolado.
- HTTP/BeautifulSoup de Mirao -> Crawlee Parsel/HTTP com parser isolado.
- Registro estatico/espalhado -> Source Registry YAML tipado.
- Daemon, threads e subprocessos de coleta -> flows/deployments Prefect.
- CLIs especificas -> `sillo collect ...`, mantendo wrappers antigos durante a
  transicao.
- Selenium Grid -> removido do caminho padrao apos paridade dos scrapers; os
  arquivos legados so serao apagados depois da validacao.

## Riscos

| Risco | Tratamento |
| --- | --- |
| Mudanca de DOM e anti-bot em marketplaces | HTTP first, fixtures, canary, falha isolada e sem bypass |
| Excesso de browsers em PCs de 16 GB | limites por worker, pool browser separado e concorrencia Crawlee conservadora |
| Duplicacao por retry/worker | manter identidade/hash e transacoes; um source run por execucao |
| Dupla orquestracao Prefect + threads internas | remover fan-out legado do caminho V2 e impor orcamento por collector |
| Perda de evidencia por filtro de preco | separar elegibilidade de compra de armazenamento de mercado |
| PC2 indisponivel | PC1 mantem workers; PC2 nao hospeda estado obrigatorio |
| PC1 indisponivel | single point of failure documentado; sem HA nesta fase |
| Banco central exposto na LAN | bind e firewall explicitos, credenciais via ENV, sem segredos no Git |
| Prefect/Redis duplicados | reutilizar o Redis existente e usar Postgres Prefect separado logicamente do SILLO |
| Regressao de parsers | fixtures offline e comparacao legado/V2 antes de desativar legado |
| Ambiente local incompleto | testes unitarios em ambiente isolado; integracao marcada e executada quando Docker estiver ativo |

## Sequencia incremental aprovada pelo estado real

1. Fase 1: criar `pipelines/collection` com modelos Pydantic v2, contrato
   assincrono, contexto, resultado, erros e Source Registry. Manter os ingests
   atuais intactos.
2. Fase 2: criar YAMLs por fonte, centralizar limites operacionais e teto de
   candidato em Settings, adicionar testes de fronteira e corrigir o filtro
   sem descartar evidencia de mercado.
3. Fase 3: adicionar Crawlee e adapters HTTP/Parsel/Adaptive/Playwright com
   limites, timeouts, requests, logging e bloqueio seletivo de recursos.
4. Fase 4: migrar primeiro Mirao (HTTP simples), depois market web e por ultimo
   detalhes Buscape/Zoom. Mercado Livre permanece API. Validar legado e V2 lado
   a lado antes de remover qualquer implementacao.
5. Fase 5: adicionar flows `collect_source`, `collect_profile` e `collect_all`
   somente para Collection -> Validation -> Bronze.
6. Fase 6: adicionar Compose/control node no PC1 e workers configuraveis no
   PC1/PC2, todos apontando para Prefect e SILLO PostgreSQL centrais.
7. Fase 7: criar deployments/schedules individuais com stagger, inicialmente
   pausados ou ativados apenas apos canary.
8. Fase 8: evoluir migrations de runs/alertas, metricas, logs estruturados,
   canary e degradacao por volume.
9. Fase 9: executar reconnaissance das novas fontes, classificar acesso e
   implementar apenas fontes publicas viaveis em lotes pequenos.
10. Fase 10: preparar interface autenticada sem implementar login ou armazenar
    credenciais.
11. Fase 11: benchmark por worker e estrategia; ajustar concorrencia sem criar
    scheduler inteligente.

## Criterios de validacao por fase

- Unit tests e parser contract tests offline.
- Teste de configuracao e Source Registry sem banco.
- Testes de persistencia e migrations com PostgreSQL.
- Comparacao de campos, contagem, tempo e erros entre legado e Crawlee.
- `sillo collect run --source kabum`, `--profile market` e `--profile full`.
- Status geral e por `run_id`.
- Falha de uma fonte sem interromper as demais.
- Dois workers registrando no mesmo banco; PC2 offline sem parar PC1.
- Fronteiras de preco `2999.99`, `3000.00` e `3000.01`.

## Pendencias de validacao externa

- Docker Desktop precisa estar ativo para testar Compose, migrations e banco.
- A instalacao Python global nao possui `pytest`, Crawlee ou Prefect; a
  validacao sera feita em ambiente isolado/reprodutivel e no container.
- As fontes novas precisam de reconnaissance atual, sem assumir preco publico
  ou viabilidade.
- Testes reais em dois PCs exigem o segundo computador e a configuracao de rede
  local; o repositorio pode fornecer scripts, Compose e runbook, mas nao simular
  hardware ausente como validado.
