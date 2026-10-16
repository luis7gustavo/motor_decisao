# 02 — Diagnóstico técnico AS-IS do SILLO

Versão: 0.1. Data da inspeção: 2026-10-10 (America/Sao_Paulo).
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: auditoria estática concluída; proposta de evolução sujeita à revisão do operador.

## 1. Linha de base do repositório

| Campo | Evidência |
|---|---|
| URL | https://github.com/luis7gustavo/motor_decisao |
| Remote | `origin`, mesma URL com `.git` |
| Caminho inspecionado | `<checkout-local-motor_decisao>` |
| Branch de origem / de trabalho | `main` / `docs/radar-licita-especificacao` |
| Commit | `261b32107ff7b053c68c1df98e88de329c41c0f5` |
| Working tree inicial | Limpo, sem saída em `git status --short` |
| Clone realizado | Não; repositório já existente, sem pull ou atualização |
| Escopo de escrita | Exclusivamente `docs/licitacoes/` |

O Git no diretório pai tem arquivos não rastreados preexistentes, inclusive um projeto separado. Não faz parte da linha de base. A auditoria usa apenas este checkout. Não havia `docs/licitacoes/`; nenhum AGENTS.md foi encontrado no repositório/ancestrais inspecionados.

**Leitura dos rótulos:** [CONFIRMADO] significa implementação/declaração encontrada no código; não comprova serviço saudável, migração aplicada ou dados atuais. [NÃO-CONFIRMADO] significa alegação documental sem comprovação atual. [PARCIAL] indica implementação insuficiente para a capacidade completa. [PROPOSTO] identifica o Radar Licita. [LEGADO] identifica candidato a sair do fluxo principal, preservado nesta missão.

## 2. Resultado da inspeção

O SILLO tem uma base real de coleta comercial, armazenamento em camadas, ranking de compra para revenda e revisão humana. O novo domínio público exige novas entidades, evidências documentais, critérios de julgamento, prazos, habilitação, logística e finanças por unidade de julgamento. Não basta renomear a recomendação `comprar_teste` para participação em licitação.

| Capacidade | Status AS-IS | Implicação para Radar Licita |
|---|---|---|
| API FastAPI, UI Jinja2 e revisão transacional | [CONFIRMADO] | Adaptar fluxos e conteúdo; preservar o padrão de auditoria |
| PostgreSQL, SQLAlchemy e migrations Alembic | [CONFIRMADO] no código | Reutilizar infraestrutura; estado físico do DB não inspecionado |
| Bronze/Silver/Gold, runs e snapshots | [CONFIRMADO] | Adaptar granularidade para contratação/lote/item e versões oficiais |
| Prefect, Crawlee, registry de fontes | [CONFIRMADO] | Reaproveitar com adaptadores e contratos públicos próprios |
| ML | [PARCIAL] | Tabelas/view e leitura na revisão existem; treino/inferência não encontrados |
| Power BI | [PARCIAL] | Exportador CSV e especificação existem; `.pbix`/`.pbit` não encontrados |
| PNCP, documentos oficiais e estados licitatórios | [PROPOSTO] | Não encontrados em app/pipelines/config/templates/tests |
| Docker operacional neste ambiente | [NÃO-CONFIRMADO] | Compose validado estaticamente, sem executar serviços |

As contagens de dados e validações de maio/agosto no README são snapshots históricos [NÃO-CONFIRMADO] para a data desta inspeção. Não foram usados como volume atual ou benchmark.

## 3. Estrutura, tecnologias e entrypoints

| Área | Responsabilidade, entradas → saídas | Dependências e integração |
|---|---|---|
| `app/main.py` | Routers → aplicação HTTP, HTML e `/static` | FastAPI; cinco routers incluídos nas linhas 23–27 |
| `app/api/`, `schemas/`, `services/`, `repositories/`, `models/` | Requisição → validação, SQL, resposta/revisão | Pydantic, SQLAlchemy; padrão reutilizável com domínio novo |
| `pipelines/collection/` | Registry/configuração → coleta tipada, persistência e métricas | Prefect, Crawlee, parsers, clientes HTTP e browsers |
| `pipelines/mercado_livre`, `market_web`, `price_history`, `suppliers` | APIs/HTML/catálogos → Bronze e resumos comerciais | Integrações comerciais; parte ainda usada pelas rotas legadas |
| `pipelines/decision_engine/build.py` | Fornecedores e evidências → Silver, Gold, snapshots | SQL textual, heurísticas e configuração de revenda |
| `pipelines/common/` | Execuções/configuração → auditoria, serialização e browsers | Compartilhado com coletas antigas e atuais |
| `alembic/versions/` | Definições versionadas → evolução proposta do schema | Dez migrations; nenhuma aplicada nesta missão |
| `templates/review/`, `static/` | Dados da revisão → fila/histórico HTML | Jinja2, JS e CSS sem frontend separado |
| `scripts/`, `sillo.py`, arquivos `.cmd` | Operação local → coleta, setup, importação/exportação | Parte dos scripts produz efeitos no banco/serviços e notificações |
| `config/`, `config/sources/` | Configuração → seleção e política de fontes | YAML atual e configuração legada coexistem |
| `data/`, `data_processed/` | Catálogos e CSVs históricos | Não representam por si só o estado do PostgreSQL |
| `ml/`, `features/`, `entity_resolution/`, `optimizer/`, `pipelines/silver/`, `pipelines/gold/`, `dashboard/` | Esqueleto de pacotes | Não tomar diretório ou docstring como funcionalidade implementada |
| `tests/`, `docs/` | Fixtures/testes e descrição histórica | Testes lidos; documentação cruzada com implementação |

[CONFIRMADO] `pyproject.toml` declara Python >=3.10, pacote `sillo-motor-decisao` 2.0.0 e entrypoint `sillo = sillo:main`. A imagem usa Python 3.11. Dependências estão em `requirements.txt` e `requirements-control.txt`, com intervalos abertos, sem lock encontrado; `project.dependencies` vazio não é a relação efetiva de dependências. Stack inspecionada: FastAPI/Uvicorn, Pydantic Settings, SQLAlchemy/psycopg, Alembic, requests/httpx, BeautifulSoup, YAML, Jinja2, Playwright/Selenium, Crawlee e Prefect. Não houve instalação nem atualização de bibliotecas.

Entrypoints [CONFIRMADO]: `app.main:app`; `sillo.py` para CLI; `scripts/build_decision_engine.py`; `scripts/collect_*.py`; `scripts/import_megamix_catalog.py` e `import_coletek_catalog.py`; `scripts/export_power_bi.py`; scripts PowerShell de setup, migração, backup/importação e status; wrappers Windows SetupMotor/ColetaMotor/PararMotor/StatusMotor. São inventário, **não instruções para executá-los durante esta missão**. `/ops/bronze-cycle` também dispara coleta e notificação externa; não foi acionado.

## 4. Endpoints FastAPI

[CONFIRMADO] por decorators nos arquivos abaixo: 18 operações de negócio e um alias, totalizando 19 decorators. Rotas automáticas do FastAPI não foram testadas em runtime.

| Método | Caminho | Entrada → saída/efeito | Evidência |
|---|---|---|---|
| GET | `/health` | Consulta DB → status e ambiente | `app/api/health.py:10` |
| GET | `/ops/recent-runs` | Janela/limite → runs, volumes, erros | `app/api/operations.py:507` |
| POST | `/ops/repair-stale-runs` | Horas → correção de runs travadas | `app/api/operations.py:694` |
| POST | `/ops/cancel-pipeline-run` | UUID/motivo → cancelamento de registro | `app/api/operations.py:701` |
| POST | `/ops/bronze-cycle` | Modo síncrono/assíncrono → coleta legada | `app/api/operations.py:715` |
| POST | `/decision-engine/run` | Parâmetros/catálogos opcionais → nova rodada | `app/api/decision_engine.py:40` |
| GET | `/decision-engine/summary` | Consulta → totais, grupos, última rodada | `app/api/decision_engine.py:72` |
| GET | `/decision-engine/opportunities` | Filtros/limite → oportunidades Gold | `app/api/decision_engine.py:132` |
| GET | `/decision-engine/runs` | Limite → histórico de rodadas | `app/api/decision_engine.py:198` |
| GET | `/integracoes/mercado-livre/auth-url` | State opcional → URL OAuth | `app/api/mercado_livre_oauth.py:34` |
| GET | `/integracoes/mercado-livre/callback` | Código → troca de tokens/metadados | `app/api/mercado_livre_oauth.py:44` |
| POST | `/integracoes/mercado-livre/refresh-token` | Credencial configurada → renovação | `app/api/mercado_livre_oauth.py:77` |
| GET | `/review`, alias `/review/` | Filtros → fila HTML | `app/api/review.py:163` |
| GET | `/review/history` | Filtros → histórico HTML | `app/api/review.py:170` |
| GET | `/review/summary` | Consulta → contadores JSON | `app/api/review.py:188` |
| GET | `/review/{opportunity_id}` | UUID → detalhe HTML | `app/api/review.py:193` |
| POST | `/review/{opportunity_id}/decision` | Formulário/CSRF → decisão e redirect | `app/api/review.py:202` |
| POST | `/review/{opportunity_id}/undo` | Formulário/CSRF → invalidação auditada | `app/api/review.py:253` |

`app/main.py:16–31` monta estáticos e deixa documentação padrão habilitada por configuração. Não há adapter ou rota PNCP. Routers não apresentam autenticação/autorização; a configuração é local e não deve ser considerada pronta para hospedagem pública. OAuth `state` é recebido/devolvido sem comparação com sessão no callback; o endpoint do motor aceita caminhos locais para importação. A API de operação pode retornar exceções/trechos de traceback. Esses comportamentos exigem revisão em evolução futura, sem alterações nesta etapa.

## 5. Pipelines, fontes e orquestração

| Componente | Entradas → processamento → saídas | Status e integração |
|---|---|---|
| Registry V2 | YAML → validação de IDs/perfis/ordem → fontes selecionadas | [CONFIRMADO], `registry.py:17–85`; adaptar contrato de fonte pública |
| Runner/contratos V2 | Fonte e limites → collector e persistência → estado, contagens e erros | [CONFIRMADO], `runner.py`, `contracts.py`; preservar idempotência e falhas parciais |
| Crawlee | URLs/config → filas internas, timeout, retry/backoff → registros e métricas | [CONFIRMADO], `crawlee/factory.py:64–75,133–148`; não é fila repartida entre PCs |
| Prefect | Registry → deployments/filas/workers → execução de flows | [CONFIRMADO], `prefect_flows.py`, `scripts/bootstrap_prefect.py`, `deploy_collection.py` |
| Mercado Livre | API OAuth → busca/produtos/preços → Bronze/Silver | [CONFIRMADO] no código; fonte desabilitada no registry atual |
| Web e histórico comercial | HTML → parser/preços → Bronze | [CONFIRMADO]; acesso externo atual não testado |
| Fornecedores B2B | HTML/catálogos → produtos brutos → normalização no motor | [CONFIRMADO]; sem gestão completa de cotações contratuais |
| Ciclo `/ops/bronze-cycle` | Configuração antiga → pipelines legados/thread → runs e possível Discord | [LEGADO], `operations.py:14–21,416–437,715–772`; consolidar futuramente com V2 |
| ETL rápido em CSV | Ofertas por query → gap de preço/proxies → `opportunities_quick` | [LEGADO] como ranking principal; não alimenta Silver/Gold do DB |

`config/sources/current.yaml` declara 28 fontes; somente Amazon, Kabum, Mirao, Grupo TEK e Cia da Informática estão habilitadas. Mercado Livre está desabilitado por configuração de OAuth; Terabyte, Buscapé, Zoom e GamerStar estão declarados bloqueados; outras fontes estão desabilitadas ou futuras. Isso não confirma disponibilidade externa hoje. Um `scheduling.enabled` interno não supera `enabled: false` da fonte.

Prefect é real, mas os decorators de `prefect_flows.py:10,25,39` não configuram retries explícitos. As retentativas encontradas ficam em Crawlee/clientes; a atribuição genérica de retries ao Prefect no README deve ser qualificada. `bootstrap_prefect.py:16–31` deriva limites de YAML, enquanto workers no Compose usam ENV, criando possível divergência de configuração. Não introduzir outro agendador para resolver isso sem decisão arquitetural.

## 6. Dados, ORM e migrations

Importações [CONFIRMADO]: `pipelines/suppliers/ingest.py:33,89` persiste produtos brutos com hash/fornecedor/dia e runs/checks; `generic_html.py:66` usa HTTP, seletores e paginação. O collector V2 reutiliza parser e persistência (`collectors/supplier.py:29,143`). `scripts/import_megamix_catalog.py:64,116` converte JSON de nome/código/barcode/modelo/preço/unidade; `import_coletek_catalog.py:81,138,177` trata referência/descrição/marca/NCM/estoque/descontinuação/data. Esses importadores não equivalem a uma interface CSV genérica com cotações versionadas.

`scripts/extract_megamix_pdf.py:106,158–163` é extrator específico de catálogo em duas colunas, com saída JSON/CSV; não extrai requisitos/evidências de edital. Sua dependência `pdfplumber` não está no requirements inspecionado. Reutilização limitada, com parser documental novo [PROPOSTO].

[CONFIRMADO] nas definições SQL: 17 tabelas e uma view. Sem consulta ao banco, não se afirma que esses objetos estejam instalados ou preenchidos. Prefixos abaixo são schemas PostgreSQL.

| Schema/objeto | Responsabilidade, origem e consumo | Migration |
|---|---|---|
| `control.pipeline_runs` | Execução geral/configuração/status → auditoria operacional | `20260430_0001` |
| `control.source_runs` | Execução por fonte/contagens/erros → observabilidade | `20260430_0001` |
| `control.data_quality_checks` | Métricas/checks → detecção de qualidade | `20260430_0001` |
| `control.collection_alerts` | Alertas de coleta → operação e investigação | `20260812_0008` |
| `bronze.mercado_livre_items_raw` | Anúncios e payloads → evidência comercial | `20260430_0001` |
| `bronze.price_history_raw` | Preços e payloads de comparadores → evidência comercial | `20260430_0001`, ampliada em `0005` |
| `bronze.supplier_products_raw` | Catálogo, SKU/EAN, preço/estoque bruto → normalização | `20260430_0001` |
| `bronze.mercado_livre_products_raw` | Produtos/API → resumo de preços | `20260430_0003` |
| `bronze.market_web_listings_raw` | Ofertas/posição/reviews → evidência do ranking | `20260501_0004` |
| `silver.mercado_livre_product_prices` | Agregados comerciais → motor | `20260430_0002` |
| `silver.supplier_products_normalized` | Título/tokens/chave, referência ao bruto → candidatos | `20260525_0006` |
| `gold.decision_opportunities` | Resultado atual por produto de fornecedor → API/UI/exportação | `20260525_0006` |
| `gold.decision_engine_runs` | Versão/configuração da rodada → reprodutibilidade | `20260526_0007` |
| `gold.decision_opportunity_snapshots` | Resultado histórico por rodada → auditoria/revisão | `20260526_0007` |
| `gold.ml_model_runs` | Metadados de treinamento → compatibilidade histórica | `20260601_0008` |
| `gold.ml_opportunity_scores` | Scores híbridos por modelo/produto → revisão | `20260601_0008` |
| `feedback.opportunity_reviews` | Decisão humana, snapshot e invalidação → histórico | `20260817_0009` |
| `gold.ml_opportunity_scores_latest` (view) | Scores da última run ML com sucesso → consulta | `20260601_0008` |

As migrations criam `pgcrypto` e `vector`; a existência da extensão declarada não demonstra uso de embeddings. Datas usam DATE/TIMESTAMPTZ; payloads usam JSONB; moedas são NUMERIC no banco. Há chaves/índices por fonte, data e payload e unicidades diárias no Bronze. Isso exige adaptação para retificações intradiárias e identidade oficial; não copiar a chave de produto como chave de contratação.

Grafo estático, por `revision` e `down_revision`:

```text
20260430_0001 → 20260430_0002 → 20260430_0003 → 20260501_0004
→ 20260501_0005 → 20260525_0006 → 20260526_0007 → 20260601_0008
→ 20260812_0008 → 20260817_0009
```

Os dois sufixos `_0008` não são heads duplicados: os IDs completos e a cadeia são distintos. `alembic/env.py:12` usa `target_metadata=None`; o schema é dirigido pelas migrations SQL, não por um catálogo completo de modelos ORM. O modelo declarativo localizado é `app/models/opportunity_review.py:13`, com base em `app/db/base.py`; demais acessos usam predominantemente SQL textual. O downgrade inicial contém remoção de schemas em cascata; nenhuma migration/downgrade foi executada.

## 7. Motor heurístico, matching, finanças e ML

[CONFIRMADO] `pipelines/decision_engine/build.py` concentra o motor `heuristic_v2_confidence_guard`. Normaliza fornecedores, busca evidências comerciais, escolhe matches e estima preço pelo percentil 35 dos matches qualificados. A chave de fornecedor prefere EAN, depois SKU e depois hash de título/preço (`:211`); alterar preço pode alterar a chave quando não há identificador estável.

Matching (`:487–524`) usa cobertura/precisão de tokens, SequenceMatcher, identificadores inferidos e penalizações. Não confirma equivalência técnica por EAN/SKU, unidade, certificação ou requisitos do edital. O serviço de revisão compara atributos extraídos do título (`app/services/review_service.py:108–163`), útil como indício, sem substituir ficha técnica.

O score atual (`build.py:698–715`) é:

```text
margin_score = clamp(100 * margem / 0,50, 0, 100)
source_score = min(100, 20 * número_de_fontes)
score = clamp(0,35*margin_score + 0,25*demand_score
              + 0,25*match_confidence + 0,15*source_score
              - min(24, 6*número_de_riscos), 0, 100)
```

Confiança e recomendação são funções separadas (`:633–695`), com bloqueios, mínimos de evidência, margem e demanda. Saídas: `comprar_teste`, `revisar`, `ignorar`. São regras de revenda [LEGADO] para o ranking principal de licitações. Reviews, posição de busca e quantidade de anúncios não medem demanda governamental.

Lacunas verificadas:

- `SupplierProduct.supplier_price` é `float`; `_to_float` converte `Decimal` (`:143–155`): NUMERIC no DB não garante aritmética financeira decimal ponta a ponta.
- Consulta de evidências (`:396–454`) não limita por idade nem deduplica todas as observações; envelhecimento/múltiplos snapshots podem alterar confiança.
- `offers_count` de Mercado Livre é usado como sinal de quantidade/demanda (`:448`), sem ser venda efetiva.
- Estoque bruto não atravessa integralmente o modelo normalizado do motor; cotação não é reserva de capacidade.
- Frete/taxas de revenda não cobrem capital ao longo do contrato, tributos específicos, garantias, lotes e recebimento financeiro.
- `scripts/quick_etl_exports.py:513–566` usa outro ranking (gap versus mediana da query, demanda/diversidade/confiança); CSVs `quick` não são snapshots do Gold nem prova do motor principal.

[PARCIAL] ML: migrations armazenam modelos/scores e a UI consegue ler a última pontuação, mas `ml/`, `features/`, `entity_resolution/` e `optimizer/` são esqueletos. Treino, inferência, validação temporal e benefício medido não foram encontrados. Preservar dados históricos; propor ML somente em fase posterior, com dados rotulados suficientes.

## 8. Revisão humana, histórico e BI

[CONFIRMADO] `ReviewRepository` seleciona oportunidades `revisar`, vincula decisão ao snapshot e exclui da fila decisões ativas equivalentes (`:54–104`). Grava julgamento com bloqueio transacional e rejeita duplicidade (`:290–424`). Desfazer invalida o registro e preserva evento `undo` (`:426–525`). É um padrão reaproveitável; estados atuais são `approved_test_purchase`, `needs_reanalysis`, `rejected`.

`needs_reanalysis` registra a intenção; não dispara nova coleta automaticamente (`ReviewService.create_decision`, `:341–346`). Campo de revisor é texto livre opcional, não identidade autenticada. Fila não possui paginação SQL e histórico limita 200 sem cursor; adaptar para o volume alvo.

UI [CONFIRMADO]: três templates de fila/histórico/base, filtros, comparação de atributos, riscos, evidências, formulários e atalhos. CSRF HMAC, TTL, cookie HttpOnly/SameSite Strict existem; cookie não usa Secure no contexto atual (`app/core/csrf.py:15–69`). Sanitização de links aceita HTTP(S) sem credenciais (`review_service.py:86–100`); não equivale à proteção SSRF exigida para baixar documentos.

Power BI [PARCIAL]: `scripts/export_power_bi.py:24–178,246–295` exporta oportunidades, snapshots, runs, qualidade, fornecedores e cenários de frete em CSV. `docs/power_bi_dashboard.md:52–97` especifica páginas, e o carrinho otimizado é futuro. Nenhum `.pbix`/`.pbit` encontrado. Reutilizar exportação depois de redefinir fatos/dimensões; não exigir BI externo para operar o MVP.

## 9. Infraestrutura Docker e dependências operacionais

[CONFIRMADO] como configuração: `docker-compose.yml`, `docker-compose.worker.yml`, `docker/Dockerfile.api`, `.dockerignore`, `.env.example`. Os serviços abaixo não foram iniciados ou inspecionados em runtime.

| Serviço/perfil | Responsabilidade/dependências | Rede/porta publicada padrão | Persistência |
|---|---|---|---|
| `postgres` | Banco da aplicação, imagem pgvector/pg15 | 55432 → 5432 | `motor_postgres_data`, nome global fixo |
| `redis` | Redis 7 Alpine, AOF; apoio ao Prefect | 6380 → 6379 | `motor_redis_data`, nome global fixo |
| `selenium` | Chrome remoto, shm 2 GB | 4444 e 7900 | Sem volume de negócio identificado |
| `api` | FastAPI, depende de postgres/redis/selenium saudáveis | 8010 → 8000 | Bind `.:/app`, `.env` |
| `daemon` (perfil daemon) | Imagem da aplicação; comando fornecido pela operação | Rede Compose | Mesmo bind do projeto |
| `prefect-postgres` (control) | Metadados exclusivos do Prefect | 55433 → 5432 | `prefect_postgres_data` |
| `prefect-server` (control) | API/UI Prefect, sem serviços embutidos | 4200 | Usa banco do Prefect e Redis |
| `prefect-services` (control) | Serviços internos do orquestrador | Rede Compose | Usa banco do Prefect e Redis |
| `prefect-init` (control) | Inicialização e bootstrap | Rede Compose | Pode executar migrations na inicialização |
| Workers http/browser/manual (control) | Filas API-HTTP/browser/manual, limites declarados 3/1/1 | Sem portas de usuário | Bind do código, dependem do init |
| Workers PC2 (Compose worker) | HTTP/browser, limites declarados 4/2 | Dependem de endereços do PC1 | Não hospedam banco, Redis ou servidor Prefect |

Evidências: Compose principal `:19–103` base, `:105–128` daemon, `:130–209` controle e `:211–263` workers; Compose worker `:3–38`. Rede padrão do Compose conecta serviços por nome; portas publicadas e nomes fixos exigem revisão antes de rodar múltiplas stacks. Não existe frontend React/container frontend.

O banco auxiliar **já existente** do Prefect não é um segundo banco de domínio Radar. A regra de usar o PostgreSQL existente significa armazenar as novas entidades no banco da aplicação, preservando o banco de metadados exigido pelo orquestrador. Formalizar esse entendimento em ADR; não criar terceiro banco de domínio.

Redis tem uso concreto na mensageria do Prefect (DB lógico 1); a aplicação declara URL para DB lógico 0, mas cliente Python de negócio encontrado está no diagnóstico `scripts/validate_setup.py:67–78`. Não justificar cache de negócio inexistente. Crawlee usa fila interna, não Redis como fila distribuída entre PCs.

Imagem única [CONFIRMADO]: `docker/Dockerfile.api`, Python 3.11, dependências de aplicação/controle e browser; API com bind mount e reload é configuração de desenvolvimento. Há custos de browser mesmo quando a coleta pública futura puder usar HTTP simples. `.dockerignore` reduz contexto de build; não supor que ele proteja arquivos presentes no bind mount.

Entrypoints/efeitos: `scripts/init_prefect.py` aplica preparação e migrations; scripts setup/up/daemon/bootstrap/deploy têm efeitos operacionais. Ler esses arquivos não autoriza executá-los nesta missão. Healthchecks e condições de dependência existem na infraestrutura; não comprovam prontidão do domínio Radar.

Limite concreto da prontidão: `app/api/health.py:10–26` responde HTTP 200 mesmo quando informa `database=false`/estado degradado; a verificação no Compose (`:99`) testa somente sucesso HTTP. Assim, pode marcar a API saudável sem acesso ao banco. A futura readiness deverá avaliar dependências essenciais. A redação de logs em `crawlee/observability.py:13–37` trata chaves estruturadas conhecidas, sem comprovar remoção de segredo em strings/URLs.

Pontos [PROPOSTO] para a Execução 2: perfis desenvolvimento/produção local, isolamento de documentos persistentes e quarentena, escopo das portas, execução como usuário sem privilégio quando aplicável, política de nomes de volumes, backup conjunto sem apagar dados e dependências de teste instaláveis em container. Windows: garantir LF nos scripts Linux, tratar permissões/bind mounts e remover dependência de caminhos absolutos antigos dos procedimentos. Não foi encontrada política `.gitattributes` que assegure LF.

### 9.1 Variáveis e scripts: inventário sem valores

Nomes lidos de `.env.example`/código, sem reproduzir valores:

| Grupo | Nomes |
|---|---|
| Aplicação | `ENVIRONMENT`, `LOG_LEVEL`, `DATABASE_URL`, `REDIS_URL`, `MOTOR_CONFIG_PATH`, `MOTOR_API_PORT` |
| Banco/Redis | `MOTOR_POSTGRES_USER`, `MOTOR_POSTGRES_PASSWORD`, `MOTOR_POSTGRES_DB`, `MOTOR_POSTGRES_PORT`, `MOTOR_REDIS_PORT` |
| Browser | `SELENIUM_PORT`, `SELENIUM_NOVNC_PORT`, `SELENIUM_REMOTE_URL`, `SELENIUM_MAX_SESSIONS` |
| SILLO | `SILLO_SOURCE_REGISTRY_PATH`, `SILLO_MAX_PRODUCT_PRICE_BRL`, `SILLO_REVIEW_CSRF_SECRET`, `SILLO_ENABLE_SCHEDULES` |
| Controle | `PREFECT_POSTGRES_USER`, `PREFECT_POSTGRES_PASSWORD`, `PREFECT_POSTGRES_DB`, `PREFECT_POSTGRES_PORT`, `PREFECT_SERVER_PORT` |
| Hosts/workers | `SILLO_PREFECT_HOST`, `SILLO_DB_HOST`, `SILLO_REDIS_HOST`, `SILLO_PC1_HTTP_FLOW_LIMIT`, `SILLO_PC1_BROWSER_FLOW_LIMIT`, `SILLO_PC2_HTTP_FLOW_LIMIT`, `SILLO_PC2_BROWSER_FLOW_LIMIT` |
| Integrações | `DISCORD_WEBHOOK_URL`, `ML_API_BASE`, `ML_AUTH_BASE`, `ML_SITE_ID`, `ML_CLIENT_ID`, `ML_CLIENT_SECRET`, `ML_REDIRECT_URI`, `ML_ACCESS_TOKEN`, `ML_REFRESH_TOKEN` |
| Internas do Compose | `PREFECT_API_URL`, `PREFECT_API_DATABASE_CONNECTION_URL`, `PREFECT_MESSAGING_BROKER`, `PREFECT_MESSAGING_CACHE`, `PREFECT_REDIS_MESSAGING_URL`, `PREFECT_SERVER_DOCKET_URL`, `PREFECT_SERVER_API_HOST`, `PREFECT_SERVER_UI_API_URL`, `SILLO_WORKER_ID` |

Há ainda variáveis de configuração da imagem Selenium com prefixo `SE_`; não são credenciais de domínio. A configuração e seus nomes deverão ser consolidados no documento 21. Nenhum arquivo real de credenciais foi alterado ou copiado.

| Script | Efeito encontrado e dependência operacional |
|---|---|
| `up.ps1:4–8`, `setup_local.ps1:6–45` | Criam arquivo de ambiente e iniciam serviços; setup também migra/valida, sem perfil control completo |
| `status.ps1:5–11` | Pode criar ambiente e iniciar serviços; nome não garante comando somente leitura |
| `coleta.ps1`, `bronze_http.ps1:15–43` | Caminho legado, inicialização e disparo de ciclo |
| `daemon.ps1:27–94` | Pode registrar tarefa/Startup e iniciar loop |
| `export_db.ps1:14–34` | Exportação com nomes de banco/usuário/container fixos |
| `import_db.ps1:18–28` | Restauração com limpeza prévia de objetos; destino precisa ser validado em missão futura |
| `restore_from_github_release.ps1:42–161` | Mesmo modo VerifyOnly pode criar arquivo/pasta e baixar; restauração completa também migra/reinicia |
| `extract_coletek_catalog.ps1:2,49–57` | Depende de caminho pessoal Windows e Excel COM |

Nenhum desses scripts foi executado. Imagem não declara `USER` nem ENTRYPOINT próprio; Crawlee configura Chromium sem sandbox no contexto root (`crawlee/factory.py`). Isso reforça que download/processamento de PDFs novos precisará de isolamento próprio. Os profiles atuais não são evidência de contenção documental.

## 10. Testes e limites de verificação

[CONFIRMADO] inventário: 12 módulos, 42 funções com prefixo `test_`; parametrização pode gerar quantidade distinta de casos. Não executados.

| Grupo | Arquivos e cobertura lida |
|---|---|
| Revisão | `test_review_model_and_schema.py`, `test_review_integration.py`: schema, filtros, persistência, duplicidade, desfazer, CSRF, histórico |
| Registry | `test_source_registry.py`, `test_current_source_registry.py`: seleção, duplicidade, prioridades e estado declarado |
| Coleta | `test_collection_contracts.py`, `test_collection_parsers.py`, `test_collection_market_persistence.py`, `test_collection_runner_resilience.py`, `test_collection_alerts.py` |
| Crawlee | `test_crawlee_base.py`: criação, políticas, métricas e storage |
| Mercado Livre | `test_mercado_livre_client.py`: 401 e renovação |
| Preços | `test_price_policy.py`: limite de candidato versus evidência |

As fixtures de `tests/test_review_integration.py:27–54` executam TRUNCATE CASCADE e verificam se o nome do banco contém `test`. Esse teste exige banco descartável isolado; a guarda nominal não comprova isolamento. Nenhum teste foi executado sobre dados existentes. Não há testes de licitação/PNCP no conjunto inspecionado.

Verificações realizadas: Git baseline/branch/status; inspeção de arquivos; leitura estática do grafo Alembic; versões locais Docker 29.8.2/Compose v5.5.1; validação silenciosa da configuração Compose com saída 0. Nenhuma configuração expandida foi exibida. Esses resultados não equivalem a build, integração ou testes aprovados. Procedimentos Docker que vierem a ser escritos nas próximas etapas devem receber o rótulo **“proposto; não executado nesta missão”**.

## 11. Fontes oficiais e conectividade

Consultadas em 2026-10-10 por GET direto, sem coleta de oportunidades:

| Fonte | Resultado observado | Limite da evidência |
|---|---|---|
| [Swagger de consulta](https://pncp.gov.br/api/consulta/swagger-ui/index.html) | HTTP 200, HTML | Não valida execução de endpoints |
| [OpenAPI de consulta](https://pncp.gov.br/api/consulta/v3/api-docs) | HTTP 200, JSON OpenAPI 3.0.1, info 1.0, 12 caminhos | Parâmetros e paginação serão especificados na Execução 2 |
| [OpenAPI principal](https://pncp.gov.br/api/pncp/v3/api-docs) | HTTP 200, JSON OpenAPI 3.0.1, info 1.0, 124 caminhos | Não assumir autorização para rotas de manutenção |
| [Manual PNCP](https://pncp.gov.br/manual/pt-br/latest/) | HTTP 200, HTML | Contrato detalhado pendente de análise |
| [Portal de dados abertos Compras.gov.br](https://www.gov.br/compras/pt-br/cidadao/portal-de-dados-abertos) | HTTP 200, HTML | Preços históricos serão planejados na Execução 2 |

O navegador de pesquisa encontrou erro intermediário para manual/contrato principal; acesso HTTP direto confirmou resposta. Não se classificou a fonte como indisponível por falha do intermediário. Contratos detalhados permanecem **pendentes de validação**; nenhum endpoint/campo foi inventado nesta etapa.

**Reconsulta em 2026-10-11, durante a retomada:** Swagger, OpenAPI de consulta (3.0.1, versão informada 1.0, 12 caminhos) e portal Compras.gov.br responderam HTTP 200 por GET direto. O OpenAPI principal excedeu o timeout local de 20 segundos; o manual apresentou erro no envio HTTP. O intermediário de pesquisa também falhou para esses dois endereços. Esses resultados não apagam a observação histórica de 2026-10-10 nem demonstram indisponibilidade global: acesso atual ao contrato principal/manual e seu conteúdo permanecem pendentes de validação na Execução 2. Nenhuma consulta operacional de contratações foi executada.

## 12. Matriz de reaproveitamento (seção 5.1 do prompt)

Custo relativo B/M/A = baixo/médio/alto; prioridade P0 = fundação ou risco impeditivo, P1 = MVP, P2 = evolução. Estimativas [PROPOSTO], não orçamento. Decisões não removem componentes existentes nesta missão.

| Componente / status atual | Ação | Justificativa e ponto de integração | Custo | Risco | Prioridade |
|---|---|---|---|---|---|
| FastAPI [CONFIRMADO] | REUTILIZAR | Routers/serviços para novo domínio | B | Mutações locais sem controle de acesso | P0 |
| PostgreSQL/SQLAlchemy/Alembic [CONFIRMADO] | REUTILIZAR | Banco da aplicação e migrações incrementais | M | Cascatas, preservação de IDs/dados | P0 |
| Bronze/Silver/Gold [CONFIRMADO] | ADAPTAR | Bruto oficial, normalização e análises com versões | M | Confundir fatos transacionais e projeções | P0 |
| Observabilidade/runs [CONFIRMADO] | ADAPTAR | Correlação fonte, documento, versão e decisão | M | Semântica comercial antiga | P1 |
| Prefect [CONFIRMADO] | ADAPTAR | Novos flows/deployments PNCP | M | Init com efeitos e limites YAML/ENV divergentes | P0 |
| Redis [CONFIRMADO] infraestrutura | REUTILIZAR | Necessário à configuração atual do Prefect | B | Criar cache paralelo sem necessidade | P1 |
| Crawlee [CONFIRMADO] | ADAPTAR | Fontes B2B HTML opcionais; HTTP público em adapter próprio | M | Sobrecarregar integração JSON com browser | P1 |
| Coleta marketplaces [CONFIRMADO] | DESCONTINUAR no fluxo principal | Preservar como evidência comercial opcional | B | Confundir preço varejista com preço contratado | P2 |
| Mercado Livre/OAuth [CONFIRMADO] | ADAPTAR como opcional | Isolar complemento de mercado | M | Credenciais e equivalência técnica limitada | P2 |
| Catálogos B2B/importadores [CONFIRMADO] | ADAPTAR | Cadastro/cotação versionados e unidade/embalagem | M | Estoque e preço sem confirmação comercial | P1 |
| Normalização/matching [PARCIAL] para licitações | ADAPTAR | Tokens sugerem candidatos; verificação de atributos confirma | A | Falso positivo técnico | P1 |
| Motor heurístico de revenda [CONFIRMADO] | SUBSTITUIR no novo domínio | Regras de elegibilidade, lote, prazo e capital próprias | A | Recomendação positiva com obrigação ignorada | P0 |
| Pipeline ML [PARCIAL] | DESCONTINUAR como dependência do MVP | Preservar tabelas/dados históricos; planejar avaliação futura | M | Alegar inteligência treinada inexistente | P2 |
| Revisão/histórico [CONFIRMADO] | ADAPTAR | Transações, snapshots e invalidação são reutilizáveis | M | Misturar aprovação de compra e de participação | P1 |
| Jinja2 [CONFIRMADO] | ADAPTAR, candidata inicial | Menor transição para operador único | M | Tabela grande, ausência de paginação | P1 |
| React [PROPOSTO] | CRIAR somente se ADR justificar | Comparar custo/estado interativo na Execução 2 | A | Stack e container extras sem ganho demonstrado | P2 |
| Power BI [PARCIAL] | ADAPTAR exportador | Reusar formato após redefinir fatos/dimensões | M | Dashboard não localizado | P2 |
| Compose/Dockerfile [CONFIRMADO] | ADAPTAR | Preservar volumes, separar modos e reduzir acoplamento | M | Portas/nomes fixos e bind mount | P0 |
| Testes existentes [CONFIRMADO] | ADAPTAR | Reusar padrões; criar fixtures públicas sintéticas | M | Limpeza destrutiva no banco errado | P0 |
| ETL/ranking CSV quick [CONFIRMADO] | DESCONTINUAR como ranking principal | Manter histórico separado de Gold | B | Scores concorrentes confundem usuário | P2 |
| Adapter PNCP/retificações [PROPOSTO] | CRIAR | Fonte oficial/idempotência/versionamento | A | Perder alteração de edital | P0 |
| Documentos/evidências/logística [PROPOSTO] | CRIAR | Download controlado, páginas e revisão A/B/C/D | A | Extração falha ou arquivo malicioso | P1 |
| Simulador por unidade de julgamento [PROPOSTO] | CRIAR | Decimal, fluxo de caixa, cenários e obrigações | A | Lucro aparente sem capital | P1 |
| Pipeline comercial público [PROPOSTO] | CRIAR | Participação manual até recebimento e encerramento | M | Automação inferida indevidamente | P1 |

## 13. Divergências, dívida técnica e próximos passos

1. Docker existe e a configuração valida; “funcional” em operação atual segue [NÃO-CONFIRMADO]. O segundo PostgreSQL encontrado é de metadados Prefect, não outro domínio.
2. ML está apenas parcialmente estruturado; Power BI tem exportação/spec, sem dashboard localizado. Preservar a diferença entre capacidade documentada e implementada.
3. Coleta V2 e rota operacional legada coexistem; seus efeitos/configurações não são equivalentes. Retentativas estão nos clientes/Crawlee, não declaradas nos flows.
4. Dinheiro vira float no motor; evidência pode envelhecer e matching lexical não basta. Estes são motivos concretos para um motor específico de licitações.
5. Fila/histórico, portabilidade, dependências sem lock, portas, backups e isolamento de testes precisam evoluir. Não há benchmark atual nem execução comprobatória.
6. A comparação detalhada Jinja2 versus React, topologia alvo, migração sem perda e contratos PNCP pertencem à Execução 2. A matriz acima é direção inicial, sem antecipar implementação.

Este diagnóstico cobre o código versionado inspecionado; arquivos ignorados, bancos, volumes, credenciais e infraestrutura remota não foram inventariados como fatos operacionais. Referências de linha correspondem ao commit base.
