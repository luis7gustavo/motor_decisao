# SILLO - Documentacao Tecnica

Data de referencia: 2026-06-01

Este documento descreve a arquitetura tecnica atual da SILLO, o modelo de dados, os pipelines implementados, o motor de decisao e os proximos objetivos de evolucao.

## Visao Geral

A SILLO e um sistema local-first para apoiar decisoes de compra para revenda. O sistema coleta sinais de mercado, historico de precos e catalogos de fornecedores, organiza os dados em arquitetura Medalhao e calcula oportunidades de compra com rastreabilidade.

O MVP atual nao compra automaticamente. Ele entrega uma fila priorizada para decisao humana:

- `comprar_teste`: candidato forte para lote pequeno;
- `revisar`: existe potencial, mas precisa validacao manual;
- `ignorar`: risco, margem ou evidencia insuficiente.

## Stack

| Camada | Tecnologia |
| --- | --- |
| API | FastAPI |
| Linguagem | Python |
| Banco | PostgreSQL 15 + pgvector |
| Cache/apoio | Redis |
| Browser automation | Selenium Grid / Playwright |
| Migracoes | Alembic |
| Orquestracao local | Docker Compose + servico `motor_daemon` |
| Ambiente alvo | Windows com Docker Desktop |

Portas locais padrao:

| Servico | Porta |
| --- | ---: |
| API | `8010` |
| Postgres | `55432` |
| Redis | `6380` |
| Selenium | `4444` |

## Estrutura do Projeto

```text
app/
  api/                         -> rotas FastAPI
  core/                        -> settings e database

pipelines/
  common/                      -> run manager, serializacao, browser helpers
  mercado_livre/               -> API Mercado Livre
  market_web/                  -> scraping de marketplaces
  price_history/               -> Zoom/Buscape e comparadores
  suppliers/                   -> fornecedores B2B por HTML/catalogo
  decision_engine/             -> motor de decisao
  ml/                          -> dataset, augmentation, treino, explicacao e score hibrido

scripts/
  collect_*.py                 -> coletores e rotinas locais
  import_megamix_catalog.py    -> importacao MegaMix
  build_decision_engine.py     -> execucao CLI do motor
  ml_run_all.py                -> treino e predicao hibrida
  export_power_bi.py           -> CSVs analiticos para Power BI
  validate_setup.py            -> validacao de ambiente

alembic/
  versions/                    -> migracoes do banco

docs/
  arquitetura_tecnica.md       -> este documento
  uso_local_e_importacao.md    -> runbook operacional
  sillo_documentacao.*         -> documentacao visual e produto
```

## Arquitetura de Dados

```text
Fontes externas
  Mercado Livre API
  Marketplaces
  Zoom / Buscape
  MegaMix
  Mirao
  Coletek

        |
        v

control
  execucoes, status e checks

        |
        v

bronze
  payload bruto, dados sem reconciliacao

        |
        v

silver
  normalizacao, chaves e produtos comparaveis

        |
        v

gold
  recomendacoes, historico, runs e scores ML versionados
```

## Schema `control`

O schema `control` registra observabilidade de pipeline.

| Tabela | Papel |
| --- | --- |
| `control.pipeline_runs` | Uma execucao de pipeline de alto nivel. |
| `control.source_runs` | Uma execucao por fonte ou etapa interna. |
| `control.data_quality_checks` | Checks de qualidade e metricas simples. |

Uso pratico:

- saber se uma coleta terminou com `success`, `partial` ou `failed`;
- identificar fonte bloqueada;
- rastrear volume extraido, carregado e ignorado;
- auditar uma rodada do motor.

## Schema `bronze`

Bronze guarda dados brutos ou quase brutos. A regra e preservar o payload e evitar decisao de negocio nessa camada.

| Tabela | Origem | Conteudo |
| --- | --- | --- |
| `bronze.market_web_listings_raw` | Scrapers de marketplace | Ofertas, preco, posicao, reviews e sinais brutos. |
| `bronze.price_history_raw` | Zoom/Buscape | Preco atual e agregados/historico visivel. |
| `bronze.mercado_livre_products_raw` | API Mercado Livre | Catalogo e produtos consultados. |
| `bronze.mercado_livre_items_raw` | API Mercado Livre | Itens/anuncios relacionados quando disponiveis. |
| `bronze.supplier_products_raw` | Fornecedores | Catalogos MegaMix/Mirao/Coletek e payload original. |

Snapshot validado em 2026-06-01:

| Tabela | Linhas |
| --- | ---: |
| `bronze.market_web_listings_raw` | 17.324 |
| `bronze.price_history_raw` | 3.824 |
| `bronze.supplier_products_raw` | 18.583 |

Fornecedores em Bronze:

| Fornecedor | Linhas |
| --- | ---: |
| MegaMix | 9.440 |
| Mirao | 8.408 |
| Coletek | 735 |

Observacao: Bronze preserva snapshots de mais de uma carga. O motor usa o
snapshot mais recente por `source_product_key` para evitar duplicidade no Gold
atual.

## Schema `silver`

Silver normaliza dados para comparacao e pontuacao.

| Tabela | Papel |
| --- | --- |
| `silver.mercado_livre_product_prices` | Resumo de precos e ofertas do Mercado Livre. |
| `silver.supplier_products_normalized` | Produtos de fornecedor normalizados e tokenizados. |

Principais transformacoes em fornecedor:

- limpeza de titulo;
- normalizacao sem acentos;
- tokenizacao;
- criacao de `source_product_key`;
- preservacao de `supplier_raw_id`;
- deduplicacao operacional por snapshot mais recente no motor.

Estado validado:

| Tabela | Linhas |
| --- | ---: |
| `silver.supplier_products_normalized` | 15.783 |

## Schema `gold`

Gold e a camada de decisao.

| Tabela | Papel |
| --- | --- |
| `gold.decision_opportunities` | Estado atual consumivel pela API. |
| `gold.decision_engine_runs` | Historico de rodadas do motor. |
| `gold.decision_opportunity_snapshots` | Snapshot completo das decisoes por rodada. |
| `gold.ml_model_runs` | Historico de modelos treinados, metricas e artefatos. |
| `gold.ml_opportunity_scores` | Scores hibridos e explicacoes por produto. |
| `gold.ml_opportunity_scores_latest` | View com o ultimo score hibrido de cada produto. |

Estado validado em 2026-06-01:

| Tabela | Linhas |
| --- | ---: |
| `gold.decision_opportunities` | 5.776 |
| `gold.decision_opportunity_snapshots` | 38.993 |
| `gold.ml_opportunity_scores_latest` | 5.776 |

## Pipelines Implementados

### Mercado Livre

Componentes:

- `pipelines/mercado_livre/`
- `scripts/collect_mercado_livre.py`
- `scripts/collect_mercado_livre_catalog_fast.py`
- `scripts/collect_mercado_livre_perifericos.py`
- `app/api/mercado_livre_oauth.py`

Uso:

- catalogo;
- enriquecimento;
- referencia auxiliar de preco/oferta.

Limitacao:

- busca aberta de anuncios pode retornar `403`;
- Mercado Livre nao deve ser a unica fonte de benchmarking.

### Market Web

Componentes:

- `pipelines/market_web/`
- `scripts/collect_market_web.py`

Uso:

- ofertas de varejo e marketplace;
- preco observado;
- reviews, posicao e sinais de demanda quando disponiveis.

Fontes ja trabalhadas:

- Amazon;
- Kabum;
- Terabyte;
- Shopee;
- AliExpress;
- Pichau;
- Magalu.

Limitacao:

- algumas fontes bloqueiam ou mudam DOM;
- o Bronze contem ruido e precisa de filtros Silver/Gold.

### Price History

Componentes:

- `pipelines/price_history/`
- `scripts/collect_price_history_web.py`

Uso:

- Zoom;
- Buscape;
- preco atual;
- minimo, maximo e agregados visiveis.

Limitacao:

- serie historica completa ainda nao e extraida ponto a ponto.

### Suppliers

Componentes:

- `pipelines/suppliers/base.py`
- `pipelines/suppliers/generic_html.py`
- `pipelines/suppliers/ingest.py`
- `scripts/collect_suppliers.py`
- `scripts/import_megamix_catalog.py`

Fornecedores:

- MegaMix via arquivo `data/megamix_catalog_raw.json`;
- Mirao via scraper HTML configurado em `config/config.yaml`.
- Coletek via planilha importada e snapshot versionado.

Melhorias atuais:

- retry/backoff em HTTP;
- tratamento de erro por pagina;
- status `partial` quando categoria falha mas ha dados validos;
- registro de `page_errors` em `control.source_runs.metadata`.

Observacao:

- a URL `https://www.mirao.com.br/audio-e-video.html` retornou `404` no ultimo teste e foi registrada como erro terminal de categoria.

## Motor de Decisao

Componente principal:

- `pipelines/decision_engine/build.py`

Execucao CLI:

```powershell
docker compose exec -T api python scripts/build_decision_engine.py --import-megamix
```

Versao atual:

```text
heuristic_v2_confidence_guard
```

### Entradas

O motor usa:

- `silver.supplier_products_normalized`;
- `bronze.market_web_listings_raw`;
- `bronze.price_history_raw`;
- `silver.mercado_livre_product_prices`;
- parametros de margem em `config/config.yaml`.

### Processamento

Fluxo interno:

1. normaliza fornecedores;
2. busca apenas produtos com preco de fornecedor;
3. usa snapshot mais recente por `supplier_slug` + `source_product_key`;
4. monta indice de tokens das evidencias de mercado;
5. calcula match textual;
6. estima preco de mercado por percentil conservador;
7. calcula margem liquida;
8. calcula demanda;
9. gera flags de risco;
10. define nivel de confianca;
11. grava Gold atual e snapshot historico.

### Saidas

Campos principais de `gold.decision_opportunities`:

- `supplier_slug`;
- `product_title`;
- `supplier_price`;
- `estimated_market_price`;
- `estimated_net_profit`;
- `net_margin_pct`;
- `market_offer_count`;
- `market_source_count`;
- `price_history_count`;
- `mercado_livre_count`;
- `demand_score`;
- `match_confidence`;
- `decision_score`;
- `recommendation`;
- `confidence_level`;
- `risk_flags`;
- `evidence`;
- `decision_run_id`;
- `scoring_version`.

### Guardrails

Flags que bloqueiam `comprar_teste`:

- `sem_preco_mercado`;
- `match_fraco`;
- `match_revisar`;
- `demanda_fraca`;
- `demanda_incompleta`;
- `margem_baixa`;
- `margem_indisponivel`;
- `fontes_insuficientes`;
- `poucas_ofertas_mercado`;
- `modelo_nao_confirmado`;
- `preco_mercado_muito_disperso`;
- `ticket_fornecedor_muito_baixo`;
- `titulo_generico_sem_identificador`.

## API

Rotas principais:

| Rota | Metodo | Uso |
| --- | --- | --- |
| `/health` | GET | Healthcheck da API e banco. |
| `/ops/recent-runs` | GET | Ultimas execucoes. |
| `/ops/bronze-cycle` | POST | Ciclo Bronze via API. |
| `/ops/repair-stale-runs` | POST | Corrige runs travadas. |
| `/ops/cancel-pipeline-run` | POST | Cancela run especifica. |
| `/integracoes/mercado-livre/auth-url` | GET | Gera URL OAuth. |
| `/integracoes/mercado-livre/callback` | GET | Callback OAuth Mercado Livre. |
| `/integracoes/mercado-livre/refresh-token` | POST | Renova token ML. |
| `/decision-engine/run` | POST | Executa motor. |
| `/decision-engine/summary` | GET | Resumo atual das decisoes. |
| `/decision-engine/opportunities` | GET | Lista oportunidades. |
| `/decision-engine/runs` | GET | Historico de rodadas. |
| `/ml-engine/train` | POST | Treina e versiona um modelo proxy. |
| `/ml-engine/predict` | POST | Atualiza scores hibridos. |
| `/ml-engine/summary` | GET | Resume heuristica x ML. |
| `/ml-engine/opportunities` | GET | Lista scores e explicacoes. |

## Resultado Atual Validado

Ultima rodada validada:

| Campo | Valor |
| --- | --- |
| `decision_run_id` | `4b79565d-51da-4a2c-9c12-cacee5c64843` |
| `scoring_version` | `heuristic_v2_confidence_guard` |
| Status | `success` |
| Finalizada em | `2026-06-01 22:00:08 UTC` |
| Produtos pontuados | 5.776 |
| `comprar_teste` | 2 |
| `revisar` | 38 |
| `ignorar` | 5.736 |

Distribuicao por confianca:

| Confianca | Produtos |
| --- | ---: |
| Alta | 2 |
| Media | 38 |
| Baixa | 5.736 |

## Limitacoes Tecnicas

- Matching ainda e heuristico, baseado em tokens e similaridade textual.
- Nao existe entidade canonica global de produto.
- Frete, estoque real e prazo ainda nao entram no score.
- O historico de preco de Zoom/Buscape ainda e parcial.
- Alguns scrapers dependem de DOM instavel e podem quebrar.
- `comprar_teste` ainda exige revisao humana antes de compra.
- O sistema ainda nao aprende com venda real, devolucao ou margem realizada.

## Objetivos e Proximos Passos

### Objetivo 1 - Confianca de produto

Criar uma entidade canonica de produto para reduzir falso positivo de match.

Entregas:

- tabela `silver.canonical_products`;
- tabela de relacionamento fornecedor -> produto canonico;
- tabela de relacionamento evidencia de mercado -> produto canonico;
- score separado para match de marca, modelo, capacidade e conectividade.

### Objetivo 2 - Margem realista

Incluir custo operacional real na decisao.

Entregas:

- frete do fornecedor;
- frete de venda;
- taxa por canal;
- imposto por tipo de operacao;
- margem minima por categoria;
- simulacao de lote.

### Objetivo 3 - Validacao humana

Criar fluxo de revisao para transformar julgamento humano em dado.

Entregas:

- endpoint ou tela para aprovar/reprovar oportunidade;
- campos `human_decision`, `decision_reason` e `reviewed_at`;
- dataset supervisionado para treinar modelos futuros;
- lista de motivos padronizados de rejeicao.

### Objetivo 4 - Evolucao estatistica e ML

Substituir gradualmente regras rigidas por modelos treinados.

Modelos candidatos:

- classificador de equivalencia de produto;
- modelo de probabilidade de venda;
- estimador de margem realizada;
- detector de preco anomalo;
- ranking learning-to-rank de oportunidades.

### Objetivo 5 - Observabilidade de producao

Melhorar confiabilidade operacional.

Entregas:

- dashboard de runs;
- alertas por fonte com `bad_streak`;
- relatorio de freshness por tabela;
- limites de rate por fonte;
- politica de backoff por tipo de erro;
- job agendado com baixo volume e sem duplicar ciclos.

### Objetivo 6 - Produto SILLO

Transformar o MVP em ferramenta usavel.

Entregas:

- dashboard de oportunidades;
- filtros por fornecedor, categoria, margem e confianca;
- tela de detalhe com evidencias;
- historico por `decision_run_id`;
- exportacao CSV;
- documentacao visual e tecnica integradas.

## Criterio de Pronto para Producao

Antes de considerar compra automatica ou semi-automatica, o sistema precisa:

- ter match canonico confiavel;
- incluir frete e estoque;
- medir margem realizada;
- registrar decisao humana;
- ter monitoramento de fonte;
- ter rollback/backup validado;
- ter testes de regressao do motor;
- separar claramente recomendacao de execucao de compra.

Enquanto esses pontos nao estiverem prontos, `comprar_teste` deve significar apenas: "vale olhar e testar pequeno".
