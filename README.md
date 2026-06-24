# SILLO - Motor de Decisao de Compra

> Sistema local-first de coleta, normalizacao, scoring heuristico e Machine Learning para apoiar decisoes de compra para revenda.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![FastAPI](https://img.shields.io/badge/API-FastAPI-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Machine Learning](https://img.shields.io/badge/ML-Scikit--Learn%20%7C%20XGBoost-orange)
![Status](https://img.shields.io/badge/Status-Production--Ready%20Local-brightgreen)

## Visao Geral

SILLO e um motor de decisao para compra de produtos de tecnologia e perifericos com foco em revenda. O projeto coleta dados de fornecedores e marketplaces, organiza os registros em uma arquitetura medalhao no PostgreSQL e gera uma fila priorizada de oportunidades.

O sistema nao executa compra automatica. Ele apoia uma decisao humana com rastreabilidade:

- `comprar_teste`: oportunidade forte para lote pequeno.
- `revisar`: produto com potencial, mas que precisa de validacao manual.
- `ignorar`: margem, demanda, match ou confianca insuficiente.

O projeto combina duas camadas de decisao:

- Motor heuristico auditavel, hoje em `heuristic_v3_attribute_guard`.
- Modelo supervisionado proxy que aprende a aproximar a heuristica com features intermediarias e trava hibrida de seguranca.

## Objetivo do Projeto

O problema pratico e reduzir o tempo e o risco da analise manual de catalogos B2B. Em vez de olhar milhares de produtos um a um, o sistema cruza:

- preco de fornecedor;
- evidencias de preco de mercado;
- sinais de demanda;
- confianca de match entre fornecedor e mercado;
- margem liquida estimada;
- flags de risco de dados.

Com isso, o operador recebe uma lista enxuta de itens para revisar, com explicacoes e historico de execucoes.

## Arquitetura

```text
Fontes externas
  Mercado Livre API
  Amazon / Kabum / Terabyte
  Zoom / Buscape
  MegaMix / Mirao / Coletek

        |
        v

Bronze
  payload bruto, snapshots e evidencias preservadas

        |
        v

Silver
  normalizacao, chaves, deduplicacao e produtos comparaveis

        |
        v

Gold
  recomendacoes, historico, modelos, scores e explicacoes

        |
        v

FastAPI / scripts / Power BI
```

### Estrutura do Repositorio

```text
motor_decisao/
  src/
    motor_decisao/
      app/                 # FastAPI, settings e database
      pipelines/           # coleta, preparo, motor de decisao e ML
      entity_resolution/   # matching e atributos de produto
      paths.py             # caminhos canonicos do projeto
  scripts/                 # CLIs operacionais e automacoes locais
  alembic/                 # migracoes do banco
  config/                  # parametros de coleta, margem e ML
  data/
    raw/                   # snapshots brutos pequenos e versionaveis
    processed/             # saidas processadas pequenas; ML/Power BI ignorados
  docs/                    # documentacao tecnica e relatorios
  notebooks/               # notebooks finais, quando houver
  reports/                 # relatorios regeneraveis de ML
  artifacts/               # modelos e metadados regeneraveis
```

## Stack Tecnologico

| Camada | Tecnologia |
| --- | --- |
| Linguagem | Python 3.11 |
| API | FastAPI + Uvicorn |
| Banco | PostgreSQL 15 + pgvector |
| Cache/apoio | Redis |
| Migracoes | Alembic |
| Browser automation | Playwright + Selenium Grid |
| ML | Scikit-Learn, XGBoost CPU, Joblib |
| Validacao | Pytest |
| Orquestracao local | Docker Compose |
| BI | Export CSV para Power BI |

## Fontes de Dados

| Fonte | Tipo | Uso |
| --- | --- | --- |
| MegaMix | Catalogo bruto em `data/raw/megamix_catalog_raw.json` | Preco de fornecedor |
| Coletek | Snapshot bruto em `data/raw/coletek_catalog_raw.json` | Preco de fornecedor |
| Mirao | Scraper HTML configurado em `config/config.yaml` | Preco de fornecedor |
| Mercado Livre | API OAuth/API publica | Catalogo, anuncios e referencia de preco |
| Amazon, Kabum, Terabyte | Web scraping | Evidencias de preco e demanda |
| Zoom, Buscape | Web scraping | Historico/resumo de preco |

Snapshot operacional validado em 2026-06-24:

| Camada/Tabela | Linhas |
| --- | ---: |
| `bronze.market_web_listings_raw` | 33.385 |
| `bronze.price_history_raw` | 9.562 |
| `bronze.supplier_products_raw` | 80.035 |
| `silver.supplier_products_normalized` | 80.035 |
| `gold.decision_opportunities` | 5.957 |
| `gold.decision_opportunity_snapshots` | 130.962 |
| `gold.ml_opportunity_scores_latest` | 24.519 |

Distribuicao atual em fornecedores no Bronze:

| Fornecedor | Linhas |
| --- | ---: |
| Mirao | 43.900 |
| MegaMix | 35.400 |
| Coletek | 735 |

## Motor de Decisao

O motor heuristico esta em:

```text
src/motor_decisao/pipelines/decision_engine/build.py
```

Ele calcula:

- `estimated_market_price`;
- `estimated_net_profit`;
- `net_margin_pct`;
- `demand_score`;
- `match_confidence`;
- `decision_score`;
- `recommendation`;
- `confidence_level`;
- `risk_flags`.

O score usa guardrails para evitar falsos positivos, incluindo verificacao de atributos tecnicos conflitantes em `src/motor_decisao/entity_resolution/product_attributes.py`.

Estado mais recente do motor heuristico:

| Recomendacao | Confianca | Produtos |
| --- | --- | ---: |
| `revisar` | media | 55 |
| `ignorar` | baixa | 5.902 |
| `comprar_teste` | alta | 0 |

## Camada de Machine Learning

A camada de ML nao substitui a heuristica. Ela aprende a imitar a decisao heuristica como rotulo proxy enquanto ainda nao ha historico suficiente de venda, margem realizada e giro.

Desenho:

- rotulo positivo: `revisar` ou `comprar_teste`;
- rotulo negativo: `ignorar`;
- features intermediarias sem usar `decision_score` como entrada;
- validacao cruzada estratificada out-of-fold;
- comparacao entre modelos;
- augmentation controlado apenas no treino;
- regra hibrida: o ML nao promove sozinho para `comprar_teste`.

Modelo mais recente validado:

| Item | Valor |
| --- | --- |
| Versao | `ml_proxy_v3_20260608T195725Z_99008136` |
| Modelo selecionado | `hist_gradient_boosting` |
| Variante | `real_plus_synthetic` |
| Linhas reais | 5.946 |
| Positivos proxy | 64 |
| Linhas sinteticas no treino final | 192 |
| Average Precision | 0,9925 |
| Precision | 0,9552 |
| Recall | 1,0000 |
| F1 | 0,9771 |
| ROC-AUC | 0,9999 |
| Precision@10 | 1,0000 |
| Recall@50 | 0,7656 |

As metricas acima medem imitacao do baseline heuristico, nao retorno comercial comprovado.

## Setup de Ambiente

### Pre-requisitos

- Windows com Docker Desktop.
- Git.
- PowerShell.
- Opcional para execucao local fora do Docker: Python 3.11.

### Configuracao Inicial

```powershell
cd "C:\Users\luisg\revenda assistida\motor_decisao"
Copy-Item .env.example .env
docker compose up -d postgres redis selenium api
docker compose exec -T api alembic upgrade head
```

Validar API:

```powershell
Invoke-WebRequest -UseBasicParsing http://localhost:8010/health
```

Resposta esperada:

```json
{"status":"ok","environment":"development","database":true}
```

### Variaveis Importantes

As credenciais ficam em `.env`, que e ignorado pelo Git.

| Variavel | Uso |
| --- | --- |
| `DATABASE_URL` | Conexao SQLAlchemy com PostgreSQL |
| `REDIS_URL` | Redis local |
| `MOTOR_CONFIG_PATH` | Caminho do `config/config.yaml` |
| `SELENIUM_REMOTE_URL` | Selenium Grid |
| `ML_CLIENT_ID` | OAuth Mercado Livre |
| `ML_CLIENT_SECRET` | OAuth Mercado Livre |
| `ML_REDIRECT_URI` | Callback OAuth Mercado Livre |
| `ML_ACCESS_TOKEN` | Token de acesso Mercado Livre |
| `ML_REFRESH_TOKEN` | Refresh token Mercado Livre |

## Uso

### Subir Servicos

```powershell
docker compose up -d postgres redis selenium api
```

Com daemon de coleta:

```powershell
docker compose --profile daemon up -d daemon
```

### Rodar Coleta Completa

```powershell
docker compose run --rm api python scripts/collect_all.py
```

Rodar apenas uma etapa:

```powershell
docker compose run --rm api python scripts/collect_all.py --only suppliers
docker compose run --rm api python scripts/collect_all.py --only web
docker compose run --rm api python scripts/collect_all.py --only engine
docker compose run --rm api python scripts/collect_all.py --only mlengine
```

### Importar Catalogos e Recalcular Motor

```powershell
docker compose exec -T api python scripts/build_decision_engine.py --import-megamix --import-coletek
```

### Treinar Modelos

```powershell
docker compose exec -T api python scripts/ml_run_all.py
```

Reusar modelo mais recente sem retreinar:

```powershell
docker compose exec -T api python scripts/ml_run_all.py --predict-only
```

Listar melhores oportunidades:

```powershell
docker compose exec -T api python scripts/ml_top_opportunities.py --limit 30
```

### Exportar Power BI

```powershell
docker compose exec -T api python scripts/export_power_bi.py
```

Saida padrao:

```text
data/processed/power_bi/
```

### Validar Projeto

```powershell
docker compose run --rm api python -m pytest -q
docker compose config --quiet
```

Validacao mais recente:

```text
14 passed
```

## API

Base local:

```text
http://localhost:8010
```

Rotas principais:

| Rota | Metodo | Uso |
| --- | --- | --- |
| `/health` | GET | Saude da API e banco |
| `/decision-engine/run` | POST | Importa snapshots opcionais e recalcula Gold |
| `/decision-engine/summary` | GET | Resumo das recomendacoes heuristicas |
| `/ml-engine/train` | POST | Treina e versiona modelo |
| `/ml-engine/predict` | POST | Recalcula score hibrido com modelo existente |
| `/ml-engine/summary` | GET | Resumo ML x heuristica |
| `/ml-engine/opportunities` | GET | Lista oportunidades pontuadas |
| `/ops/recent-runs` | GET | Observabilidade de execucoes recentes |
| `/ops/repair-stale-runs` | POST | Repara execucoes antigas presas em `running` |

## Qualidade e Observabilidade

O projeto registra execucoes em:

- `control.pipeline_runs`;
- `control.source_runs`;
- `control.data_quality_checks`.

Estado operacional validado em 2026-06-24:

- API em `healthy`;
- `source_runs` com `running = 0`;
- `pipeline_runs` com `running = 0`;
- teste Docker com 14 testes passando.

## Artefatos

Artefatos regeneraveis e geralmente ignorados pelo Git:

```text
artifacts/ml/models/
artifacts/ml/metadata/
artifacts/ml/feature_lists/
reports/ml/
data/processed/ml/
data/processed/power_bi/
```

Arquivos pequenos versionaveis de amostra/snapshot:

```text
data/raw/megamix_catalog_raw.json
data/raw/megamix_catalog_raw.csv
data/raw/coletek_catalog_raw.json
data/processed/*_quick_*.csv
data/processed/etl_summary_*.json
```

## Limitacoes

- As metricas de ML sao proxy e ainda nao comprovam lucro real.
- Web scraping pode quebrar por mudanca de DOM, bloqueio anti-bot ou latencia.
- O Mercado Livre depende de OAuth e tokens validos.
- O ranking ainda precisa de revisao humana antes de compra.
- A camada de augmentation usa perturbacao controlada e herda rotulos proxy.
- O projeto e local-first; para producao multiusuario faltam CI/CD, secrets manager, observabilidade centralizada e jobs agendados externos.

## Proximos Passos

- Substituir rotulos proxy por dados reais de compra, venda, margem e giro.
- Criar fila de revisao com feedback humano versionado.
- Adicionar monitoramento de drift de fontes e modelos.
- Separar ambiente local, homologacao e producao.
- Automatizar CI com lint, testes, migracoes e smoke test de API.
- Publicar dashboards Power BI finais com manifest versionado.

## Autor

Luis Gustavo
GitHub: https://github.com/luis7gustavo
