# SILLO - Motor de Decisao de Compra

**SILLO - Intelligence for better buying**

Backend local-first para apoiar decisoes de compra para revenda usando dados de mercado, historico de precos, catalogos de fornecedores e um motor de decisao auditavel.

O sistema coleta e organiza sinais em arquitetura Medalhao, calcula margem e confianca, e separa produtos em:

- `comprar_teste`;
- `revisar`;
- `ignorar`.

O objetivo do MVP e apoiar decisao humana. Ele ainda nao executa compra automatica.

## Documentacao

| Documento | Uso |
| --- | --- |
| `docs/sillo_documentacao.md` | Visao de produto, identidade SILLO e resumo executivo. |
| `docs/sillo_documentacao.html` | Versao visual da documentacao com identidade da marca. |
| `docs/arquitetura_tecnica.md` | Arquitetura, schemas, pipelines, motor, objetivos e proximos passos. |
| `docs/uso_local_e_importacao.md` | Runbook operacional: setup, coleta, motor, diagnostico e importacao/exportacao. |
| `docs/power_bi_dashboard.md` | Modelo de dados, atualizacao e paginas recomendadas no Power BI. |
| `docs/ml_engine.md` | Camada hibrida de ML, limites dos rotulos proxy, treino e evolucao. |
| `docs/mercado_livre_ngrok.md` | Guia de OAuth Mercado Livre com ngrok. |

Logo:

```text
docs/assets/sillo-logo.png
```

Versoes Word geradas:

```text
output/doc/SILLO_README.docx
output/doc/SILLO_Documentacao_Produto.docx
output/doc/SILLO_Arquitetura_Tecnica.docx
output/doc/SILLO_Runbook_Operacional.docx
output/doc/SILLO_Mercado_Livre_Ngrok.docx
output/doc/SILLO_Documentacao_Completa.docx
```

## Estado Atual Validado

Validado em 2026-06-02:

| Indicador | Valor |
| --- | ---: |
| Produtos pontuados no Gold atual | 5.817 |
| `comprar_teste` | 2 |
| `revisar` heuristico | 41 |
| `ignorar` heuristico | 5.774 |
| Versao do motor | `heuristic_v2_confidence_guard` |
| Modelo ML selecionado | `xgboost` |
| Variante de treino ML | `real_plus_synthetic` |
| Linhas sinteticas usadas | 129 |
| `revisar` hibrido | 176 |

Tabelas principais:

| Tabela | Linhas |
| --- | ---: |
| `bronze.market_web_listings_raw` | 19.035 |
| `bronze.price_history_raw` | 4.252 |
| `bronze.supplier_products_raw` | 23.774 |
| `silver.supplier_products_normalized` | 20.943 |
| `gold.decision_opportunities` | 5.817 |
| `gold.decision_opportunity_snapshots` | 44.810 |
| `gold.ml_opportunity_scores_latest` | 5.817 |

Fornecedores carregados:

| Fornecedor | Registros Bronze |
| --- | ---: |
| MegaMix | 9.440 |
| Mirao | 8.408 |
| Coletek | 735 |

## Stack

- FastAPI
- PostgreSQL 15 + pgvector
- Redis
- Selenium Grid
- Playwright
- Alembic
- Docker Compose
- Python

Portas locais:

| Servico | Porta |
| --- | ---: |
| API | `8010` |
| Postgres | `55432` |
| Redis | `6380` |
| Selenium | `4444` |

## Arquitetura Resumida

```text
Fontes externas
  Mercado Livre
  Marketplaces
  Zoom / Buscape
  MegaMix
  Mirao
  Coletek

        |
        v

Bronze
  dados brutos e payloads

        |
        v

Silver
  normalizacao e chaves comparaveis

        |
        v

Gold
  oportunidades, historico, runs e scores ML versionados

        |
        v

API FastAPI
  operacao e consulta
```

## Setup Rapido

Na raiz do projeto:

```powershell
cd "C:\Users\luisg\revenda assistida\motor_decisao"
.\SetupMotor.cmd
```

Validar API:

```powershell
Invoke-RestMethod http://127.0.0.1:8010/health
```

Resposta esperada:

```json
{
  "status": "ok",
  "environment": "development",
  "database": true
}
```

## Rodar Coleta e Motor

O ciclo Bronze de mercado coleta perifericos e hardware com preco entre
`R$ 100,00` e `R$ 1.500,00`. A faixa e aplicada a novas evidencias de
marketplace e historico de preco; catalogos brutos de fornecedores continuam
preservados integralmente para auditoria.

Iniciar coleta continua gerenciada:

```powershell
.\scripts\daemon.ps1
docker compose --profile daemon ps daemon
docker logs --tail 120 motor_daemon
```

Coletar Mirao:

```powershell
docker compose exec -T api python scripts/collect_suppliers.py --supplier mirao
```

Importar MegaMix e rodar motor:

```powershell
.\scripts\extract_coletek_catalog.ps1
docker compose exec -T api python scripts/build_decision_engine.py --import-megamix --import-coletek
```

Consultar resumo:

```powershell
Invoke-RestMethod http://127.0.0.1:8010/decision-engine/summary
```

Consultar oportunidades:

```powershell
Invoke-RestMethod "http://127.0.0.1:8010/decision-engine/opportunities?recommendation=comprar_teste"
```

Exportar a camada analitica para o Power BI:

```powershell
docker compose exec -T api python scripts/export_power_bi.py
```

## Rodar Camada Hibrida de ML

O baseline heuristico continua ativo e auditavel. A camada de ML aprende
rotulos proxy da heuristica, compara Logistic Regression, Random Forest,
HistGradientBoosting e XGBoost CPU e combina o score selecionado com as regras
existentes. Cada candidato e avaliado com treino `real_only` e
`real_plus_synthetic`, sempre em folds de validacao exclusivamente reais. Ela
nao autoriza compra automaticamente.

Treinar, pontuar e comparar:

```powershell
docker compose exec -T api python scripts/ml_run_all.py
```

Atualizar apenas as previsoes depois de um novo ciclo do motor:

```powershell
docker compose exec -T api python scripts/ml_run_all.py --predict-only
```

Consultar resumo:

```powershell
Invoke-RestMethod http://127.0.0.1:8010/ml-engine/summary
```

Detalhes em `docs/ml_engine.md`.

## Endpoints Principais

| Rota | Metodo | Uso |
| --- | --- | --- |
| `/health` | GET | Healthcheck da API e banco. |
| `/ops/recent-runs` | GET | Ultimas execucoes. |
| `/ops/bronze-cycle` | POST | Ciclo Bronze via API. |
| `/ops/repair-stale-runs` | POST | Reparo de runs travadas. |
| `/decision-engine/run` | POST | Executa motor. |
| `/decision-engine/summary` | GET | Resumo das recomendacoes atuais. |
| `/decision-engine/opportunities` | GET | Lista oportunidades. |
| `/decision-engine/runs` | GET | Historico de rodadas do motor. |
| `/ml-engine/train` | POST | Treina e versiona um modelo proxy. |
| `/ml-engine/predict` | POST | Atualiza scores hibridos com o ultimo modelo. |
| `/ml-engine/summary` | GET | Resume a comparacao heuristica x ML. |
| `/ml-engine/opportunities` | GET | Lista scores hibridos e explicacoes. |

## Conexao no DBeaver

```text
Host: localhost
Port: 55432
Database: motor_decisao
User: motor
Password: motor
```

## Regra de Uso das Recomendacoes

`comprar_teste` significa oportunidade forte para lote pequeno, nao compra automatica.

Antes de comprar:

1. confirmar estoque;
2. confirmar frete;
3. validar equivalencia do produto;
4. checar concorrencia atual;
5. comprar pouco;
6. registrar margem e giro real.

## Dados e Seguranca

Bases versionadas para reproducao rapida:

- `data/megamix_catalog_raw.csv`;
- `data/megamix_catalog_raw.json`;
- `data_processed/`.

Nao versionar:

- `.env`;
- `.env.*`;
- dumps em `backups/`;
- dados sensiveis;
- tokens OAuth;
- `data/mercado_livre_pkce.json`;
- logs locais.

Para levar dados a outra maquina, use:

```powershell
.\scripts\export_db.ps1
.\scripts\import_db.ps1 -DumpPath .\backups\motor_decisao_YYYYMMDD_HHMMSS.dump
```

Detalhes completos em `docs/uso_local_e_importacao.md`.
