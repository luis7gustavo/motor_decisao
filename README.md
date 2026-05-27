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

Validado em 2026-05-26:

| Indicador | Valor |
| --- | ---: |
| Produtos pontuados no Gold atual | 5.139 |
| `comprar_teste` | 2 |
| `revisar` | 34 |
| `ignorar` | 5.103 |
| Versao do motor | `heuristic_v2_confidence_guard` |

Tabelas principais:

| Tabela | Linhas |
| --- | ---: |
| `bronze.market_web_listings_raw` | 14.516 |
| `bronze.price_history_raw` | 3.459 |
| `bronze.supplier_products_raw` | 7.527 |
| `silver.supplier_products_normalized` | 7.527 |
| `gold.decision_opportunities` | 5.139 |
| `gold.decision_opportunity_snapshots` | 12.661 |

Fornecedores carregados:

| Fornecedor | Registros Bronze |
| --- | ---: |
| MegaMix | 4.720 |
| Mirao | 2.807 |

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
  oportunidades, historico e runs versionadas

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

Coletar Mirao:

```powershell
docker compose exec -T api python scripts/collect_suppliers.py --supplier mirao
```

Importar MegaMix e rodar motor:

```powershell
docker compose exec -T api python scripts/build_decision_engine.py --import-megamix
```

Consultar resumo:

```powershell
Invoke-RestMethod http://127.0.0.1:8010/decision-engine/summary
```

Consultar oportunidades:

```powershell
Invoke-RestMethod "http://127.0.0.1:8010/decision-engine/opportunities?recommendation=comprar_teste"
```

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

Nao versionar:

- `.env`;
- `.env.*`;
- dumps em `backups/`;
- dados sensiveis;
- tokens OAuth;
- logs locais.

Para levar dados a outra maquina, use:

```powershell
.\scripts\export_db.ps1
.\scripts\import_db.ps1 -DumpPath .\backups\motor_decisao_YYYYMMDD_HHMMSS.dump
```

Detalhes completos em `docs/uso_local_e_importacao.md`.
