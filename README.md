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
| `docs/mercado_livre_ngrok.md` | Guia de OAuth Mercado Livre com ngrok. |
| `docs/collection_v2/README.md` | Plataforma de coleta V2: arquitetura, CLI, Prefect, fontes, operacao e benchmark. |
| `docs/Relatorio_Mudancas_Coleta_SILLO_2026-08-14.docx` | Relatorio executivo e tecnico das mudancas, validacoes e pendencias da Coleta V2. |

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

## Estado do Motor Heuristico

Snapshot validado em 2026-05-26 e preservado como base atual do motor:

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

## Estado da Coleta V2

Validado em 2026-08-14:

| Fonte | Agendamento | Situacao operacional |
| --- | --- | --- |
| Amazon | ativo | HTTP adaptativo, canario e coleta completa |
| Kabum | ativo | HTTP adaptativo, 420 itens na validacao de referencia |
| Cia Informatica | ativo | catalogo publico com canario |
| Grupo Tek | ativo | catalogo publico com canario |
| Mirao | ativo | catalogo de fornecedor; itens sem preco valido sao contabilizados e nao persistidos |
| Mercado Livre | pausado | requer credenciais OAuth completas; renovacao automatica implementada |
| Terabyte | pausado | acesso publico classificado como bloqueado |
| Buscape e Zoom | pausado | fontes sem acesso publico estavel; nenhum bloqueio e contornado |

A plataforma usa Prefect para agendamento, retries e observabilidade; Crawlee para
fila, politicas de requisicao e coleta; e contratos tipados para impedir que uma
execucao inconsistente seja marcada como sucesso.

## Stack

- FastAPI
- PostgreSQL 15 + pgvector
- Redis
- Selenium Grid
- Playwright
- Crawlee
- Prefect
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
| Prefect UI | `4200` |

## Arquitetura Resumida

```text
Prefect (agendamentos e retries)
        |
        v
Registro de fontes e contrato de coleta
        |
        v
Crawlee / APIs publicas
  Marketplaces
  Catalogos de fornecedores
  Mercado Livre (quando autenticado)

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

Na raiz do projeto, suba a API, o banco e o plano de controle:

```powershell
cd "C:\Users\luisg\revenda assistida\motor_decisao"
docker compose --profile control up -d --build
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

Validar o plano de controle:

```powershell
Invoke-RestMethod http://127.0.0.1:4200/api/health
docker compose ps
```

## Rodar Coleta e Motor

Listar as fontes e seus estados declarativos:

```powershell
docker compose exec -T api python sillo.py collect list
```

Executar o canario de uma fonte antes da coleta completa:

```powershell
docker compose exec -T api python sillo.py collect run --source amazon --canary
docker compose exec -T api python sillo.py collect run --source amazon
```

O limite de elegibilidade para compra e aplicado aos candidatos de fornecedor
no motor de decisao. Evidencias de mercado acima desse limite permanecem no
Bronze para comparacao, rastreabilidade e auditoria.

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
