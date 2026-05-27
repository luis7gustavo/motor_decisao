# SILLO - Runbook Operacional Local

Data de referencia: 2026-05-26

Este documento descreve como instalar, rodar, validar, operar e transportar os dados locais da SILLO em uma maquina Windows com Docker Desktop.

## Quando usar este documento

Use este runbook para:

- subir o ambiente local;
- validar API e banco;
- coletar dados;
- importar fornecedores;
- rodar o motor de decisao;
- consultar oportunidades;
- diagnosticar falhas;
- exportar/importar banco para outra maquina.

Para arquitetura e objetivos tecnicos, leia `docs/arquitetura_tecnica.md`.

## Requisitos

- Windows com PowerShell.
- Docker Desktop instalado e aberto.
- Git.
- Acesso a internet para coletas.
- Opcional: DBeaver para explorar o banco.

## Portas Locais

| Servico | URL/porta |
| --- | --- |
| API | `http://127.0.0.1:8010` |
| Postgres | `localhost:55432` |
| Redis | `localhost:6380` |
| Selenium Grid | `http://127.0.0.1:4444` |

## Primeiro Setup

Na raiz do projeto:

```powershell
cd "C:\Users\luisg\revenda assistida\motor_decisao"
.\SetupMotor.cmd
```

O setup deve:

1. criar `.env` a partir de `.env.example`, se necessario;
2. subir `postgres`, `redis` e `selenium`;
3. aplicar migracoes Alembic;
4. rodar validacao de setup;
5. subir a API;
6. validar `/health`.

## Subir e Parar

Subir tudo:

```powershell
docker compose up -d
```

Parar tudo:

```powershell
.\PararMotor.cmd
```

Ver containers:

```powershell
docker compose ps
```

Estado esperado:

- `motor_api` healthy;
- `motor_postgres` healthy;
- `motor_redis` healthy;
- `motor_selenium` healthy.

## Validacao Rapida

Healthcheck:

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

Testar porta do banco:

```powershell
Test-NetConnection 127.0.0.1 -Port 55432
```

Validar versao Alembic:

```powershell
docker compose exec -T postgres psql -U motor -d motor_decisao -c "SELECT version_num FROM alembic_version;"
```

Versao esperada apos a fase atual:

```text
20260526_0007
```

## Fluxo Operacional Recomendado

Para rotina local completa:

```powershell
.\SetupMotor.cmd
docker compose exec -T api python scripts/collect_suppliers.py --supplier mirao
docker compose exec -T api python scripts/build_decision_engine.py --import-megamix
Invoke-RestMethod http://127.0.0.1:8010/decision-engine/summary
```

Para ciclo Bronze padrao via API:

```powershell
.\ColetaMotorHTTP.cmd
```

Para status:

```powershell
.\StatusMotor.cmd
```

## Coleta Bronze via API

O caminho operacional preferido para ciclo Bronze e HTTP-first, porque evita duplicar runs e usa os endpoints operacionais da API.

Iniciar ciclo Bronze assincrono:

```powershell
Invoke-RestMethod -Method Post "http://127.0.0.1:8010/ops/bronze-cycle?async_run=true"
```

Ver runs recentes:

```powershell
Invoke-RestMethod "http://127.0.0.1:8010/ops/recent-runs?limit=20"
```

Reparar runs travadas:

```powershell
Invoke-RestMethod -Method Post "http://127.0.0.1:8010/ops/repair-stale-runs?stale_after_hours=8"
```

Cancelar run especifica:

```powershell
Invoke-RestMethod -Method Post "http://127.0.0.1:8010/ops/cancel-pipeline-run?pipeline_run_id=COLE_O_ID_AQUI"
```

## Fornecedores

### Listar fornecedores habilitados

```powershell
docker compose exec -T api python scripts/collect_suppliers.py --list
```

Estado atual esperado:

```text
Fornecedores habilitados: ['mirao']
```

### Coletar Mirao

```powershell
docker compose exec -T api python scripts/collect_suppliers.py --supplier mirao
```

Observacao operacional:

- a categoria `audio-e-video.html` retornou `404` na ultima validacao;
- isso e registrado como `partial`, mas nao impede carregar as categorias validas;
- se `records_loaded=0` mas `records_extracted>0`, pode ser apenas deduplicacao do dia.

### Importar MegaMix

O caminho mais simples e rodar o motor com importacao:

```powershell
docker compose exec -T api python scripts/build_decision_engine.py --import-megamix
```

Tambem e possivel importar diretamente:

```powershell
docker compose exec -T api python scripts/import_megamix_catalog.py
```

Arquivo padrao:

```text
data/megamix_catalog_raw.json
```

## Rodar Motor de Decisao

Com importacao MegaMix antes da pontuacao:

```powershell
docker compose exec -T api python scripts/build_decision_engine.py --import-megamix
```

Sem reimportar MegaMix:

```powershell
docker compose exec -T api python scripts/build_decision_engine.py
```

Via API:

```powershell
Invoke-RestMethod -Method Post "http://127.0.0.1:8010/decision-engine/run?import_megamix=true"
```

Saida esperada no CLI:

```text
Motor de decisao: 5139 produtos pontuados / 2 comprar_teste / 34 revisar / 5103 ignorar
pipeline_run_id=...
decision_run_id=...
scoring_version=heuristic_v2_confidence_guard
```

## Consultar Resultados

Resumo:

```powershell
Invoke-RestMethod http://127.0.0.1:8010/decision-engine/summary
```

Oportunidades de compra teste:

```powershell
Invoke-RestMethod "http://127.0.0.1:8010/decision-engine/opportunities?recommendation=comprar_teste"
```

Oportunidades por fornecedor:

```powershell
Invoke-RestMethod "http://127.0.0.1:8010/decision-engine/opportunities?supplier_slug=mirao&limit=100"
```

Oportunidades por confianca:

```powershell
Invoke-RestMethod "http://127.0.0.1:8010/decision-engine/opportunities?confidence_level=alta"
```

Historico de rodadas:

```powershell
Invoke-RestMethod "http://127.0.0.1:8010/decision-engine/runs?limit=10"
```

## Consultas SQL Uteis

Resumo por recomendacao:

```powershell
docker compose exec -T postgres psql -U motor -d motor_decisao -c "SELECT recommendation, confidence_level, COUNT(*) FROM gold.decision_opportunities GROUP BY recommendation, confidence_level ORDER BY recommendation, confidence_level;"
```

Resumo por fornecedor:

```powershell
docker compose exec -T postgres psql -U motor -d motor_decisao -c "SELECT supplier_slug, recommendation, confidence_level, COUNT(*) FROM gold.decision_opportunities GROUP BY supplier_slug, recommendation, confidence_level ORDER BY supplier_slug, recommendation, confidence_level;"
```

Top oportunidades:

```powershell
docker compose exec -T postgres psql -U motor -d motor_decisao -c "SELECT product_title, supplier_slug, supplier_price, estimated_market_price, net_margin_pct, demand_score, match_confidence, decision_score, risk_flags FROM gold.decision_opportunities WHERE recommendation='comprar_teste' ORDER BY decision_score DESC;"
```

Ultima rodada do motor:

```powershell
docker compose exec -T postgres psql -U motor -d motor_decisao -c "SELECT id, scoring_version, status, metadata, started_at, finished_at FROM gold.decision_engine_runs ORDER BY started_at DESC LIMIT 1;"
```

Runs de fornecedor Mirao:

```powershell
docker compose exec -T postgres psql -U motor -d motor_decisao -c "SELECT status, records_extracted, records_loaded, records_skipped, metadata->'page_errors' AS page_errors FROM control.source_runs WHERE source_name='mirao' ORDER BY started_at DESC LIMIT 5;"
```

## Estado Atual Validado

Validado em 2026-05-26:

| Indicador | Valor |
| --- | ---: |
| `bronze.market_web_listings_raw` | 14.516 |
| `bronze.price_history_raw` | 3.459 |
| `bronze.supplier_products_raw` | 7.527 |
| `silver.supplier_products_normalized` | 7.527 |
| `gold.decision_opportunities` | 5.139 |
| `gold.decision_opportunity_snapshots` | 12.661 |

Fornecedores:

| Fornecedor | Registros Bronze |
| --- | ---: |
| MegaMix | 4.720 |
| Mirao | 2.807 |

Resultado atual do motor:

| Recomendacao | Confianca | Produtos |
| --- | --- | ---: |
| `comprar_teste` | Alta | 2 |
| `revisar` | Media | 34 |
| `ignorar` | Baixa | 5.103 |

## Conexao no DBeaver

Use:

```text
Host: localhost
Port: 55432
Database: motor_decisao
User: motor
Password: motor
```

Tabelas mais uteis:

- `control.pipeline_runs`
- `control.source_runs`
- `control.data_quality_checks`
- `bronze.market_web_listings_raw`
- `bronze.price_history_raw`
- `bronze.supplier_products_raw`
- `silver.supplier_products_normalized`
- `gold.decision_opportunities`
- `gold.decision_engine_runs`
- `gold.decision_opportunity_snapshots`

## Diagnostico de Problemas

### API nao responde

1. Verifique containers:

```powershell
docker compose ps
```

2. Veja logs:

```powershell
docker compose logs --tail=120 api
```

3. Reinicie API:

```powershell
docker compose restart api
```

### Banco nao responde

```powershell
docker compose logs --tail=120 postgres
docker compose restart postgres
```

Depois:

```powershell
Invoke-RestMethod http://127.0.0.1:8010/health
```

### Alembic fora da versao esperada

```powershell
docker compose exec -T api alembic upgrade head
```

### Fonte retorna zero registros

Checar:

- se houve bloqueio/captcha;
- se a fonte mudou HTML;
- se a query esta muito especifica;
- se `records_loaded=0` e apenas deduplicacao;
- se `metadata.page_errors` mostra status 403, 404, 429 ou timeout.

### Docker CLI bloqueado, mas API ativa

Use o caminho HTTP:

```powershell
Invoke-RestMethod http://127.0.0.1:8010/health
Invoke-RestMethod "http://127.0.0.1:8010/ops/recent-runs?limit=20"
Invoke-RestMethod -Method Post "http://127.0.0.1:8010/ops/repair-stale-runs?stale_after_hours=8"
```

## Exportar e Importar Dados

Por padrao, cada maquina tem seu proprio volume Docker de Postgres. GitHub leva codigo, nao leva o banco.

Exportar banco da maquina atual:

```powershell
.\scripts\export_db.ps1
```

O dump fica em `backups/`.

Importar em outra maquina:

```powershell
.\scripts\import_db.ps1 -DumpPath .\backups\motor_decisao_YYYYMMDD_HHMMSS.dump
```

Use dump/restore quando quiser clonar o estado local dos dados.

## Usar Banco Central

Nao e o padrao recomendado agora. Se precisar centralizar, ajuste `.env`:

```env
DATABASE_URL=postgresql+psycopg://usuario:senha@host:5432/motor_decisao
```

Cuidados:

- evitar ciclos duplicados;
- proteger credenciais;
- validar backup;
- monitorar latencia;
- controlar quem escreve no banco.

## Seguranca

Nao versionar:

- `.env`;
- `.env.*`;
- dumps em `backups/`;
- dados brutos sensiveis;
- logs locais;
- credenciais Mercado Livre;
- tokens OAuth.

Use `.env.example` como referencia.

## Checklist de Operacao Diaria

1. `docker compose ps`
2. `Invoke-RestMethod http://127.0.0.1:8010/health`
3. `Invoke-RestMethod "http://127.0.0.1:8010/ops/recent-runs?limit=20"`
4. `docker compose exec -T api python scripts/collect_suppliers.py --supplier mirao`
5. `docker compose exec -T api python scripts/build_decision_engine.py --import-megamix`
6. `Invoke-RestMethod http://127.0.0.1:8010/decision-engine/summary`
7. Revisar manualmente os itens `comprar_teste`

## Regra de Compra

`comprar_teste` nao significa compra automatica.

Antes de comprar:

1. confirmar estoque;
2. confirmar frete;
3. confirmar equivalencia do produto;
4. checar concorrencia atual;
5. comprar lote pequeno;
6. registrar margem e giro real.
