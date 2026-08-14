# SILLO Collection Platform V2

Esta pasta documenta a modernizacao incremental da camada de coleta. O motor de decisao, Silver, Gold, ML, FastAPI, Redis e os dados historicos foram preservados.

## Documentos

- `current_state.md`: inventario anterior a qualquer mudanca.
- `architecture.md`: arquitetura implementada e limites de responsabilidade.
- `operations.md`: instalacao, CLI, PC1/PC2, agendas e recuperacao.
- `source_reconnaissance.md`: classificacao tecnica das novas fontes.
- `migration_report.md`: resultado das fases 0 a 11 e pendencias de validacao fisica.
- `benchmark.md`: consultas e protocolo para comparar workers e estrategias.

## Fluxo de dados desta fase

```text
Prefect -> Collector Contract -> Crawlee/API -> Validacao -> Bronze
```

Silver, Gold, heuristica e ML continuam fora do Prefect. A unica alteracao no motor foi aplicar a configuracao central de elegibilidade ao custo de fornecedor.

## Inicio rapido

```powershell
Copy-Item .env.example .env
docker compose --profile control up -d --build
docker compose --profile control exec worker-manual-pc1 sillo collect list
docker compose --profile control exec worker-manual-pc1 sillo collect run --source kabum
docker compose --profile control exec worker-manual-pc1 sillo collect run --profile market
```

As agendas nascem pausadas. Consulte `operations.md` antes de ativa-las.
