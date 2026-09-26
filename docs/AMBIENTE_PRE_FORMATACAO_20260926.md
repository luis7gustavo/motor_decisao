# Snapshot do ambiente antes da formatacao

Registro coletado em 26/09/2026 para orientar a reconstrucao do projeto.

## Host

| Componente | Versao |
| --- | --- |
| Windows | Windows 11 Pro, build 26200 |
| PowerShell | 7.6.5 |
| WSL | 2.6.3.0 |
| Kernel WSL | 6.6.87.2-1 |
| Git | 2.52.0.windows.1 |
| Python local | 3.10.11 |
| Docker Engine/CLI | 29.4.0 |
| Docker Compose | 5.1.2 |
| GitHub CLI | 2.97.0 |

O Python local e a pasta `.venv` nao precisam ser copiados: a execucao oficial ocorre nos containers e as dependencias estao em `requirements.txt` e `requirements-control.txt`.

## Imagens Docker

- `motor_decisao-api`, reconstruida por `docker/Dockerfile.api`;
- `pgvector/pgvector:pg15`;
- `postgres:15-alpine` para o Prefect;
- `redis:7-alpine`;
- `selenium/standalone-chrome:4.43.0-20260404`.

## Bancos preservados

| Banco | Estado no snapshot | Asset do release |
| --- | --- | --- |
| `motor_decisao` | PostgreSQL 15.17, tamanho logico 3,8 GB, Alembic `20260817_0009` | `motor_decisao_pre_format_20260926.dump` |
| `prefect` | PostgreSQL 15.17, tamanho logico 26 MB | `prefect_pre_format_20260926.dump` |

O ultimo motor de decisao registrado antes do snapshot iniciou em `2026-09-25 19:47:22 UTC`. O dump principal foi criado em formato custom do PostgreSQL e seu catalogo foi validado com `pg_restore -l`.

## Portas e servicos

| Servico | Porta no host |
| --- | ---: |
| API FastAPI | 8010 |
| PostgreSQL principal | 55432 |
| Redis | 6380 |
| Selenium Grid | 4444 |
| Selenium noVNC | 7900 |
| Prefect UI/API | 4200 |
| PostgreSQL do Prefect | 55433 |

## Integridade dos assets

Os hashes oficiais estao em `SHA256SUMS.txt`, publicado no mesmo release dos dumps. O script `scripts/restore_from_github_release.ps1` valida os hashes antes de modificar os bancos.
