# Restauracao completa apos formatar o PC

Este guia reconstrói o ambiente SILLO no Windows e restaura o estado dos dados salvo em 26/09/2026.

## O que foi preservado

- codigo, migrations, configuracoes publicas e testes no GitHub;
- banco principal `motor_decisao`, com 3,8 GB no momento do snapshot;
- banco do Prefect, incluindo historico e metadados de orquestracao;
- hash SHA-256 dos dois dumps;
- versoes do ambiente usadas antes da formatacao.

Os dumps ficam nos assets do release `pre-format-2026-09-26`. Eles nao entram no historico Git e, por isso, o clone continua leve.

## O que nao vai para o GitHub

Por seguranca, os itens abaixo devem ser guardados separadamente em um gerenciador de senhas ou outro cofre privado:

- valores atuais do arquivo `.env`;
- `ML_CLIENT_ID`, `ML_CLIENT_SECRET`, `ML_ACCESS_TOKEN` e `ML_REFRESH_TOKEN`;
- `DISCORD_WEBHOOK_URL`;
- `SILLO_REVIEW_CSRF_SECRET`;
- estado de login do Playwright/Selenium e arquivo PKCE do Mercado Livre;
- configuracoes pessoais do DBeaver e Power BI.

Antes de formatar, copie somente esses segredos para um cofre privado. Nunca envie o `.env` ao GitHub.

## 1. Instalar os requisitos no PC novo

Instale:

1. Git para Windows;
2. Docker Desktop com WSL 2;
3. PowerShell 7;
4. opcionalmente Python 3.10, GitHub CLI, DBeaver e Power BI Desktop.

Reinicie o Windows depois de habilitar WSL 2 e confirme que o Docker Desktop esta aberto.

Versoes de referencia registradas em `docs/AMBIENTE_PRE_FORMATACAO_20260926.md`.

## 2. Clonar o projeto

```powershell
New-Item -ItemType Directory -Force -Path "$HOME\projetos" | Out-Null
Set-Location "$HOME\projetos"
git clone https://github.com/luis7gustavo/motor_decisao.git
Set-Location .\motor_decisao
```

## 3. Recriar o arquivo de ambiente

```powershell
Copy-Item .env.example .env
notepad .env
```

Preencha no novo `.env` apenas os segredos guardados no cofre privado. Mantenha `SILLO_ENABLE_SCHEDULES=false` durante a restauracao para evitar coletas antes da validacao.

## 4. Restaurar automaticamente

Na raiz do repositorio:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\restore_from_github_release.ps1
```

O script:

1. baixa os dumps do release;
2. confere os hashes SHA-256;
3. sobe somente PostgreSQL e Prefect PostgreSQL;
4. restaura os dois bancos;
5. reconstrói as imagens Docker;
6. aplica as migrations Alembic;
7. inicia API, Redis, Selenium, Prefect e workers;
8. valida a API e o Prefect.

Para somente baixar e verificar os assets, sem modificar os bancos:

```powershell
.\scripts\restore_from_github_release.ps1 -VerifyOnly
```

Para ignorar o historico do Prefect e recriar sua base do zero:

```powershell
.\scripts\restore_from_github_release.ps1 -SkipPrefectHistory
```

## 5. Validar a restauracao

```powershell
docker compose --profile control ps
Invoke-RestMethod http://127.0.0.1:8010/health
Invoke-RestMethod http://127.0.0.1:4200/api/health
docker compose exec -T postgres psql -U motor -d motor_decisao -c "SELECT version_num FROM alembic_version;"
docker compose exec -T postgres psql -U motor -d motor_decisao -c "SELECT COUNT(*) FROM gold.decision_opportunities;"
```

Abra:

- API: `http://127.0.0.1:8010`;
- fila de revisao: `http://127.0.0.1:8010/review`;
- Prefect: `http://127.0.0.1:4200`;
- Selenium: `http://127.0.0.1:4444`;
- Selenium noVNC: `http://127.0.0.1:7900`.

## 6. Reativar integracoes e agendamentos

1. refaça o OAuth do Mercado Livre conforme `docs/mercado_livre_ngrok.md`;
2. recrie logins assistidos de fornecedores, se forem usados;
3. teste uma coleta canario;
4. altere `SILLO_ENABLE_SCHEDULES=true` somente depois das validacoes;
5. execute novamente a inicializacao do Prefect:

```powershell
docker compose --profile control run --rm prefect-init
docker compose --profile control restart worker-http-pc1 worker-browser-pc1 worker-manual-pc1
```

## 7. Recriar o worker do segundo PC

No PC2, clone o mesmo repositorio e configure no `.env`:

```env
SILLO_PREFECT_HOST=IP_DO_PC1
SILLO_DB_HOST=IP_DO_PC1
SILLO_REDIS_HOST=IP_DO_PC1
```

No PC1, libere na rede privada as portas `4200`, `55432` e `6380`. Depois, no PC2:

```powershell
docker compose -f docker-compose.worker.yml up -d --build
```

Valide no Prefect se `pc2-http` e `pc2-browser` aparecem online.

## 8. Recriar ferramentas de analise

### DBeaver

```text
Host: localhost
Porta: 55432
Database: motor_decisao
Usuario: motor
Senha: valor de MOTOR_POSTGRES_PASSWORD no .env
```

### Power BI

```powershell
docker compose exec -T api python scripts/export_power_bi.py
```

Use os arquivos gerados em `data_processed/power_bi/`. Os arquivos `.pbix` pessoais nao fazem parte do repositorio.

## Diagnostico rapido

Se o Docker Desktop falhar antes de iniciar o engine, desabilite temporariamente os recursos de IA nas configuracoes do Docker e reinicie. O ambiente de 26/09/2026 apresentou um socket temporario `dockerInference` corrompido; os volumes do banco permaneceram intactos.

Para outros problemas, consulte `docs/uso_local_e_importacao.md`.
