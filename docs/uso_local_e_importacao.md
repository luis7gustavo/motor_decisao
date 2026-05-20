# Uso local, fontes ativas e importacao em outra maquina

Este documento descreve o modo recomendado para rodar o `motor_decisao` em uma maquina Windows com Docker Desktop.

## Estado recomendado das fontes

O ciclo padrao foi reduzido para fontes que ja demonstraram utilidade real no banco local:

| Grupo | Fontes padrao | Motivo |
| --- | --- | --- |
| Market web | Amazon, Kabum, Terabyte | Geraram volume util com preco. |
| Historico/comparacao | Buscape, Zoom | Geraram preco atual e agregados visiveis; rodam sequencialmente. |
| Mercado Livre | Catalogo + itens/precos por produto | Serve como catalogo e enriquecimento, nao como principal benchmark. |

Fontes fora do ciclo principal:

| Fonte | Situacao atual | Como tratar |
| --- | --- | --- |
| Shopee | Parcial/instavel nos ultimos runs | Manter fora do padrao; testar manualmente em lote pequeno. |
| AliExpress | Parcial; util como proxy de custo/importacao | Manter experimental ate validar seletores e filtro de preco. |
| Pichau | Mapeada, mas sem preco util no ambiente atual | Manter desabilitada. |
| Magalu | Bloqueada/sem preco util no ambiente atual | Manter desabilitada. |

As fontes desabilitadas continuam no codigo para testes futuros, mas nao entram no ciclo padrao.

## Arquivos de duplo clique

Na raiz do projeto existem launchers `.cmd` para uso simples:

| Arquivo | Uso |
| --- | --- |
| `SetupMotor.cmd` | Primeiro setup da maquina: cria `.env`, sobe containers, aplica migracoes, valida e sobe API. |
| `ColetaMotorHTTP.cmd` | Aciona o ciclo Bronze oficial via API local `/ops/bronze-cycle`. Recomendado para rotina. |
| `ColetaMotor.cmd` | Roda a coleta direta via `scripts/collect_all.py`, incluindo ETL rapido. Bom para uso manual. |
| `StatusMotor.cmd` | Mostra status das coletas e tabelas recentes. |
| `PararMotor.cmd` | Para os containers Docker do projeto. |

O caminho mais seguro para rotina local e:

```powershell
.\SetupMotor.cmd
.\ColetaMotorHTTP.cmd
.\StatusMotor.cmd
```

O `ColetaMotorHTTP.cmd` e o melhor acionamento operacional porque passa pela API, que ja possui reparo de runs travadas, trava contra ciclo duplicado e endpoint de status.

O `ColetaMotor.cmd` continua util quando voce quer rodar a coleta direta e o ETL local em seguida.

## Instalacao em outra maquina

Requisitos:

- Windows com PowerShell.
- Docker Desktop instalado e aberto.
- Git.
- A pasta do projeto ou o clone do repositorio.

Passos:

```powershell
git clone https://github.com/luis7gustavo/motor_decisao.git
cd motor_decisao
.\SetupMotor.cmd
```

O setup faz:

1. Cria `.env` a partir de `.env.example`, se ainda nao existir.
2. Sobe `postgres`, `redis` e `selenium`.
3. Roda `alembic upgrade head`.
4. Executa `scripts/validate_setup.py`.
5. Sobe a API em `http://127.0.0.1:8010`.
6. Valida `GET /health`.

Depois do setup:

```powershell
.\ColetaMotorHTTP.cmd
.\StatusMotor.cmd
```

## Para onde os dados vao?

Por padrao, cada maquina cria o seu proprio banco local em um volume Docker chamado:

```text
motor_postgres_data
```

Ou seja:

- Maquina A tem seu banco local.
- Maquina B tem outro banco local.
- Os dados nao vao automaticamente para o mesmo lugar.
- O GitHub leva codigo e configuracao, mas nao leva os dados do Postgres.

Isso e o comportamento mais seguro para desenvolvimento local.

## Como usar o mesmo banco em mais de uma maquina

So use esse modelo se voce realmente quiser centralizar dados.

Opcoes:

1. Rodar um Postgres central em uma maquina/servidor e apontar todas as maquinas para ele.
2. Usar um banco gerenciado na nuvem.
3. Manter coletas locais separadas e depois consolidar por dump/CSV.

Para apontar para banco central, seria necessario ajustar `.env`, especialmente:

```env
DATABASE_URL=postgresql+psycopg://usuario:senha@host:5432/motor_decisao
```

Nesse caso, todas as coletas gravam no mesmo banco. Nao e o padrao recomendado agora, porque exige rede estavel, credenciais, backup e cuidado para nao rodar ciclos duplicados.

## Como levar os dados para outra maquina

Se voce quer levar o banco atual para outro computador, faca dump/restaure.

Na maquina de origem:

```powershell
.\scripts\export_db.ps1
```

O arquivo sera criado em `backups/`.

Copie o `.dump` gerado para a outra maquina.

Na maquina de destino, depois de rodar `SetupMotor.cmd`:

```powershell
.\scripts\import_db.ps1 -DumpPath .\motor_decisao_YYYYMMDD_HHMMSS.dump
```

Use isso apenas quando quiser clonar o estado dos dados. Para instalar do zero, nao precisa de dump.

## Configuracoes sensiveis

O arquivo `.env` nao deve ser commitado com segredos.

Campos que podem precisar de preenchimento manual:

- `DISCORD_WEBHOOK_URL`
- `ML_CLIENT_ID`
- `ML_CLIENT_SECRET`
- `ML_REDIRECT_URI`
- `ML_ACCESS_TOKEN`
- `ML_REFRESH_TOKEN`

Sem esses campos, o setup basico roda. Algumas funcoes de notificacao/OAuth ficam limitadas.

## Como testar fontes fora do padrao

Para testar fonte experimental sem mudar o ciclo principal:

```powershell
docker compose run --rm api python scripts/collect_market_web.py --source shopee --query "mouse gamer" --max-results 3
docker compose run --rm api python scripts/collect_market_web.py --source aliexpress --query "mouse gamer" --max-results 3
```

Se a fonte gerar muitos bloqueios ou zero preco, mantenha fora do `config/config.yaml`.

## Se abrir este chat em outra maquina

Se a outra maquina tiver a pasta do projeto e Docker Desktop funcionando, o assistente consegue ajudar a configurar e validar o ambiente por este mesmo fluxo:

1. Conferir `.env`.
2. Rodar `SetupMotor.cmd`.
3. Validar `docker compose ps`.
4. Validar `GET http://127.0.0.1:8010/health`.
5. Rodar uma coleta controlada.
6. Verificar `StatusMotor.cmd`.

O que nao vai junto automaticamente e o banco local antigo. Para levar dados, use o dump descrito acima.
