# Operação da agenda PNCP histórica no Prefect

## Estado operacional vigente — 03/11/2026

A missão roda na pilha **dedicada** `compose.radar-history-dedicated.yml`. O Redis compartilhado voltou a reiniciar; o worker antigo `radar-history-worker` está parado e o flow bloqueia o URL do Prefect compartilhado antes de qualquer coleta. Os comandos da seção histórica abaixo descrevem o ensaio de 28/10 e **não são o procedimento atual de operação**. Não executar `prefect-init` nem reparar o Redis compartilhado como parte desta missão.

As agendas dedicadas estão ativas: backfill **a cada hora** (até 6 páginas por run), sync às 00/06/12/18 em `America/Sao_Paulo` e reconciliação às segundas 03:30. A concorrência permanece 1, com intervalo mínimo de 1 segundo. O teto cumulativo autorizado em 03/11 é **20.000 tentativas da missão**, incluindo as 2.000 anteriores, dentro de **20.099 da fonte**, incluindo 99 tentativas anteriores de outras missões. O limitador consulta o ledger persistente antes de cada reserva e impõe **200 tentativas por dia civil de São Paulo**, contadas no escopo compartilhado da fonte; ao atingir o limite diário, adia sem enviar requisição até a meia-noite local. O teto cumulativo não é renovado automaticamente.

PostgreSQL e Redis dedicados usam volumes Docker nomeados; servidor, serviços e worker têm `restart: unless-stopped`. O Prefect persiste as três agendas e o worker volta após o Docker reiniciar. Com o PC ou Docker desligado, não há coleta; execuções antigas além da janela configurada são ignoradas. O cliente PNCP da raiz permanece travado por `backups/pncp-control/exclusive.lock` durante a missão. O run manual de verificação pelo deployment `61033e41-7e06-4087-9f68-e07ccfcf972d` em 03/11 confirmou worker e ledger após a ampliação: três páginas de publicação persistidas e uma atualização adiada por HTTP 500, `business_outcome=partial_or_deferred`. A cobertura de negócio continua parcial. Ver `plans/coleta-historica-periodica-2026-10-27/STATUS.md`.

Execute no checkout `motor_decisao`:

```powershell
docker compose -f docker-compose.yml -f compose.radar-history-dedicated.yml --profile control --profile radar-history-dedicated ps
docker compose -f docker-compose.yml -f compose.radar-history-dedicated.yml --profile control --profile radar-history-dedicated run --rm -T --entrypoint python radar-history-dedicated-ops -m scripts.radar_pncp_schedule status
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history status
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history audit
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm --no-deps -T --entrypoint python radar-ops -m scripts.radar_pncp_history audit --summary-only
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_s06_discover status
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm --no-deps -T --entrypoint python radar-ops -m scripts.radar_pncp_s08_resource_plan
```

Use `audit --summary-only` para acompanhamento frequente; `audit` sem a opção mantém as linhas de cada partição para investigação. `historical_partition_states` conta o marco fixo até 26/10 e suas divisões; `recurring_partition_states` conta os ciclos posteriores de sync/reconciliação. `coverage_status` agrega os dois; `historical_coverage_status` acompanha o aceite S08, incluindo as lacunas de recursos. Uma partição `complete` continua sem prova de enumeração integral independente.

`radar_pncp_s08_resource_plan` confere os hashes/recibos dos OpenAPI arquivados, as rotas GET documentadas, a projeção local das identidades históricas, a fila de enriquecimento e os saldos compartilhados. É somente leitura e não habilita detalhe, itens, resultados ou anexos. O orçamento restante é um teto superior: a coleta e a sincronização seguem consumindo tentativas. A rota documentada e os campos locais são candidatos, não validação de paginação, download nem cobertura por recurso.

Para conter um problema, pausar primeiro o deployment dedicado, esperar o run em curso encerrar e depois pausar a fonte; isso mantém todos os dados e tentativas:

```powershell
docker compose -f docker-compose.yml -f compose.radar-history-dedicated.yml --profile control --profile radar-history-dedicated run --rm -T --entrypoint python radar-history-dedicated-ops -m scripts.radar_pncp_schedule pause
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_s06_activate_history pause
```

Para retomar após verificar integridade, limite, worker `ONLINE` e trava exclusiva, retomar fonte e então deployment com os mesmos dois comandos e `resume`. O `status` deve mostrar três agendas com `parameters.mode` iguais a `backfill`, `sync` e `reconcile`. Se aparecer `{}`, não retomar; registrar drift. O `Completed` do Prefect só prova conclusão do processo, não cobertura dos dados.

## Histórico S05/S06 — instruções antigas preservadas para rastreio

Estado em 28/10/2026: implementação e ensaio isolado da S05 concluídos; validação offline da S06 ampliada. O servidor Prefect compartilhado está sem scheduler saudável por falha no Redis AOF; nenhuma agenda real da missão foi aplicada ou ativada. A política da fonte permanece bloqueada e com orçamento externo zero.

## Serviços e limites

`compose.radar-history.yml` adiciona apenas os serviços da missão e uma pilha de ensaio separada. O worker real usa pool `radar-pncp-history`, fila `history` e limite 1. O deployment `radar-pncp-history/mission` tem três agendas, inicialmente pausadas: backfill a cada 30 minutos, sync às 00/06/12/18 em `America/Sao_Paulo` e reconciliação às segundas 03:30. São valores locais em `config/contracts/pncp-history-schedule.json`, não limites oficiais da PNCP. Cada fluxo usa lotes finitos, sem retry Prefect, com limitador persistente da missão; execuções agendadas antigas são ignoradas antes da rede. Prefect é o único agendador deste pipeline.

O flow publica um artefato `pncp-history-business-outcome` e logs com `blocked_contract`, `skipped_stale_schedule`, `partial_or_deferred`, `failed` ou `batch_completed`. Após cada lote com política habilitada, executa `audit` local e somente leitura, mesmo se o lote foi parcial. O artefato informa a classificação conservadora da cobertura; `pagination_consistent_unverified` não significa enumeração integral. O estado `Completed` do Prefect não significa cobertura histórica completa.

## Comandos PowerShell para o destino real

Execute na raiz `motor_decisao`. Não rode `prefect-init`: `scripts/init_prefect.py` chama `alembic upgrade head` e pode atingir o principal. Não use `docker compose up` indiscriminado com o perfil `control`.

```powershell
docker compose -f docker-compose.yml -f compose.radar-history.yml --profile control --profile radar-history config --quiet
docker compose -f docker-compose.yml -f compose.radar-history.yml --profile control --profile radar-history run --rm --no-deps -T --entrypoint python radar-history-ops -m scripts.radar_pncp_schedule plan
docker compose -f docker-compose.yml -f compose.radar-history.yml --profile control --profile radar-history run --rm --no-deps -T --entrypoint python radar-history-ops -m scripts.radar_pncp_schedule apply
docker compose -f docker-compose.yml -f compose.radar-history.yml --profile control --profile radar-history run --rm --no-deps -T --entrypoint python radar-history-ops -m scripts.radar_pncp_schedule status
docker compose -f docker-compose.yml -f compose.radar-history.yml --profile control --profile radar-history run --rm --no-deps -T --entrypoint python radar-history-ops -m scripts.radar_pncp_schedule pause
docker compose -f docker-compose.yml -f compose.radar-history.yml --profile control --profile radar-history run --rm --no-deps -T --entrypoint python radar-history-ops -m scripts.radar_pncp_schedule resume
```

`plan` e `status` são somente leitura. `apply` cria/atualiza somente pool, fila e deployment da missão, mantendo tudo pausado enquanto a política estiver bloqueada. Repita `apply` e compare IDs/slugs no `status`; não devem duplicar. `pause` afeta só este deployment. `resume` exige política pronta, destino C2 validado e worker online; com os gates atuais retorna bloqueio. O CLI define exit 0 para sucesso, 3 para bloqueio e 2 para falha; o invólucro Compose/PowerShell desta máquina reportou 1 para alguns bloqueios com JSON explicativo.

Após o conserto separado do Redis compartilhado e do servidor Prefect, aplique o deployment pausado. Somente após os gates de fonte, contrato, cobertura, modalidades, orçamento e destino, inicie o worker específico:

```powershell
docker compose -f docker-compose.yml -f compose.radar-history.yml --profile control --profile radar-history up -d --no-deps radar-history-worker
```

Confira `status` e saúde do worker; então use `resume` apenas na etapa de ativação autorizada. Para parar ou reverter, `pause` primeiro e depois `docker compose -f docker-compose.yml -f compose.radar-history.yml --profile control --profile radar-history stop radar-history-worker`. Isso conserva Prefect DB, volumes, Bronze, filas, watermarks e orçamento. Em caso de PC/Docker desligado, o scheduler pode deixar execuções antigas; o flow as ignora por idade e o sync recupera apenas intervalos limitados pelo cursor. Um teste curto de reinício não comprova disponibilidade 24/7.

## Ensaios S05 e pendências

A pilha `prefect-history-test-*` usa PostgreSQL/Redis próprios e o flow `radar-pncp-s05-probe`; o probe escreve apenas um checkpoint em `backups/s05-prefect-probe/state.json`. O ensaio aplicou o probe duas vezes com um deployment/uma agenda, testou `resume`, execução agendada pelo worker, reinício do worker e `pause`. Também aplicou duas vezes o deployment da missão na pilha de teste: um deployment, três agendas inativas e limites 1 em pool/fila/deployment. A tentativa de `resume` da missão foi bloqueada por 13 gates; o flow retornou `blocked_contract`, sem comandos de coleta. Os serviços de ensaio foram parados sem remover volumes.

O runtime compartilhado tem Redis com `Bad file format reading the append only file ... incr.aof`, e o Prefect API devolveu erro ao tentar criar o pool real. Não reparar o AOF por truncamento nem substituir o Redis da aplicação sem backup e procedimento próprio: isso atinge outros fluxos. A imagem compartilhada também está em Prefect 3.8.8/SQLAlchemy 2.1.4, combinação que reproduziu falha de agendamento no ensaio. `requirements.txt` fixa Prefect 3.8.8 e SQLAlchemy 2.0.49 para rebuild futuro; a imagem isolada `radar-history-prefect:3.8.8-sa2.0.49` comprovou o scheduler e verifica as versões no build. O servidor compartilhado ainda não foi reconstruído.

Antes da S06/S07 reais, resolver e comprovar: Redis compartilhado íntegro, scheduler/worker saudáveis, contratos e limites PNCP atuais, cobertura inicial e catálogo de modalidades, semântica de datas, orçamento externo autorizado e contratos de enriquecimento. Nenhuma parte deste runbook autoriza GET PNCP ou ampliação do C2.

## Validação offline S06 e auditoria

Em 28/10/2026, o runner isolado passou 186 testes, incluindo espera durável de `Retry-After` em segundos e data HTTP, teto de três tentativas após reinícios, e auditoria após lote parcial. O backup C2 `s06-current-0012.dump` foi restaurado integralmente em banco descartável; `s06-restore-report.json` registra SHA-256, contagens e impressões digitais iguais. O Prefect de ensaio executou o flow `sync` com resultado `blocked_contract`, `commands=[]` e zero chamadas PNCP; seus serviços foram parados sem apagar volumes. O estado real segue `not_started`.

Comandos de inspeção e restauração já executados na raiz do checkout:

```powershell
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history plan
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history status
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history audit
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_s06_restore
```

O último comando é de uso único: recusa sobrescrever o backup ou o relatório existente. Para uma nova restauração, crie um procedimento com novo nome de evidência, mantendo o backup S06 intacto. Enquanto a política estiver bloqueada, `plan`, `status` e `audit` retornam estado de bloqueio/não iniciado; nenhum deles habilita coleta.

Em 28/10/2026, uma pesquisa documental autorizada arquivou OpenAPI atuais em `config/contracts/pncp-consulta-20261028.json` e `pncp-integracao-20261028.json`. O contrato de Consulta foi revalidado estruturalmente, mas o `plan` ainda mostra 12 gates e zero orçamento de dados. Consulte `docs/licitacoes/fontes/pncp-consulta-2026-10-28.md`. O PDF do manual de consultas não foi arquivado; a tentativa ficou registrada sem promover seu conteúdo a evidência local. A missão e a agenda real continuam desativadas.
