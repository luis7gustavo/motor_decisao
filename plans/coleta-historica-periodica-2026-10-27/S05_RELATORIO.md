# S05 — agenda Prefect e operação Docker

Data: 27/10/2026, America/Sao_Paulo. **Estado: implementação e ensaio isolado concluídos; aplicação no Prefect compartilhado pendente por falha de infraestrutura.** Dependência S04 cumprida. Próxima etapa S06, cuja prova real continua bloqueada. Nenhum GET PNCP, job histórico real, migração, reset de quota, ativação de agenda da missão ou escrita no principal/C2.

## Implementação

- `scripts/radar_pncp_schedule.py`: `plan/apply/pause/resume/status` para pool, fila e deployment específicos. `apply` reconcilia slugs e parâmetros das agendas, sem duplicá-las; `pause` afeta somente o deployment da missão; `resume` exige política pronta, alvo C2 validado e worker online. Saída JSON sanitizada; exit 0 sucesso, 3 bloqueio, 2 erro. `--rehearsal` só funciona no servidor de ensaio separado.
- `pipelines/procurement/history_flow.py`: um flow Prefect com modos finitos `backfill`, `sync`, `reconcile`, `retries=0`, gate de política antes do DB/rede, recusa de execução agendada muito atrasada e artefato de resultado de negócio. `Completed` do Prefect não afirma cobertura. `pipelines/procurement/history_probe.py` é flow sintético sem PNCP/DB de licitações, com IDs únicos em checkpoint persistente.
- `config/contracts/pncp-history-schedule.json`: backfill 30 minutos; sync 00/06/12/18; reconciliação segunda 03:30, `America/Sao_Paulo`, limites 1 em pool/fila/deployment, no máximo 3 runs agendados por agenda e lotes finitos. Valores locais configuráveis, não quota/limite oficial.
- `compose.radar-history.yml`: worker/ops da missão e pilha Prefect de ensaio com PostgreSQL/Redis próprios. Nenhum serviço depende de `prefect-init`; não houve bootstrap. `docker/Dockerfile.radar-history-prefect` usa imagem derivada isolada com SQLAlchemy 2.0.49 e verifica as versões no build. `requirements.txt` fixa Prefect 3.8.8 e SQLAlchemy 2.0.49 para reconstruções futuras, sem reconstruir a imagem compartilhada nesta sessão. Runbook em `docs/licitacoes/operacao-coleta-historica-prefect.md`.

## Interface instalada e diagnóstico

Prefect instalado: **3.8.8**. Foram inspecionadas as assinaturas locais de `Flow.to_deployment`, `RunnerDeployment.apply`, `PrefectClient` para pool/fila/deployment/schedules/runs, e modelos `DeploymentScheduleCreate`, `CronSchedule`, `IntervalSchedule`. A [documentação oficial de agendas](https://docs.prefect.io/v3/concepts/schedules) distingue agenda ativa de execução e documenta cron/intervalo/fuso. `scripts/init_prefect.py` foi lido: chama `alembic upgrade head`, portanto não foi executado.

O Redis compartilhado `motor_redis` reinicia por `Bad file format reading the append only file appendonly.aof.29.incr.aof`; `prefect-services` também reinicia. `apply` contra o Prefect compartilhado retornou `PrefectHTTPStatusError` antes de criar pool/fila/deployment. `backups/s05-live-prefect-status.json` confirma todos ausentes. O AOF/volume compartilhado foi preservado; não houve tentativa de truncamento ou reset.

No primeiro ensaio isolado, Prefect 3.8.8 com SQLAlchemy 2.1.4 reproduziu `InvalidRequestError: Can't evaluate bulk DML statement` no scheduler. A [issue oficial Prefect #23199](https://github.com/PrefectHQ/prefect/issues/23199) descreve essa combinação e a retomada com SQLAlchemy 2.0.49. Após derivar a imagem com Prefect 3.8.8/SQLAlchemy 2.0.49 e recriar apenas os serviços de ensaio, o scheduler gerou e o worker executou runs agendados.

## Evidências do ensaio isolado

| Prova | Resultado observado |
|---|---|
| Compose | `docker compose ... config --quiet` exit 0. |
| `apply --rehearsal` duas vezes | Mesmo deployment `58469a73-1981-46b8-acf7-b333a5811a61`, uma agenda `probe-minute`, sem duplicação. `resume` e `pause` exit 0. |
| Worker e scheduler | Worker `pncp-history-s05-test-pc1` `ONLINE`; três runs agendados `Completed` em `backups/s05-probe-final.json`. Checkpoint `backups/s05-prefect-probe/state.json` guardou IDs únicos; após reinício do worker houve execução adicional. `pause` deixou deployment pausado, agenda inativa e contador estável durante a observação seguinte. O teste posterior do gate de worker em `resume` também passou, seguido de nova pausa. |
| Deployment da missão no Prefect de ensaio | `apply` duas vezes preservou ID `e5bcc8c9-af8a-47a4-8c1a-718ebe713a76`, três agendas inativas com parâmetros `backfill/sync/reconcile`, pool/fila/deployment limitados a 1. `backups/s05-mission-test-final.json`: pausado, zero runs da missão. `resume` bloqueou por 13 gates. |
| Flow real em modo bloqueado | `backups/s05-history-flow-blocked.txt`: `business_outcome=blocked_contract`, `commands=[]`, `network_calls_when_blocked=0`; artefato de negócio criado no Prefect de ensaio. |
| Persistência/reinício | Probe manteve IDs em volume após reinício do worker. O limitador de fonte e cursor já tiveram reinício de processo/concorrência verificados na S03; nesta S05 não foi declarado novo teste de quota real, pois a missão segue bloqueada. |
| Integridade C2/principal | `backups/s05-c2-inventory.json`: C2 OID 2508600/revisão `20261027_0012`, 3 jobs, 4 Bronze, 10 identidades/versões/observações; principal `20260817_0009` sem schema `procurement`. `backups/s05-c2-verify.json`: integridade válida, negócio `partial`, cobertura não verificada, zero issues/warnings. |

Após o ensaio, worker, servidor, serviços, Redis e PostgreSQL **do teste** foram parados sem remover volumes. O último checkpoint do probe tem quatro IDs únicos porque houve uma segunda retomada para testar o gate de worker. Nenhum run da missão real foi criado. `git diff --check` e Compose `config --quiet` passaram.

## Comandos executados e limites de aceite

Os comandos completos de operação estão no runbook. Principais verificações:

```powershell
docker compose -f docker-compose.yml -f compose.radar-history.yml --profile control --profile radar-history --profile radar-history-test config --quiet
docker compose -f docker-compose.yml -f compose.radar-history.yml --profile control --profile radar-history-test up -d prefect-history-test-postgres prefect-history-test-redis prefect-history-test-server prefect-history-test-services radar-history-test-worker
docker compose -f docker-compose.yml -f compose.radar-history.yml --profile control --profile radar-history-test stop radar-history-test-worker prefect-history-test-services prefect-history-test-server prefect-history-test-redis prefect-history-test-postgres
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_s00_inventory
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_live_validation verify
```

O CLI definiu exit 3 para bloqueios; alguns `docker compose run` no invólucro PowerShell retornaram 1 com JSON de bloqueio. `apply` no Prefect compartilhado teve falha (exit 2 no CLI) por infraestrutura; não foi tratado como aplicação bem-sucedida. O teste isolado prova a implementação, idempotência e execução sintética na combinação corrigida, **não** a saúde do Prefect compartilhado nem recorrência real da missão. Uma pausa/reinício curto não demonstra operação 24/7 nem ausência de backlog em indisponibilidade prolongada; lotes, prioridade, cursor e gate de idade reduzem o risco de rajada, sujeitos a validação operacional futura.

## Pendências e rollback

Antes de qualquer aplicação real/ativação, reparar o Redis compartilhado por procedimento separado que preserve seu AOF, reconstruir/validar o servidor Prefect com SQLAlchemy compatível e comprovar scheduler/worker saudáveis. A política PNCP ainda tem 13 gates e orçamento externo zero; contratos de detalhe/itens/resultados/documentos continuam pendentes. Não buscar manual PNCP pela rede nem prometer completude histórica.

Rollback desta etapa: `pause` somente do deployment da missão e `stop` apenas do worker específico; conservar volumes Prefect, histórico, filas, watermarks, Bronze e quotas. No momento não há deployment/worker real da missão para pausar. A pilha de ensaio está parada e seus volumes preservados.
