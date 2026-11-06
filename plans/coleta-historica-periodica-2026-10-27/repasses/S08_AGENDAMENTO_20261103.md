# Repasse S08 — coleta automática persistente em 03/11/2026

## Objetivo e autorização

O usuário pediu agendamentos persistentes para coleta automática constante e escolheu 200 tentativas por dia civil de `America/Sao_Paulo` e teto cumulativo 20.000 para a missão. A agenda Prefect dedicada já existia, mas as tentativas 2.000/2.000 impediam novos GETs. Esta sessão ampliou o teto sem reset, distribuiu o backfill ao longo do dia e manteve os três modos na pilha dedicada. O marco histórico S08 continua aberto.

## Proteção e evidência antes de escrever

- Deployment dedicado pausado e fonte/mission pausadas em 2.000/2.000 e 2.099/2.100. Zero reservas sem outcome; `backups/pncp-control/exclusive.lock` preservado com dono `pncp-history-2026-10-27`.
- C2 `test_20261020_ba894ce71405`, OID 2508600, revisão `20261027_0012`; principal `motor_decisao`, revisão `20260817_0009`, sem schema `procurement`.
- `backups/c2/c2-20261013/history-20261103-prebudget-current-0012.dump`: 924.589.639 bytes, SHA-256 `da06c0d12b585681a1c91831151aea4dc8c14b6818d19a2d78df06124f7571a5`. Relatório de restauração adjacente: `restored_equal=true`, `full_restore_exit_code=0`, fingerprints iguais, zero hashes Bronze inválidos, base de ensaio removida.
- Primeira tentativa de backup no worker não encontrou `pg_dump`, sem criar dump. A execução correta ocorreu no serviço `radar-ops`, com `--entrypoint python`; exit 0.

## Alterações

- `app/procurement/source_control.py`: reserva consulta tentativas do escopo compartilhado no dia civil local, sob lock do orçamento. Ao chegar a 200, retorna `deferred` até o próximo dia sem reservar nem enviar GET. Ampliação de teto e escopo diário é atômica, requer fonte/missões pausadas, contadores esperados e todos os outcomes conciliados.
- `app/procurement/history.py`, `scripts/radar_pncp_history.py`, `scripts/radar_pncp_schedule.py`, `scripts/radar_pncp_s06_activate_history.py`: política diária propagada e conferida em plano, persistência, reserva, retomada e agenda.
- `config/contracts/pncp-history-mission-policy.json`: missão 20.000, fonte 20.099, 200/dia. `config/contracts/pncp-history-schedule.json`: backfill horário com até 6 passos; sync 6 horas; reconciliação semanal. Concorrência 1 e intervalo mínimo 1 segundo.
- `scripts/radar_pncp_s08_continuous_budget.py`: comando único auditado, com verificação do backup/restauração e baseline exato. Recibo `backups/pncp-history-budget/20261103-continuous-budget.json`.
- `tests/test_source_control.py`, `tests/test_history_policy_plan.py`, runbook e `STATUS.md` atualizados. A agenda antiga `backfill-half-hour`, inativa, foi removida após a nova `backfill-hourly` ser aplicada; três agendas permaneceram.

## Validação e estado final

- `docker compose -f docker-compose.yml -f compose.radar-history-dedicated.yml --profile radar-history-dedicated config --quiet`: exit 0.
- `radar-ops tests --origin 20261027_0012` em alvo descartável: **212 passed, 1 warning**, exit 0, manifesto `backups/20261103T171640Z_9b70824e.json`, sem erros de limpeza.
- `radar_pncp_history plan`: `ready=true`, zero gates, zero rede/escritas. Ampliação no C2 pausado: missão 2.000/20.000, fonte 2.099/20.099, limite diário 200, zero outcomes faltantes, zero GETs.
- Fonte e deployment retomados sem reset. `radar_pncp_schedule status`: deployment ativo `9183361f-fe0e-4a10-8469-d348f765fdb7`, worker `ONLINE`, três schedules ativos com modos corretos, backfill 3.600 segundos, sync 00/06/12/18 locais e reconciliação segunda 03h30. Próximo backfill automático agendado para 03/11 às 15h locais; ainda não observado nesta sessão.
- Um passo direto de backfill de publicação: página 72 `page_committed`, exit 0. Run manual de verificação pelo deployment `61033e41-7e06-4087-9f68-e07ccfcf972d`: worker submeteu, três páginas de publicação `page_committed`, atualização adiada após HTTP 500; Prefect `Completed`, negócio `partial_or_deferred`. Cinco novas reservas/outcomes no dia, missão 2.005/20.000 e fonte 2.104/20.099. Não interpretar `Completed` como cobertura.
- Auditoria S08: `partial`, 108 partições completas, 610 pendentes, 22 em reconciliação; detalhes, itens, resultados e documentos continuam lacunas explícitas.

## Próxima verificação e reversão

Verificar o primeiro disparo **automático** da agenda horária após 15h locais, correlacionando `parameters.mode=backfill`, logs de negócio, ledger e Bronze. Depois acompanhar a distribuição diária e as partições de atualização com 500/504; o teto de 20.000 é finito e nova ampliação exige pausa, fotografia de ledger e backup/restauração auditados. Para conter problema, pausar primeiro o deployment dedicado, aguardar run em curso e pausar fonte/mission; preservar C2, Bronze, recibos, cursor e trava exclusiva. Não tocar no banco principal, no worker compartilhado nem no Redis compartilhado.
