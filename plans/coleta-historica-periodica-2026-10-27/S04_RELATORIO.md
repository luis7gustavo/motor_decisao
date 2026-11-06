# S04 — sincronização, enriquecimento, replay e reconciliação

Data: 27/10/2026, America/Sao_Paulo. **Estado: concluída offline em fonte sintética; missão histórica real bloqueada.** S03 era a dependência. S05 é a próxima etapa. Não houve GET PNCP, agenda, missão histórica real, reset de quota, alteração do job C2 nem escrita no banco principal.

## Implementação

- `app/procurement/history.py` aceita as duas rotas do contrato arquivado, publicação e atualização global, com partições, Bronze e orçamento comuns. A prioridade do backfill reserva parte do orçamento da missão; sincronização e reconciliação usam faixas separadas.
- `app/procurement/sync.py` planeja ciclos de seis horas com intervalos diários e recuperação limitada, sobreposição, marcos independentes por recurso/modalidade e avanço apenas por datas contíguas completas. A varredura ampla de 30 dias tem passadas distintas. Duas passadas filhas com identidades coincidentes e cobertura do pai produzem apenas `reconciliado_sem_snapshot`.
- `app/procurement/repository.py` registra cronologia oficial em cada observação e usa hash canônico v2 que exclui metadados temporais/rota da versão de negócio. Retificação antiga obtida hoje pode atualizar o processo; replay mais antigo, horário empatado ou sem fuso não regride o ponteiro atual. A data textual de origem é preservada.
- `app/procurement/enrichment.py` oferece fila durável por identidade/recurso/ciclo, com contrato de leitura validado como pré-condição, limite compartilhado, tentativa reservada antes do envio, Bronze de bytes originais e resultado `raw_only`. Não projeta itens/anexos sem parser validado nem declara documentação completa. Os quatro recursos de detalhe/itens/resultados/documentos seguem como lacunas explícitas.
- `scripts/radar_pncp_history.py` expõe `sync`, `reconcile`, `enrich`, `replay`, `status`, `audit` e `plan`, com bloqueio pré-rede quando a política não está pronta. `replay` usa apenas Bronze comprometido da própria missão e não consome tentativa externa. A agenda Prefect pertence à S05.
- Migração aditiva `20261027_0012` acrescenta `source_updated_at`, `source_time_text`, `observed_resource` e índice parcial nas observações. Observações legadas ficam sem timestamp inventado. O downgrade destrutivo é proibido.

## Provas executadas

| Prova | Resultado |
|---|---|
| Runner Docker, revisão `20261027_0012`, banco e role descartáveis | **181 testes aprovados, 1 aviso**, `pytest_exit_code=0`, limpeza sem erros. Manifesto `backups/20261027T235824Z_3fd05348.json`. |
| Retificação e cancelamento de processo publicado em período antigo | Rota de atualização global gerou nova versão; watermarks de publicação/atualização avançaram separadamente. |
| Janela antiga falha, janela posterior conclui | Watermark parou antes da lacuna; partição parcial permaneceu auditável. |
| 204 vazio | Identidade e versão válidas permaneceram. |
| Mesmo conteúdo com horário oficial novo; mudança tardia com horário antigo | Uma nova observação sem versão redundante; mudança tardia não regrediu o estado corrente. Horários empatado/sem fuso ficaram ambíguos. |
| Anexo A→B em dois ciclos | Dois corpos e hashes distintos retidos em Bronze; fila concluída como `raw_collected`, sem alegação de domínio completo. |
| Replay do Bronze com rede bloqueada | Zero chamadas e zero novas reservas; estado corrente não regrediu. |
| Paginação mutável | Duas passadas filhas coincidentes encerraram o pai como `reconciliado_sem_snapshot`; completude absoluta continuou falsa. |
| Orçamento compartilhado | Enriquecimento parou quando só restava a reserva mínima do backfill. |
| `plan` real bloqueado | `backups/s04-cli-plan-final.json`: 13 gates, zero chamadas/escritas, cobertura inicial e catálogo ainda não verificados, publicação/atualização global bloqueadas. |
| `status`/`audit` reais | `not_started`; `sync`, `reconcile`, `enrich` bloqueados antes da rede; nenhuma missão histórica ou schedule criado. |

## Proteção do C2 e do principal

`scripts/radar_pncp_s04_migrate.py` verificou sistema PostgreSQL, OID 2508600, role e revisões antes do dump; executou `pg_restore --list` e restauração integral em banco de ensaio descartável, comparou as 13 tabelas e limpou o ensaio. Só então aplicou `20261027_0012` ao C2. Evidência: `backups/c2/c2-20261013/s04-migration-report.json`; dump `s04-before-0012.dump`, 854.469.360 bytes, SHA-256 `0e409ee255b7be85d45db4fdf756f9322f78ba5ea76d27ce70601a2f408e39e9`.

Contagens C2 antes/depois: 3 jobs, 4 Bronze, 10 identidades, 10 versões, 10 observações; oito tabelas da missão nova vazias. Novo índice válido. Principal `20260817_0009`, sem schema `procurement`. Inventário S00 e verificador C2 executados novamente com exit 0; job `e47c6fe2-2cc9-4b58-803a-49a4aa44a443` permanece `partial/SourceChanged`, página 2. Ledger C2: 11 reservas, DOCUMENTAÇÃO 7/15, DADOS 4/50, calibração 2/3. A missão nova ainda tem orçamento externo zero e nenhuma tentativa.

## Comandos e saídas

```powershell
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T radar-ops tests --origin 20261027_0012
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_s04_migrate
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_s00_inventory
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_live_validation verify
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history plan
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history status
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history audit
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history sync --steps 1
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history reconcile --steps 1
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history enrich --steps 1
```

Testes, migração, inventário e verificador concluíram com exit 0. O CLI define exit 3 para missão bloqueada/não iniciada e 2 para erro de entrada/operação; nesta invocação por Compose/PowerShell os comandos bloqueados tiveram código não zero 1 no invólucro, com JSON explicando o bloqueio. `git diff --check` concluiu com exit 0.

## Limites, risco residual e próxima etapa

O OpenAPI arquivado descreve as duas listas, mas não prova contrato vigente, cobertura inicial, semântica dos filtros de data, catálogo de modalidades, taxa/quota oficial nem contratos de detalhe/itens/resultados/documentos. Manual local continua ausente; não foi buscado na rede. Nenhuma licitação histórica adicional foi coletada. Testes sintéticos comprovam as regras locais, não completude do universo nacional. Sem snapshot/controle independente, `reconciliado_sem_snapshot` mantém risco de omissões.

S05 deve preparar e testar o agendamento Prefect idempotente e a operação Docker sobre este mesmo destino e orçamento, mantendo aplicações reais bloqueadas até os gates da política serem resolvidos. Rollback S04: parar consumidores de sync/enrich (nenhum ativo hoje), manter Bronze, filas, watermarks e versões; usar o dump verificado somente para restauração em alvo segregado caso se comprove corrupção. Não executar downgrade destrutivo nem tocar o principal.
