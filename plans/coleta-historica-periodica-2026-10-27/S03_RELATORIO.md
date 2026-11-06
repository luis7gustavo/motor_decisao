# S03 — backfill histórico retomável e cobertura auditável

Data: 27/10/2026, America/Sao_Paulo. Escopo: somente S03 do `PROMPT_MESTRE.md`.

## Resultado

O worker de backfill foi implementado em `app/procurement/history.py` sobre as
tabelas e o histórico existentes. Partições por recurso, modalidade e janela têm
fim fixo, prioridade, cursor independente, claim com lease e fencing. Cada
tentativa passa pelo orçamento persistente compartilhado antes de chamar o
transporte. A URL é validada pela allowlist. O corpo e o resultado da tentativa
são gravados no Bronze antes da interpretação. Identidades oficiais, versões,
observações e cursor da partição/job são promovidos em uma única transação.
O checkpoint contém somente contagens, totais e estado; IDs observados ficam em
`control.source_partition_identities`.

O job de cada partição tem UUID determinístico derivado do UUID da partição,
permitindo ligar tentativas, Bronze e auditoria sem nova migração. Dado repetido
na página ou em página anterior, total mutável, página desaparecida e contagens
incompatíveis retêm o Bronze e deixam a partição em `reconcile` ou `partial`,
sem avançar o cursor. Uma janela com mais de um dia é subdividida em duas
partições filhas na mesma transação que registra `SourceChanged`. O pai continua
em `reconcile`; filhos completos não são descritos como prova de integralidade.
Em janela de um dia, o erro permanece para reconciliação posterior. Respostas
429/5xx e falhas de transporte mantêm `not_before` compartilhado; `Retry-After`
válido é respeitado, e três falhas transitórias consecutivas abrem espera local
de pelo menos 15 minutos. Cada URL tem no máximo três tentativas de ponta a
ponta, inclusive após reinício.

`scripts/radar_pncp_history.py` oferece `plan`, `backfill`, `status` e `audit`.
`plan` e `--dry-run` não acessam a API nem o banco. `status/audit` verificam a
identidade do principal e do alvo C2 antes de leitura. `backfill` exige política
geral habilitada, cobertura inicial e catálogo de modalidades comprovados,
semântica de datas validada, hash do contrato, escopo estável do limitador,
orçamento e gate de publicação. A política atual não cumpre esses critérios:
`backfill` retorna bloqueio antes de abrir o destino ou fazer requisição.

## Prova executada

| Verificação | Resultado |
|---|---|
| Runner Docker, revisão `20261027_0011` em banco/role descartáveis | **172 testes aprovados**, 1 aviso Starlette/httpx, `pytest_exit_code=0`; manifesto `backups/20261027T233544Z_363a13f0.json`; role de teste com 0 privilégios nas tabelas do principal; limpeza sem erros |
| Fonte sintética | 2 modalidades, 2 janelas, página vazia confirmada e 2 páginas com IDs; Bronze/versões/identidades/cursor coerentes; checkpoint menor que 512 bytes |
| Reinício de processo | Um processo concluiu a página 1; outro retomou a página 2; 2 tentativas e 2 identidades, sem terceira busca |
| Concorrência entre processos | Dois processos disputaram 1 tentativa para 2 partições; somente 1 recebeu reserva; contador permaneceu em 1 |
| Espaçamento entre processos | Dois processos disputaram 2 partições com intervalo persistente de 60 segundos; só 1 enviou, o outro recebeu `deferred`, sem consumir segunda tentativa |
| Crash após Bronze | Bronze e recibo sobreviveram; lease expirado foi retomado na mesma página com nova reserva; uma identidade/projeção lógica, duas respostas Bronze |
| Paginação mutável | Bronze da página 2 retido, pai sem avanço, duas filhas criadas e processadas; pai continuou `reconcile` |
| Total estável com ID repetido | `repeated_identity_across_pages`, Bronze mantido, cursor na página 2 |
| 429 | Uma reserva e um Bronze; `not_before` persistente impediu segundo envio sem devolver quota |
| Missão desabilitada | Nenhum job, tentativa ou envio criado |
| `plan` real | 12 gates explícitos, `network_calls=0`, `database_writes=0`, saída interna 3 (`blocked`) |
| `backfill` real | Bloqueado com `network_calls=0`, saída interna 3 |
| `status` real | `not_started` para a missão histórica; nenhuma escrita |
| Inventário final C2/principal | Principal `motor_decisao` em `20260817_0009`, sem schema `procurement`; C2 OID 2508600 em `20261027_0011`, 3 jobs, 4 Bronze, 10 identidades/versões/observações |
| C2 preservado | Job `e47c6fe2-2cc9-4b58-803a-49a4aa44a443` `partial/SourceChanged`, página 2; ledger 11 reservas, DOCUMENTAÇÃO 7/15, DADOS 4/50, calibração 2/3 |

`git diff --check` passou. Não houve GET PNCP, migração, job histórico no C2,
schedule, reset do ledger, escrita no principal ou alteração do mapa visual.
A suíte usa respostas sintéticas em banco descartável; não prova cobertura real.

## Comandos observados

Executar no checkout `motor_decisao` em PowerShell. O Compose usa o Docker e
`--entrypoint python` porque o serviço `radar-ops` tem entrypoint próprio.

```powershell
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T radar-ops tests --origin 20261027_0011
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history plan
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history status
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history audit
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_history backfill --steps 1
```

`plan` e `backfill` retornam 3 enquanto os gates estiverem bloqueados;
`status` retorna 3 quando a missão ainda não foi criada; 0 indica operação
concluída; 2 indica entrada ou operação inválida. `audit` também retorna 3
antes do manifesto existir. Nenhuma dessas saídas bloqueadas consome quota.

## Limites e próxima etapa

A política `pncp-history-mission-policy.json` permanece `enabled=false`, com
orçamento externo zero. Data inicial recuperável, modalidades, limites atuais,
semântica das bordas, escopo do rate limit e contratos complementares ainda
carecem de prova. Não há fonte real habilitada nem dados históricos novos no
C2. `pagination_consistent_unverified` é a classificação máxima dos filhos
sintéticos completos: sem snapshot/controle independente, a API paginada pode
omitir identidades mesmo com totais estáveis.

S04 deve implementar sincronização, enriquecimento, replay e reconciliação,
incluindo fechamento dos pais em `reconcile` somente com critério verificável.
Deve compartilhar o orçamento S02/S03 e não avançar watermarks sobre lacunas.
S06 real e S07 seguem bloqueadas pelos gates da S00. Rollback: interromper
consumidores e manter partições, Bronze, histórico e reservas; nenhum downgrade
ou limpeza destrutiva.
