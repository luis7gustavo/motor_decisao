# S01 — paginação mutável, verificador e estado parcial tipado

Data: 27/10/2026, America/Sao_Paulo. Resultado: **concluída offline**. Dependência S00 concluída documentalmente. Nenhum GET PNCP foi feito nesta etapa; nenhuma agenda foi criada ou ativada.

## Diagnóstico reproduzido

Antes da correção, o `verify` do C2 terminou com exit 1 apenas por exigir 3 respostas Bronze e DADOS 3/50, enquanto havia 4 respostas e DADOS 4/50. Dois testes de regressão no runner descartável reproduziram o segundo erro: totais 9577/958 → 9562/957 e ID repetido entre páginas com totais estáveis terminavam em `partial/ValueError`. Resultado de referência: **2 falhas, 145 aprovações**. O problema era a exceção genérica em `Store.apply_page`, seguida por `Collector.run` que guardava somente seu tipo.

## Correção e limites

- `SourceChanged` carrega metadados sanitizados: motivo, página, ID Bronze, totais esperados/observados, política de reconciliação e limite local de duas reexecuções futuras. Não guarda corpo ou credencial no checkpoint.
- A resposta Bronze é confirmada antes de aplicar a página. Se totais mudam, página some, sequência diverge ou ID reaparece, o job fica `partial/SourceChanged`; a transação da página não promove registros nem avança o cursor. `Collector.run` bloqueia nova tentativa automática desse job. Uma futura reconciliação deve criar partição/job filho com genealogia e orçamento compartilhado; **isso ainda não foi implementado nem autorizado para rede**.
- O flow manual expõe `business_outcome=reconciliation_required` e `reconciliation_required=true` quando aplicável. Prefect pode registrar o flow como `Completed`; esse estado de orquestração não altera o resultado de negócio parcial. O comando manual `collect` retorna exit 2 para parcial não amostral, preservando exit 0 da parada intencional `sample_page_limit`.
- A classificação explícita `classify` reexaminou apenas a página 2 já preservada e tipou o job C2 existente. Antes/depois: mesmo job, `next_page=2`, `request_count=2`, `records_seen=10`, 4 Bronze e 10 identidades/versões/observações. Somente `error_code` e metadados de causa foram atualizados. Não houve reserva nova no ledger.
- A conclusão técnica de paginação marca `coverage_status=pagination_consistent_unverified`. Totais estáveis com IDs repetidos são detectados; omissão sem duplicata pode ser invisível sem snapshot ou enumeração independente. Por isso sucesso do loop não significa cobertura integral.

## Verificador

O `verify` agora confere os números reais da sessão, IDs de job e escopo, sequência e contadores do ledger, arquivos de recibo por SHA-256/tamanho, correspondência multiconjunto entre respostas de dados e Bronze, bytes/hash Bronze, páginas confirmadas antes do cursor e observações correlacionadas por job. Não usa as constantes 3 ou 10 como aceite. Informa `evidence_integrity_valid`, `collection_business_state` e `coverage_verified` separadamente. Testes com 1, 2 e 4 páginas reconciliadas passam; bytes adulterados, observação faltante e cursor avançado sem Bronze falham. O antigo C2 podia ser lido como `legacy_untyped_pagination_change`; após `classify`, a auditoria não tem advertências.

## Validação executada

| Comando/ensaio | Resultado |
|---|---|
| `docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps radar-ops tests --origin 20261013_0010` | 153 aprovados, 1 aviso de depreciação Starlette/httpx, exit 0; manifesto `backups/20261027T173116Z_53390ed2.json`; runner descartável sem privilégios no principal e com limpeza sem erros |
| `... --entrypoint python radar-ops -m scripts.radar_pncp_live_validation classify` | exit 0; job C2 tipado offline, contadores/cursor invariantes |
| `... --entrypoint python radar-ops -m scripts.radar_pncp_live_validation verify` | exit 0; `evidence_integrity_valid=true`, `collection_business_state=partial`, `coverage_verified=false`, `issues=[]`, `warnings=[]` |
| `... --entrypoint python radar-ops -m scripts.radar_pncp_s00_inventory` | exit 0; principal `20260817_0009` sem `procurement`, destino `20261013_0010` com OID 2508600, 4 Bronze e ledger 7/15 + 4/50 |

Quotas antes/depois: DOCUMENTAÇÃO **7/15 → 7/15**, DADOS **4/50 → 4/50**, calibração **2/3 → 2/3**; 11 reservas em ambos. Chamadas externas antes/depois da S01: **0/0**. Não houve migração, reset, limpeza de volume, alteração no principal, job novo ou schedule.

## Revisão crítica e próximo passo

Há proteção contra promoção/avanço após divergência e contra replay automático do mesmo job. A página 2 retida não foi normalizada. A ausência de snapshot e de contrato atualizado impede afirmar cobertura integral, mesmo quando totais parecem estáveis. O limite de reexecuções está registrado, mas o mecanismo de filhos e de limitador compartilhado depende de S02/S03; não trate o metadado como permissão de uso da API. S02 deve projetar controle persistente aditivo, sem um segundo histórico e somente em alvo segregado validado, com backup/restauração antes de migração.
