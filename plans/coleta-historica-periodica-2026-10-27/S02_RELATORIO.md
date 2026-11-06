# S02 — controle e histórico persistentes

Data: 27/10/2026, America/Sao_Paulo. Escopo: apenas S02 do `PROMPT_MESTRE.md`.

## Resultado

Foi mantido o histórico C1 como fonte única de identidades, versões e observações.
A revisão aditiva `20261027_0011` criou controle durável para orçamento
compartilhado, missão, partições, watermarks, fila e reservas de tentativas. O
destino segregado C2 foi migrado de `20261013_0010` para `20261027_0011` após
backup e restauração integral verificados. O principal `motor_decisao` permaneceu
em `20260817_0009`, sem schema `procurement` e sem privilégios da role C2 nas
tabelas do principal.

O módulo `app/procurement/source_control.py` reserva cada tentativa em transação
antes de qualquer transporte. Orçamento e missão começam bloqueados, com limite
zero. A linha do orçamento é bloqueada antes da missão; consumo, intervalo mínimo,
concorrência e reserva imutável são persistidos na mesma transação. O método de
registro de resultado é idempotente e liga um recibo Bronze apenas ao job da
reserva. `not_before` só aumenta. Partições e itens da fila possuem claim com
`SKIP LOCKED`, lease e token de fencing. O cursor da nova partição não avança
isoladamente: a promoção atômica da página será implementada junto do worker S03.
O checkpoint novo contém apenas cursor/estado; identidades por partição ficam em
tabela com chave e índices. O payload da fila tem limite de 8192 bytes.

O repositório C1 agora decide o ponteiro atual pela hora oficial de atualização
da fonte, sem usar `observed_at` como ordem da fonte. Registra em cada observação
`ordering_status`: inicial, mesmo conteúdo, fonte mais recente, replay antigo,
empate ambíguo, hora desconhecida ou recurso cruzado pendente. Um replay antigo
preserva o ponteiro; A→B→A com atualização oficial posterior cria nova versão
e avança o ponteiro. Dados legados recebem `legacy` e não foram reordenados.

## Banco, backup e permissões

| Evidência | Resultado |
|---|---|
| Destino C2 | `test_20261020_ba894ce71405`, OID `2508600`; servidor e role OID conferidos com o manifesto C1 preservado |
| Dump antes da revisão | `backups/c2/c2-20261013/s02-before-0011.dump`, 854.439.511 bytes, SHA-256 `b943710883b92e9bf65e6d3067b0d47376db075f97f0192a67bf566241e8ffa0` |
| Restauração | `pg_restore --list` e restauração integral com `--exit-on-error --single-transaction --no-owner` em `rehearsal_...`; revisão e contagens idênticas; banco de ensaio removido por OID |
| Migração C2 | `20261013_0010` → `20261027_0011`; contagens preservadas: 3 jobs, 4 Bronze, 10 identidades, 10 versões e 10 observações |
| Objetos novos | 8 tabelas `control.source_*` vazias; 10 índices secundários válidos; FKs validadas com `ON DELETE RESTRICT`; três gatilhos de imutabilidade |
| Role C2 | `SELECT`, `INSERT`, `UPDATE` nas 8 tabelas novas do destino; sem privilégios fora da lista isolada e zero privilégios em tabelas do principal |
| Evidência máquina | `backups/c2/c2-20261013/s02-backup-report.json` e `s02-migration-report.json` (artefatos locais ignorados pelo Git) |

Índices acrescentados: missão por orçamento; partição por missão/estado/prioridade
e por pai; identidade por Bronze; fila pronta por missão/estado/instante/prioridade
e por partição; tentativa por missão, job e lease de orçamento; resultado por
Bronze. As PKs e chaves únicas cobrem busca de escopo, idempotência e watermarks.
A migração recusa banco fora dos prefixos de teste/ensaio e não oferece downgrade
que apague histórico. O aplicador exige nome/OID/revisão do principal, destino,
servidor e role, hash do backup e contagens anteriores antes de escrever.

## Validação offline

- Runner Docker em banco e role descartáveis: **162 testes aprovados** após o
  ajuste de fixtures/SQL, um aviso de depreciação Starlette/httpx; manifesto
  `backups/20261027T184031Z_4816b716.json`, exit 0 e limpeza sem erros.
  Inclui reservas concorrentes de 8 workers com
  teto 3, quota sem devolução após falha, `not_before` monotônico, manifesto/fila
  idempotentes, fencing, outcome imutável, recusa do principal, replay fora de
  ordem e A→B→A.
- Fronteiras já cobertas pela suíte: reserva durável antes do envio com falha do
  chamador; Bronze persistido antes da projeção e falha de página sem avanço;
  projeção/checkpoint C1 na mesma transação e falha de banco sem histórico
  parcial. O novo cursor de partição bloqueia avanço sem a futura promoção
  atômica. A retomada completa do worker S03 ainda requer teste de processo
  interrompido e reiniciado com fonte sintética.
- `radar_pncp_live_validation verify` após a migração: exit 0,
  `evidence_integrity_valid=true`, negócio `partial`, `coverage_verified=false`,
  `issues=[]`, `warnings=[]`. O job C2 continua `partial/SourceChanged`, página 2;
  o ledger C2 permaneceu 11 reservas, DOCUMENTAÇÃO 7/15, DADOS 4/50,
  calibração 2/3. Nenhum GET PNCP, job, schedule ou reset de quota nesta etapa.
- `git diff --check` sem erro. Alterações preexistentes e o mapa visual fora do
  escopo foram preservados.

## Limites e próxima etapa

O novo limitador **ainda não está conectado ao transporte C2 nem ao futuro
worker histórico**. Portanto, a concorrência/intervalo foram provados em testes
de banco descartável, não em chamadas reais. O ledger C2 não foi importado,
recriado ou zerado; não afirmar que sua cota foi incorporada ao orçamento novo.
Os contratos, catálogo de modalidades, cobertura inicial e limites externos
pendentes da S00 mantêm a missão histórica desabilitada. Nenhuma cobertura
integral foi demonstrada.

S03 deve ligar planejamento/backfill ao controle persistente, promover
identidades/versões/checkpoint em uma transação por página, testar crash real de
processo nas fronteiras e só então considerar prova externa se o recurso estiver
habilitado pela política. O job C2 com `SourceChanged` não deve ser retomado
diretamente.
