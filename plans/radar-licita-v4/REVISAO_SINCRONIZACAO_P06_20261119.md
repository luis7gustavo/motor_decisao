---
id: REV-P06-SINCRONIZACAO
tipo: revisao
estado: sem_bloqueio_no_ensaio_e2
classe_evidencia: E2
atualizado_em: 2026-11-19
---

# Revisão independente da sincronização P06 — 19/11/2026

**Versão examinada:** `d015653b00bfc3030293cb4e4cda0e2c5d5a363d`, em execução distinta da autoria. O worktree P06 estava limpo no início; o checkout principal do produto com alterações preexistentes não foi tocado.

## Escopo e evidência

- O diff `d4c843a..d015653` altera o teste de concorrência e seus registros. O processo pai agora aguarda um importador adquirir o bloqueio consultivo e observa o outro esperando por `advisory` em `pg_stat_activity` antes de liberar a primeira transação. Os eventos e o `finally` limitam e encerram a espera. A consulta exige `wait_event_type = 'Lock'`, evitando contar a própria consulta como processo bloqueado.
- `git diff d4c843a..d015653 --check` e `git diff e0eaa0b..HEAD --check` passaram. Em contêiner local sem rede, worktree somente leitura, a suíte focada passou com 8 testes e 1 integração ignorada sem o sinalizador de banco. O lint documental `--no-git` passou. Houve apenas o aviso de depreciação Starlette/TestClient.
- Criei rede e PostgreSQL/pgvector temporários com nomes exclusivos, sem portas ou volumes operacionais, e banco `test_20261119_<12 hex>`. A migração até `20261013_0010` e o teste de integração retornaram código zero; o comando encerrou com `INTEGRATION_PASS` condicionado a ambos. O teste verifica uma carga de 2 itens, uma recusa por data conflitante, sobreposição das importações, 2 linhas Bronze e 1 execução de fonte. O contêiner e a rede foram removidos no bloco `finally`; a enumeração posterior não encontrou recursos P06 da revisão.

**Parecer:** sem bloqueio técnico no cenário sintético E2 examinado. A sincronização demonstra a disputa pelo bloqueio sem depender de atraso fixo; os limites de tempo continuam necessários para que uma falha encerre o ensaio.

## Limites e decisão

O ensaio não comprova a data de origem do JSON MegaMix real, não cobre execuções legadas sem SHA-256 e não valida cotação, estoque ou uso comercial. P06 continua parcial. G2, publicação, integração e uso de dados reais permanecem para decisões separadas. Nenhum banco operacional, worker, coleta, PNCP, push ou PR remoto foi alterado.
