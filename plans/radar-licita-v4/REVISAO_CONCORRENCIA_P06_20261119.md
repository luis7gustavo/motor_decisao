---
id: REV-P06-CONCORRENCIA
tipo: revisao
estado: sem_bloqueio_no_ensaio_e2
classe_evidencia: E2
atualizado_em: 2026-11-19
---

# Revisão independente do ensaio de concorrência P06 — 19/11/2026

**Versão examinada:** `61eb3144a6792fe8f06a0453c10c8aa5848fb843`, posterior ao [reexame das entradas](REVISAO_CORRECAO_P06_20261119.md). Esta revisão ocorreu em execução distinta da autoria do teste. O worktree estava limpo no início; o checkout de produto com alterações humanas não foi tocado.

## Escopo e resultado

- O diff `40a0d09..61eb314` adiciona o teste `tests/test_p06_concurrency_integration.py` e atualiza cartão, controle e pacote. `git diff 40a0d09..61eb314 --check` e `git diff e0eaa0b..HEAD --check` passaram.
- O teste exige sinalizador explícito, banco `test_...` e host `radar-p06-pg-...`. Inicia dois processos sobre a mesma fixture E2 com datas de origem conflitantes, verifica uma carga de 2 itens, uma recusa, sobreposição temporal e estado final de 2 linhas Bronze e 1 execução de fonte. A guarda SQL usa bloqueio transacional pelo hash antes da consulta de execuções.
- Repeti os testes focados em imagem local, sem rede e com worktree somente leitura: `pytest` aprovou 8 testes e ignorou o teste de integração sem sinalizador. O lint documental `--no-git` passou. Houve um aviso de depreciação Starlette/TestClient, sem falha.
- Repeti a migração até `20261013_0010` e o teste de integração em PostgreSQL/pgvector e rede Docker exclusivos, com banco descartável criado nesta execução. O teste passou 1/1. Contêiner e rede foram removidos em bloco `finally`; a verificação posterior não encontrou recursos com os nomes usados pela revisão.

**Parecer:** sem bloqueio no cenário E2 de disputa entre dois processos. O resultado é coerente com a guarda transacional implementada. O teste usa espera de 1,5 s e limite de 1 s para aproximar a disputa; sob carga elevada, essa janela pode tornar o teste instável. Uma futura melhoria pode instrumentar a chegada de cada processo ao bloqueio para provar a disputa de modo determinístico. Isso não altera o resultado observado neste ensaio.

## Limites e decisões

A data de origem do JSON MegaMix real continua desconhecida e não foi inferida do arquivo ou da carga. A guarda não cobre execuções legadas sem SHA-256. O ensaio não valida cotação, estoque, disponibilidade ou uso comercial. P06 permanece parcial; G2, integração e promoção de dados exigem decisões separadas. Nenhum banco operacional, worker, coleta, PNCP, push ou PR remoto foi alterado.
