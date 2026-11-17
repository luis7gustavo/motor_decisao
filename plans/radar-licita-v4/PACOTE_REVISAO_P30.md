# Pacote de revisão — P30

**Data:** 17/11/2026

**Estado:** [PR #4 aberto](https://github.com/luis7gustavo/motor_decisao/pull/4); publicação autorizada por Luis em 17/11/2026; merge pendente.

**Atualização de 17/11/2026:** o PR #4 foi integrado à branch de produto no commit `b46aec77a3448f51ac212676216c675822fb12f3`. O texto acima preserva o estado do pacote antes da integração. G0 e a aprovação integral da V4 continuam pendentes.

## Decisão pedida

O repositório `luis7gustavo/motor_decisao` é **público**. O P30 inclui a [proposta V4 integral](PROPOSTA_V4_20261117.md), com estratégia comercial, referência de caixa, nomes de fornecedores e detalhes operacionais. Informado desse conteúdo e da visibilidade pública, Luis respondeu **“pode publicar”** em 17/11/2026. A decisão pendente é a revisão do [PR #4](https://github.com/luis7gustavo/motor_decisao/pull/4) para merge; publicação não equivale a aprovação do plano técnico integral, de G0 ou do merge.

## Entrega publicada para revisão

- [PR #4](https://github.com/luis7gustavo/motor_decisao/pull/4) na branch `codex/radar-p30-execucao-20261117`, derivada do commit `fadfc6024f2039c40313b82363b9326a02dc1422` da branch de produto.
- [Instruções de sessão](INSTRUCOES_SESSOES.md), [decisões](DECISOES.md), [permissões](PERMISSOES_E_FONTES.md), [controle com fila](CONTROLE_DE_PRS.md) e 36 [cartões de PR](prs/P30.md) neste repositório.
- O checkout original de produto, que tinha mudanças locais preexistentes, não foi editado.

## Verificação

- 36 cartões com IDs únicos, P00–P35, e 36 linhas na fila.
- Dependências de implementação da fila respeitam a precedência dos cartões; P24 é apenas candidato técnico antes de P34/P35.
- Links Markdown locais dos arquivos criados resolvem no worktree.
- `git diff --cached --check` passou após normalização de espaços da cópia V4.
- Sessão verificadora independente em 17/11/2026: apontou o risco de publicação e uma referência ambígua ao cofre. A referência foi corrigida; Luis resolveu PUB-01 autorizando a publicação.

## Limites e reversão

P30 modifica apenas Markdown e a regra para ignorar `.obsidian/`. Não modifica código, worker, coleta, banco ou dados. O trabalho pode ser revisto e descartado removendo a branch/worktree **somente após conferir que não há mudanças a preservar**. Nenhum gate ou merge está aprovado neste pacote.
