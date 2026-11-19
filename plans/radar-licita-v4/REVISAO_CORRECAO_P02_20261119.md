---
id: REV-P02-CORRECAO
tipo: revisao
estado: sem_bloqueio_no_achado
classe_evidencia: E2
atualizado_em: 2026-11-19
---

# Reexame independente da correção P02 — 19/11/2026

**Versão examinada:** `fa72979c6514d5eb97c9bcf913c19e748213af98`, após o [parecer anterior](REVISAO_INDEPENDENTE_P02_20261119.md). Este reexame ocorreu em sessão distinta da autoria.

A saída `page_attempts` agora mantém página, número da tentativa, HTTP, horários, arquivo, SHA esperado/observado, integridade e aceite. O caso E2 de HTTP 500 seguido de 200 retém as duas tentativas e conta a falha recuperada; hash divergente e sucesso seguido de falha também têm asserções. O achado de perda da trilha de tentativas está resolvido no commit examinado.

Em contêiner local `motor_decisao-radar-ops:latest`, sem rede, com worktree somente leitura e `/tmp` efêmero, `python -m pytest -p no:cacheprovider tests/test_pncp_offline_ledger.py tests/test_lint_radar_vault.py -q` passou: 14/14. `git diff e0eaa0b..HEAD --check` passou. A primeira tentativa de executar o contêiner sem substituir seu entrypoint falhou por uso do CLI da imagem; a repetição com `--entrypoint python` passou. Nenhum banco, PNCP ou worker foi acessado.

**Limites:** o teste é sintético; cobertura e frescor reais permanecem desconhecidos. O plano 1.0 exige P01 antes de P02, enquanto o controle V4 lista P31. Essa divergência e a decisão humana de integração continuam pendentes; este parecer não aprova G1/G2 nem a janela de 12–13/11.
