---
id: REV-P02
tipo: revisao
estado: aguardando_revisao_independente
classe_evidencia: E2
atualizado_em: 2026-11-19
---

# P02 — pacote de revisão da autoria

## Base, entrega e preservação

- Base local: merge P31 `e0eaa0b3b8c6034cf08dd4d18f8354efcbdc186f` da branch de produto. Worktree P02 exclusivo; o checkout de produto com mudanças humanas e o worktree P01 em andamento foram preservados.
- Entrega: [contrato do ledger](LEDGER_OFFLINE_P02.md), [cartão](prs/P02.md), `scripts/pncp_offline_ledger.py`, `tests/test_pncp_offline_ledger.py` e atualização do [controle](CONTROLE_DE_PRS.md).
- Fonte: [inventário P31](SCHEMA_E_EXEMPLOS.md) e [manifesto P31](MANIFESTOS_E_FIXTURES.md). O contrato de entrada da P02 é novo e requer um adaptador explícito para qualquer exportação real futura. Nenhum backup ou fixture real foi copiado ou publicado.

## Verificações da autoria

- `python -m pytest -p no:cacheprovider tests/test_pncp_offline_ledger.py tests/test_lint_radar_vault.py -q`: 14 testes passaram (6 da P02, incluindo CLI e códigos de saída; 8 do lint).
- `python scripts/lint_radar_vault.py --no-git`: OK estrutural nas notas, sem conferência de commits pelo modo usado.
- `git diff --check`: sem erro nas alterações rastreadas antes do checkpoint; repetir após adicionar os arquivos novos.
- Primeira tentativa de teste com `python` do PATH falhou por ser atalho da Microsoft Store. Foi usado o Python 3.12.14 já existente em `motor publico/backend/.venv/Scripts/python.exe`, sem instalação.

## Revisão ainda necessária

Uma sessão verificadora independente deve revisar o diff e reproduzir os testes, em especial hash divergente, página faltante, totais contraditórios, HTTP 500 após sucesso, idade/fuso e limite de caminho. G1/G2, cobertura, frescor, fechamento da janela 12–13/11, publicação e merge continuam pendentes. A reversão é restrita aos arquivos desta entrega; nenhum dado operacional foi alterado.

## Correção após revisão independente de 19/11/2026

O [parecer independente](REVISAO_INDEPENDENTE_P02_20261119.md) apontou perda da trilha por tentativa. A autoria acrescentou `page_attempts` com HTTP, horários, arquivo, hash esperado/observado e estado final de cada tentativa, além de contadores de falhas HTTP e falhas recuperadas. O teste sintético agora exige que o HTTP 500 e a recuperação 200 apareçam na mesma saída, com horários e hashes; cobre também hash divergente e falha posterior a sucesso. Nesta revisão local, 14 testes focados e de lint passaram. Requer nova sessão verificadora para o novo diff; o parecer anterior permanece preservado como histórico. Nenhuma decisão de integração, G1/G2 ou uso de dados reais decorre desta correção.
