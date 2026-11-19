---
id: REV-P03
tipo: revisao
estado: aguardando_revisao_independente
classe_evidencia: E2
atualizado_em: 2026-11-19
---

# P03 — pacote de revisão da autoria

- Base: merge P31 `e0eaa0b3b8c6034cf08dd4d18f8354efcbdc186f`; branch local `codex/radar-p03-import-label-20261119` em worktree exclusivo. P01 e P02 não são dependências e não foram modificados.
- Entrega: [contrato](CONTRATO_AMOSTRA_P03.md), [cartão](prs/P03.md), `scripts/pncp_sample_import.py`, `tests/test_pncp_sample_import.py` e [controle](CONTROLE_DE_PRS.md). Os únicos corpos JSON usados em testes são E2 sintéticos em diretório temporário.
- Revisão técnica pendente: contrato de origem/uso e D14 com Carlos, identidade entre partições, formato real de itens/lotes/documentos, proteção do conjunto cego e regras humanas de adjudicação. O CLI não prova esses pontos.
- Reprodução: no worktree P03, `python -m pytest -p no:cacheprovider tests/test_pncp_sample_import.py tests/test_lint_radar_vault.py -q` passou em 15 testes (7 P03, incluindo CLI; 8 lint). `python scripts/lint_radar_vault.py --no-git` retornou OK estrutural e `git diff --check` não apontou erro nas alterações rastreadas. Repetir a checagem do diff após adicionar os arquivos novos.
- Estado: autoria local. Revisão independente, aprovação humana, publicação, merge, G2 técnico e G3 com dados reais pendentes. Reverter o commit P03 para desfazer sem tocar bancos, backups ou coleta.
