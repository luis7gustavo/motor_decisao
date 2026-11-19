---
id: REV-P02-INDEPENDENTE
tipo: revisao
estado: mudancas_solicitadas
classe_evidencia: E2
atualizado_em: 2026-11-19
---

# Revisão independente da P02 — 19/11/2026

**Versão examinada:** `d2b64a0bc3779926b2e3f71c862a1d75dcf59d8c`, contra `e0eaa0b3b8c6034cf08dd4d18f8354efcbdc186f`.

**Resultado:** mudanças solicitadas no contrato de saída. A implementação preserva corretamente os estados de cobertura e frescor como desconhecidos, mas o relatório não é ainda um ledger auditável de tentativas, HTTP e hashes conforme a entrega P02 e I01 do plano 1.0.

## Achado

`scripts/pncp_offline_ledger.py` valida `http_status`, `received_at`, `source_updated_at` e SHA-256 de cada tentativa, mas a saída por grupo expõe apenas contagem, número das páginas finais e idades máximas. Não inclui o HTTP, horário, arquivo ou hash por tentativa, nem um resumo de falhas HTTP recuperadas. No caso já testado de HTTP 500 seguido de sucesso 200, o 500 desaparece do resultado exceto pela contagem total 2. Isso impede reproduzir do relatório a auditoria por página/tentativa/recurso e verificar as falhas que foram recuperadas. A entrada original ainda existe, mas o relatório não materializa a trilha prevista.

**Correção sugerida à autoria:** incluir registros de tentativa por página, com HTTP, horários, hash esperado/observado ou estado de integridade, e resultado final; conservar os estados globais desconhecidos. Adicionar um teste que confirme a presença do 500 e do 200, seus hashes e horários na saída. Se a intenção for somente um resumo, revisar explicitamente o escopo/contrato antes de tratar P02 como entregue.

## Verificações e limites

- `python -m pytest -p no:cacheprovider tests/test_pncp_offline_ledger.py tests/test_lint_radar_vault.py -q`: 14/14 passaram em imagem local `motor_decisao-radar-ops:latest`, sem rede, worktree somente leitura e `/tmp` efêmero.
- `git diff --check e0eaa0b..HEAD`: sem falhas antes desta nota. Nenhuma fonte externa, banco, worker ou PNCP foi acessado.
- A divergência de dependências continua: plano 1.0 pede P01 antes de P02; controle V4 lista P31. Esta revisão offline não concede integração, G1/G2 ou fechamento da janela 12–13/11.

Após a correção, repetir testes e revisar novo commit em sessão distinta da autoria. Dados E2 não atestam cobertura ou frescor real.
