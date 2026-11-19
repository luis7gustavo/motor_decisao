---
id: REV-P03-INDEPENDENTE
tipo: revisao
estado: sem_bloqueio_escopo_e2
classe_evidencia: E2
atualizado_em: 2026-11-19
---

# Revisão independente da P03 — 19/11/2026

**Versão examinada:** `dcae0d972bc6e06df0690246b4355f82ce6d3f4e`, contra a base P31 `e0eaa0b3b8c6034cf08dd4d18f8354efcbdc186f`.

**Parecer técnico:** sem achado bloqueante no escopo provisório E2. O importador confirma manifesto, caminhos locais, JSON e SHA-256; o comparador exige revisores distintos, mesmo manifesto e mesmo conjunto de rótulos, sem promover concordância a métrica adjudicada. Isso prepara o contrato, mas não valida dados reais, rótulos ou G2/G3.

## Verificações

- Diff de seis arquivos e contrato P03 examinados. `git diff --check e0eaa0b..HEAD` sem falhas.
- `python -m pytest -p no:cacheprovider tests/test_pncp_sample_import.py tests/test_lint_radar_vault.py -q`: 15/15 passaram na imagem local `motor_decisao-radar-ops:latest`, sem rede, com worktree somente leitura e `/tmp` efêmero.
- `scripts/lint_radar_vault.py --no-git`: OK estrutural. O modo não valida referências de commit.
- Nenhum banco, worker, PNCP, dado real, publicação ou remoto foi acessado nesta revisão.

## Limites para a decisão humana

- `evidence_class` e `evidence_refs` são declarações do manifesto e dos revisores; o código não valida elegibilidade E3/E4 nem confirma que cada referência de rótulo aponta para um recurso/trecho existente. A saída mantém `ready_for_adjudicated_metrics=false`.
- A identidade não é reconciliada entre manifestos de partições diferentes; o contrato atribui esse controle ao responsável pelos dados. Manter o conjunto `blind` fora da implementação.
- Plano 1.0 coloca P01 antes de P03; controle V4 coloca P31. A revisão E2 não resolve essa divergência nem fecha a janela 12–13/11. D13/D14, Carlos, G1/G2/G3, publicação, integração e uso comercial permanecem pendentes.

O próximo passo é decisão humana sobre o pacote local e alinhamento de dependências, preservando as ressalvas acima. Este parecer não é aprovação de gate ou merge.
