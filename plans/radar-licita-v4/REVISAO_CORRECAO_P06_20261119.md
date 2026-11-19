---
id: REV-P06-CORRECAO
tipo: revisao
estado: sem_bloqueio_nas_entradas
classe_evidencia: E2
atualizado_em: 2026-11-19
---

# Reexame independente da correção P06 — 19/11/2026

**Versão examinada:** `0df7c74f5f29a389904ccf1ebc66863adf728f9e`, após o [parecer anterior](REVISAO_INDEPENDENTE_P06_20261119.md). Este reexame ocorreu em sessão distinta da autoria.

A rota `POST /decision-engine/run` não importa MegaMix por padrão, retorna HTTP 422 antes do banco quando a importação é pedida sem `megamix_snapshot_date` e passa a data explícita ao importador. `collect_all.py` só acrescenta a importação MegaMix ao estágio quando recebe a data. README e guia local foram corrigidos. Os dois caminhos quebrados no parecer anterior estão resolvidos no escopo simulado examinado.

Em contêiner local `motor_decisao-radar-ops:latest`, sem rede, com worktree somente leitura e `/tmp` efêmero, os testes de auditoria, guarda e entradas passaram: 8/8. `scripts/lint_radar_vault.py --no-git` e `git diff e0eaa0b..HEAD --check` passaram. A primeira tentativa de teste sem substituir o entrypoint da imagem falhou por uso do CLI da imagem; a repetição com `--entrypoint python` passou. TestClient simulou importador e motor; nenhum banco, coleta, PNCP ou worker foi acessado.

**Limites:** a data de origem do JSON MegaMix real continua sem comprovação; o teste não valida importação real com a guarda nova nem concorrência de dois processos. Execuções legadas sem SHA-256 permanecem fora da guarda entre dias. P06 continua parcial; G2 e integração exigem decisões separadas. Este parecer não é aprovação comercial.
