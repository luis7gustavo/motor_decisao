---
id: REV-P06-INDEPENDENTE
tipo: revisao
estado: mudancas_solicitadas
classe_evidencia: E2
atualizado_em: 2026-11-19
---

# Revisão independente da P06 — 19/11/2026

**Versão examinada:** `dd564c6c93bbaba699a9818a7cdbda0f1f962afb`, contra a base `e0eaa0b3b8c6034cf08dd4d18f8354efcbdc186f`.

**Resultado:** mudanças solicitadas. A guarda de data de origem é conservadora, mas dois caminhos existentes deixaram de funcionar. Esta revisão não aprova a P06, G2 ou integração.

## Achado bloqueante

1. `app/api/decision_engine.py` mantém `import_megamix=True` como padrão em `POST /decision-engine/run` e chama `import_megamix_catalog` sem `snapshot_date`. A nova guarda lança `ValueError` antes de acessar o banco. Reproduzido invocando a função da rota com os parâmetros padrão de importação; saída 1 com `MegaMix requires an explicit source snapshot date`. Uma chamada HTTP alcançaria uma exceção sem tratamento.
2. `scripts/collect_all.py`, em `stage_engine`, chama `build_decision_engine.py --import-megamix --import-coletek` sem `--megamix-snapshot-date`. O CLI sai com código 1 e a mensagem `--import-megamix exige --megamix-snapshot-date`, também antes do banco. O comando documentado em `README.md` tem a mesma lacuna. O exemplo de importação em `docs/uso_local_e_importacao.md` também precisa ser atualizado.

**Correção sugerida à autoria:** fazer cada entrada aceitar e validar uma data de origem explicitamente comprovada, ou deixar a importação MegaMix desativada quando essa data não existir. Não inferir a data da carga ou do arquivo. Atualizar os comandos documentados e adicionar testes de regressão das entradas HTTP e `collect_all.py`. Repetir a verificação isolada. A data real do JSON MegaMix continua desconhecida.

## Verificações desta sessão

- Diff e chamadas de `import_megamix_catalog` examinados no commit acima. `git diff --check e0eaa0b..HEAD` sem falhas.
- Em `motor_decisao-radar-ops:latest`, sem rede, com worktree somente leitura e `/tmp` efêmero: `python -m unittest tests.test_audit_supplier_catalogs tests.test_import_megamix_snapshot` passou, 5/5. `scripts/lint_radar_vault.py --no-git` passou.
- O primeiro ensaio dos testes com filesystem inteiro somente leitura falhou por falta de diretório temporário. Repetido com `--tmpfs /tmp`; falha ambiental resolvida sem mudar código.
- A reprodução do CLI e da função da rota saiu com código 1 conforme descrito. Nenhum banco operacional, worker, PNCP ou fonte externa foi acessado.

**Limites:** não foi ensaiada concorrência simultânea de dois processos nem importado o JSON MegaMix real com a guarda nova. A nova proteção não alcança execuções legadas sem SHA-256. Revisão posterior deve reexaminar o commit corrigido; teste verde da guarda isolada não demonstra funcionamento das entradas existentes.
