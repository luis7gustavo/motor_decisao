---
id: REV-P01-INDEPENDENTE
tipo: revisao
estado: mudancas_documentais_solicitadas
classe_evidencia: acervo_real_local_nao_aprovado
atualizado_em: 2026-11-19
---

# Revisão independente da P01 — 19/11/2026

**Versão examinada:** `548eb7882b56ccbf3940528a98e8dc4e866c4e7e`, contra a base P31 `e0eaa0b3b8c6034cf08dd4d18f8354efcbdc186f`.

**Resultado:** auditoria local do pacote piloto reproduzida; correção documental solicitada antes da decisão de integração. Isto não fecha a janela 12–13/11, não aprova G1 e não equivale ao aceite de Carlos ou de Luis para uso de dados.

## Evidência reproduzida

- `node scripts/audit_pncp_inspection_manifest.mjs <backup-local>` retornou código 0, manifesto raiz SHA-256 `62172be9b4ae314b4eb1e3eaab4aa28c025e06d2415b75db273cdd3d516f6106`, 20 entradas, 186 arquivos e zero divergência. Soma dos campos do manifesto: 2.748 itens vistos/informados, 40 documentos baixados/informados, zero hash de corpo inválido no índice.
- SHA-256 atual do manifesto, `LEIA-ME.md` e recibo `20261112-enrichment-rewindow.json` confere com os três valores do relatório. `git diff --check e0eaa0b..HEAD` sem falhas.
- O pacote foi recebido entre 11h55 e 12h03 de São Paulo em 12/11, antes do início da janela às 12h38. O `STATUS.md` atual ainda não apresenta fechamento auditado da janela de 12–13/11; registra retomada de 19/11 em outra janela finita.

## Correção documental solicitada

O relatório `FECHAMENTO_MANIFESTOS_P01.md` atribui ao `STATUS.md` da coleta SHA-256 `a82de8bc4742a1d60844aeeb9666c1a2ecd60cdbac514e4163544a42848a73f5`. O arquivo atual no caminho citado tem SHA-256 `28a0237cddcee5ddb8687c58be44b501859b4e3a9e8d8cd1496ffcfd4d10f7cb`. Conferir novamente com `Get-FileHash` antes de atualizar o relatório, pois a coleta está ativa e o arquivo pode mudar. Registrar se o hash antigo era um snapshot datado, com caminho/versão recuperável, ou substituir pela versão atual e distinguir as observações de 12/11 e 19/11.

**Limites:** o verificador confere a consistência dos manifestos e hashes locais; não consulta a fonte externa, nem demonstra completude nacional, frescor, resultado por item ou adequação comercial. Nenhum banco, worker, PNCP, fonte externa ou remoto foi alterado. Carlos e Luis ainda precisam decidir seus respectivos aceites; a P01 permanece em revisão.
