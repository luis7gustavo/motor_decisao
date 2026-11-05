# Controle da trilha de produto — 04/11/2026

Fotografia operacional somente de leitura em 04/11/2026, cerca de 21h12 (America/Sao_Paulo). O controle anterior, às 17h04, está no checkout operacional em `plans/radar-licita-pos-coleta-2026-11-04/CONTROLE_EXECUCAO_20261104.md`. Estes números mudam durante a coleta e não são aceite S08.

## Isolamento e baseline

- Checkout operacional: `motor_decisao`, branch `codex/radar-licita-mvp-sacy-2026-10-16`, HEAD `4a9b91d`; 64 entradas preexistentes em `git status --short` na retomada. Nenhuma foi editada aqui. O worker usa mounts desse checkout.
- Produto: worktree `motor_decisao_produto`, branch `codex/radar-licita-produto-2026-11-04`, HEAD inicial desta retomada `e2e5dea`, limpo antes da Etapa C. B e D já estavam em `a15558c` e `e2e5dea`.
- C2 segregado: banco `test_20261020_ba894ce71405`; principal: `motor_decisao`, conforme controle anterior. Esta etapa não abriu conexão com nenhum deles. `exclusive.lock`, Bronze, ledger, recibos e backups não foram alterados.

## Gate A observado

| Ponto | Estado | Evidência e limite |
| --- | --- | --- |
| 1. Coleta oficial | parcial | Deployment/pool/fila ativos com concorrência 1, worker `ONLINE`, três schedules ativos. Relatório às 21h11: missão 8.412/100.000, fonte 8.511/100.099, 6.299 tentativas na janela e 66 HTTP 5xx, zero 403/429/200 malformado. Último resultado de negócio visto nos logs às 21h06: `backfill partial_or_deferred`; Prefect `Completed` não o altera. |
| 2. Cobertura e qualidade | parcial | Auditoria às 21h12: histórico 230 completas, 313 pendentes, 40 em reconciliação e 1 em execução; recorrência 11 completas e 293 pendentes. `historical_coverage_status=partial`. Uma partição completa não prova enumeração integral. |
| 3. Base organizada | parcial | Relatório: 168.379 identidades históricas únicas e 105.426 acima do baseline de rejanela. `history status` mostrou 405.551 identidades sob definição diferente; não equiparar os contadores. Hashes Bronze, versões, duplicidade e vínculos reais do recorte não foram revalidados nesta retomada. |
| 4. Enriquecimento | pendente | Detalhe: fila/identidade e orçamento; itens: paginação e orçamento; resultados: validação runtime e semeadura por item; documentos: política de armazenamento binário e download. |

O relatório às 21h11 encontrou **1 outcome da fonte ausente** enquanto havia um run `RUNNING`. Uma consulta SQL somente leitura às 21h14 não encontrou linha sem outcome; novo relatório segundos depois voltou a contar 1. A oscilação é compatível com reserva em andamento, mas não prova sozinha a causa nem a integridade final. Reconsultar após o run e na avaliação da janela. A janela de 24 horas termina em 05/11 às 09h42 locais. Frescor oficial e sync integral continuam sem prova: `source_updated_at` estava nulo na fotografia anterior, e aqui não houve nova medição de fonte/sync que permita fechar essa lacuna.

## Produto e sequência

B e D são protótipos sintéticos anteriores. Nesta retomada, C ganhou comparação nominal, exclusões explicadas e candidatos para investigação manual; a matriz de evidências está em `radar-historical-offline.md`. Os testes focados B/C/D passaram em container sem rede e mount somente de leitura. C não alimenta o cálculo D. Nenhum gate libera integração real de B, C ou D agora.

Próxima etapa: revisão independente do diff de C e, em seguida, E sintética com fila de revisão, eventos comerciais e snapshots imutáveis. Integração real exige evidência específica de Gate A, G0–G4 em cópia e a janela operacional própria para G5–G6. A categoria, geografia, capital e modelo de fornecimento do piloto seguem decisões do operador.
