---
id: LEDGER-P02
tipo: contrato
estado: em_autoria
classe_evidencia: E2
atualizado_em: 2026-11-19
---

# P02 — ledger offline de páginas

## Recorte e fonte

Este contrato implementa apenas o [cartão P02](prs/P02.md), a partir do [inventário e schema P31](SCHEMA_E_EXEMPLOS.md). A branch de produto usada como base local está no merge P31 `e0eaa0b3b8c6034cf08dd4d18f8354efcbdc186f`. Os testes criam arquivos **E2 sintéticos** no diretório temporário. Não há leitura dos backups do piloto, das amostras reais, do PNCP, do banco ou da coleta de Carlos.

## Entrada e saída

`scripts/pncp_offline_ledger.py <manifest.json>` recebe um objeto JSON `kind: "pncp-offline-ledger-v1"`, `captured_at` com fuso, `evidence_class` e `attempts`. Cada tentativa contém `window`, `resource`, `partition`, `version`, `page`, `attempt`, `http_status`, `received_at` com fuso e, opcionalmente, `source_updated_at` e `total_pages_reported`. Um HTTP 2xx exige `file` relativo à pasta do manifesto e SHA-256 dos bytes. Falha HTTP não pode carregar corpo/hash como página aceita. A entrada é um contrato de exportação futura, não o formato bruto da API nem uma conversão automática dos exemplos P31.

A saída JSON agrupa por janela, recurso, partição e versão. Para cada página, prevalece a última tentativa pelo número de tentativa. O relatório mostra tentativas, páginas aceitas, falhas finais, lacunas dentro do **total informado**, conflitos de total, integridade SHA-256, idade máxima desde `received_at` e desde `source_updated_at` quando presente. Idades negativas, duplicidade de tentativa, hash divergente e totais conflitantes aparecem em `issues`; o CLI retorna código 1. Entrada inválida retorna código 2. Total acima de 10.000 páginas por registro é recusado para limitar consumo local.

`all_reported_pages_observed` significa apenas que o total **informado no manifesto** foi visto com hash válido. Os estados `coverage_state` e `freshness_state` permanecem sempre `unknown_without_independent_reference` e `unknown_without_independent_reference_and_sla`. Hora da última atualização da fonte, quando informada, permite medir idade desse campo; não comprova a ausência de publicações omitidas nem o atraso real frente ao PNCP. O ledger não marca contratação aberta, resultado zero, lote completo ou oportunidade comercial.

## Validação e limites

Executar `python -m pytest tests/test_pncp_offline_ledger.py -q` e `git diff --check` no worktree P02. Os testes cobrem retentativa HTTP, página ausente, hash alterado, total conflitante, escape de caminho e falha posterior a sucesso. O contrato requer adaptação explícita ao formato de exportação autorizado antes de ingestão real; o fechamento da janela 12–13/11 e G1/G2 seguem pendentes. Para desfazer, reverter apenas os arquivos da P02, sem alterar os snapshots existentes.
