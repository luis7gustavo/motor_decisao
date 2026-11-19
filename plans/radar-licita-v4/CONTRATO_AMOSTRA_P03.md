---
id: AMOSTRA-P03
tipo: contrato
estado: em_autoria
classe_evidencia: E2
atualizado_em: 2026-11-19
---

# P03 — contrato de amostra e rotulagem offline

## Alcance

Este é um contrato **provisório** para uma exportação futura e autorizada de Carlos, derivado do [cartão P03](prs/P03.md) e do [schema observado P31](SCHEMA_E_EXEMPLOS.md). Não declara o formato canônico da coleta; D14 e G1 ainda aguardam revisão técnica e de Carlos. Os testes usam somente E2 sintético. O código não acessa rede, banco ou dados operacionais e não copia respostas para o repositório.

## Manifesto e importação

`scripts/pncp_sample_import.py import <manifest.json>` valida um manifesto `kind: "pncp-sample-manifest-v1"` com `partition` (`development`, `validation` ou `blind`), `evidence_class` E0–E4, `captured_at` com fuso e `cases`. Cada caso identifica `source_id` e `version` e lista recursos com `kind`, `resource_id`, `file` JSON relativo, `sha256`, `source_url`, `received_at` com fuso e `source_updated_at` opcional. Um caso pode ter vários recursos e versões da mesma identidade podem coexistir. O arquivo bruto continua no local informado; o importador confirma JSON válido e hash dos bytes, rejeita caminhos externos, casos e recursos duplicados, e emite inventário JSON com SHA-256 do manifesto. Ausência de `source_updated_at` permanece nula.

Cada partição tem manifesto próprio e precisa ser congelada **antes** da exploração. O importador não faz divisão automática. O responsável pelos dados deve verificar que a mesma identidade, com todas as versões e itens, aparece em apenas uma partição e manter o conjunto `blind` inacessível à implementação. O inventário contém identidades e caminhos; sua publicação exige a autorização D13 quando houver dados reais. O CLI escreve JSON na saída padrão para redirecionamento a destino local controlado pelo responsável, sem persistência automática.

## Rotulagem

`template <inventory.json> <reviewer_id>` gera um arquivo separado por revisor com quatro rótulos vazios por caso e versão: `mouse`, `keyboard`, `webcam` e `headset`. Cada revisor preenche independentemente `decision` (`positive`, `negative` ou `uncertain`), `reason` e `evidence_refs` apontando ao recurso/trecho visto. `compare <labels-A.json> <labels-B.json>` exige revisores distintos, o mesmo hash do manifesto e o mesmo conjunto de casos/categorias; devolve `agreement` ou `needs_adjudication`. Dois `uncertain` continuam pendentes. A saída mantém `ready_for_adjudicated_metrics=false`: concordância entre revisores não substitui adjudicação, confirmação de item/lote, assinatura humana nem a amostra cega aprovada no P00.

O contrato ainda não define item/lote, contexto, regras de elegibilidade E3, mínimos de amostra, limiares nem SLA. Ele deve ser confrontado com uma exportação real **somente após autorização e revisão de origem/uso**, na P33. Não usar seus rótulos para afirmar precisão, revocação ou viabilidade comercial nesta etapa.

## Verificação e reversão

Executar `python -m pytest tests/test_pncp_sample_import.py -q`, `python scripts/lint_radar_vault.py --no-git` e `git diff --check` no worktree P03. Para desfazer, reverter apenas código e documentos P03. Nenhum snapshot operacional ou dado pessoal deve ser excluído ou modificado.
