---
id: REV-P31
tipo: revisao
estado: aguardando_verificacao_independente
classe_evidencia: E0_E2
atualizado_em: 2026-11-18
---

# P31 — pacote para revisão

## Escopo entregue pela autoria

Base `a80cdfcb7fd397126d32c57b73fddcfc227096a9` da branch de produto; branch exclusiva `codex/radar-p31-inventario-20261118`. Arquivos: [inventário](INVENTARIO_AMOSTRAS_P31.md), [schema observado](SCHEMA_E_EXEMPLOS.md), [manifesto de hashes](MANIFESTOS_E_FIXTURES.md), [caso sintético](../../fixtures/radar_p31/opportunities_boundary.json), [cartão](prs/P31.md), controle, decisões e README. O checkout de produto original e a coleta ficaram intactos.

## Verificações da autoria em 18/11/2026

| Verificação | Resultado observado |
| --- | --- |
| Parse JSON dos cinco arquivos de demonstração, três arquivos históricos e novo caso E2 | 9/9 válidos com `ConvertFrom-Json` no PowerShell |
| SHA-256 | 9/9 hashes dos bytes correspondem ao manifesto, conferidos por `Get-FileHash` |
| Estrutura externa E2 | Chaves de topo e de cada versão iguais às de `fixtures/radar_demo/opportunities.json`; três versões em duas identidades; `source_available=false` |
| Integridade de diff | `git diff --check` sem falhas |
| Lint documental P32 | `python scripts/lint_radar_vault.py --no-git` em contêiner sem rede: **OK** após incluir este pacote. O modo não resolve referências Git. |

Não houve novas chamadas PNCP, leitura do conteúdo das reservas P00, alteração de worker, cópia de dados privados, nem criação de fixture E1. Os hashes de E0 são dos snapshots **já versionados** e não uma revalidação da fonte remota. O caso E2 é inventado e usa `example.invalid`.

Os testes `pytest` do lint P32 não foram reexecutados nesta autoria: a imagem local usada para o lint não contém `pytest`. O escopo novo é documental e JSON; parse, hashes, estrutura, lint e diff foram verificados separadamente.

## Revisão independente solicitada

Conferir diff e consistência entre inventário, schema e os nove arquivos; comparar hashes e links; verificar que nenhum dado local da coleta foi publicado; examinar o caso sintético com prazo nulo, retificação e falso positivo; repetir lint e checagem de diff. Registrar achados e correções neste PR antes de decisão humana. A sessão autora não aprova o próprio trabalho.

## Decisões pendentes

- **D13:** Luis define a origem e o uso permitidos de recortes reais como E1/anexos. Este pacote não solicita aprovação retroativa de dados não publicados.
- **D14/G1:** responsável técnico e Carlos revisam o contrato observado e o gate estrutural completo. Este PR entrega apenas a etapa de inventário e fixture E2; parser, deduplicação e testes de borda amplos permanecem para entregas dependentes.
- **PR P31:** revisão independente, aceite humano e merge pendentes. A publicação deste pacote não autoriza a integração.

O modo de desfazer é reverter o PR. E0/E2 não comprovam cobertura, atualidade, compra de bens elegível ou viabilidade comercial.
