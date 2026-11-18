---
id: MAN-P31
tipo: evidencia
estado: em_revisao
classe_evidencia: E0_E2
atualizado_em: 2026-11-18
---

# P31 — manifesto de exemplos e fixtures

SHA-256 dos **bytes dos arquivos locais** calculado em 18/11/2026 no worktree P31. O hash permite detectar mudança do snapshot, não confirma atualidade, licença, precisão ou origem remota. Todos os caminhos abaixo são relativos à raiz do repositório. `E0` indica exemplo histórico já versionado; `E2` indica material de demonstração explicitamente sintético. Os arquivos E0 abaixo **não** foram copiados ou promovidos a E1 por este PR.

| Classe | Arquivo | SHA-256 |
| --- | --- | --- |
| E0 | [`PNCP_TECNOLOGIA_HISTORICA_2026-10-15.json`](../../radar_licita/examples/demo/fixtures/official/PNCP_TECNOLOGIA_HISTORICA_2026-10-15.json) | `b606c4d8fdd7ebbde3fe2a541a0f2e1f54dd1d146d9d8ae12a17760c4b8eb463` |
| E0 | [`PNCP_TECNOLOGIA_HISTORICA_EXPANDIDA_2026-10-15.json`](../../radar_licita/examples/demo/fixtures/official/PNCP_TECNOLOGIA_HISTORICA_EXPANDIDA_2026-10-15.json) | `503ce560e9a0ef8ca755cec706ac0be2a3170a78878777e337fa661bda9f064d` |
| E0/contexto | [`PNCP_VERIFICACAO_2026-10-15.json`](../../radar_licita/examples/demo/fixtures/official/PNCP_VERIFICACAO_2026-10-15.json) | `47613f9e4d4b1faa2548da020c0a5954eddd2373a1ceca22352318df10d066f4` |
| E2 | [`commercial_journey.json`](../../fixtures/radar_demo/commercial_journey.json) | `26aaedf6b72c66468833c1cb075038f04a1e04e104f760a06af865a8e3219693` |
| E2 | [`historical_results.json`](../../fixtures/radar_demo/historical_results.json) | `72e4efe5e8979263f397245d837537d5241ef6069f1d387fc51868ab6d46bca5` |
| E2 | [`opportunities.json`](../../fixtures/radar_demo/opportunities.json) | `50b536eef0039a1c1a5ec02f20d9a172531e923b0bc45033bd32be3fffe49f7f` |
| E2 | [`review_queue.json`](../../fixtures/radar_demo/review_queue.json) | `e3a6d6136e407152db4aff6cc2ba4ac7344fe564727f3ba110c9f44eb351d045` |
| E2 | [`viability_base.json`](../../fixtures/radar_demo/viability_base.json) | `004da731cf0179835b8f12c266b692d6c3a71b360d7053b60b3681b66e3410e3` |
| E2 | [`opportunities_boundary.json`](../../fixtures/radar_p31/opportunities_boundary.json) | `f3a7535e10aca0406b3ade435515134249f9a23c82c7cf498f3b71f335c1ccde` |

**E1:** nenhum recorte novo publicado. D13 (autorização de origem e uso dos dados reais) permanece pendente em [decisões](DECISOES.md); não existe hash de E1 a registrar aqui. A [linha de base P00](LINHA_DE_BASE_P00.md) é referência documental agregada, não fixture deste manifesto. As cinco candidatas de desenvolvimento e quatro reservas descritas em P00 não são rótulos confirmados nem foram abertas nesta P31.

**Verificação reproduzível:** na raiz do repositório, executar `Get-FileHash -Algorithm SHA256 <caminho>` no PowerShell para cada arquivo e comparar com a coluna acima; parsear os JSON e conferir `records` ou `versions` antes de usar. Para `opportunities_boundary.json`, conferir `source_available=false`, URLs em `example.invalid`, três versões em duas identidades e nenhuma chave de item/lote. Os hashes dos campos `purchase_hash`, `item_hash`, `result_hashes` e `source_hash` são dados internos das respectivas amostras e não substituem o SHA-256 dos arquivos nesta tabela.
