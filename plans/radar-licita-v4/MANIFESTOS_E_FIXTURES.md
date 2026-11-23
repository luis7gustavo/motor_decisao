---
id: MAN-P31
tipo: evidencia
estado: aprovado_com_limites
classe_evidencia: E0_E1_E2
atualizado_em: 2026-11-23
---

# P31 — manifesto de exemplos e fixtures

SHA-256 dos **bytes dos arquivos locais**, iniciado em 18/11/2026 no worktree P31 e atualizado em 23/11/2026. O hash permite detectar mudança do snapshot, não confirma atualidade, precisão ou origem remota. Todos os caminhos abaixo são relativos à raiz do repositório. `E0` indica exemplo histórico; `E1` é recorte real autorizado e congelado; `E2` é demonstração explicitamente sintética.

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
| E1 | [`pncp_complete_20261123/manifest.json`](../../fixtures/radar_real/pncp_complete_20261123/manifest.json) | `14c835a9f8829ee04ce4148b9ae89a141dda96e83dd2a8661f4d0c41c01b7a54` |

**E1:** D13/D14 e G1 estrutural foram aprovados em 23/11/2026. O diretório `fixtures/radar_real/pncp_complete_20261123/` contém 100 payloads reais nomeados por SHA-256 e um manifesto com identidade, versão, aquisição, contagens e limites. O recorte soma 101 arquivos e 4.149.479 bytes. A [linha de base P00](LINHA_DE_BASE_P00.md) continua referência documental agregada; as candidatas e reservas P00 não ganham rótulo comercial por esta publicação.

**Verificação reproduzível:** na raiz do repositório, executar `Get-FileHash -Algorithm SHA256 <caminho>` no PowerShell para cada arquivo e comparar com a coluna acima; parsear os JSON e conferir `records`, `versions` ou `entries` antes de usar. Para `opportunities_boundary.json`, conferir `source_available=false`, URLs em `example.invalid`, três versões em duas identidades e nenhuma chave de item/lote. Para E1, conferir cada `file` e `sha256` do manifesto e manter falsos os quatro limites de cobertura, frescor, snapshot e elegibilidade. Hashes internos não substituem o SHA-256 dos arquivos nesta tabela.
