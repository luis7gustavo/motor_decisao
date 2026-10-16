# Evidências da demonstração ativa

A [tela atual do acervo histórico real](evidencias/s09_historico_real_resumo.png) registra a instância local em 15/10/2026: **7 pregões, 19 itens, 19 resultados e 11 empresas distintas**. Mostra a compra de microcomputadores do Exército e o resultado associado à INFOG. A [tela do Radar](evidencias/s09_radar_acervo_real.png) mostra as sete contratações reais acessíveis também fora do painel de Tecnologia. As imagens são registros da interface, sem representar consulta ao vivo ou cotação atual.

A [interface com a paleta creme, verde azulado, azul ardósia e terracota](evidencias/sillo_tons_sacy_2026-10-16.png) registra o ajuste visual de 16/10/2026. O logo SILLO permanece o arquivo original e recebe a cor por CSS.

O [manifesto atual](EVIDENCIAS_HISTORICO_REAL.json) contém tamanho e SHA-256 da tela, da [captura oficial do PNCP](../../examples/demo/fixtures/official/PNCP_TECNOLOGIA_HISTORICA_EXPANDIDA_2026-10-15.json) e do snapshot SQLite. A captura guarda URL e hash por contratação, item e resultado selecionado. A validação `scripts/demo/check_demo_db.py` confere que o banco reproduz exatamente esse recorte.

O [roteiro atual](ROTEIRO_7_MIN.md) usa somente o acervo histórico oficial. As etapas anteriores constam nos registros de planejamento, sem arquivos de casos sintéticos nesta publicação.
