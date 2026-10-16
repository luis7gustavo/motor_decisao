# Captura histórica oficial

Este diretório contém a captura de compras, itens e resultados publicados no PNCP, além dos scripts para capturar, validar e carregar o recorte histórico. A demonstração atual usa exclusivamente `fixtures/official/PNCP_TECNOLOGIA_HISTORICA_EXPANDIDA_2026-10-15.json`.

Para gerar um banco isolado a partir dessa captura, siga [README_DEMO.md](../../README_DEMO.md). O comando de carga é `seed_real_history.py`; ele cria um novo arquivo sob `backend/data/demo/` e valida compras, itens, resultados e CNPJs antes de disponibilizá-lo.

Os antigos casos e documentos sintéticos não fazem parte deste pacote.
