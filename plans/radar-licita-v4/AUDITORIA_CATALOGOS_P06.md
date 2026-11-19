---
id: EVID-P06
tipo: evidencia
estado: parcial_para_revisao
classe_evidencia: acervo_local_versionado
atualizado_em: 2026-11-19
---

# P06 — reconciliação offline dos catálogos

Em 19/11/2026, foi executado `python scripts/audit_supplier_catalogs.py` em contêiner já disponível, sem rede e com o worktree montado somente para leitura. O verificador lê os dois JSON rastreados no repositório, confere contagem, identidade de origem única, título e preço estruturalmente válido e emite o SHA-256 dos bytes de entrada. O CSV MegaMix alternativo não foi somado como terceiro catálogo.

| Fonte rastreada | SHA-256 | Registros | IDs únicos | Sem preço |
| --- | --- | ---: | ---: | ---: |
| `data/megamix_catalog_raw.json` | `b6dd728fa89027f85828f075a0d7f19aaed81ed957479d548ee9be6d200e3a40` | 2.360 | 2.360 | 0 |
| `data/coletek_catalog_raw.json` | `83b0f0aa9634ec61654bf88064a1a4c427c6fe6af5eeabe12398bfd809077b65` | 735 | 735 | 111 |

**Resultado:** 3.095 registros de origem reconciliados nos arquivos examinados, sem erro estrutural detectado pelo verificador. Os 111 registros Coletek sem preço permanecem consultáveis como candidatos sem preço confirmado. Preços preenchidos também são referências históricas, sem confirmação de estoque, frete, prazo ou cotação atual. O arquivo Coletek informa `source_snapshot_date` de 01/06/2026; o JSON MegaMix não informa data de origem por registro. A data desta auditoria não substitui a data da fonte.

Os importadores existentes em `scripts/import_megamix_catalog.py` e `scripts/import_coletek_catalog.py` foram executados **somente** em PostgreSQL 15 com pgvector, em contêiner descartável, rede Docker própria e banco de teste com nome aceito pela guarda da migração. O worktree foi montado somente para leitura; nenhum serviço, banco ou credencial operacional foi usado. A migração até `20261013_0010` passou. Na primeira carga, MegaMix inseriu 2.360/2.360 e Coletek 735/735. Na segunda carga, ambos inseriram zero e marcaram os 3.095 registros como duplicados/ignorados. Consulta SQL ao Bronze de teste: 2.360 linhas MegaMix com preço e 735 Coletek, das quais 111 com `raw_price IS NULL`.

**Limite da idempotência:** a repetição foi feita no mesmo dia. O importador MegaMix usa `fetched_date` padrão do banco (19/11/2026 no teste), embora o arquivo não declare essa data como data de origem; a restrição de unicidade inclui esse campo. Reimportar os mesmos bytes em outro dia pode criar novas linhas. Coletek gravou `fetched_date=2026-06-01` a partir de `source_snapshot_date`. Identidade de snapshot e deduplicação entre dias para MegaMix exigem correção/revisão antes de considerar a importação versionada pronta. `fetched_date` do teste MegaMix não deve ser comunicado como frescor de preço.

Verificação local: três testes sintéticos do auditor passaram (`python -m unittest discover -s tests -p test_audit_supplier_catalogs.py -v`). O teste de fonte real retornou código 0 e `errors: []` para ambos os arquivos. Uma primeira tentativa de migração na imagem `postgres:15-alpine` falhou por ausência da extensão `vector`; a imagem local `pgvector/pgvector:pg15` resolveu isso. A guarda de nome do banco impediu migrar um nome genérico; o ensaio foi feito em `test_20261119_ab12cd34ef56`, conforme a exigência da migração. Não houve chamada externa, alteração do banco operacional, cópia de dados novos ou mudança na coleta. Revisão independente, correção da identidade de snapshot, aceite G2 e integração ainda são pendentes.

## Complemento: guarda de identidade do snapshot

O importador MegaMix passou a exigir a data de origem como entrada explícita. Ele guarda SHA-256 do arquivo e recusa reapresentar os mesmos bytes com outra data; a data da carga não é usada como substituta. Em banco `test_20261119_cdef12345678` e rede Docker exclusivos, o arquivo E2 `tests/fixtures/megamix_snapshot_synthetic.json` inseriu 2 linhas em 01/06/2026; a segunda execução inseriu 0; a mesma fonte com 02/06/2026 retornou erro antes de criar execução. Consulta final: 2 linhas, ambas com `fetched_date=2026-06-01`. Migração até `20261013_0010` passou; contêiner/rede descartáveis foram encerrados. Dois testes unitários da guarda passaram. Isso não comprova a data do arquivo real nem corrige retroativamente cargas legadas sem SHA-256 registrado.
