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

O código existente em `scripts/import_megamix_catalog.py` e `scripts/import_coletek_catalog.py` grava no Bronze operacional. **Nenhum importador foi executado nesta sessão**, e a contagem acima não é contagem de linhas carregadas no banco. Antes de considerar P06 implementada, é necessário testar a carga e a reimportação em banco descartável, comprovar identidade de snapshot, deduplicação entre dias, preservação dos 111 preços desconhecidos e reconciliação das linhas persistidas. As credenciais e o banco em operação não devem ser usados para esse ensaio.

Verificação local: três testes sintéticos do auditor passaram (`python -m unittest discover -s tests -p test_audit_supplier_catalogs.py -v`). O teste de fonte real retornou código 0 e `errors: []` para ambos os arquivos. Não houve chamada externa, alteração de banco, cópia de dados novos ou mudança na coleta. Revisão independente, aceite G2 e integração ainda são pendentes.
