---
id: SCH-P31
tipo: contrato_proposto
estado: aprovado_com_limites
classe_evidencia: E0_E1_E2
atualizado_em: 2026-11-23
---

# P31 — schema observado e campos ausentes

Este é o **contrato estrutural v1 provisório** aprovado por Luis em 23/11/2026, com o aceite operacional de Carlos delegado por ele. A base combina o mapa offline do commit `a80cdfcb7fd397126d32c57b73fddcfc227096a9` e a exportação E1 real autorizada. Campos observados não se tornam obrigatórios em toda resposta futura: divergências devem ser registradas e campos ausentes permanecem desconhecidos.

| Envelope | Campos observados e tipos JSON | Relação observada / ressalva |
| --- | --- | --- |
| Exemplo PNCP `E0` | `kind` texto, `captured_at` texto, `source` texto, `selection` texto, `request_count` número, `records` lista | O envelope representa a seleção da amostra, não uma página bruta universal da API. |
| `records[]` PNCP | `purchase_url` texto, `purchase` objeto, `purchase_hash` texto, `items_url` texto, `items` lista | Um registro associa uma contratação aos itens consultados. Não presume completude de itens/versões. |
| `purchase` | `numeroControlePNCP` texto, `objetoCompra` texto, `anoCompra` número, `sequencialCompra` número, `modalidadeId` número, `dataPublicacaoPncp` texto, `dataAberturaProposta` texto, `dataEncerramentoProposta` texto, `dataAtualizacao` texto, `unidadeOrgao` objeto | Datas são strings na amostra. `unidadeOrgao.ufSigla` é UF do órgão, **não** destino de entrega. Outros campos e nulos existem no raw. |
| `items[]` | `item` objeto, `item_hash` texto, `result_url` texto, `results` lista, `result_hashes` lista | Item, resultados e hashes são recursos distintos. Lista vazia não significa ausência confirmada de resultado sem estado de coleta. |
| `item` | `numeroItem` número, `descricao` texto, `quantidade` número, `unidadeMedida` texto, `valorUnitarioEstimado` número ou nulo, `dataAtualizacao` texto | A descrição de item isolada não comprova composição nem atendimento de lote. |
| `results[]` | Objetos de resultado com campos de fornecedor, situação, valor, quantidade e datas | Campos podem variar; não atribuir resultado a item/lote sem identidade e versão conferidas. |
| Demonstração `E2` | `observed_at` texto, `source_available` booleano, `versions` lista | Estrutura externa própria da demonstração, não formato de exportação PNCP. |
| `versions[]` E2 | `source_id` texto, `version` número, `object` texto, `uf` texto, `municipality` texto, `category` texto, `opening_state` texto, `deadline` texto ou nulo, `source_url` texto, `source_hash` texto, `published_at` texto, `change` texto | `source_hash` é identificador **ilustrativo**, não SHA-256; o hash de bytes da fixture está no manifesto. `opening_state` não prova abertura atual. |
| Exportação real `E1` | `identity` texto, `source` texto, `version` texto, `acquired_at` texto, `payload` objeto e `coverage` objeto | Cada payload é nomeado por SHA-256; o manifesto liga identidade, versão, arquivo, contagens e limites. |
| `coverage` E1 | `observed_resources_complete`, contagens esperadas/observadas e listas de pendências/órfãos | `true` comprova somente conciliação perante o índice observado; `source_snapshot_verified=false` e `commercial_eligibility_verified=false` impedem promoção indevida. |

## Ausências relevantes para o produto

| Informação necessária | Estado nesta amostra | Tratamento proposto |
| --- | --- | --- |
| Identidade/versionamento de edital, anexo, página e trecho | Não há no envelope E2; não inventariado como recurso completo nos dois arquivos E0 | `desconhecido` até contrato e documento versionado |
| Unidade de julgamento, composição integral do lote e vínculo item–lote | Não comprovados pelo envelope simplificado | `desconhecido`; não concluir compatibilidade a partir de um item |
| Destino e prazo de entrega | UF do órgão existe; destino de entrega não foi demonstrado aqui | `desconhecido` até edital/anexos revisados |
| Estado de coleta por recurso, hora de recepção e atraso frente à fonte | `captured_at` do arquivo E0 existe; ledger de cobertura/frescor não está nesses envelopes | `não_consultado`/`desconhecido` conforme recurso, sem selo de atualidade |
| Preço e estoque de fornecedor, frete, caixa e rótulo humano | Não integram o envelope PNCP observado | `desconhecido`; não calcular viabilidade confirmada |
| Autorização para reutilizar dados reais como E1, licença e hash do recorte congelado | D13 aprovado com controles em 23/11/2026 | Recorte E1 público controlado; integral somente no Kaggle privado |

## Contrato de normalização proposto para o próximo PR

Preservar o JSON original e seu SHA-256; associar cada projeção à classe de evidência, caminho/URL, instante de captura e versão; mapear ausências para `desconhecido` sem converter nulo em zero. Adaptadores para os envelopes devem ser separados e testados com duplicidade, retificação, lista vazia, campos opcionais e texto longo. D14 torna este mapa v1 provisório; P33 deve confrontar novas exportações sem converter divergência silenciosamente. O caso sintético P31 cobre somente bordas E2, enquanto a exportação E1 valida estrutura real; nenhuma das duas prova G3–G6.
