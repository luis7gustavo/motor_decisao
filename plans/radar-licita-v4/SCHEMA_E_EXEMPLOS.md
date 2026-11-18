---
id: SCH-P31
tipo: contrato_proposto
estado: em_revisao
classe_evidencia: E0_E2
atualizado_em: 2026-11-18
---

# P31 — schema observado e campos ausentes

Este é um **mapa do formato observado**, não um schema canônico aprovado nem o contrato de exportação de Carlos. A observação foi feita offline nos [arquivos inventariados](INVENTARIO_AMOSTRAS_P31.md), no commit base `a80cdfcb7fd397126d32c57b73fddcfc227096a9`. O arquivo oficial expandido é um snapshot histórico já versionado; campos abaixo descrevem seus registros, sem garantir que sejam obrigatórios em toda resposta futura.

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

## Ausências relevantes para o produto

| Informação necessária | Estado nesta amostra | Tratamento proposto |
| --- | --- | --- |
| Identidade/versionamento de edital, anexo, página e trecho | Não há no envelope E2; não inventariado como recurso completo nos dois arquivos E0 | `desconhecido` até contrato e documento versionado |
| Unidade de julgamento, composição integral do lote e vínculo item–lote | Não comprovados pelo envelope simplificado | `desconhecido`; não concluir compatibilidade a partir de um item |
| Destino e prazo de entrega | UF do órgão existe; destino de entrega não foi demonstrado aqui | `desconhecido` até edital/anexos revisados |
| Estado de coleta por recurso, hora de recepção e atraso frente à fonte | `captured_at` do arquivo E0 existe; ledger de cobertura/frescor não está nesses envelopes | `não_consultado`/`desconhecido` conforme recurso, sem selo de atualidade |
| Preço e estoque de fornecedor, frete, caixa e rótulo humano | Não integram o envelope PNCP observado | `desconhecido`; não calcular viabilidade confirmada |
| Autorização para reutilizar dados reais como E1, licença e hash do recorte congelado | D13 pendente | Nenhuma nova E1 pública nesta P31 |

## Contrato de normalização proposto para o próximo PR

Preservar o JSON original e seu SHA-256; associar cada projeção à classe de evidência, caminho/URL, instante de captura e versão; mapear ausências para `desconhecido` sem converter nulo em zero. Adaptadores para os dois envelopes devem ser separados e testados com duplicidade, retificação, lista vazia, campos opcionais e texto longo. Carlos e o responsável técnico ainda precisam revisar D14 antes de tratar esse mapa como schema canônico. O caso sintético P31 cobre apenas três bordas do envelope E2; ele não exercita um parser PNCP, deduplicação de itens ou o G1 completo.
