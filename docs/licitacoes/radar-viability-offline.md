# Radar Licita — protótipo D offline, 04/11/2026

Este protótipo usa somente `fixtures/radar_demo/viability_base.json`. Não lê PNCP, C2, banco principal nem preços históricos. O cenário e os resultados são inventados para testar regras; `viável` significa apenas que **as premissas sintéticas** passaram pelos guardas do protótipo, nunca recomendação de participação.

## Contrato implementado

- A unidade de julgamento informa o conjunto obrigatório de itens. Item ausente deixa o cenário `pendente`; IDs duplicados são rejeitados.
- Cotação vencida, incompatibilidade técnica, estoque insuficiente, prazo de participação vencido, obrigação ou logística comprovadamente inviável, margem abaixo da política e capital insuficiente bloqueiam. Versão divergente, fonte sem frescor e evidência obrigatória ausente deixam pendência. Bloqueio conhecido prevalece sobre pendência.
- Entradas numéricas são `Decimal`, sem conversão de `float`; quantidades/preços respeitam limites e até seis casas. Cada item compõe a receita e o custo de aquisição; custos compartilhados entram uma vez. `R`, `V`, `F`, `J`, `Z`, tributo linear, lucro e margem seguem 12 §3. O cálculo arredonda cada linha monetária ao centavo antes de somar, em `ROUND_HALF_UP`; essa política é local ao protótipo e precisa ser confrontada com o edital antes do piloto.
- Eventos de caixa são datados, com saídas antes das entradas no mesmo dia. Garantia recuperável entra no fluxo e não no lucro; o prazo de exposição acompanha sua liberação mesmo após o recebimento. `G_base` é o maior déficit acumulado; `G=G_base+reserva de liquidez`. Capital informado é o **livre após outros compromissos**, constante durante o cenário. Atraso além da validade da condição financeira deixa pendência e exige nova simulação.
- O resultado carrega versão da fórmula, data da avaliação, hash das entradas e motivos. Reavaliar em outra data muda o hash. Nenhuma classificação aciona proposta, compra ou pagamento.

## Evidência do cenário base

Lote sintético A+B: receita R$ 13.300,00, custo variável R$ 10.300,00, fixo R$ 200,00, financeiro R$ 180,00, tributo R$ 798,00, reserva econômica R$ 200,00, lucro ajustado R$ 1.622,00, necessidade de capital R$ 12.178,00. O item B isoladamente é deficitário; o resultado usa o lote inteiro. O CLI `python -m scripts.radar_viability_preview` foi executado em container sem rede, com `status=viável`, zero chamadas PNCP e zero escritas.

Oito testes focados de D e quatro de B passaram no container sem rede. D cobre lote completo, cotação vencida, incompatibilidade, retificação concorrente, capital insuficiente, custo compartilhado ausente, atraso de recebimento, precisão inválida e devolução de garantia impossível. `git diff --check` passou.

## Limites e gates

Ainda faltam identidade e composição reais da unidade, extração/revisão de exigências, matching por atributos com evidências, cotação/estoque atuais de fornecedor, logística detalhada, política tributária aplicável, cronograma e capital comprovados, limites por data, taxa financeira recalculável, memória de rateio, ROI e persistência de snapshots/eventos. O protótipo usa taxa linear e custo financeiro fixo válido até a data declarada; atraso além dessa data não reutiliza a taxa. Zero obrigatório sem justificativa só é guardado para os três custos compartilhados; ampliar a política antes do piloto.

Integração real exige Gate A3/A4 por recurso, evidência de fornecimento atual e G0–G4 em cópia. Etapa E e revisão independente permanecem abertas. Não apresentar o `status=viável` sintético como análise de licitação real.
