---
id: P12-CONTRATO
tipo: contrato_proposto
estado: autoria_offline_preparada
atualizado_em: 2026-11-19
---

# P12 — contrato financeiro proposto

**Preparado em:** 19/11/2026. **Estado:** proposta técnica E2. O financeiro ainda precisa revisar as premissas e aprovar os casos de ouro; G2 permanece pendente. Nada aqui é cotação, regra tributária aplicável ou avaliação de uma licitação real.

## Escopo e fontes

Este contrato prepara P13 sem alterar o motor da demonstração. Usa o plano interno 1.0, §5 I06 e §5A P12, a proposta V4 e [12 — Simulação financeira](../../docs/licitacoes/12-simulacao-financeira.md) como referências. O protótipo [D offline](../../docs/licitacoes/radar-viability-offline.md) é histórico e contém uma guarda de margem mínima que não deve ser copiada para P13. As entradas em [casos sintéticos](../../fixtures/radar_finance_p12/cases.json) foram inventadas para testar aritmética e estados.

## Contrato de entrada para P13

Uma avaliação recebe `scenario_id`, data de avaliação, versão da fórmula, moeda BRL, identidade e versão da contratação (quando houver), unidade de julgamento e todos os itens obrigatórios. Cada item tem chave estável, quantidade, unidade, preço de aquisição e preço de proposta **hipotético**; preço de catálogo, cotação confirmada e preço de proposta são campos distintos, com fonte, data, validade e estado. Não preencher campo ausente com zero. Lote obrigatório incompleto impede lucro e capital confiáveis.

O cenário informa separadamente custos por item e compartilhados, tributos com base e regra revisadas, custo financeiro, reserva econômica de risco, garantia recuperável, reserva de liquidez e eventos datados de saída e entrada. Cada custo tem ID estável e entra uma vez; a garantia sai e retorna no caixa, sem virar despesa integral. A reserva econômica reduz o lucro ajustado, mas não é desembolso automático. A reserva de liquidez aumenta a necessidade de capital; se ambas cobrem o mesmo risco, o cenário registra esse vínculo para evitar duplicação.

Cada valor monetário entra como texto decimal finito. Sem `float`; valores monetários de linha são quantizados em centavos com `ROUND_HALF_UP`, conforme a política documentada em [12 — Simulação financeira](../../docs/licitacoes/12-simulacao-financeira.md). O regime de faturamento/tributação real deve ser confirmado antes de usar esta política em uma oportunidade. Fluxos do mesmo dia processam **todas as saídas antes das entradas** quando a ordem efetiva não é comprovada.

## Saída e estados

- `revenue`: receita própria simulada. Na intermediação, somente comissão/remuneração da equipe; o valor do edital não é receita própria.
- `profit`: receita menos aquisição, demais custos, tributos e reserva econômica de risco. A margem é informativa; não existe piso universal nem veredito automático de participação.
- `base_capital_need`: máximo entre zero e o maior saldo acumulado negativo, após ordenar saídas antes de entradas no mesmo dia.
- `capital_need`: pico de caixa mais reserva de liquidez vinculada, sob a hipótese documentada de reserva constante.
- `ending_cash`: saldo final dos eventos efetivos. Pode diferir do lucro ajustado quando a reserva econômica não se realizou.
- `missing_reasons`: lista de insumos obrigatórios ausentes. Campos financeiros afetados ficam `null`, nunca zero fabricado.

Resultado calculado deve carregar entradas, versão da fórmula, data/hora, hash do snapshot, proveniência e premissas. A classe E2 permite apenas demonstrar estrutura e contas sintéticas. `confirmado` exige cotação, tributos aplicáveis, cronograma e revisão humana; ausência de qualquer um mantém o cenário preliminar/pendente. Não usar `viável` como recomendação de licitar.

## Casos propostos

| Caso | Condição exercitada | Esperado sintético |
| --- | --- | --- |
| `lote_misto` | Dois itens obrigatórios, B deficitário, custo compartilhado único, compra à vista, garantia devolvida, recebimento em parcelas | Receita 380,00; lucro ajustado 20,00; pico 385,00; capital com reserva 400,00 |
| `recebimento_tardio` | Compra à vista, tributo e recebimento no mesmo dia, sete dias de exposição | Receita 100,00; lucro 25,00; pico 75,00 |
| `preco_ausente` | Preço de proposta não informado | Receita, lucro e capital `null`; motivo explícito |
| `carteira_simultanea` | Os dois primeiros casos coexistem; picos em datas distintas | Pico conjunto 450,00, diferente da soma dos picos 460,00; capital com reserva 465,00 |

Os exemplos não são números comerciais. A carteira simultânea é um caso para P14; P12 apenas congela a expectativa para futura revisão. Rotas alternativas mutuamente exclusivas não são somadas na carteira.

## Pendências antes de aceite

Financeiro/contábil deve confirmar bases de tributo, tratamento da reserva de risco, eventos de garantias, cronograma de recebimento, caixa livre e casos de ouro ao centavo. Luis decide o uso do contrato e G2; P13 depende de revisão independente e decisão humana, sem promoção automática por testes verdes. O retorno é abandonar esta proposta mantendo intactos o protótipo D e o motor atual.
