# Radar Licita — comparação histórica sintética, 04/11/2026

Esta é a Etapa C **offline** no worktree `motor_decisao_produto`. `python -m scripts.radar_historical_preview` lê somente `fixtures/radar_demo/historical_results.json`. A fixture declara `synthetic: true`; não há consulta ao PNCP, C2, banco principal ou fornecedor. Os preços não entram no motor de viabilidade e não representam cotação, estoque, capacidade, entrega ou custo atual.

## Contrato demonstrado

- Cada resultado conserva chave, versão, item de origem, data, etapa, quantidade, unidade, moeda, fornecedor e referência de fonte sintética. A versão mais nova substitui a antiga; revogação da mais nova não ressuscita preço anterior. Versões conflitantes ou uma identidade que abranja itens diferentes falham.
- O filtro conservador usa apenas resultado homologado, especificação **exatamente igual** à meta, mesma unidade e moeda, identidade de estabelecimento preenchida e ausência de possível duplicidade. Conversões e equivalências sem prova ficam excluídas com motivo. A comparação técnica por atributos e revisão humana ainda não foi implementada.
- A faixa nominal mínima, mediana e máxima usa `Decimal` sobre as observações utilizáveis, sem corrigir inflação nem ponderar por quantidade. O limiar de três observações é apenas uma regra de demonstração; amostra menor é marcada insuficiente.
- Candidatos são agrupados por identificador sintético de estabelecimento, nunca por nome. Frequência histórica indica somente eventos observados, sem estimar capacidade, taxa de vitória ou aptidão atual.

Na fixture, três observações são utilizáveis e seis versões atuais são excluídas: unidade sem conversão, etapa diferente, especificação diferente, duplicidade pendente, identidade pendente e revogação. A faixa é R$ 1,20–1,80 por unidade, mediana R$ 1,50. Dois estabelecimentos inventados têm o mesmo nome e permanecem separados.

## Liberação por capacidade

| Saída | Evidência offline | Evidência faltante para dados reais |
| --- | --- | --- |
| Identidade, versão e proveniência | IDs e referências inventados, retificação e revogação testadas | Gate A3: vínculo real resultado–item–versão, Bronze/hash, duplicidade e integridade |
| Faixa histórica | Etapa, especificação, unidade, moeda e data explícitas na fixture | Gate A4 resultados: paginação, semeadura por item, cobertura, qualidade e revisão de equivalência |
| Candidatos | IDs sintéticos distintos apesar de nome igual | Validação fiscal/estabelecimento, possíveis duplicidades e cobertura de resultados |
| Uso no produto | CLI sem efeitos externos | G0–G4 em cópia, revisão humana, política de amostra e integração de leitura validada |

`sample_sufficient=true` só descreve três linhas inventadas. Não é aceite de cobertura, comparabilidade real ou recomendação de fornecedor. A etapa permanece **parcial** até revisão independente e dados reais validados por recurso.
