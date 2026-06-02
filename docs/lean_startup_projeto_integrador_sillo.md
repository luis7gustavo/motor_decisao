# SILLO - Projeto Integrador Lean Startup

**Documento de resposta às questões da apresentação Lean Startup para Projetos de Data Science & Machine Learning**

**Tagline:** SILLO - Intelligence for better buying

**Data de referência:** 01/06/2026, com resultados técnicos validados na mesma data

**Natureza do projeto:** MVP local-first de inteligência de compras para revenda, com pipeline de dados e motor de decisão auditável.

> **Tese do projeto:** A SILLO transforma catálogos de fornecedores e sinais de mercado em uma fila priorizada de oportunidades de compra, reduzindo o risco de decisões baseadas apenas em intuição.

## 1. Síntese Executiva

A SILLO é uma plataforma inteligente para apoiar decisões de compra orientadas por dados. O projeto parte de um problema real de revenda: escolher, entre milhares de produtos de fornecedores, quais itens têm maior chance de gerar margem e giro sem expor o comprador a risco excessivo.

O MVP já implementado cobre ingestão de dados, arquitetura Medalhão, normalização de fornecedores, motor de decisão e API operacional. A entrega atual não executa compras automaticamente; ela recomenda ações com níveis de confiança e preserva evidências para revisão humana.

- Fontes de fornecedores: MegaMix via arquivo JSON, Mirão via web scraper e Coletek via planilha já carregados no banco.
- Fontes de mercado: Mercado Livre, marketplaces, comparadores e histórico parcial de preços.
- Camadas de dados: Bronze para dados brutos, Silver para normalização e Gold para decisões auditáveis.
- Saída do motor: `comprar_teste`, `revisar` ou `ignorar`, sempre com guardrails de risco.

## 2. Canvas do Projeto Integrador

Abaixo estão as respostas diretas ao Canvas apresentado nos slides do workshop.

| Bloco do Canvas | Resposta para a SILLO |
| --- | --- |
| Problema de negócio | Pequenos revendedores e compradores operacionais precisam decidir o que comprar para revenda com dados fragmentados. A decisão manual consome tempo, aumenta o risco de estoque parado e pode destruir margem quando preço, demanda, frete ou equivalência de produto são mal avaliados. |
| Quem é afetado | Compradores de revenda, pequenos lojistas, analistas operacionais e donos de negócio que precisam transformar catálogos grandes em decisões práticas de compra. |
| Impacto | Perda de margem, capital parado em produtos ruins, excesso de tempo gasto em comparação manual, risco de comprar produto sem demanda ou com concorrência agressiva. |
| Hipótese central | Acredito que preço de fornecedor, preço estimado de mercado, diversidade de fontes, sinais de demanda, histórico de preço e qualidade do match estão relacionados à viabilidade de compra; e que é possível ranquear oportunidades com confiança operacional usando dados de fornecedores, marketplaces e comparadores. |
| Fonte de dados | MegaMix (`data/megamix_catalog_raw.json`), Mirão via web scraper, Coletek via planilha, Mercado Livre, Amazon, Kabum, Terabyte, Shopee, AliExpress, Pichau, Magalu, Zoom e Buscapé. Os dados são armazenados em PostgreSQL nas camadas Bronze, Silver e Gold. |
| MVP - primeira entrega | Um motor local que ingere dados, normaliza produtos, calcula margem, confiança e risco, e gera uma lista priorizada de oportunidades. A primeira pergunta respondida é: quais produtos merecem compra experimental pequena agora? |
| Métricas de sucesso | Cobertura de produtos elegíveis, quantidade de evidências por produto, margem líquida estimada, diversidade de fontes, nível de confiança, taxa futura de aprovação humana, margem realizada e giro após compra-teste. |
| Stakeholder / público | Revendedores, compradores B2B, pequenos e-commerces, equipes comerciais e avaliadores acadêmicos interessados em um projeto aplicado de Data Science com impacto operacional real. |

## 3. Validação do Problema

| Pergunta de validação | Resposta | Evidência no projeto |
| --- | --- | --- |
| O problema é real? | Sim. | A decisão de compra usa múltiplas fontes e milhares de produtos. O risco de falso positivo, margem baixa e falta de demanda já aparece nas flags do motor. |
| É relevante para alguém? | Sim. | Revendedores precisam comprar melhor, reduzir estoque parado e priorizar produtos com evidência de mercado. |
| Posso medir o problema? | Sim. | O projeto mede margem estimada, preço de mercado, score de demanda, quantidade de evidências, diversidade de fontes, confiança de match e recomendação. |
| Tenho dados para analisar? | Sim. | O snapshot validado contém 18.583 registros Bronze de fornecedor, 17.324 ofertas web e 3.824 registros de histórico de preço. |
| É viável em 1 semestre? | Sim. | O MVP já existe localmente com API, banco, pipelines, documentação e motor versionado. O escopo final deve continuar como apoio à decisão, não compra automática. |

**Conclusão da validação:** o problema passa nas 5 perguntas da apresentação. O escopo é bom para projeto integrador porque tem dado real, decisão de negócio clara, MVP funcional e espaço natural para iteração.

## 4. Objetivo e Hipótese Testável

**Pergunta central de pesquisa:** entre os produtos disponíveis em fornecedores, quais apresentam evidência suficiente de margem, demanda e equivalência para justificar uma compra-teste controlada?

**Hipótese no template da apresentação:** acredito que sinais de preço de fornecedor, preço de mercado, diversidade de fontes, demanda observável e qualidade de match estão relacionados à oportunidade real de revenda; e que é possível classificar produtos em `comprar_teste`, `revisar` ou `ignorar` com precisão operacional crescente usando dados de fornecedores, marketplaces e histórico de preços.

**Objetivo analítico:** construir e validar um pipeline que reduza um catálogo amplo de fornecedores a uma lista curta, auditável e priorizada de oportunidades de compra, mantendo rastreabilidade das evidências usadas em cada decisão.

| Elemento | Definição no projeto |
| --- | --- |
| Variável X | Preço de fornecedor, preço estimado de mercado, quantidade de evidências, diversidade de fontes, score de demanda, similaridade textual e flags de risco. |
| Resultado Y | Recomendação de compra: `comprar_teste`, `revisar` ou `ignorar`. |
| Tipo de análise | Pipeline de dados, EDA operacional, regras heurísticas versionadas, ranking de oportunidades e validação futura supervisionada. |
| Métrica de sucesso | No MVP: coerência operacional, rastreabilidade e redução da lista para oportunidades seguras. Na evolução: aprovação humana, margem realizada, giro e precisão@k das recomendações. |

## 5. MVP de Data Science

O MVP não tenta resolver todo o ciclo de compra. Ele entrega a menor versão funcional capaz de gerar aprendizado real: coletar dados, comparar fornecedor versus mercado e produzir uma recomendação auditável.

| Etapa Lean Startup | Implementação na SILLO | Aprendizado gerado |
| --- | --- | --- |
| Build | Pipelines de ingestão, banco PostgreSQL, camadas Bronze/Silver/Gold, motor `heuristic_v2_confidence_guard` e rotas FastAPI. | É possível transformar fontes heterogêneas em uma base comparável de decisão. |
| Measure | Rodada validada com 5.776 produtos pontuados, 21.031 evidências usadas, 2 `comprar_teste`, 38 `revisar` e 5.736 `ignorar`. A camada híbrida amplia a fila explicável para 171 revisões. | O motor é conservador: prefere bloquear compra quando há pouca evidência, match fraco ou margem insuficiente. |
| Learn | A saída mostra que a compra automática ainda seria prematura, mas a triagem para revisão humana já tem valor. | A próxima iteração deve focar em match canônico, margem real e validação humana para criar dataset supervisionado. |

## 6. Resultados Atuais do Motor

A última rodada validada em 01/06/2026 mostra um comportamento coerente para um MVP de decisão de compra: o motor é restritivo e libera poucos itens como compra-teste.

| Indicador | Valor |
| --- | ---: |
| Versão do motor | `heuristic_v2_confidence_guard` |
| Produtos pontuados | 5.776 |
| Evidências usadas | 21.031 |
| `comprar_teste` | 2 |
| `revisar` | 38 |
| `ignorar` | 5.736 |
| Confiança alta | 2 produtos |
| Confiança média | 38 produtos |
| Confiança baixa | 5.736 produtos |

| Produto | Fornecedor | Preço fornecedor | Preço mercado | Margem líquida | Confiança |
| --- | --- | ---: | ---: | ---: | --- |
| Adaptador USB 3.0 para RJ45 10/100/1000 Mbps Dex - UR-03 | Mirão | R$ 71,01 | R$ 169,00 | 25,98% | Alta |
| ADAPTADOR WIRELESS 600MBPS | MegaMix | R$ 34,99 | R$ 81,99 | 25,32% | Alta |

**Leitura crítica:** o resultado é lógico para um MVP conservador. Duas recomendações fortes entre mais de cinco mil produtos indicam que os guardrails estão evitando decisões frágeis. A limitação é que ainda não sabemos a precisão real até registrar revisão humana e resultado de venda.

## 7. Métricas de Sucesso

As métricas abaixo conectam a metodologia da apresentação com a realidade do projeto. Algumas já são medidas; outras são objetivos da próxima iteração.

| Métrica | Status atual | Meta de evolução |
| --- | --- | --- |
| Cobertura de pontuação | 5.776 produtos elegíveis pontuados na rodada validada. | Pontuar todo produto com preço e dados mínimos, com controle de freshness por fonte. |
| Qualidade da recomendação | Classificação em 3 níveis com guardrails de risco. | Medir precisão@k e taxa de aprovação humana das recomendações `comprar_teste`. |
| Margem | Margem líquida estimada calculada pelo motor. | Incluir frete, taxas, impostos, estoque e margem realizada após venda. |
| Demanda | Score de demanda por evidências disponíveis. | Usar sinais históricos e vendas reais para estimar probabilidade de giro. |
| Confiabilidade do match | Match textual e flags de risco. | Criar entidade canônica de produto e classificador de equivalência. |
| Aprendizado validado | Runs versionadas e snapshots em Gold. | Registrar decisão humana, motivo de rejeição e resultado comercial da compra-teste. |

## 8. Análise e Metodologia

A metodologia técnica segue uma arquitetura Medalhão, que separa coleta, normalização e decisão. Essa separação fortalece o projeto porque cada camada responde a uma função clara.

| Camada | Papel | Exemplo no projeto |
| --- | --- | --- |
| Bronze | Preservar dados brutos e payloads originais. | Ofertas web, histórico de preço e produtos de fornecedor sem decisão de negócio. |
| Silver | Normalizar dados e criar campos comparáveis. | Produtos de fornecedor normalizados, tokens e chaves de origem. |
| Gold | Gerar saída de decisão e histórico auditável. | Tabela `gold.decision_opportunities`, runs versionadas e snapshots. |
| API | Operar e consultar o sistema. | Rotas `/health`, `/ops/*` e `/decision-engine/*`. |

O baseline continua heurístico e explicável. A camada híbrida de ML compara Logistic Regression, Random Forest, HistGradientBoosting e XGBoost CPU, registra scores auditáveis e mantém a heurística como trava de segurança. A rodada atual também testa augmentation controlado somente no treino e preserva avaliação exclusivamente real. Como os rótulos atuais são proxy da heurística, ainda falta registrar vendas reais para validar desempenho comercial.

## 9. Pivotar ou Perseverar

A recomendação é perseverar na direção do produto, mas manter o posicionamento correto: a SILLO ainda é uma ferramenta de apoio à decisão, não um sistema de compra automática.

| Decisão | Justificativa |
| --- | --- |
| Perseverar | O problema é real, os dados existem, o MVP funciona e a saída é interpretável. A arquitetura já permite medir e aprender a cada rodada. |
| Pivot já realizado | O projeto saiu da ideia de compra automática para recomendação auditável com compra-teste controlada. Esse pivô reduz risco e melhora a validade acadêmica e operacional. |
| Próximo ponto de decisão | Após revisão humana dos itens recomendados, decidir se o motor precisa mudar pesos, ampliar fontes ou melhorar o matching antes de substituir rótulos proxy por vendas reais. |

## 10. Avaliação pelos Critérios do Projeto

| Critério | Peso | Avaliação da SILLO |
| --- | ---: | --- |
| Problema de negócio | 25% | Forte. O problema é real, específico e conectado a uma decisão econômica clara: comprar ou não comprar para revenda. |
| Hipótese e objetivo | 20% | Forte, mas deve ser refinado com métricas futuras de precisão e margem realizada. A hipótese é mensurável e falsificável. |
| Análise e metodologia | 30% | Boa para MVP. Há pipeline, arquitetura Medalhão, motor versionado e evidências auditáveis. O ponto fraco é a ausência de labels reais de venda e revisão humana. |
| MVP e entrega | 25% | Forte. Existe sistema local rodável, documentação, banco, API, coleta de fornecedores e motor com saída operacional. |

## 11. Limitações e Riscos

- Matching ainda é heurístico e pode confundir produtos parecidos, marcas, modelos ou variações técnicas.
- Frete, impostos, taxa do canal, estoque real e prazo de entrega ainda não entram de forma completa no score.
- Scrapers podem sofrer bloqueio, mudança de DOM, rate limit ou coleta parcial.
- Histórico de preço de comparadores ainda é parcial e não substitui uma série temporal completa.
- Ainda não há base supervisionada de decisões humanas, vendas reais, devoluções e margem realizada.
- `comprar_teste` não significa compra automática; significa candidato para validação manual e lote pequeno.

## 12. Próximos Passos e Objetivos

| Objetivo | Entrega esperada | Por que importa |
| --- | --- | --- |
| Produto canônico | Criar `silver.canonical_products` e relacionamentos fornecedor/mercado. | Reduz falso positivo de match e aumenta segurança da recomendação. |
| Margem realista | Adicionar frete, taxa por canal, imposto, estoque e simulação de lote. | A margem estimada precisa se aproximar da margem realizada. |
| Validação humana | Tela ou endpoint para aprovar/reprovar oportunidades e registrar motivo. | Transforma julgamento operacional em dado supervisionado para evolução do motor. |
| Evolução estatística e ML | Classificador de equivalência, probabilidade de venda, margem realizada e ranking learning-to-rank. | Substitui gradualmente regras rígidas por modelos treinados com dados reais. |
| Observabilidade | Dashboard de runs, freshness por fonte, bad streak, backoff e alertas. | Aumenta confiabilidade operacional das coletas e do pipeline. |
| Produto SILLO | Dashboard com filtros, detalhe de evidências, histórico por run e exportação CSV. | Transforma o MVP técnico em ferramenta usável para decisão de compra. |

## 13. Resumo para Apresentação em 2 Minutos

A SILLO resolve um problema comum em revenda: decidir o que comprar quando existem milhares de produtos de fornecedores e sinais de mercado espalhados em várias fontes. Em vez de começar pelo dataset, o projeto começa pela decisão de negócio: encontrar produtos com margem, demanda e evidência suficiente para uma compra-teste segura.

O MVP coleta dados de fornecedores como MegaMix, Mirão e Coletek, junta sinais de marketplaces e comparadores, organiza tudo em camadas Bronze, Silver e Gold e roda um motor de decisão auditável. Na rodada validada, 5.776 produtos foram pontuados e apenas 2 passaram como `comprar_teste`, mostrando uma postura conservadora adequada para reduzir falso positivo.

O próximo ciclo é transformar revisão humana em dado, melhorar o matching de produto e incluir custos reais como frete e taxas. Assim, o projeto evolui de regras explicáveis para modelos estatísticos e de Machine Learning treinados com evidências reais.

## 14. Conclusão

A SILLO está bem alinhada com Lean Startup porque entrega aprendizado rápido sem inflar o escopo. O projeto já possui MVP funcional, dados reais, hipótese testável, critérios de medição e uma rota clara de evolução. A decisão correta agora é continuar iterando com validação humana e métricas de resultado comercial antes de qualquer automação de compra.
