# 03 — Documento de Análise de Negócio

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-10 (America/Sao_Paulo).
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: rascunho para revisão do operador. Análise futura [PROPOSTO]; linha de base comercial desconhecida.

## 1. Contexto, justificativa e oportunidade

O SILLO foi construído para compra e revenda, com [CONFIRMADO] coleta de sinais comerciais, fornecedores, heurística e revisão. O [diagnóstico](02-diagnostico-as-is.md) confirma investimentos reaproveitáveis e delimita a ausência do domínio público. A oportunidade proposta é usar essa base para reduzir trabalho de triagem e explicitar a viabilidade de fornecimentos governamentais.

O problema de negócio é avaliar conjuntamente edital, produto, disponibilidade, execução e caixa. Uma lista de editais ou comparação de preço isolada não responde se o operador consegue fornecer a unidade completa de julgamento, no prazo e com retorno ajustado às incertezas. A dimensão do mercado e a economia de tempo ainda não foram medidas; não se atribui valor financeiro aos benefícios sem piloto.

## 2. Personas e necessidades

| Persona lógica | Tarefa | Dor/necessidade | Evidência de sucesso a coletar |
|---|---|---|---|
| Operador comercial individual | Escolher oportunidades para estudo e decidir participar | Muitos documentos, dados incompletos e comparação manual | Tempo ativo de triagem e taxa de decisões justificadas |
| Operador financeiro, possivelmente a mesma pessoa | Planejar desembolsos e recebimentos | Margem aparente com caixa insuficiente | Pico de capital e diferença previsto/realizado |
| Especialista técnico | Conferir produto e equivalência | Similaridade textual mascara especificações divergentes | Precisão do matching confirmado |
| Contador/assessoria jurídica | Avaliar regras aplicáveis, habilitação, tributos e envio direto | Premissas não verificadas | Pendências formalmente resolvidas por responsável |
| Fornecedor/distribuidor | Confirmar oferta e capacidade | Cotação incompleta ou expirada | Cotações completas, válidas e rastreáveis |
| Operador técnico | Manter dados atuais e recuperáveis | Fonte indisponível, retificação perdida, restore inseguro | Frescor, reconciliação, backup/restauração verificados |

São personas de trabalho, não novos usuários do sistema. O MVP não exige acesso do fornecedor ou do assessor ao aplicativo.

## 3. Objetivos e escopo

Objetivos [PROPOSTO]: priorizar fornecimentos verificáveis, favorecer execução sem deslocamento do operador, evitar recomendações positivas diante de bloqueios/pendências, medir viabilidade econômica por unidade de julgamento e preservar histórico para aprender com resultados.

MVP: fases 1–5, capacidades Must descritas em [04](04-requisitos-funcionais.md); cobre descoberta, documentação, logística, fornecedor/matching, finanças, decisão e registro comercial básico. Sensibilidade ampliada é Should; histórico básico/candidatos são Should em 2-H e contato na fase 3; inteligência ampla fica fase 6 (revisado em R1). OCR/LLM/ML avançado são posteriores. Revisão humana é transversal desde o enriquecimento; a fila comercial consolidada chega na fase 5.

Exclusões: propostas/lances automáticos, compras/pagamentos, garantia de contratação ou lucro, parecer especializado e multiusuário complexo. O sistema registra atos realizados fora dele; não os executa por inferência.

## 4. Fluxo atual e futuro

| Passo | AS-IS do SILLO | TO-BE [PROPOSTO] | Mudança de negócio |
|---|---|---|---|
| Descoberta | [CONFIRMADO] ofertas comerciais e catálogos | Contratações oficiais abertas/atualizadas | Obrigação contratual substitui proxy de demanda |
| Preparação | [CONFIRMADO] tokens e preços | Itens, lotes, documentos, requisitos e evidências | Unidade de julgamento governa análise |
| Fornecimento | [PARCIAL] produto/preço/estoque bruto | Cotação com validade, capacidade e cobertura | Disponibilidade precisa ser confirmada |
| Comparação | [CONFIRMADO] similaridade lexical/proxies | Atributos obrigatórios/unidade/embalagem | Verificação técnica antes da aprovação |
| Economia | [CONFIRMADO] margem heurística de revenda | Custos contratuais, cenários e curva de caixa | Capital e prazo tornam-se bloqueios |
| Decisão | [CONFIRMADO] comprar teste/revisar/ignorar | Elegibilidade, viabilidade, ranking e decisão humana | Sem migração automática de rótulos |
| Resultado | [CONFIRMADO] snapshots e feedback de revisão | Participação manual, disputa, contrato, entrega e recebimento | Separar estimativa de resultado efetivo |

Jornada futura: descoberta → enriquecimento → cotação/revisão → pré-aprovação de estudo → avaliação completa → apta para decisão humana → participação registrada manualmente → disputa/resultado → execução/entrega/recebimento → encerramento. Descarte e reanálise podem ocorrer antes da participação; após ela, pendências não apagam o estágio factual. Detalhes em D07/D10.

## 5. Restrições, premissas e dependências

| Tipo | Restrição/premissa | Consequência |
|---|---|---|
| Técnica | Mesmo PostgreSQL de aplicação; evolução Docker incremental | Preservar volumes/IDs; novos schemas são decisão futura |
| Técnica | APIs públicas e PDFs podem falhar ou mudar | Preservar cache/histórico e sinalizar atualidade |
| Financeira | Capital disponível não informado | Parâmetro obrigatório para recomendar oportunidade real |
| Comercial | Cotação não reserva estoque nem garante preço futuro | Evidência/validade e reconfirmação perto da decisão |
| Operacional | Preferência por remoto | Não excluir C automaticamente se houver execução local comprovada; manter prioridade configurável |
| Humana | Operador confirma eventos e recebe apoio especializado externo | Registrar responsável/data/fundamento da resolução |
| Regulatória | Regras dependem do regime, edital e caso concreto | Nunca inferir habilitação ou benefício automaticamente |
| Documental | Extração pode omitir tabelas, anexos ou conteúdo de imagem | Encaminhar para revisão; desconhecido não vira aprovação |

Referências oficiais consultadas em 2026-10-10: [Lei 14.133/2021, texto compilado](https://www.planalto.gov.br/ccivil_03/_ato2019-2022/2021/lei/l14133.htm) e [Lei Complementar 123/2006](https://www.planalto.gov.br/ccivil_03/leis/lcp/lcp123.htm). São referências para registrar regime e pendências de aplicabilidade; esta etapa não codifica prazos legais, alíquotas, limites monetários ou benefícios. Vigência e aplicabilidade de cada regra concreta serão verificadas no documento 13/Execução 3 e novamente quando usadas numa contratação.

## 6. Indicadores de sucesso e protocolo de medição

**Todas as metas são propostas iniciais calibráveis, não fatos nem compromissos de desempenho comprovados.** Linha de base de todos os indicadores: **desconhecida**. Responsável pela medição: operador, com revisão técnica dos rótulos e cálculos. Janela inicial: quatro semanas de piloto e pelo menos 50 unidades de julgamento; onde houver poucos resultados realizados, publicar tamanho da amostra e não concluir eficácia.

Para MN-07 e MN-08, **decisão positiva** é a decisão final que declara aptidão para participação, correspondente a E07 (RN-032). Pré-aprovação para estudo, E05, é medida separadamente e não entra nesse denominador, pois admite lacunas ainda em estudo. Em qualquer indicador de proporção, denominador zero produz “não calculável”, sem declarar meta atingida.

| ID | Indicador/definição operacional | Fonte de dados e denominador | Meta inicial [PROPOSTO] | Método de validação/calibração |
|---|---|---|---|---|
| MN-01 | Mediana de minutos ativos entre abrir oportunidade e registrar triagem, excluindo espera externa | Eventos UI e cronômetro; todas as triagens completas do piloto | Reduzir 30% frente ao processo manual na mesma amostra | Medir manual primeiro, distribuir casos equivalentes e revisar complexidade |
| MN-02 | Precisão do descarte = descartes corretos / descartes revisados | Amostra estratificada por motivo, avaliada independentemente | >=95%, com quantidade e intervalo de incerteza publicados | Revisar todos os casos controversos; separar ausência de dado de inviabilidade |
| MN-03 | Falso descarte = oportunidades viáveis descartadas / oportunidades viáveis na amostra | Amostra inclui descartadas e mantidas; referência humana documentada | <=5% | Não medir apenas descartadas; investigar cada oportunidade viável perdida |
| MN-04 | Precisão do matching confirmado = pares tecnicamente corretos / pares classificados confirmados | Revisão por atributos, ficha técnica e unidade; >=50 pares quando disponíveis | >=95%; nenhum conflito obrigatório conhecido aceito | Separar categoria e atributo; limiar só muda com versão registrada |
| MN-05 | Erro absoluto percentual do custo total = abs(previsto-real)/real, para real>0 | Cenário congelado antes da decisão e documentos de custos realizados | Mediana <=10% em >=10 operações concluídas | Se amostra insuficiente, relatório descritivo sem declarar meta atingida |
| MN-06 | Proporção operacionalmente atendível = unidades com produto, capacidade, prazo e execução confirmados / unidades triadas | Checklist de viabilidade; incluir classe logística e pendências | Hipótese exploratória >=20%, sem incentivo a afrouxar bloqueios | Segmentar por categoria/região e revisar seleção, não alterar rótulos para cumprir meta |
| MN-07 | Cobertura de evidência = decisões positivas com fonte/versão/trecho/data/revisor completos / decisões positivas | Snapshots e trilha de decisão | 100% | Auditoria de todos os positivos no piloto |
| MN-08 | Pendências críticas na decisão = decisões positivas com pendência impeditiva / decisões positivas | Histórico de verificações na versão usada | 0% | Casos adversos: edital retificado, cotação vencida, prazo desconhecido |
| MN-09 | Preferência remota = aptas sem deslocamento do operador / aptas | Confirmações de logística e obrigações pós-venda | Hipótese exploratória >=70% | Relatar D separadamente; não tratar ausência de informação como remoto |
| MN-10 | Desvio de capital = pico real de desembolso acumulado - pico previsto | Cronogramas previstos e entradas/saídas realizadas | Nenhuma falta de caixa ignorada; meta monetária calibrada após piloto | Investigar atraso de recebimento e custos não previstos |

Eficiência técnica (latência, RPO/RTO, disponibilidade e atualização) terá metas separadas em [05](05-requisitos-nao-funcionais.md). Não confundir cobertura documental, classe logística, score ou taxa de vitória com probabilidade estatística calibrada. Lucro estimado, adjudicação, contratação e recebimento são eventos diferentes.

## 7. Riscos de negócio e resposta inicial

| Risco | Consequência | Tratamento proposto | Sinal de acompanhamento |
|---|---|---|---|
| Ganho aparente por usar preço estimado como venda | Margem superestimada | Cenário de disputa explícito; referência desconhecida não vira zero | Receita sem premissa documentada |
| Item lucrativo em lote inviável | Incapacidade de fornecer tudo | Análise integral da unidade de julgamento | Item obrigatório sem fornecedor/custo |
| Frete/prazo/pós-venda subestimados | Atraso ou custo extra | Evidência por destino, parcela e obrigação | Informação logística não confirmada |
| Capital insuficiente ou recebimento tardio | Interrupção de execução | Curva de caixa, reserva e cenário adverso | Pico acima do capital declarado |
| Cotação vencida ou estoque volátil | Custo mudar após decisão | Validade, reconfirmação e alternativas | Cotação expirando antes da compra |
| Matching falso positivo | Produto recusado | Verificação por atributo e revisão humana | Conflito obrigatório ou unidade divergente |
| Retificação ou dado desatualizado | Decisão baseada em condição anterior | Versionar e invalidar atualidade das análises afetadas | Versão oficial posterior à revisão |
| Aplicação indevida de norma/benefício | Participação irregular | Regime/fonte e verificação especializada | Habilitação/envio direto sem responsável |
| Excesso de confiança no ranking | Automatização informal da decisão | Bloqueios precedem score; explicação e confirmação explícita | Aprovação sem abertura de evidência |

Probabilidade, impacto e risco residual detalhados serão tratados na matriz da Execução 3. Não há avaliação empírica suficiente nesta etapa para inventar frequências de ocorrência.

## 8. Questões para calibração e critérios do piloto

Questões a resolver: categorias iniciais, UFs/cidades, fornecedores disponíveis, política de capital e limite de exposição, dias de recebimento tolerados, capacidade de atendimento presencial por terceiros, suporte especializado e limiares de custo/margem. Registrar as respostas como parâmetros versionados; não embutir valores pessoais arbitrários.

Piloto pronto para avaliação quando: fonte oficial acessível ou falha explícita; casos sintéticos representarem todos os bloqueios; toda recomendação positiva tiver evidência e revisão; cenários financeiros forem reproduzíveis; atualização invalidar conclusão anterior sem apagá-la; rastreabilidade permitir comparar previsto e realizado. A execução desse piloto está fora desta missão documental.


## 9. Hipótese de descoberta e métricas complementares — revisado em R1

[PROPOSTO] O piloto de [23](23-especificacao-piloto-fornecedores.md) testa utilidade para obter cotações, sem comprovar economia global do negócio. É distinto do piloto integral da seção 6: até 20 itens/quatro semanas propostos, categoria/UF a decidir, seguimento de contatos com censura dos casos sem resposta. MN-01–10 e suas metas não mudam. MN-11–18 abaixo têm **baseline desconhecida** e definições normativas completas, denominadores, metas calibráveis e responsáveis em 23 §8.

| ID | Indicador adicional | Protocolo referenciado |
|---|---|---|
| MN-11 | Cobertura de resultados comparáveis por item pesquisado | Incluir itens sem resultado e escopo/janela explícitos |
| MN-12 | Estabelecimentos candidatos por item e raízes em separado | Identidades provisórias e itens zero à parte |
| MN-13 | Qualidade de observações e possível duplicidade | Utilizável/rejeitado/pendente exclusivos; duplicidade subconjunto, recepções repetidas separadas |
| MN-14 | Respostas por contato de candidato no escopo | Tentativas não multiplicam contatos; silêncio no denominador |
| MN-15 | Aceite de cotar sobre contatados e respondentes separadamente | Evidência da resposta, sem inferir disponibilidade |
| MN-16 | Cotações vigentes por contato e por item | Percentual com ao menos uma e razão de cotações são medidas nomeadas separadamente |
| MN-17 | Tempo até primeira cotação válida e censura | Duração dos casos ainda sem cotação e corte publicados junto dos sucessos |
| MN-18 | Esforço ativo por item e cotação obtida | Inclui insucessos no total; espera separada; n e N=0 explícitos |

Técnico confere métricas, operador registra/revisa; nenhuma medida obtida na R1. Ausência de perdedores/participações impede taxa de vitória; resultados de vencedores não são amostra de aptidão. Conclusão comercial pode ser inconclusiva/rejeitada sem afrouxar controles. RSK-19–23 e ADR-018/019 registram riscos/decisões; alternativa manual independente de histórico mantém valor enquanto gates de fonte seguem abertos.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: fluxo, escopo e MN-11–18 referenciados ao piloto restrito; preservados indicadores do MVP e suas populações.

