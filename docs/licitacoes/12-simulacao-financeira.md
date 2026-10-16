# 12 — Simulação financeira e capital

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: [PROPOSTO] — política `v0.1-financeiro`; exemplos sintéticos, sem tributação presumida para o operador.

## 1. Contrato de cálculo

RF-020–024, RN-004–009/016–021 e RNF-010 exigem análise da unidade inteira de julgamento. Cada cenário fixa itens/quantidades, cotações, preços hipotéticos, modelo de fornecimento, tributos com bases, cronograma, reserva e parâmetros. Distinguir base/conservador/otimista e realizado. Referência oficial é contexto, não preço de venda garantido. Cenário incompleto pode ser salvo; saídas afetadas ficam NULL com motivos e não sustentam ranking.

Toda linha de custo informa natureza, valor/taxa, moeda, base de incidência, competência/data de pagamento, fonte, status (confirmado/estimado fundamentado/não aplicável/desconhecido), inclusão prévia na cotação e política de rateio. Registrar aquisição, frete, seguro, embalagem, tributos/encargos, operação, garantia/despesa contratual, financiamento, risco e adicionais. Não aplicável/zero exigem motivo. Reembolso/garantia recuperável são fluxos patrimoniais; prêmio/taxa da garantia é despesa.

## 2. Decimal, limites e arredondamento

Entradas e saídas monetárias textuais; converter diretamente string→Decimal, nunca float. Contexto de cálculo de 50 dígitos significativos, intermediários sem quantização prematura e ao menos oito casas para divisões. Persistir memória intermediária decimal como string de precisão suficiente; não reduzir intermediários à escala dos totais.

Limites iniciais de aplicação: até 10.000 itens por cenário; quantidade positiva <=1.000.000; preço/custo unitário entre 0 e 1.000.000 BRL, até seis casas; valores agregados absolutos <=1.000.000.000.000 BRL; taxas finitas e domínio explícito; horizonte <=3.650 dias. Quantidade fracionária só quando unidade permitir. Valores fora dos limites, NaN/Infinity, denominador inválido ou casas excedentes produzem erro explícito. Esses limites são capacidade proposta, não restrições oficiais; RNF-009 mede até 100 itens e três cenários, sem prometer mesma latência no máximo aceito.

Política de quantização:

1. Multiplicar quantidade × unitário sem arredondar intermediário; total monetário da linha em centavos com ROUND_HALF_UP.
2. Receita contratual simulada = soma dos totais de linha em centavos. Se edital definir outra regra de faturamento/arredondamento, versionar política por cenário e revalidar, nunca aplicar regra silenciosa.
3. Base de tributo segue regra específica: por item, documento ou total. Arredondar obrigação no nível legal/comercial validado, não cada fração sem fundamento. Não presumir que todos os tributos incidem sobre receita.
4. Custo compartilhado tem um único total em centavos. Ratear com pesos não negativos cuja soma>0, frações exatas, piso de centavos e distribuir resíduo aos maiores restos; empate por item_id estável. Estornos usam sinal separado. Soma do rateio = total, exatamente.
5. Resultado = somas/subtrações dos valores conciliados; percentuais e scores só arredondam para exibição (quatro casas no score, duas no percentual). Ranking usa precisão interna e desempate, não texto formatado.

Exemplo de rateio: 100,00 em três parcelas iguais → 33,34 / 33,33 / 33,33, primeiro ID recebe centavo residual. Receita de 3 × 0,335000 → 1,01; não arredondar unitário para 0,34 antes de multiplicar. Essas regras serão fixtures, não adaptação do motor float existente.

## 3. Formação de resultado

Definições por unidade de julgamento, moeda BRL:

```text
R = Σ receita_i                      receita hipotética conciliada
V = custos variáveis não incidentes sobre receita
F = custos fixos atribuídos à operação
J = custo financeiro
Z = reserva econômica de risco (não desembolso automático)
T(R) = tributos/encargos variáveis conforme bases validadas
C = V + F + J + Z + T(R)              custo total estimado ajustado
MC = R − V − T(R)                    contribuição, antes de F/J/Z
lucro = R − C
margem_contribuição = MC / R          se R>0
margem_líquida_estimada = lucro / R   se R>0
```

“Margem líquida estimada” é líquida do conjunto de custos declarado, não lucro contábil certificado. Exibir composição/itens não abrangidos; falta obrigatória deixa cenário incompleto. Não duplicar frete/tributo já incluído no preço. Custo unitário só para unidade homogênea com quantidade conhecida; em lote heterogêneo mostrar custo por item/rateio e custo total do lote, não dividir reais por “quantidade de produtos” incomparáveis.

Caso linear limitado: se T(R)=t×R, t>=0, e V/F/J/Z independem de R, chamar C0=V+F+J+Z. Então receita mínima R_min=C0/(1−t), e receita para margem m é R_meta=C0/(1−t−m). Exigir 1−t>0 e 1−t−m>0. Se custo financeiro/taxa depende do preço ou é não linear, usar cálculo determinístico iterativo com bases explícitas e critério de convergência; se não implementado, “não calculável”, jamais aplicar fórmula linear inadequada.

Preço mínimo unitário homogêneo = R_min/q; lote exige vetor de preços por item, limites e arredondamento conciliados. Calcular candidato decimal e testar receita resultante no pipeline real: ajustar para cima pelo menor incremento permitido (até seis casas internamente; respeitar regra de proposta do caso) até lucro>=0 ou meta satisfeita. Mostrar resultado econômico alcançado, não apenas divisão teórica. Se teto aplicável torna solução impossível, marcar inviável; preço estimado não vira teto universal.

Ponto de equilíbrio: no caso homogêneo linear com preço p, custo variável unitário v e t, contribuição unitária c=p×(1−t)−v. Para c>0, q_eq=(F+J+Z)/c, arredondado para unidade/embalagem admitida. A licitação pode fixar q: não vender unidades fictícias para alcançar equilíbrio. Se c<=0, não há equilíbrio por aumento de quantidade nesse modelo. Para lote fixo, usar R_min e análise completa, sem soma de pontos individuais.

## 4. Cronograma de caixa, financiamento e ROI

Para eventos datados de entradas I e saídas O, calcular B(t)=Σ(I−O) até t. Empates conservadores processam saídas antes de entradas se ordem não comprovada. G_base=max(0,−min B(t)). Garantia recuperável entra como saída na constituição e entrada na liberação, nunca como despesa inteira; taxa/prêmio separado. Recebimento do órgão só na data prevista fundamentada; nota fiscal/aceite não é entrada de caixa.

Reserva de liquidez R_l é saldo retido para risco, com política própria e vínculo à reserva econômica Z para evitar dupla contagem: G=G_base+R_l quando mantida durante todo o horizonte de exposição; cronograma explícito se liberação variar. Não adicionar Z como saída de caixa e R_l novamente pelo mesmo risco. No cenário adverso em que risco se realiza, substituir provisão por custo/fluxo correspondente e recalcular, sem contá-lo duas vezes.

Capital livre demonstrado A(t)=recursos disponíveis menos compromissos de outras operações e restrições comprovadas, sem subtrair novamente desembolsos já modelados. Testar déficit(t)+reserva(t)<=A(t) em cada data e limite de exposição. Para A constante, usar G<=A e G<=limite. Crédito só reduz falta se disponibilidade/custo/desembolso forem comprovados; não tratar limite hipotético como caixa. Falta conhecida bloqueia; A/cronograma desconhecido gera pendência.

Financiamento: preferir cronograma contratual de encargos datados. Modelo simples opcional J=principal×taxa_por_período×dias/base_dias, desde que hipótese apropriada, fonte e calendário identificados. Modelo de custo de oportunidade pode usar base previamente definida (por exemplo capital operacional sem juros) para evitar circularidade. Juros compostos/capitalização exigem regra própria; não usar fórmula simples como verdade universal. Se juros dependem da própria necessidade de caixa, resolver fluxo de crédito/encargos iterativamente e declarar convergência; caso não suportado, pendência.

Definir **ROI operacional ajustado = lucro/G**, com G como capital empregado sob política de liquidez. G=0 → não calculável; não infinito. Esse ROI não é anualizado nem retorno do capital próprio alavancado. Mostrar G_base, reserva, garantias e taxa/horizonte. Se desejar retorno sobre equity, criar indicador distinto em evolução posterior. Atraso no recebimento deve manter ou aumentar exposição/custo, nunca melhorar por erro de sinal.

## 5. Exemplo numérico completo — hardware

Exemplo sintético, parâmetros fiscais apenas didáticos: 100 unidades, aquisição 80,00, preço hipotético 120,00, taxa linear t=6%. Transporte por operador+transportadora. Obrigações/cotações/fontes consideradas verificadas somente para o exercício.

| Componente | BRL | Composição |
|---|---:|---|
| Aquisição | 8.000,00 | 100 × 80 |
| Frete/seguro/embalagem/operação variável | 800,00 | 400 + 100 + 100 + 200 |
| V | 8.800,00 | Custos variáveis sem taxa sobre receita |
| F | 200,00 | Administração atribuída |
| J | 180,00 | Encargo comercial de prazo cotado: 9.000 × 1% × 60/30; base e fórmula didáticas pactuadas |
| Z | 200,00 | Reserva econômica |
| C0 | 9.380,00 | V+F+J+Z |
| Receita R | 12.000,00 | 100 × 120 |
| T(R) | 720,00 | Hipótese 6% da receita |
| C | 10.100,00 | C0+T |
| Lucro ajustado | 1.900,00 | R−C |
| MC | 2.480,00 | R−V−T |

Margem líquida estimada=15,833333…%; contribuição=20,666666…%; custo unitário=101,00. R_min teórico=9.978,723404…; candidato unitário 99,787235 gera receita conciliada 9.978,72 e tributo 598,72, lucro zero. Para meta de 15%, R_meta teórico=11.873,417721…; candidato inicial 118,734178 gera receita 11.873,42, tributo 712,41 e lucro 1.781,01, ligeiramente abaixo de 15% após conciliação. Incrementar o unitário de um milionésimo até 118,734250 gera receita 11.873,43, tributo 712,41 e lucro 1.781,02, satisfazendo a meta. A verificação pelo pipeline conciliado é obrigatória mesmo quando resultado teórico parece suficiente.

Equilíbrio de quantidade nesse exemplo: v=88; contribuição unitária=120×0,94−88=24,80; F+J+Z=580; q_eq=23,387096… → 24 unidades se unidade indivisível. A contratação exige 100: o indicador não autoriza oferecer 24.

Cronograma didático com garantia recuperável de 500: dia 0 aquisição+seguro+embalagem+garantia=8.700; dia 5 frete+operação+F=800; dia 30 encargo comercial de prazo=180; dia 60 taxa=720 antes do recebimento de 12.000; dia 75 liberação da garantia=500. G_base=10.400 e R_l=200 → G=10.600. J é despesa efetivamente paga neste exercício, sem pressupor entrada de empréstimo. Em cenário distinto, se J representar apenas custo de oportunidade sem desembolso, retirar esse fluxo do caixa e mantê-lo no resultado econômico com identificação própria. ROI ajustado=1.900/10.600=17,924528…%. Saldo final antes da realização de risco=2.100; lucro ajustado é 1.900 porque Z não é saída realizada.

## 6. Cenários e sensibilidade

Recalcular tudo em cada cenário, não variar apenas margem. Para o caso acima:

| Cenário sintético | Alterações | C0 | R | Taxa | Lucro ajustado |
|---|---|---:|---:|---:|---:|
| Base | Como seção 5 | 9.380 | 12.000 | 720 | 1.900 |
| Conservador | Aquisição +5%=8.400; frete 600; recebimento em 90 dias; J=270; preço 114 | 10.070 | 11.400 | 684 | 646 |
| Otimista | Aquisição 7.800; frete 350; J=135 por 45 dias; preço 120 | 9.085 | 12.000 | 720 | 2.195 |

Z/F/outros permanecem iguais nesses exemplos. Reserva não é realização de custo adicional. Conservador: G_base=11.054, G=11.254, margem≈5,666667%; otimista: G_base=10.105, G=10.305, margem≈18,291667%, com mesma ordem conservadora de pagamentos. Atraso e preço menor precisam de nova verificação de capital, validade da cotação e margem mínima. Cenário otimista não elimina bloqueio conhecido no base/conservador exigido pela política.

Sensibilidades propostas: frete ±20%, aquisição ±5%, preço de disputa −5%/−10%, atraso +30/+60 dias e câmbio ±10% somente se compra estiver exposta a moeda estrangeira. Hipóteses configuráveis, não previsões. Converter moeda com taxa/fonte/data/encargos explícitos e manter moeda original. Mostrar variação isolada e combinada; meta 15% e taxa 90% produzem denominador negativo e resultado inviável.

## 7. Consolidação e aceite futuro

Unidade com dois itens: item A lucro 1.900 e item B lucro −1.420 → lote lucro 480, sem escolher só A. Frete conjunto é contabilizado uma vez, rateado para explicação; caixa consolidado por datas, não soma de picos quando não simultâneos. Capital/margem/prazos avaliados no nível exigido pelo julgamento. Itens alternativos não entram simultaneamente sem compromisso de participar nos dois.

Aceites futuros: soma dos rateios igual ao total; identidade MC−F−J−Z=lucro; repetição Decimal idêntica; entrada excessiva rejeitada; desconhecido não vira zero; garantia recuperável altera capital sem duplicar despesa; atraso não melhora custo; lote obrigatório não pode omitir item; revisão reconstrói todos os insumos. [09](09-motor-de-decisao.md) usa indicadores condicionados à completude, e [08](08-modelo-de-dados.md) preserva cenários. Estes exemplos foram propostos para validação aritmética documental; não equivalem a execução do simulador.

## 8. Referência histórica e custo atual — revisado em R1

Revisão 0.3-R1-E2, 2026-10-13. [PROPOSTO]. Todas as fórmulas, contexto Decimal, limites, rateios e números anteriores permanecem. Histórico público só pode sustentar hipótese explícita de preço de venda/disputa. Registrar preço/natureza nominal, historical_price_id, comparison_id, política/versão, fonte/data/janela, unidade/fator comprovado, autor/instante da seleção e justificativa. Agregado insuficiente pode oferecer observação individual qualificada; não cria média confiável por conveniência. Preço estimado, homologado, registrado, contratado e cotação nunca são custo equivalente.

Custos de aquisição continuam cotação vigente ou premissa admitida por RN-020 com evidência e incerteza; quando cotação for obrigatória, a premissa histórica não resolve a falta. Cenário com preço de venda histórico exige sensibilidade registrada para redução de preço e mudanças de contexto; parâmetros são hipóteses do operador e não previsão. RF-024 é extensão opcional de comparação ampliada, sem impedir o cenário adverso/checagens obrigatórios já previstos. Não deduzir margem do vencedor a partir do resultado público.

| Caso sintético adicional | Entradas / resultado esperado |
|---|---|
| Histórico comparável/candidatos, sem cotação | Item I-A com dois candidatos e referência homologada contextualizada. Aquisição, disponibilidade e insumos obrigatórios ausentes: salvar cenário incompleto; C, lucro, margem, ROI e score afetados NULL com motivo. Não preencher aquisição por preço público nem afirmar capacidade |
| Sem histórico, cotação e insumos suficientes | Reutilizar exatamente caso hardware da seção 5: aquisição cotada 80, preço hipotético manual 120, todos os custos/cronograma/obrigações evidenciados no exercício. Falha histórica não muda R=12.000, C=10.100, lucro=1.900, G=10.600 ou a verificação atual. Números reutilizados do exemplo, sem recálculo ou nova estatística |

Ambos integram TST-068 futuro em Docker, RN-005/006/036 e RT-006/008. Histórico indisponível não impede análise suficientemente fundamentada; cotação expirada ou incompleta continua afetando guardas independentemente do número de candidatos. Nenhuma métrica comercial do piloto foi obtida na revisão.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: contrato da hipótese de venda com proveniência/sensibilidade e dois casos sintéticos; fórmulas/exemplos anteriores intactos, sem execução de cálculo do produto.

