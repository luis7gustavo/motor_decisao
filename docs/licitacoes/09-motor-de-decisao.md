# 09 — Motor determinístico de decisão e ranking

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: [PROPOSTO] — modelo `v0.1-ranking`, parâmetros iniciais calibráveis; não é probabilidade de vitória.

## 1. Contrato e etapas

Entrada: unidade completa de julgamento em versão, itens/requisitos/evidências, produtos/matches/cotações, cenário base e adverso, capacidade de capital/prazo, parâmetros e instante da avaliação. Saída: verificações por RN, bloqueios, pendências, classe logística, componentes/intervalos, score global quando permitido e snapshot reproduzível. RF-025–027 e RN-029 orientam quatro etapas: elegibilidade → viabilidade → priorização → revisão humana. [D03](diagramas/D03-motor.md) mostra componentes.

Executar todos os checks aplicáveis, preservando tanto bloqueios como lacunas. Classificação analítica: qualquer falha impeditiva conhecida → **bloqueada**; sem falha mas falta obrigatória → **pendente**; demais condições suficientes → **viável para priorização**. Nenhuma ponderação compensa falha. Revisão pode adicionar evidência e recalcular, não ignorar trava com clique. Após revisão final, RN-032 permite E07; score nunca executa essa transição.

## 2. Guardas eliminatórias e pendências

| Verificação | Bloqueio conhecido | Pendência obrigatória | RN |
|---|---|---|---|
| Prazo/atualidade | Prazo de participação encerrado; plano excede prazo obrigatório | Fuso/prazo, retificação ou consulta atual não resolvidos | 001–003/028 |
| Unidade de julgamento | Item obrigatório não fornecível | Composição/critério desconhecido | 007–009 |
| Produto | Atributo obrigatório incompatível | Matching não confirmado suficientemente | 015 |
| Oferta | Cotação vencida, estoque confirmado insuficiente, mínimo inviável | Validade/disponibilidade/compromisso não confirmados | 006/014 |
| Execução | Obrigação comprovada inexequível no plano; envio direto vedado quando indispensável | Classe D, autorização específica ou responsável local pendente | 010–014 |
| Habilitação | Requisito aplicável comprovadamente não atendido | Checklist/fundamento/especialista necessário faltante | 022–025 |
| Economia | Lucro/margem abaixo da política explícita no cenário exigido; solução inviável | Custo/tributo obrigatório ou preço hipotético não definido | 004/019–021 |
| Capital | Déficit acima de capital livre/limite em qualquer data | Recursos ou cronograma não demonstrados | 016–018 |
| Evidência | Contradição impede atender regra confirmada | Origem/versão/confiança crítica insuficiente | 026–028 |

Margem mínima, limite de exposição, buffer de prazo, política de cenário adverso e frescor são parâmetros de negócio, sem defaults pessoais. Ausência de configuração requerida gera pendência. Preço estimado ausente não impede todo estudo se houver preço hipotético fundamentado, mas não permite declarar teto/receita oficial. Cenário adverso de atraso é obrigatório para caixa; sensibilidade ampliada RF-024 e histórico RF-028 continuam opcionais.

## 3. Matemática de priorização

Definir clamp(x)=min(1,max(0,x)); todos os scores em [0;100]. Calcular Decimal; valores não calculáveis continuam NULL. Pesos iniciais globais: financeiro 0,30; logístico 0,20; técnico 0,20; confiabilidade 0,15; exposição a capital/prazo 0,15. Não são os pesos do motor SILLO. Pesos finitos >=0 e soma=1; mudança cria versão e não altera histórico. Mesmo peso zero não remove verificação obrigatória.

### 3.1 Financeiro F

F=100×clamp(margem_líquida_estimada/m_ref), onde m_ref>0 é margem de referência configurada. Para os exercícios, m_ref=20%. Piso eliminatório de margem é parâmetro separado; m_ref é normalizador, não mínimo de aprovação. Usar cenário base completo; cenário adverso exigido precisa passar controles próprios. Lucro em reais, ROI e G aparecem como informação adicional, sem favorecer automaticamente contratação grande. Se R<=0, G=0 para ROI ou algum custo obrigatório faltar, sinalizar não calculável; falha do indicador usado impede score global.

### 3.2 Logístico L

L é exatamente lower do `v0.1-logistica` em [11](11-logistica-e-fornecimento.md). Não usar upper ou normalizar pela cobertura. A/B/C/D é separado; presença local comprovadamente atendida pode ter L baixo e continuar viável. Pendência crítica sempre impede ranking, inclusive quando peso da dimensão é zero.

### 3.3 Técnico T

Lista de atributos aplicáveis registrada antes de comparar candidatos. Cada atributo tem peso a_j>=0, soma=1, resultado y_j∈{0;0,5;1;null}. 1=atendimento verificado, 0,5=atendimento parcial de preferência não obrigatória, 0=não atendimento de preferência, null=sem evidência. Atributo obrigatório só admite atendimento 1 para aptidão; conflito bloqueia e ausência deixa pendente. T=100×Σ conhecidos(a_j×y_j), cobertura_T=Σ conhecidos a_j; upper_T=min(100,T+100×(1−cobertura_T)). Não renormalizar. Pesos iniciais iguais entre atributos aplicáveis, alteráveis por categoria com versão; remover atributo exige fundamento, não adaptação para favorecer produto.

### 3.4 Confiabilidade C

Quatro grupos de evidência: oficial/documental b=0,40; fornecedor/produto b=0,30; custos/prazos b=0,20; revisão/proveniência b=0,10. Em cada grupo, nota conservadora igual à menor nota das evidências necessárias: 1=fonte verificável, atual e suficiente; 0,5=estimativa fundamentada admitida pela política ou evidência secundária explicitamente revisada; 0=refutada/inconsistente; null=ausente. C=100×Σ conhecidos(b×nota), cobertura_C e upper_C calculados como em T. Evidência crítica insuficiente impede positivo, mesmo se C alto. Incerteza de frete estimado pode ser aceita se política e cenário adverso sustentarem, sem chamar estimativa de confirmação. Confiança não é probabilidade estatística.

### 3.5 Exposição E

Definir u=max_t((déficit(t)+reserva(t))/A(t)) e respeitar limite de exposição separado. A(t)>0 em todo ponto relevante; caso contrário pendência ou bloqueio conforme insuficiência conhecida. Folga s em dias = prazo disponível − preparação − transporte − buffer obrigatório, após conversão de calendário explícita. s<0 bloqueia; s desconhecida impede score. Referência de folga s_ref>0 é parâmetro; exemplos usam 20 dias.

```text
E_capital = 100 × clamp(1 − u)
E_prazo = 100 × clamp(s / s_ref)
E = 0,60 × E_capital + 0,40 × E_prazo
```

Utilização u<=1 não basta para aprovar: cenário adverso e limite também verificados. Custo financeiro em F e folga de capital em E têm sentidos distintos (resultado e resiliência); mostrar essa escolha para calibração, sem contabilizar juros duas vezes no custo.

### 3.6 Penalizações e score global

Riscos residuais aceitos, sem impedimento: concentração em um único fornecedor confirmado sem alternativa validada → p1=3 pontos; exposição cambial residual documentada, suportada pelo cenário adverso, sem proteção contratada → p2=3 pontos. Quando não aplicável justificado, zero. Conhecimento insuficiente para avaliar risco requerido pela política deixa pendência; não presume p=0. Outros riscos não recebem penalidade arbitrária: registrar e revisar versão do modelo antes de incluí-los. Não aplicar penalidade adicional por fato já codificado em outra penalidade.

```text
P = p1 + p2                         0 ≤ P ≤ 6 na versão inicial
S_bruto = 0,30 F + 0,20 L + 0,20 T + 0,15 C + 0,15 E
S = min(100, max(0, S_bruto − P))
```

Publicar S apenas para elegibilidade viável, dados necessários calculáveis e snapshot atual. Bloqueada/pendente: S=NULL com componentes diagnósticos identificados, nunca S=0 como se fosse comparação válida. Cobertura global=Σ peso_k×cobertura_k, com F/E=1 somente quando completos; T/L/C conforme suas rubricas. Mostrar coberturas parciais e nenhuma estimativa superior como promessa. Ordenação decrescente por S interno; empate: maior cobertura, menor G, prazo de participação mais próximo ainda atendível e UUID estável. Não há limiar automático de aprovação; score ordena trabalho humano.

## 4. Caso 1 — hardware, transportadora e boa margem

Caso sintético da seção 5 de [12](12-simulacao-financeira.md): 100 unidades, R=12.000, C=10.100, lucro=1.900, margem=15,833333…%, G=10.600. Capital livre constante 20.000, limite de exposição suficiente, folga s=10 dias após buffer. Produto confirmado, documentação/habilitação atual verificadas, cotação vigente, envio direto não aplicável justificado, sem obrigação presencial: classe A, L=T=C=100 e coberturas=1. Único fornecedor confirmado, sem câmbio: P=3. Todos são pressupostos do exercício, não fatos de uma oportunidade real.

F=79,166666…; u=0,53; E_capital=47; E_prazo=50; E=48,2. S_bruto=85,98; S=**82,98**. Cenário conservador de 12: lucro 646, margem≈5,666667%, G=11.254. Política sintética de margem mínima 5% nos cenários exigidos e capital 20.000 permite prosseguir; nenhum limiar real do operador é presumido. Resultado: viável para priorização e encaminhamento à revisão final, nunca proposta automática. Se a fonte/cotação vencer antes da decisão, o mesmo S histórico não autoriza E07.

## 5. Caso 2 — item atraente em lote sem capital

Lote obrigatório: item A do caso anterior (R=12.000, C=10.100, lucro=1.900, G=10.600); item B com R=7.000, C0=8.000, taxa didática 420 e lucro=−1.420. Para B, custos pagos antes do recebimento totalizam 8.420, sem garantia/reserva adicional; picos simultâneos comprovados no exercício. Custos compartilhados já foram atribuídos sem duplicação.

Lote: R=19.000, C=18.520, lucro=480, margem≈2,526316%; G=19.020. Capital livre/limite=15.000. Déficit de capital=4.020 e margem inferior ao piso sintético de 5%. Resultado: **bloqueada**, por RN-007–009/018/021; S=NULL, mesmo se produto/logística forem adequados. Não usar lucro do item A para recomendar participação parcial onde o julgamento exige lote. Pode-se obter nova cotação/capital comprovado e recalcular nova versão; retirar B não é solução válida para o mesmo lote.

## 6. Caso 3 — preço atraente, presença não resolvida

Hardware com R hipotético=12.000 e custos preliminares iguais ao caso 1 sugeriria lucro 1.900. Porém cláusula exige “assistência/instalação no local” sem escopo/responsável suficientemente esclarecido. Custo de atendimento e plano de execução faltam; o resultado financeiro completo é não calculável, não lucro confirmado de 1.900.

No exercício, quatro dimensões logísticas conhecidas em 1 e pós-venda (peso 0,15) null → L=85, cobertura=0,85, intervalo [85;100], **classe D**. Guardar alerta de presença já identificado; esclarecer obrigação, confirmar prestador/região/contrato e inserir custo/prazo. Resultado: **pendente**, S=NULL. Se revisão confirmar presença e plano local atendível, classe C e nova simulação; se comprovar obrigação inviável, bloqueada. Similaridade/preço/limite superior não resolvem a lacuna.

## 7. Reprodução, calibração e verificação

Persistir versão de fórmula, pesos, parâmetros, verificações, evidências, entradas Decimal, cenário, instante e precisão. Retificação/cotação nova invalida atualidade sem apagar score histórico. Publicação e revisão usam controle de concorrência de 08. Painel mostra listas separadas de viáveis, bloqueadas e pendentes, sem esconder motivos atrás de número.

Verificações documentais futuras/fixtures: todos desconhecidos → sem global; aumentar margem com demais dados fixos não reduz F; aumentar déficit não melhora E; reduzir folga não melhora E; alterar pesos não remove bloqueio; replay reproduz S; retry não duplica análise; entrada não finita é rejeitada; expiração invalida ranking atual. Calibrar pesos/penalidades com MN-01–10 e amostra de decisões revisadas, guardando falsos positivos/descartes e exposição por categoria. Não treinar ML nem otimizar taxa de vitória com dados insuficientes. ADR-006/007/015, RNF-005/008/010/016/020 e RF-025–029 são as referências.

## 8. Histórico como contexto independente — revisado em R1

Revisão 0.3-R1-E2, 2026-10-13. [PROPOSTO]. RN-036/037 e ADR-018/019 preservam integralmente v0.1-ranking, pesos, penalidades, precisão ADR-017 e os três exemplos anteriores. Lista de candidatos ordena pesquisa por comparabilidade/qualidade/atualidade e chave estável; não é score de oportunidade nem probabilidade de fornecimento. Quantidade de vencedores históricos não resolve penalidade de concentração: só alternativa de fornecimento atual validada pode afetar a condição já definida no modelo, nunca candidato ainda em pesquisa.

Sinais históricos aparecem em painel separado com etapa/natureza, proveniência, qualidade, consulta e versão. Só entram como insumo opcional de preço de venda hipotético se selecionados explicitamente pelo operador em cenário, com comparabilidade e sensibilidade (12 §8); nunca no custo nem como bônus F/L/T/C/E. Correção futura de fórmula exigirá novo ADR, versão e validação. Uma referência selecionada invalidada requer reavaliação do cenário dependente; falha genérica da busca não invalida insumos comerciais independentes.

### Caso sintético 4 — candidatos sem cotação

Item I-A possui resultado homologado comparável e dois estabelecimentos identificados, E-A/E-B. Não há cotação atual de aquisição nem confirmação de disponibilidade; demais insumos obrigatórios de custo ainda faltam. Resultado: pendente, custo/margem/ROI/global não calculáveis (NULL com motivo), capacidade desconhecida. A lista serve para pesquisa/contacto externo manual futuro; não define E07, reserva ou dispensa cotação. Nenhum preço histórico vira custo. TST-068, RN-006/020/036.

### Caso sintético 5 — análise suficiente sem histórico

Usar exatamente os insumos e premissas sintéticas do caso 1, incluindo cotação vigente, atributos, custos, capital e revisão requeridos; somente a pesquisa de histórico está indisponível. Resultado permanece viável para priorização, S=82,98 sob aqueles mesmos pressupostos, sem novo bloqueio ou penalidade. Não houve novo cálculo necessário: a fórmula e todos os seus insumos são idênticos aos do caso preservado. A aptidão depende da revisão/guardas atuais; não implica participação automática. TST-068 inclui comparar versões com histórico ausente/presente sem alterar insumos decisórios.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: dois casos sintéticos acrescentados; corpos e números anteriores preservados, nenhum recálculo ou teste de aplicação nesta revisão.

