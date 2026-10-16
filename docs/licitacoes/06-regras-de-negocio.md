# 06 — Regras de negócio

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-10 (America/Sao_Paulo).  
Revisão: 0.2-R1, 2026-10-12 (America/Sao_Paulo). Seções indicadas revisadas em R1-E1.
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: [PROPOSTO] — regras do Radar Licita para revisão; não implementadas nesta missão.

## 1. Convenções e limites

Estas regras dão semântica às referências RN dos [38 requisitos funcionais](04-requisitos-funcionais.md). Os exemplos são critérios de verificação futura, sem representar contratos reais, aconselhamento jurídico ou testes executados. Requisitos de qualidade estão em [05](05-requisitos-nao-funcionais.md).

Cada verificação obrigatória resulta em **atendida**, **não atendida** ou **pendente**, com evidência. Não aplicável exige justificativa verificável. Para elegibilidade: falha impeditiva conhecida determina **bloqueada**; na ausência dela, lacuna obrigatória determina **pendente**; apenas o atendimento suficiente de todos os controles permite **viável para priorização**. Preservar simultaneamente todos os bloqueios e pendências. “Revisada” registra a intervenção humana e sua decisão, sem substituir o resultado de elegibilidade.

A classe logística possui sua própria precedência D/C/B/A (RN-011), independente da elegibilidade. Classe, score, estado comercial e situação oficial são dimensões distintas. Uma decisão favorável exige condições atuais, evidências e revisão, sem certificar habilitação jurídica. Os parâmetros de risco, capital e margem devem ser versionados, sem limiares pessoais inventados.

## 2. Prazos — domínio 9.1

### RN-001 — Janela de participação verificável

**Regra:** uma recomendação positiva requer prazo de recebimento de propostas conhecido, fonte vigente e tempo restante suficiente para concluir os preparativos. Armazenar instante e fuso da origem; comparar instantes normalizados e mostrar o fuso ao operador. Prazo vencido ou cancelamento confirmado bloqueia nova recomendação; prazo ausente, ambíguo ou sem confirmação atual gera pendência. Uma sessão eletrônica futura não demonstra que o recebimento de propostas continue aberto.

**Verificação:** prazo anterior ao instante da avaliação bloqueia mesmo com lucro elevado; ausência de fuso não permite inferência silenciosa. Uma reabertura só restabelece a avaliação após evidência oficial e nova análise. **RF:** 002, 025.

### RN-002 — Cronograma completo de fornecimento

**Regra:** considerar tempo para obter/reconfirmar cotação, preparar, comprar quando aplicável, transportar, agendar recebimento, entregar cada parcela e cumprir obrigações extraordinárias. O início da contagem de entrega deve vir do instrumento aplicável (por exemplo, ordem formal), nunca ser presumido como a data da disputa. Dias úteis/corridos, feriados e margens operacionais exigem premissa e fonte; nenhuma duração legal padrão é fixada aqui.

**Verificação:** preparação de 5 dias + trânsito de 4 + recebimento de 1 não atende uma janela comparável de 8 dias; gatilho ou calendário desconhecido mantém pendência. Atraso em uma parcela obrigatória afeta o conjunto. **RF:** 008, 025, 032.

### RN-003 — Mudança oficial e atualidade do prazo

**Regra:** guardar última tentativa de sincronização separada da última confirmação da fonte. Encerramento, cancelamento, adiamento e retificação são eventos distintos. Mudança relevante exige revalidação dos controles dependentes. Falha da fonte não é prova de cancelamento nem de manutenção da abertura; antes de decidir, aplicar a política de atualidade versionada definida no RNF-018.

**Verificação:** nova data oficial substitui a atual com histórico; indisponibilidade preserva a base e sinaliza informação não confirmada. Participação já realizada continua registrada mesmo que o prazo mude. **RF:** 002, 004, 025.

## 3. Preços e condições comerciais — domínio 9.2

### RN-004 — Referência não representa receita

**Regra:** preço estimado oficial nunca é receita garantida. Ausente, sigiloso ou zero na referência devem manter o valor econômico utilizável desconhecido, preservando o dado original e o motivo. Um preço hipotético de proposta pode ser informado com origem manual, sem convertê-lo em preço oficial ou promessa de vitória.

**Verificação:** referência bruta zero resulta “referência desconhecida/zero na origem”; não produz lucro baseado em venda garantida. Uma hipótese manual permite cenário identificado, sujeito aos demais controles. **RF:** 020, 021, 028, 033.

### RN-005 — Tipos de preço e comparabilidade

**Extensão — revisado em R1:** entradas incluem lance/proposta e preço registrado em ata, além das naturezas abaixo; não misturar naturezas em agregados. Preço público nunca alimenta custo de aquisição; hipótese de preço de venda só com seleção explícita, contexto e sensibilidade. Quantidade em ata é registrada, não entregue. Total global de lote/contrato não vira unitário por divisão arbitrária. Comparabilidade segue política versionada de 22: especificação, unidade/fator evidenciado, quantidade, data e região pertinente. Positivo sintético: caixa com 10 unidades e fator provado pode ser comparada após conversão documentada. Negativo: caixa sem fator, código CATMAT igual com atributo incompatível ou contrato global sem item não geram referência unitária. RF-028/036 também vinculados.

**Regra:** diferenciar estimado, homologado, contratado, cotação comercial, hipótese de proposta e receita efetivamente recebida. Toda comparação exige moeda, unidade, conteúdo da embalagem, quantidade, data, região quando pertinente e condições comerciais/contratuais. Comparáveis históricos exigem especificação técnica, CATMAT/CATSER quando disponíveis e justificativa de equivalência; código ou nome semelhante não basta.

**Verificação:** caixa e unidade avulsa não são comparadas sem fator evidenciado; dado homologado não preenche automaticamente receita recebida. Amostra histórica sem equivalência fica excluída com motivo. **RF:** 016, 020, 021, 024, 028, 033.

### RN-006 — Cotação vigente e capacidade de fornecimento

**Extensão — revisado em R1:** entradas fixam estabelecimento completo e oportunidade/escopo. Cotação/disponibilidade não se propagam a outro produto, filial, quantidade, condição ou período; confirmação de preço não reserva estoque. Positivo: cotação vigente para produto P/E1/10 unidades é candidata apenas nesse escopo, sujeita aos demais controles. Negativo: cotação E1 não confirma 20 unidades ou filial E2. RF-038 vincula evidências sem duplicar a obrigação de RF-015.

**Regra:** cotação deve identificar fornecedor, produto/versão, preço, moeda, quantidade atendível, pedido mínimo, validade, preparação, pagamento, cobertura, garantia/devolução e origem. Estoque informado, confirmado e reservado são distintos. Avaliar validade na decisão e no gatilho previsto de compra; expiração anterior ao gatilho requer reconfirmação ou compromisso documentado suficiente. Quantidade insuficiente conhecida bloqueia a alternativa; ausência de confirmação mantém pendência.

**Verificação:** cotação para 10 unidades não comprova demanda de 20; duas alternativas só compõem a cobertura se a combinação for admitida e validada. Cotação vencida continua no histórico, sem sustentar recomendação favorável. **RF:** 013, 015, 016, 025.

## 4. Lotes, grupos e unidade de julgamento — domínio 9.3

### RN-007 — Definir a unidade efetiva de participação

**Regra:** registrar se o julgamento/fornecimento exige item, lote, grupo ou conjunto global e identificar todos os vínculos obrigatórios. Modalidade do procedimento não determina sozinha a unidade de julgamento. A análise comercial e financeira precisa de uma unidade conhecida e amparada pelo edital.

**Verificação:** lote de três itens deve identificar os três; critério desconhecido impede consolidação favorável, mesmo com um item totalmente conhecido. **RF:** 008, 017, 023, 025, 032.

### RN-008 — Completude dos itens obrigatórios

**Regra:** todos os itens exigidos precisam de correspondência técnica, quantidade, fornecedor/capacidade, prazo e custos suficientes. Não remover item deficitário para tornar um lote viável. Fracionamento de participação só é aceito se permitido e evidenciado; não confundir entrega parcelada com participação parcial.

**Verificação:** dois itens atendidos e um obrigatório incompatível bloqueiam o lote; um item sem cotação torna o conjunto pendente. **RF:** 023, 025, 032.

### RN-009 — Consolidação sem dupla contagem

**Regra:** somar receitas hipotéticas, custos e obrigações do conjunto exigido, mantendo decomposição por item/parcela/destino. Frete, seguro e outras despesas compartilhadas entram uma única vez; política de rateio e reconciliação deve ser explícita. Margem agregada não supera incompatibilidade ou limite de preço por item quando exigido pelo instrumento.

**Verificação:** se itens geram resultados +100 e −150, resultado agregado é −50 antes de outros custos; frete comum de 30 não vira 60. Capital decorre do cronograma combinado, não da soma ingênua de picos que ocorrem em datas distintas. **RF:** 023, 025, 032.

## 5. Logística, fornecimento e equivalência — domínio 9.4

### RN-010 — Separar participação e execução remotas

**Regra:** avaliar separadamente participação eletrônica, transporte, envio direto, instalação/montagem/treinamento, equipe local, assistência/manutenção, entregas parceladas, múltiplos destinos e pós-venda presencial. Execução por terceiro pode evitar deslocamento do operador, mas não elimina obrigação contratual, custo nem necessidade de verificar autorização e capacidade.

**Verificação:** licitação eletrônica que exige instalação local não recebe classe A apenas por admitir proposta on-line. **RF:** 008, 010, 012, 025.

### RN-011 — Classificação logística A/B/C/D

**Regra:** aplicar nesta ordem: **D**, informação crítica insuficiente/conflitante para concluir o conjunto; **C**, execução com operação local ou presencial relevante suficientemente identificada; **B**, atendimento potencialmente remoto com restrições relevantes e condições documentadas; **A**, aparentemente compatível com envio convencional, sem presença/restrição relevante não resolvida e sujeito à confirmação do edital. D preserva alertas de presença já conhecidos. Não derivar classe de limiar de score.

**Verificação:** instalação conhecida + endereço obrigatório desconhecido resulta D com alerta de instalação; após confirmar endereço, C. Entrega convencional com todas as obrigações verificadas pode ser A, ainda sem aprovação automática. **RF:** 010, 025.

### RN-012 — Indicador de prontidão logística

**Regra:** adotar a proposta **v0.1-logistica**, definida em [04, seção 9](04-requisitos-funcionais.md#9-proposta-inicial-de-remote_delivery_score). Pesos iniciais: envio convencional 0,35; preparação/prazo 0,25; destinos/parcelas 0,15; pós-venda 0,15; envio direto 0,10. Pesos configuráveis devem ser finitos, não negativos, somar 1 e não eliminar verificação obrigatória. Cada dimensão assume 0, 0,5, 1 ou null, com rubrica/evidência.

Para K = dimensões conhecidas: `coverage=Σw(K)`; `score=lower=100×Σ(w×x)(K)`; `upper=min(100,lower+100×(1−coverage))`. Não dividir pela cobertura. Upper indica incerteza, jamais pontuação favorável. Não aplicabilidade de envio direto exige justificar alternativa operador+transportadora; nesse caso x=1, sem afirmar autorização de envio direto.

**Verificação:** prazo/peso 0,25 desconhecido e demais x=1 resultam score 75, cobertura 75%, intervalo [75;100]; a lacuna continua crítica. Todas desconhecidas: score conservador 0, cobertura 0%, intervalo [0;100], classe D, sem confundir desconhecido com incompatível. **RF:** 011, 026.

### RN-013 — Modelo remoto sustentado por evidências

**Regra:** registrar modelo de fornecimento, responsáveis e condições de transporte, recebimento, faturamento e pós-venda. Classe A não certifica permissão legal ou capacidade real. Classe C não é descarte obrigatório: pode haver alternativa local comprovada que atenda à preferência de não deslocamento do operador. Essa alternativa deve permanecer visível, custeada e revisada; a obrigação presencial continua existindo.

**Verificação:** mudar para distribuidor regional não apaga obrigação de instalação; cenário sem confirmação do prestador continua pendente. **RF:** 010, 011, 012.

### RN-014 — Envio direto e compromisso do fornecedor

**Regra:** existência de fornecedor, catálogo, cotação ou promessa genérica de envio não confirma capacidade. Para envio direto exigir evidências específicas de atendimento do fornecedor e validação contratual/fiscal aplicável, registradas pelo responsável competente; definir faturamento, transporte, garantia, devolução, recebimento e responsabilidade por falhas. Se autorização faltar, a alternativa fica pendente ou bloqueada quando vedação for comprovada. Não generalizar uma validação para todos os contratos.

**Verificação:** fornecedor confirma frete, mas condições fiscais/contratuais não foram analisadas: envio direto permanece pendente. Alternativa por transportadora pode ser avaliada em cenário distinto. **RF:** 010, 012, 013, 015.

### RN-015 — Compatibilidade técnica e de fornecimento

**Regra:** comparar categoria, especificação, marca/modelo quando pertinente, dimensões, capacidade, potência, material, compatibilidade, certificações, quantidade, unidade, embalagem/conteúdo e garantia. Similaridade textual recupera candidatos; não confirma equivalência. Divergência obrigatória conhecida → **incompatível**; ausência de evidência crítica → **informação insuficiente**; candidato com indícios suficientes para estudo e verificação ainda incompleta não crítica → **possivelmente compatível**; todos os atributos críticos atendidos, sem conflito e com verificação humana registrada → **compatível confirmado após verificação suficiente**. Estado confirmado vale apenas para o par e versões examinados.

**Verificação:** título semelhante e potência insuficiente resulta incompatível, mesmo com outros campos faltantes; caixa de 12 converte para unidades só com fator comprovado. Retificação invalida confirmação dependente. **RF:** 014, 016 a 019, 025, 028.

## 6. Capital e cálculo financeiro — domínio 9.5

### RN-016 — Fluxo financeiro datado

**Regra:** separar desembolso inicial, custos durante execução e entrada esperada. Planejar compra, frete, tributos, garantias, entregas/aceites e recebimentos, com datas ou intervalos fundamentados. Não presumir antecipação do órgão nem equiparar faturamento a caixa disponível. Garantia recuperável pode imobilizar capital sem ser despesa definitiva; distinguir custo, saída de caixa e posterior liberação para não contar duas vezes.

**Verificação:** compra hoje e recebimento após aceite futuro geram exposição intermediária mesmo com lucro esperado. Registro de nota fiscal não reduz saldo a receber. **RF:** 022, 024, 032.

### RN-017 — Pico de capital e atraso

**Regra:** calcular necessidade de financiamento pelo maior déficit acumulado do cronograma, incluindo parcelas e capital simultaneamente comprometido em outras operações quando informado. Prazo até o recebimento e custo financeiro devem ser parametrizados; atraso precisa de cenário adverso explícito. A fórmula detalhada de capital, taxa/período e reservas será consolidada em 12, sem tratar todos os tributos como percentuais uniformes.

**Verificação:** adiar recebimento não melhora custo financeiro mantidos os demais parâmetros; custos/garantias anteriores ao pagamento permanecem na curva de caixa. **RF:** 022, 024.

### RN-018 — Capital como condição obrigatória

**Regra:** comparar pico necessário com capital disponível demonstrado e limite de exposição configurado. Insuficiência conhecida bloqueia a alternativa; capital ou cronograma desconhecido gera pendência. Expectativa de crédito sem comprovação não preenche a lacuna. A revisão humana pode registrar novas evidências/financiamento e recalcular, mas não remover o bloqueio por um clique.

**Verificação:** necessidade de 12.000 e capital livre de 8.000 bloqueiam mesmo com lucro estimado positivo; capital null não equivale a ilimitado. **RF:** 022, 024, 025.

### RN-019 — Precisão monetária

**Regra:** Decimal de entrada a saída, sem passagem por float. Preços/custos unitários e quantidades aceitam até seis casas decimais; entradas excedentes são rejeitadas ou explicitamente corrigidas, nunca truncadas silenciosamente. Intermediários mantêm pelo menos oito casas decimais e contexto suficiente para a magnitude. Totais BRL são reconciliados em centavos com ROUND_HALF_UP; moeda/taxa/unidade devem estar explícitas. Política e pontos de quantização são versionados.

**Verificação:** fixture de rateio reconcilia linhas e total em centavos; denominador inválido ou estouro de capacidade não gera número plausível silencioso. A Execução 2 fechará os exemplos numéricos e a política de distribuição de centavos. **RF:** 020, 021, 023, 024.

### RN-020 — Cenário completo e reproduzível

**Regra:** registrar aquisição, quantidade, frete, seguro, embalagem, tributos/encargos com base de incidência, operação, garantias/despesas contratuais, custo financeiro, reserva de risco e adicionais. Ausência não vira zero; zero ou não aplicável exigem fundamento. Snapshot do cenário fixa cotações, modelo de fornecimento, cronograma, regras, parâmetros, fontes e rateios. Estimativa fundamentada pode integrar cenário com incerteza explícita; lacuna obrigatória impede positivo.

**Verificação:** cenário sem frete pode ser salvo como incompleto, sem recomendação. Alterar cotação não muda memória da análise anterior. Custo já incluído no preço cotado não é somado novamente. **RF:** 020, 021, 023, 024, 027.

### RN-021 — Bases e limites dos indicadores

**Regra:** custo total e unitário, preço mínimo, margem de contribuição, margem líquida estimada, lucro, ROI, giro e equilíbrio precisam de definições e memória. Margens usam receita; ROI usa capital empregado explicitamente definido no cenário; giro decorre do cronograma. Preço por meta considera custos incidentes sobre receita e identifica solução inviável. Receita/quantidade/capital denominador nulo produz “não calculável”, com motivo. Indicador nulo não recebe nota financeira favorável. Reserva de risco deve ser distinguida do desembolso efetivo.

**Verificação:** preço com taxa e meta que tornam denominador não positivo é inviável; receita zero não gera margem infinita. Detalhes matemáticos e cenários conservador/base/otimista ficam para 12 e 09. **RF:** 021, 022, 024.

## 7. Habilitação e conformidade — domínio 9.6

### RN-022 — Checklist por contratação

**Regra:** registrar exigências de regularidade fiscal/trabalhista, qualificação técnica, capacidade econômico-financeira, cadastros, certificações, garantias, subcontratação e condições específicas. Cada requisito tem fonte, aplicabilidade, evidência, validade, responsável e situação. Um cadastro existente não demonstra atendimento de todas as exigências.

**Verificação:** certificado obrigatório vencido afeta o requisito correspondente; documento não fornecido permanece pendente. Nenhum checklist gera “habilitação jurídica certificada pelo sistema”. **RF:** 002, 008, 025.

### RN-023 — Conflitos e interpretação

**Regra:** cláusulas ambíguas, anexos incompatíveis, tabelas ilegíveis e fontes divergentes geram pendência vinculada aos trechos. O sistema não escolhe automaticamente o texto mais favorável. Resolução exige fundamento de precedência/esclarecimento aplicável, fonte e responsável; tema especializado pode exigir assessor externo.

**Verificação:** dois prazos conflitantes impedem positivo até resolução registrada; baixa confiança na extração não se converte em cláusula dispensada. **RF:** 008, 009, 025.

### RN-024 — Verificação humana e especializada

**Regra:** distinguir checagem objetiva (por exemplo, comparação de atributo/data), interpretação do operador e validação jurídica/contábil/técnica especializada. Registrar quem verificou, quando, escopo, conclusão e evidência. Revisão humana não pode sobrescrever silenciosamente fato impeditivo; deve corrigir premissa com prova e gerar nova versão ou manter pendência. Não exigir conta externa de fornecedor/assessor no MVP.

**Verificação:** clicar “aprovar” com certificado obrigatório desconhecido não torna a análise apta; inserir evidência válida resolve a pendência com trilha. **RF:** 009, 018, 025, 029.

### RN-025 — Regime, procedimento e tratamentos específicos

**Regra:** registrar regime aplicável, modalidade/procedimento e regras particulares do edital, inclusive tratamento ME/EPP quando pertinente, sem inferir benefício, prazo ou dispensa por nome da empresa. A aplicabilidade de uma regra legal deve ter fonte oficial vigente, data de consulta e análise competente. Esta versão não fixa alíquotas, limites legais ou prazos normativos; essa verificação integra 13/Execução 3.

**Verificação:** ME/EPP sem análise das condições do caso permanece pendente para aquele benefício; procedimento com fluxo distinto não é forçado à jornada padrão. **RF:** 002, 008, 025.

## 8. Evidências, confiança e decisões — domínio 9.7

### RN-026 — Procedência de ponta a ponta

**Extensão — revisado em R1:** fonte e campo são registrados por atributo; conservar natureza, evento, situação, instante da consulta e versão. Contato é exclusivamente registrado manualmente, com autor/evidência, sem disparo externo. Dados públicos exigem minimização/finalidade e política ADR-016; não enriquecer candidato com sócios/CPF/contatos pessoais desnecessários. Positivo: revisão resolve preço conflitante preservando as duas origens e motivo. Negativo: substituir valor sem proveniência ou registrar mensagem como enviada pelo Radar é rejeitado. RF-035–038 também vinculados.

**Regra:** preservar origem oficial/comercial/manual, identificador original, URL quando existente, documento/hash/versão, campo ou página/trecho, data de obtenção, status de atualização, confiança e pendências. Informação manual exige responsável e fundamento. Documento bruto e interpretação são registros distintos; dado bruto não deve ser reescrito para coincidir com normalização.

**Verificação:** recomendação permite chegar ao campo JSON ou página do PDF que a sustenta, além da cotação/versão financeira usada. Origem ausente gera pendência explícita. **RF:** 001, 003, 005 a 009, 013 a 015, 027, 028, 033.

### RN-027 — Desconhecido não é favorável

**Regra:** distinguir ausente, não aplicável justificado, não atendido, estimado e confirmado. Confiança é qualidade da evidência/interpretação; score não é probabilidade de vitória ou lucro. Cobertura e pendências acompanham qualquer indicador. Um dado faltante não pode gerar bônus por renormalização dos pesos, equivalência textual ou custo zero implícito.

**Verificação:** extração vazia não significa ausência de instalação; falta de prazo bloqueia positivo como pendência; valor financeiro não calculável não vira ROI=0 nem ROI alto. **RF:** 001, 005, 007 a 011, 018, 025 a 027, 033.

### RN-028 — Identidade, versões e invalidação

**Extensão — revisado em R1:** resultado homologado/ata/contrato também recebe versões e invalidação por retificação/cancelamento; consulta nova não é versão de negócio quando conteúdo não muda. Falha de fonte não apaga último histórico válido nem fornecedor com validação comercial própria. Positivo: cancelamento preserva resultado anterior e retira referência atual dependente. Negativo: timeout não transforma resultado em cancelado nem cotação válida em inexistente. Identidade fiscal/eventos seguem RN-033/034, sem repetir sua obrigação aqui.

**Regra:** identidade de fonte/contratação/item/lote deve ser estável e guardar o original. Mesmo payload não cria nova versão de negócio; conteúdo diferente com mesma identidade preserva versões e diferenças. Retificação relevante de item, quantidade, prazo, julgamento, documento, cotação ou produto invalida a atualidade dos resultados dependentes. Falha de sincronização não apaga registros; histórico de revisão e atos externos permanece íntegro.

**Verificação:** reprocessar duas vezes não duplica contratação; alterar quantidade gera versão e revalidação de matching, logística/finanças afetados. Participação anterior continua comprovada. **RF:** 003 a 006, 019, 027, 030, 034.

### RN-029 — Bloqueios antes da priorização

**Regra:** executar elegibilidade, viabilidade, priorização e revisão nesta ordem lógica. Só oportunidades sem bloqueio e sem pendência obrigatória entram no ranking positivo. Bloqueadas/pendentes permanecem em listas próprias com razões. O score global deverá combinar dimensões financeira, logística, técnica, confiabilidade e exposição a capital/prazo com pesos versionados e normalização explícita; fórmula global e exemplos ficam para 09, sem reutilizar pesos de revenda por analogia.

**Verificação:** candidato de alto valor estimado com prazo vencido não supera candidato viável; mudar pesos não elimina uma trava nem modifica ranking histórico. **RF:** 005, 025 a 027, 029.

### RN-030 — Decisão auditada

**Regra:** decisão ou correção registra autor, instante, motivo, versão da análise, estado anterior/novo e evidências. Descarte requer motivo; complementação identifica lacuna. Desfazer cria evento de invalidação/substituição, sem apagar decisão. Controlar concorrência e duplicidade para impedir duas decisões ativas conflitantes sobre a mesma versão. Históricos de revenda mantêm sua semântica original.

**Verificação:** dupla submissão resulta em uma decisão ativa; reanálise preserva conteúdo e autor da anterior. **RF:** 009, 019, 027, 029 a 034.

### RN-031 — Registro de atos externos

**Regra:** não enviar propostas/lances, comprar, contratar, pagar nem comunicar automaticamente fornecedores/órgãos. O operador registra participação, disputa, resultado e atos subsequentes já realizados externamente com evidência. Aptidão é apoio à decisão, não ato de participação; vitória não é contrato; entrega não é aceite; faturamento não é recebimento.

**Verificação:** salvar participação não faz requisição de envio a portal; resultado vencedor sem fundamento contratual não abre execução automaticamente. Fato externo ocorrido fora do fluxo pode ser registrado por reconciliação excepcional auditada (RN-032), sem simular aprovações anteriores. **RF:** 029 a 034.

### RN-032 — Máquina de estados comerciais

**Regra:** usar os 16 estados e transições abaixo, fonte normativa de [D10](diagramas/D10-estados.md). Situação oficial, elegibilidade, classe logística, pendências e estado comercial são campos distintos. Todo salto não previsto é rejeitado; eventuais fatos externos fora da sequência usam a exceção controlada descrita após a tabela.

| Código lógico | Nome fixado | Significado |
|---|---|---|
| E01 | oportunidade descoberta | Unidade candidata identificada |
| E02 | em enriquecimento | Coleta/revisão de dados e exigências |
| E03 | aguardando cotação | Aguardando confirmação comercial |
| E04 | aguardando revisão | Pacote ou pendência requer decisão humana |
| E05 | pré-aprovada para estudo | Operador autoriza aprofundar estudo; sem aptidão final |
| E06 | descartada | Não prosseguir na análise, com motivo |
| E07 | apta para decisão humana de participação | Controles suficientes e revisão favorável atual |
| E08 | participação registrada manualmente | Ato externo comprovado; não enviado pelo sistema |
| E09 | em disputa | Evento externo de disputa registrado |
| E10 | resultado conhecido | Resultado disponível; pode exigir confirmação/interpretação |
| E11 | vencida | Resultado comercial vencedor; não significa prazo expirado |
| E12 | perdida | Resultado comercial não vencedor |
| E13 | contrato em execução | Fundamento contratual aplicável e início registrados |
| E14 | entrega concluída | Todas as parcelas e aceite/requisitos de conclusão comprovados |
| E15 | recebimento financeiro | Total devido recebido e conciliado; não simples faturamento |
| E16 | encerrada | Fechamento normal ou excepcional justificado |

| Origem | Destino permitido | Evento e guarda |
|---|---|---|
| Início | E01 | Identidade suficiente e origem registrada |
| E01 | E02 | Iniciar enriquecimento |
| E02 | E03 | Solicitar cotação/confirmação, com item e lacuna definidos |
| E02 | E04 | Submeter informações disponíveis ou conflito à revisão |
| E03 | E02 | Receber cotação/dados ou revisar escopo |
| E03 | E04 | Submeter falta de resposta/condição comercial à revisão |
| E04 | E02 | Pedir complementação, corrigir premissa ou reabrir estudo |
| E04 | E05 | Revisão preliminar favorável ao estudo, com lacunas remanescentes visíveis |
| E05 | E02 | Complementar estudo/matching/simulação |
| E05 | E04 | Estudo pronto para revisão final ou necessidade de resolver conflito |
| E04 | E07 | Revisão final; todas as guardas de prazo, dados, produto, logística, habilitação e capital atendidas |
| E01/E02/E03/E04/E05/E07 | E06 | Descartar manualmente com motivo; não há participação registrada |
| E06 | E02 | Reabrir com justificativa/evidência nova; sem restaurar aprovação anterior |
| E07 | E04 | Dado alterado, prazo/cotação vencido ou desfazimento exige revalidação |
| E07 | E08 | Registrar ato externo com evidência; aptidão reavaliada no momento da decisão |
| E08 | E09 | Disputa iniciada e registrada manualmente |
| E08 | E10 | Resultado registrado em procedimento sem etapa de disputa aplicável |
| E09 | E10 | Resultado oficial conhecido e evidenciado |
| E10 | E11/E12 | Operador registra resultado vencedor/não vencedor com fundamento e condição provisória/definitiva |
| E10 | E09 | Reabertura oficial de disputa comprovada |
| E11/E12 | E10 | Resultado retificado/anulado/reaberto oficialmente, preservando histórico |
| E11 | E13 | Fundamento contratual aplicável e início de execução comprovados |
| E13 | E14 | Todas as entregas e condições de aceite/conclusão atendidas; saldo de execução verificado |
| E14 | E15 | Total devido efetivamente recebido e conciliado |
| E15 | E16 | Encerrar normalmente; obrigações residuais e garantias resolvidas ou acompanhamento formal registrado |
| E06/E12 | E16 | Fechar estudo perdido/descartado, com motivo e pendências residuais registradas |
| E08/E09/E10/E11/E13/E14 | E16 | Encerramento excepcional por cancelamento, rescisão ou outro fato comprovado; discriminar saldos e plano/responsável para pendências |
| E16 | E04 | Reabrir estudo apenas se nunca houve participação, com justificativa |
| E16 | E10/E13/E14/E15 | Reabertura excepcional no estágio factual comprovado, com nova evidência; não gerar atos por inferência |

**Eventos sem avanço automático:** entregas parciais, recebimentos parciais, adiantamentos efetivamente comprovados, recusa de entrega, saldo contestado, atraso e revisão pós-participação geram eventos/pendências; não declaram entrega ou pagamento completos. Se pagamento integral comprovado anteceder a entrega total, guardar o fato em E13; ao concluir entrega, E14 → E15 pode ocorrer na mesma operação auditada com dois eventos, sem inventar novo recebimento. Pós-venda/garantia remanescente continua monitorado mesmo com E16 quando houver responsável e forma de acompanhamento.

**Retificação após participação:** E08 em diante conserva o estágio factual e abre revisão de análise em paralelo. Eventual bloqueio impede nova recomendação, sem apagar ato ocorrido ou recuar automaticamente o estado comercial.

**Reconciliação excepcional:** permitir registrar fato externo comprovado ocorrido sem uso prévio da ferramenta, inclusive participação feita apesar de um bloqueio. Exigir origem, destino factual E08–E15, autor, data real e data de registro, motivo, evidência e sinalização de desvio. Não criar retrospectivamente E07 nem apagar bloqueios; isso é registro de realidade, não aprovação. Transições comerciais comuns continuam restritas à tabela. Correção de fato errado invalida/substitui o evento com histórico, sem exclusão.

**Verificação:** E02 → E13 sem contrato é rejeitada; retificação após E08 preserva comprovante e abre revisão; entrega parcial mantém E13; recebimento parcial mantém saldo; “vencida” jamais é atribuído por expiração de prazo. **RF:** 029 a 034.

## 9. Cobertura e continuidade

| Domínio solicitado | Regras |
|---|---|
| 9.1 Prazos | RN-001 a RN-003 |
| 9.2 Preço estimado | RN-004 a RN-006 |
| 9.3 Lotes/grupos | RN-007 a RN-009 |
| 9.4 Operação remota | RN-010 a RN-015 |
| 9.5 Capital | RN-016 a RN-021 |
| 9.6 Habilitação/conformidade | RN-022 a RN-025 |
| 9.7 Evidência/confiança | RN-026 a RN-032 |

Na Execução 2, detalhar regras como parâmetros/modelos versionados, definir fórmulas globais e financeiras, rubricas técnicas, modelo de dados e contratos oficiais. Na Execução 3, rastrear RN até CP/RF/RT/componentes/entidades/casos/testes/fases e validar regras legais específicas em fontes oficiais. Nenhuma lacuna crítica pode ser silenciosamente suprida na implementação.

## 10. Descoberta a partir de resultados — revisado em R1

As execuções originais citadas acima já terminaram. Regras novas [PROPOSTO]; exemplos sintéticos, não testes realizados. Modelo e demais rastreabilidade propagados na R1-E2 em 08/17/23. [22](22-inteligencia-de-mercado-fornecedores.md) detalha política e dimensões.

### RN-033 — Estabelecimento e identidade fiscal textual

**Entradas:** ID original, tipo de pessoa, país, ID canônico/versão do validador, fonte e chave interna.
**Regra/condição:** havendo CNPJ, preservar completo/original/letras/zeros, validar formato e DV pela Receita; raiz somente para agrupamento. Sem identificação suficiente, manter candidato provisório com pendência, sem chave oficial inventada. Nome/raiz não confirmam fusão; estrangeiro ou pessoa física requer política própria.
**Consequência:** estabelecimentos e escopos comerciais independentes; letras nunca removidas por conversão numérica.
**Verificação:** positivo: matriz E1 e filial E2 permanecem dois estabelecimentos no mesmo agrupamento; negativo: raiz compartilhada não copia cotação/estoque, ID inválido não vira identidade fiscal confirmada. Fixtures válidas numéricas/alfanuméricas devem vir de verificação oficial futura, não de número inventado. **RF:** 003, 013, 035, 037, 038.

### RN-034 — Mesmo evento versus etapas relacionadas

**Entradas:** fonte, chave de compra/item/resultado/contrato, etapa, natureza, versão e evidência de correspondência.
**Regra/condição:** mesma identidade/grão comprovados entre fontes contam uma vez na etapa, conservando proveniências; resultado e contrato associados são eventos distintos, sem somar valores/quantidades como negócios independentes. Correspondência fuzzy/incerta fica pendente fora de agregado padrão, com impacto divulgado.
**Consequência:** nenhuma receita/volume entregue é inferida de homologação/ata; estatística tem etapa e unidade homogênea explícitas.
**Verificação:** positivo: resultado R replicado em A/B tem uma contagem e duas fontes; negativo: R homologado e C contratado de 100 unidades não resultam em 200 entregues. Registro de preços não obriga contratação (Lei 14.133 art. 83, fonte em matriz R1; regime concreto deve ser validado). **RF:** 003, 028, 035, 037.

### RN-035 — Suficiência, qualidade e atualidade da referência

**Entradas:** comparáveis por natureza/unidade/janela, situação/versão, pendências, n eventos/estabelecimentos e política `r1-comparabilidade-1`.
**Regra/condição:** agregar só após equivalência suficiente, deduplicação e revisão de outliers; limiar inicial proposto de cinco eventos e três estabelecimentos não prova representatividade. Abaixo disso, informação individual qualificada, sem agregado confiável. Fonte desatualizada/conflito crítico gera pendência, não confiança automática; denominação “não calculável” quando N=0.
**Consequência:** mostrar exclusões/motivos/n/janela; dado nominal preservado, correção monetária futura exige método/índice. Ausência de CATMAT não impede atributos comprovados; código igual não confirma equivalência.
**Verificação:** positivo: coorte suficiente, comparável e revisada permite mediana descritiva separada por natureza; negativo: dois preços, embalagem desconhecida ou possível duplicidade não produzem mediana representativa nem descarte silencioso do outlier. **RF:** 028, 036, 037.

### RN-036 — Candidato não é capacidade nem inelegibilidade

**Entradas:** histórico observado por etapa, papel observado/declarado/inferido/desconhecido, qualidade e insumos atuais de análise.
**Regra/condição:** histórico orienta investigação sem comprovar entrega/disposição/capacidade; vencedor pode ser concorrente que não vende ao operador. CNAE opcional/nome não decide aptidão. Ausência de histórico, fonte falha, concorrência ou falta de resposta não geram inaptidão universal. Preservar pesos/fórmulas e guardas atuais.
**Consequência:** sem cotação/custo obrigatório, análise pendente mesmo com histórico; com fornecedor/cotação e demais insumos suficientes, histórico ausente não bloqueia. Não calcular taxa de vitória sem participações observadas; frequência de resultados é outra métrica.
**Verificação:** positivo: oportunidade suficiente segue análise quando fonte histórica falha; negativo: dez resultados sem cotação não liberam margem/score positivo ou afirmação “executou”. **RF:** 005, 025, 026, 028, 037.

### RN-037 — Dimensões de relacionamento e reutilização do cadastro

**Entradas:** candidato, estabelecimento reconciliado, produto/quantidade/prazo/condições, autoria, evidência e validade.
**Regra/condição:** descoberta, contato, disposição de venda, cotação e disponibilidade seguem transições independentes de 22 §9. Contato sem resposta não implica recusa; aceitação de cotar não confirma disponibilidade. Identidade já cadastrada reutiliza supplier_id; conflito abre revisão. Retificação/expiração perde atualidade apenas no escopo afetado, conservando histórico.
**Consequência:** não existe estado genérico “fornecedor confirmado”; E01–E16 permanecem intactos; registro manual não dispara mensagem. Cotação recebida só vira vigente com requisitos suficientes de RN-006.
**Verificação:** positivo: candidato ligado a cadastro existente recebe contato e cotação no mesmo estabelecimento com versões; negativo: aceite para cotar não promove disponibilidade nem cria segundo fornecedor; cotação vencida não valida outra filial/produto. **RF:** 013, 015, 038.

## Histórico de revisões

2026-10-12 — 0.2-R1: RN-033–037 novas; RN-005/006/026/028 estendidas. Preservadas RN-001–032, transições, score e políticas financeiras. Sem teste ou mudança operacional.


2026-10-13 — 0.3-R1-E2: contagem e referências de propagação atualizadas; RN-032 e suas transições preservadas.

