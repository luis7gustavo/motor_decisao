# 15 — Roadmap e backlog priorizado

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: rascunho para revisão do operador; [PROPOSTO], sem implementação nesta missão.

## 1. Entrega e prioridade

Fases são futuras etapas de implementação; execuções 1–3 são a missão documental. O MVP contém os 32 RF Must das fases 1–5. RF-024 e RF-028 são Should: sensibilidade ampliada e histórico não podem ser dependências ocultas de um Must. Revisão mínima começa em cada módulo; fase 5 consolida a carteira. Nenhuma estimativa abaixo é prazo contratado. Complexidade relativa: M (média), A (alta); esforço deve ser refinado após spike de contratos e inventário de dados.

| Fase / prioridade / complexidade | Objetivo e funcionalidades | Dependências e risco principal | Entregáveis e critério de conclusão |
|---|---|---|---|
| 0 — planejamento / P0 / M | Diagnóstico, RF/RNF/RN, arquitetura e plano | Código e fontes disponíveis; risco de confundir proposta com realidade | Especificação desta pasta, decisões e pendências; revisão do operador para iniciar implementação |
| 1 — fundação / P0 / A | Schemas aditivos, consulta PNCP, versões e busca local | Fase 0, gates G0–G4 de 16, QA-001 por recurso; isolamento e perda de dados | RF-001–005 e RNF iniciais; contrato/fixtures, migration revisada, restore ensaiado e consulta resiliente |
| 2 — documentos/logística / P0 / A | Parser isolado, evidências, requisitos e A/B/C/D | Identidade/versões da fase 1; contrato de anexos; risco de extração/SSRF | RF-006–012, corpus adverso e D13 implementado; ausência crítica impede positivo |
| 3 — fornecedores/matching / P0 / A | CSV, cotações, atributos e revisão técnica | Versões e requisitos de 1/2; equivalência falsa e estoque volátil | RF-013–019, importação idempotente e matching com evidência; replay e mudança de versão cobertos |
| 4 — finanças/ranking / P0 / A | Decimal, caixa, lote completo, bloqueios e score | Fases 2/3 e parâmetros do operador; falso lucro e falta de capital | RF-020–023/025–027; três casos numéricos passam; versão persistida ordena corretamente; RF-024 opcional P1 |
| 5 — operação / P0 / M | Fila, estados, fatos externos, recebimento e painel | Fase 4 e revisão já disponível; risco de presumir atos externos | RF-029–034; 45 transições/negativos, jornada completa e piloto MN/RNF; MVP completo somente aqui |
| 6 — inteligência / P2 / A | Mercado amplo, concentração/tendências, CNAE em escala, lances completos e ML | Evidência/contratos próprios e avaliação; sem dependência do piloto 2-H | Parcela ampliada RF-028; nenhuma soma entre etapas e ML só com hipótese/rótulos/avaliação temporal (revisado em R1) |

Sequência crítica: infraestrutura/restore → identidade e contrato → evidências → logística e fornecimento → finanças → elegibilidade/ranking → operação. UI de revisão acompanha cada incremento. Fornecedores podem ter cadastro prototipado enquanto documentos são concluídos, mas matching confirmado depende dos requisitos. Não criar bancos/microsserviços/agendadores adicionais para acelerar fases sem ADR.

## 2. Épicos e histórias de usuário

EP-01–EP-10 correspondem às capacidades CP-01–CP-10 do PRD, por convenção de rastreabilidade reservada nesta consolidação. HU-001–034 conservam o número do RF de origem. Cada história é [PROPOSTO]; critérios abaixo foram trazidos do RF, que continua fonte normativa. Testes adicionais de segurança/qualidade em 14 são obrigatórios por fase.

### EP-01 — Descoberta e atualização

#### HU-001 — Consultar contratações públicas nacionais

Como operador, quero consultar contratações públicas nacionais para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-001; **prioridade:** Must; **fase:** 1; **teste:** TST-001.

**Dependências:** Contrato oficial a detalhar em 10, persistência proposta em 08; nenhuma dependência de marketplaces.

**Aceite:** Dada uma fixture oficial sintética com duas páginas, a coleta processa ambas e preserva fonte e payload de cada registro. Se a segunda página falhar, a execução fica parcial, com ponto de retomada e sem afirmar completude.
#### HU-002 — Pesquisar, filtrar e identificar abertura

Como operador, quero pesquisar, filtrar e identificar abertura para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-002; **prioridade:** Must; **fase:** 1; **teste:** TST-002.

**Dependências:** RF-001, RF-003, RF-004.

**Aceite:** Uma consulta UF+modalidade retorna apenas os registros que satisfazem os dois filtros. Um registro sem prazo não aparece como aberto confirmado; um prazo vencido deixa de permitir recomendação positiva.
#### HU-003 — Normalizar identidade e impedir duplicação

Como operador, quero normalizar identidade e impedir duplicação para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-003; **prioridade:** Must; **fase:** 1; **teste:** TST-003.

**Dependências:** RF-001; modelo de identidade a fechar em 08 e contrato 10.

**Aceite:** Reprocessar o mesmo identificador oficial e conteúdo mantém uma contratação e o número de itens esperado. Duas contratações com títulos iguais e IDs oficiais distintos permanecem separadas; item sem identidade confiável fica sinalizado.
#### HU-004 — Sincronizar alterações e invalidar análises afetadas

Como operador, quero sincronizar alterações e invalidar análises afetadas para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-004; **prioridade:** Must; **fase:** 1; **teste:** TST-004.

**Dependências:** RF-001, RF-003; RF-027 e RF-034 quando implementados.

**Aceite:** Ao receber retificação com quantidade e prazo alterados, ambas as versões continuam consultáveis e análises vinculadas à anterior são marcadas desatualizadas. Payload repetido sem mudança não cria nova versão comercial.
#### HU-005 — Manter consulta local e sinalizar indisponibilidade

Como operador, quero manter consulta local e sinalizar indisponibilidade para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-005; **prioridade:** Must; **fase:** 1; **teste:** TST-005.

**Dependências:** RF-001 a RF-004; metas operacionais do documento 05.

**Aceite:** Simulada indisponibilidade após coleta bem-sucedida, as oportunidades continuam consultáveis com indicação de desatualização. Após retomada, registros reconciliados ganham nova confirmação e os demais não desaparecem silenciosamente.

### EP-02 — Documentos e evidências

#### HU-006 — Obter e versionar documentos oficiais

Como operador, quero obter e versionar documentos oficiais para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-006; **prioridade:** Must; **fase:** 2; **teste:** TST-006.

**Dependências:** RF-001, RF-003, RF-004; segurança documental do documento 05.

**Aceite:** Dois downloads do mesmo conteúdo não duplicam a versão. Novo conteúdo no mesmo endereço cria versão; arquivo rejeitado por política não é aberto/executado e a análise fica pendente com motivo.
#### HU-007 — Extrair texto e localizar evidências

Como operador, quero extrair texto e localizar evidências para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-007; **prioridade:** Must; **fase:** 2; **teste:** TST-007.

**Dependências:** RF-006.

**Aceite:** PDF sintético com condição de entrega na página 3 gera evidência vinculada à página 3 e versão correta. PDF escaneado sem texto é identificado como insuficiente e segue à revisão, sem texto inventado.
#### HU-008 — Estruturar exigências do edital

Como operador, quero estruturar exigências do edital para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-008; **prioridade:** Must; **fase:** 2; **teste:** TST-008.

**Dependências:** RF-007; regras do documento 06 e validação especializada quando exigida.

**Aceite:** Fixture com garantia de 24 meses e entrega parcelada em dois endereços produz os três requisitos com suas origens. Ausência de regra sobre instalação gera pendência, não conclusão de dispensa; não há declaração automática de conformidade jurídica.
#### HU-009 — Revisar ambiguidade e conflito documental

Como operador, quero revisar ambiguidade e conflito documental para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-009; **prioridade:** Must; **fase:** 2; **teste:** TST-009.

**Dependências:** RF-006 a RF-008.

**Aceite:** Dois documentos com prazos diferentes produzem conflito visível e bloqueiam conclusão até o operador registrar a evidência que resolve a precedência. Corrigir uma extração preserva o valor anterior e o autor da revisão.

### EP-03 — Logística e fornecimento remoto

#### HU-010 — Classificar viabilidade de execução remota

Como operador, quero classificar viabilidade de execução remota para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-010; **prioridade:** Must; **fase:** 2; **teste:** TST-010.

**Dependências:** RF-008, RF-009; classificação detalhada em 06 e 11.

**Aceite:** Caso sem instalação e com transporte/recebimento documentados pode resultar A, com ressalvas preservadas. Instalação presencial obrigatória resulta C; local de entrega desconhecido resulta D. Nenhuma classe libera participação automaticamente.
#### HU-011 — Calcular score de prontidão logística explicável

Como operador, quero calcular score de prontidão logística explicável para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-011; **prioridade:** Must; **fase:** 2; **teste:** TST-011.

**Dependências:** RF-010; detalhamento/calibração em 09 e 11.

**Aceite:** Com todas as dimensões evidenciadas e avaliação 1, score=100 e cobertura=100% (`coverage=1`). Se preparação/prazo (peso 0,25) é desconhecida e as demais valem 1, limite inferior=75, cobertura=75% (`coverage=0,75`) e limite superior=100; a dimensão permanece desconhecida e classe/elegibilidade não são automaticamente favoráveis.
#### HU-012 — Comparar modelos de fornecimento e envio direto

Como operador, quero comparar modelos de fornecimento e envio direto para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-012; **prioridade:** Must; **fase:** 2; **teste:** TST-012.

**Dependências:** RF-008 a RF-010; RF-013 a RF-016 na fase 3; RF-020 na fase 4.

**Aceite:** Oferta de envio direto sem evidência contratual/fiscal suficiente permanece pendente. Duas alternativas podem coexistir sem misturar custos ou obrigações. Nesta fase aceita cadastro manual; integração com catálogo entra após RF-013.

### EP-04 — Fornecedores e cotações

#### HU-013 — Cadastrar fornecedor e condições comerciais

Como operador, quero cadastrar fornecedor e condições comerciais para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-013; **prioridade:** Must; **fase:** 3; **teste:** TST-013.

**Dependências (revisado em R1):** modelo 08 e política de identidade/retenção; cadastro básico não depende de RF-012, RF-035–038 ou cotação. RF-012 consome condições disponíveis, sem ciclo.

**Aceite:** Fornecedor sem informação de entrega direta mantém esse campo desconhecido. Alteração de pedido mínimo preserva condição anterior e não reescreve análise financeira passada.
#### HU-014 — Importar catálogo CSV e manter atributos de produto

Como operador, quero importar catálogo CSV e manter atributos de produto para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-014; **prioridade:** Must; **fase:** 3; **teste:** TST-014.

**Dependências:** RF-013; preservação dos catálogos SILLO existentes.

**Aceite:** CSV de 10 linhas com duas inválidas gera oito aceitas e duas rejeitadas identificáveis, sem sucesso integral indevido. Mesmo SKU de fornecedores distintos não é fundido; catálogo repetido não duplica produtos.
#### HU-015 — Registrar cotação, validade e disponibilidade

Como operador, quero registrar cotação, validade e disponibilidade para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-015; **prioridade:** Must; **fase:** 3; **teste:** TST-015.

**Dependências:** RF-013, RF-014.

**Aceite:** Cotação vencida continua no histórico, mas não sustenta decisão positiva sem reconfirmação. Quantidade ofertada de 10 para demanda de 20 registra insuficiência; uma página de catálogo não vira reserva de estoque.
#### HU-016 — Consultar histórico e alternativas de cotação

Como operador, quero consultar histórico e alternativas de cotação para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-016; **prioridade:** Must; **fase:** 3; **teste:** TST-016.

**Dependências:** RF-014, RF-015, RF-017 quando disponível.

**Aceite:** Revisar preço de uma cotação cria versão e permite reconstruir cenário anterior. Caixa de 10 e unidade avulsa só são comparadas após fator documentado, nunca pelo número bruto do preço.

### EP-05 — Matching técnico revisável

#### HU-017 — Comparar atributos técnicos e fornecimento

Como operador, quero comparar atributos técnicos e fornecimento para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-017; **prioridade:** Must; **fase:** 3; **teste:** TST-017.

**Dependências:** RF-008, RF-014, RF-015.

**Aceite:** Produto com título muito semelhante e potência inferior à exigida resulta em incompatibilidade explícita no atributo. Quantidade em caixas é convertida somente com conteúdo comprovado, preservando origem e fator.
#### HU-018 — Classificar compatibilidade técnica

Como operador, quero classificar compatibilidade técnica para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-018; **prioridade:** Must; **fase:** 3; **teste:** TST-018.

**Dependências:** RF-017, RF-009.

**Aceite:** Um atributo obrigatório não atendido resulta incompatível; atributo crítico sem evidência impede confirmado. Produto completo e revisado pode ser confirmado, vinculado às versões do item e da ficha técnica.
#### HU-019 — Revisar e versionar o matching

Como operador, quero revisar e versionar o matching para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-019; **prioridade:** Must; **fase:** 3; **teste:** TST-019.

**Dependências:** RF-004, RF-017, RF-018.

**Aceite:** Retificação que altera capacidade exigida marca matching anterior como desatualizado. Correção manual preserva algoritmo/resultado original e registra avaliação que o substituiu.

### EP-06 — Finanças, capital e unidade de julgamento

#### HU-020 — Cadastrar cenários financeiros completos

Como operador, quero cadastrar cenários financeiros completos para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-020; **prioridade:** Must; **fase:** 4; **teste:** TST-020.

**Dependências:** RF-012, RF-015, RF-018; regra financeira do documento 06.

**Aceite:** Cenário com frete desconhecido é marcado incompleto, sem lucro confirmado. Valor zero explicitamente justificado pode ser usado e permanece auditável; simulação nova não reescreve a anterior.
#### HU-021 — Calcular custos, preços, margem e retorno com Decimal

Como operador, quero calcular custos, preços, margem e retorno com Decimal para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-021; **prioridade:** Must; **fase:** 4; **teste:** TST-021.

**Dependências:** RF-020; fórmulas e validação numérica a detalhar em 12.

**Aceite:** Fixture Decimal com quantidades fracionárias e custos rateados reconcilia total e linhas pela política declarada. Receita zero, denominador nulo ou taxa inviável gera indicador não calculável/motivo, sem divisão por zero ou rentabilidade artificial.
#### HU-022 — Simular desembolsos e capital de giro

Como operador, quero simular desembolsos e capital de giro para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-022; **prioridade:** Must; **fase:** 4; **teste:** TST-022.

**Dependências:** RF-020, RF-021.

**Aceite:** Operação lucrativa com desembolso anterior superior ao capital disponível fica impedida pela regra de capital. Adiar o recebimento aumenta exposição/custo quando aplicável e preserva o cenário original.
#### HU-023 — Consolidar viabilidade no nível de julgamento

Como operador, quero consolidar viabilidade no nível de julgamento para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-023; **prioridade:** Must; **fase:** 4; **teste:** TST-023.

**Dependências:** RF-008, RF-020 a RF-022.

**Aceite:** Lote com um item lucrativo e outro deficitário exibe resultado agregado e não recomenda participar apenas pelo primeiro. Frete compartilhado é somado uma vez e sua alocação reconcilia ao total.

### EP-07 — Sensibilidade ampliada

#### HU-024 — Comparar sensibilidade e metas de margem

Como operador, quero comparar sensibilidade e metas de margem para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-024; **prioridade:** Should; **fase:** 4; **teste:** TST-024.

**Dependências:** RF-020 a RF-023.

**Aceite:** Aumentar apenas frete não melhora lucro, mantidas demais entradas. Cenário em BRL sem exposição cambial mostra câmbio não aplicável; todos os cenários preservam entradas e versão.

### EP-08 — Elegibilidade e priorização explicável

#### HU-025 — Aplicar bloqueios e pendências antes de recomendar

Como operador, quero aplicar bloqueios e pendências antes de recomendar para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-025; **prioridade:** Must; **fase:** 4; **teste:** TST-025.

**Dependências:** RF-002, RF-008, RF-010, RF-015, RF-018, RF-022, RF-023; regras 06.

**Aceite:** Prazo vencido impede recomendação positiva mesmo com margem alta. Pendência documental crítica permanece pendência, e não elegível; score alto não remove um bloqueio.
#### HU-026 — Produzir ranking configurável e transparente

Como operador, quero produzir ranking configurável e transparente para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-026; **prioridade:** Must; **fase:** 4; **teste:** TST-026.

**Dependências:** RF-011, RF-018, RF-021 a RF-023 e RF-025; modelo matemático em 09. RF-024 é incremento opcional quando disponível, sem impedir o ranking obrigatório do MVP.

**Aceite:** Oportunidade de valor estimado maior mas bloqueada não supera candidato elegível no ranking positivo. Alterar peso válido cria versão e explica mudança; peso inválido não é aceito.
#### HU-027 — Explicar e reconstruir cada análise

Como operador, quero explicar e reconstruir cada análise para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-027; **prioridade:** Must; **fase:** 4; **teste:** TST-027.

**Dependências:** RF-004, RF-007, RF-016, RF-019, RF-020, RF-025, RF-026.

**Aceite:** Após alterar cotação e pesos, a análise anterior continua mostrando valores/regras originais. Todo bloqueio e recomendação aponta para fonte ou premissa explícita; ausência de evidência fica identificada.

### EP-09 — Preços históricos comparáveis

#### HU-028 — Contextualizar preços históricos oficiais

Como operador, quero contextualizar preços históricos oficiais para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-028; **prioridade:** Should; **fase (revisado em R1):** 2-H básico; 6 ampliado; **testes:** TST-028/061–064.

**Dependências (revisado em R1):** RF-003/035/036, política de 22 e contratos de 10; RF-017/027 somente usos posteriores, sem pré-requisito do básico.

**Aceite:** Preço homologado por caixa não entra em série unitária sem conversão comprovada. Amostra incompatível ou insuficiente gera ausência de referência confiável, não preço imputado como fato.

### EP-10 — Operação e revisão comercial

#### HU-029 — Operar fila de revisão comercial

Como operador, quero operar fila de revisão comercial para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-029; **prioridade:** Must; **fase:** 5; **teste:** TST-029.

**Dependências:** RF-025 a RF-027; padrão de revisão SILLO adaptado.

**Aceite:** Descartar exige motivo; pedir complemento identifica pendência/responsável. Dupla submissão não cria decisões ativas conflitantes. Revisão favorável não envia lance/proposta nem supera bloqueio crítico.
#### HU-030 — Controlar estados e transições comerciais

Como operador, quero controlar estados e transições comerciais para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-030; **prioridade:** Must; **fase:** 5; **teste:** TST-030.

**Dependências:** RF-004, RF-025, RF-029; estados 06/D10.

**Aceite:** Uma transição não autorizada é rejeitada sem modificar o estado. Encerramento oficial afeta elegibilidade, mas não converte participação registrada em inexistente; cada mudança aparece na linha do tempo.
#### HU-031 — Registrar participação, disputa e resultado manualmente

Como operador, quero registrar participação, disputa e resultado manualmente para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-031; **prioridade:** Must; **fase:** 5; **teste:** TST-031.

**Dependências:** RF-029, RF-030; validação de transição em 06.

**Aceite:** Salvar participação exige referência verificável e responsável. Registrar resultado vencedor não cria contrato automaticamente; nenhum evento dispara envio externo de lance ou proposta.
#### HU-032 — Acompanhar contratação, execução e recebimento

Como operador, quero acompanhar contratação, execução e recebimento para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-032; **prioridade:** Must; **fase:** 5; **teste:** TST-032.

**Dependências:** RF-022, RF-023, RF-030, RF-031.

**Aceite:** Entrega parcial não marca entrega total. Recebimento parcial mantém saldo; vencedor sem contratação não inicia execução por inferência. Encerramento excepcional registra motivo e pendências residuais.
#### HU-033 — Visualizar carteira e indicadores comerciais

Como operador, quero visualizar carteira e indicadores comerciais para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-033; **prioridade:** Must; **fase:** 5; **teste:** TST-033.

**Dependências:** RF-002, RF-025 a RF-027 e RF-029 a RF-032; metas e métricas do DAN. RF-028 é extensão opcional (2-H básico/6 ampliado), sem impedir o dashboard obrigatório da fase 5 (revisado em R1).

**Aceite:** Contador de pendências coincide com a lista filtrada de origem. Valor estimado, proposta hipotética e receita efetivamente recebida são identificados separadamente; dados desatualizados aparecem sinalizados.
#### HU-034 — Preservar decisões e reabrir revisão

Como operador, quero preservar decisões e reabrir revisão para fundamentar a análise comercial e conservar seu histórico. **Origem:** RF-034; **prioridade:** Must; **fase:** 5; **teste:** TST-034.

**Dependências:** RF-004, RF-027, RF-029, RF-030.

**Aceite:** Desfazer pré-aprovação mantém autor/motivo/valores originais e registra novo evento. Retificação após participação abre revisão operacional sem excluir o comprovante de participação; histórico SILLO permanece com seu significado original.

## 3. Trabalho técnico habilitador

| Item | Prioridade / fase | Entregável verificável / dependências |
|---|---|---|
| TEC-01 — isolamento e runtime | P0 / 1 | Compose de teste independente, volumes/credenciais próprios, guarda pré-escrita e imagens reprodutíveis; RT-011/015, TST-046/053 |
| TEC-02 — migração e recuperação | P0 / 1 | G0–G4 e restore aprovado antes do corte; RT-014, TST-048/051 |
| TEC-03 — superfície e segredos | P0 / 1–2 | Loopback, CSRF, logs sanitizados e downloader/parser isolados; RT-012, TST-035–038 |
| TEC-04 — contratos/frescor | P0 / 1–2 | Fechar QA-001 por recurso, snapshots oficiais e fixtures; RF-001/006, TST-041/052 |
| TEC-05 — observabilidade/capacidade | P0 / 1–4 | Correlação, readiness e protocolo RNF medidos; RT-013/016, TST-040/043/047/055 |
| TEC-06 — calibração comercial | P0 / antes do piloto | Operador registra categoria/UF/capital/margens/fontes fiscais e risco; QA-004/008/010; sem defaults pessoais |

## 4. Definição de pronto

Cada incremento tem código revisado, documentação/ADRs atualizados, contratos e testes pertinentes passando em container, evidência de preservação, observabilidade e retorno de versão. Falha crítica não é compensada por percentual de cobertura. Aprovação do piloto do MVP completo exige capacidade Must completa e parâmetros reais confirmados; conclusão documental não libera operação. Relatar amostras e desvios de MN-01–10, sem inventar retorno financeiro. Pendências de contrato bloqueiam somente o recurso dependente, mas impedem declarar o RF correspondente completo.


## 5. Marco 2-H e jornada de cotações — revisado em R1

Revisão 0.3-R1-E2, 2026-10-13. Fases 0–6 e EP-01–10 preservados; EP-09/CP-09 ampliados para descoberta básica, EP-04/CP-04 incorpora contato opcional. Complexidade B/M/A é relativa, não estimativa em horas. Histórico não é pré-requisito universal de fornecedor/finanças/ranking; os Should só são obrigatórios dentro do incremento explicitamente ativado. Definição de pronto do MVP das seções anteriores não é gate de exigir fase 5 para experimentar pesquisa/cotação: o piloto limitado de 23 tem aceite próprio, sem autorizar E07/participação.

| Incremento / ordem | Objetivo e dependência | Prioridade / complexidade / risco | Entregável e conclusão com revisão humana |
|---|---|---|---|
| Preparação 2-H | TEC-01/02/04: isolamento/contratos depois da fundação 1 | P0 para habilitar / M / fonte ou destino incorretos | Manifesto de imagem/lock, contrato/hash/termos/limites por recurso e TST-070/071; operador delimita recorte |
| Identidade/fonte | RF-003/004 estendidos + HU-035 | P1 Should histórico / A / filial, alfanumérico, retificação | Eventos/proveniência e pendências auditáveis, TST-056–060/065/069; revisão de conflitos |
| Qualidade/comparáveis | HU-036 após observações identificadas | P1 Should / A / código/unidade equivocados | Comparações por atributo/fator/evidência, qualidade/exclusões, TST-061–063; operador confirma pares |
| Preços/candidatos | HU-028 básico e HU-037 após comparabilidade | P1 Should / M / histórico parecer capacidade | Lista explicável e preços separados, agregado insuficiente identificado, TST-063/064/066; operador escolhe pesquisa |
| Fase 3: contato/cotação | HU-038 com cadastro básico RF-013; RF-015 quando cotar | P1 Should jornada; cadastro/cotação continuam P0 Must / M / transferência de escopo | Ledger manual e reaproveitamento do supplier_id; TST-059/066/067/071; conferir evidências de oferta |
| Jornada/medição limitada | TEC-05/06 e aceites de 23; dados dos passos anteriores | P0 para piloto / M / seleção só de sucessos | MN-11–18 com casos sem resposta, custo manual e gates; TST-068 quando integrar finanças/decisão |

### HU-035 — Ingerir resultados delimitados (EP-09)

Como operador, quero resultados por item com etapa, fonte e situação para pesquisar fornecedores sem inventar execução. **Origem:** RF-035; **prioridade:** Should/P1; **fase:** 2-H; **complexidade:** A; **dependências:** RF-001/003/004/005, TEC-01/04 e QA-R1-01–04. **Aceite:** duas páginas e replay preservam eventos/IDs/proveniência; replicação conta uma vez, contrato relacionado não vira entregue; quota/checkpoint durável e parcial explícito. **Testes:** TST-056–060/065/069/070. Revisão humana de identidade e contrato antes de habilitar recurso.

### HU-036 — Revisar comparáveis (EP-09)

Como operador, quero comparar atributos/unidade/contexto para justificar cada referência. **Origem:** RF-036; **prioridade:** Should/P1; **fase:** 2-H; **complexidade:** A; **dependências:** RF-003/035 e item descrito com evidência, sem exigir RF-017. **Aceite:** código igual incompatível rejeitado; caixa sem fator pendente; código ausente com atributos suficientes revisável; autor/versões registrados. **Testes:** TST-061/062/063/065. Sem agregado/preço calculado neste dono; apresentação permanece HU-028.

### HU-037 — Investigar candidatos explicáveis (EP-09)

Como operador, quero estabelecimentos ligados ao item com razões para escolher quem investigar. **Origem:** RF-037; **prioridade:** Should/P1; **fase:** 2-H; **complexidade:** M; **dependências:** RF-003/035/036; RF-028 apenas contexto. **Aceite:** matriz/filial separadas, histórico por etapa e incerteza visíveis, desempate estável; sem taxa de vitória/capacidade/score global inferidos. **Testes:** TST-059/063/066/068/071. Operador pode descartar com motivo; ausência de histórico não significa inapto.

### HU-038 — Registrar contato e aproveitar cotação (EP-04)

Como operador, quero registrar respostas e vincular cotações por escopo ao cadastro existente. **Origem:** RF-038; **prioridade:** Should/P1; **fase:** 3; **complexidade:** M; **dependências:** RF-037, RF-013 para cadastro e RF-015 somente para cotação; contato pode preceder cotação. **Aceite:** nenhuma mensagem enviada pelo Radar; supplier_id reutilizado; disposição/contato/cotação/disponibilidade separados e datados; expiração/outra filial não confirma; métricas incluem insucessos. **Testes:** TST-059/066/067/071. Revisão humana da evidência comercial antes de uso.

Extensões de HU existentes: HU-003/004/005 recebem aceites TST-056/059/060/065/068/069; HU-013/015 recebem TST-059/060/067; HU-020/025/026/027 recebem TST-068 sem mudar matemática; HU-028 básico entrega apenas apresentação de RF-028, mantém nome/ID e parcela ampliada fase 6. TEC-01/04/05/06 agora abrangem isolamento, contrato histórico, qualidade e medição de 23, sem novos habilitadores redundantes. CNAE, painel de mercado, histórico completo de lances e ML não bloqueiam nenhuma linha acima.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: marco 2-H, HU-035–038, HU-013/028/033 e épicos/habilitadores conciliados; fases preservadas e controles de piloto separados do MVP completo.

