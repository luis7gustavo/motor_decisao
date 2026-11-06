# 18 — Glossário do Radar Licita

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Revisão: 0.2-R1, 2026-10-12 (America/Sao_Paulo). Revisado em R1-E1; versão de produção original preservada abaixo.

Versão: 0.1. Data de produção: 2026-10-11 (America/Sao_Paulo).  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Data da inspeção da linha de base: 2026-10-10 (America/Sao_Paulo).  
Status: rascunho para revisão do operador; vocabulário do novo domínio [PROPOSTO].

## 1. Finalidade e convenções de leitura

Este glossário fixa a terminologia dos documentos [01 — visão](01-visao-produto.md), [02 — diagnóstico](02-diagnostico-as-is.md), [03 — DAN](03-analise-negocio-dan.md), [04 — RF](04-requisitos-funcionais.md), [05 — RNF](05-requisitos-nao-funcionais.md) e [06 — RN](06-regras-de-negocio.md). As definições de compras públicas são convenções operacionais para organizar dados, evidências e análises; não substituem a definição ou a condição aplicável no instrumento oficial nem constituem parecer especializado.

As seções 3 a 8 descrevem conceitos [PROPOSTO] para o Radar Licita. Um conceito definido não comprova funcionalidade implementada. O diagnóstico é a referência das evidências AS-IS; RN-032 é a fonte dos estados e transições; RN-012 e a seção 9 do documento 04 são as fontes do score logístico. O glossário não cria novas regras nem altera essas fontes.

| Rótulo | Significado fixado |
|---|---|
| [CONFIRMADO] | Implementação ou declaração identificada no código/configuração, com evidência no diagnóstico. Não comprova execução saudável, migration aplicada ou dado atualizado. |
| [NÃO-CONFIRMADO] | Alegação documental ou operacional não confirmada pela inspeção disponível. |
| [PARCIAL] | Há implementação de parte da capacidade, insuficiente para afirmar a capacidade completa. |
| [PROPOSTO] | Capacidade, requisito, regra ou decisão a implementar/revisar para o Radar Licita. |
| [LEGADO] | Componente ou semântica anterior, descontinuado ou candidato a sair do fluxo principal. O rótulo não autoriza excluir código, dados ou história. |

Esses rótulos classificam **capacidade do sistema**. “Estoque confirmado”, “matching confirmado” e “evidência confirmada” classificam **informação de uma análise**; não recebem automaticamente o rótulo de implementação [CONFIRMADO].

| Abreviação ou convenção | Uso nesta especificação |
|---|---|
| AS-IS / TO-BE | Estado atual inspecionado / estado futuro proposto. |
| SILLO / Radar Licita | Sistema atual de apoio à compra para revenda / evolução proposta para inteligência comercial em compras públicas. |
| DAN | Documento de Análise de Negócio, documento 03. |
| PRD / CP | Documento consolidado de requisitos de produto / capacidade de produto. Serão consolidados na Execução 3; não há CP atribuído por este glossário. |
| TRD / RT | Documento consolidado de requisitos técnicos / requisito técnico. Serão consolidados na Execução 3; não há RT atribuído por este glossário. |
| RF / RNF / RN | Requisito funcional / requisito não funcional / regra de negócio. Intervalos desta execução: RF-001–RF-034, RNF-001–RNF-021 e RN-001–RN-032. |
| ADR | Registro de decisão arquitetural, centralizado em 19. O glossário não atribui ADR. |
| MN / E / D | Indicador de negócio do DAN / código lógico de estado comercial / identificador de diagrama. MN-01–MN-10 e E01–E16 já estão definidos; D01–D14 identificam os diagramas previstos. |
| Must / Should | Necessário para concluir a fase atribuída / incremento relevante passível de postergação sem omitir controle obrigatório. Prioridade segue MoSCoW nos documentos 04/05. |
| MVP | Conjunto das capacidades Must das fases 1–5, com operação comercial básica. A fase 1 isolada não é o MVP completo. |
| Fase / execução documental | Fase é etapa futura de implementação: 0 análise, 1 fundação pública, 2 documentos/logística, 3 fornecedores/matching, 4 finanças/ranking, 5 operação comercial, 6 inteligência avançada. Execução documental é uma das três etapas desta missão. |

## 2. Vocabulário técnico atual e limites de reaproveitamento

| Termo | Definição e status | Referência |
|---|---|---|
| Bronze | [CONFIRMADO] Schema/camada de dados comerciais brutos e payloads do SILLO. Preservar dados oficiais brutos nessa abordagem é [PROPOSTO], com identidade e versionamento próprios. | 02, seções 5/6/12; RF-001, RF-003 |
| Silver | [CONFIRMADO] Schema/camada com normalização e agregações comerciais. A normalização do domínio público é [PROPOSTO]; título parecido não resolve identidade oficial. | 02, seção 6; RF-003 |
| Gold | [CONFIRMADO] Schema/camada com oportunidades, rodadas, snapshots e estruturas de score do SILLO. Novas projeções comerciais públicas não devem reinterpretar registros de revenda. | 02, seções 6/7; RNF-017 |
| `control` / `feedback` | [CONFIRMADO] Schemas existentes de execução/qualidade e revisão humana, respectivamente. Nomes novos de schemas e entidades físicas serão decididos no documento 08. | 02, seção 6; RNF-017 |
| Run / snapshot | [CONFIRMADO] Execução identificada / registro histórico dos resultados e insumos referenciados de uma rodada. A reconstrução completa de análise pública é [PROPOSTO]. | 02, seções 6/8; RF-027; RN-028 |
| Motor heurístico de revenda | [CONFIRMADO] Motor `heuristic_v2_confidence_guard`, com sinais e recomendações de compra para revenda. [LEGADO] para o ranking principal do novo domínio. Seus pesos e limiares não definem o ranking licitatório. | 02, seção 7; RF-025/RF-026; RN-029 |
| Revisão SILLO | [CONFIRMADO] Decisão transacional com snapshot, rejeição de duplicidade e desfazimento auditado. Rótulos `approved_test_purchase`, `needs_reanalysis` e `rejected` mantêm o significado original. | 02, seção 8; RN-030 |
| FastAPI / Jinja2 | [CONFIRMADO] Backend HTTP / templates da interface atual. FastAPI é base da evolução; comparar manutenção de Jinja2 e adoção de React pertence ao documento 07. | 02, seções 3/4/8/12 |
| SQLAlchemy / Alembic | [CONFIRMADO] Componentes usados para acesso ao banco / definições versionadas de evolução do schema. Predomina SQL textual; schema declarado em migrations não comprova instalação no banco atual. | 02, seção 6 |
| Prefect | [CONFIRMADO] Orquestração de flows, deployments e workers no código/configuração. Operação atual não comprovada por execução; é o ponto de partida para evolução. | 02, seções 5/9 |
| Crawlee | [CONFIRMADO] Ferramenta usada na coleta, com filas internas, limites e retentativas. Não é fila distribuída entre computadores nem exige uso de browser na API pública. | 02, seções 5/9 |
| Redis | [CONFIRMADO] Serviço com uso configurado na mensageria do Prefect. Cache de negócio do Radar Licita não está confirmado nem é criado por esta definição. | 02, seção 9 |
| Banco de aplicação / banco de metadados Prefect | [CONFIRMADO] Finalidades distintas já declaradas na infraestrutura. Novas entidades públicas devem usar o PostgreSQL existente da aplicação; metadados do orquestrador não são um segundo domínio comercial. | 01, seção 6; 02, seção 9; RNF-017 |
| Docker-first | [PROPOSTO] Restrição de desenvolver, executar, testar e operar a evolução via Docker/Compose. [CONFIRMADO] Há infraestrutura declarada; funcionamento atual [NÃO-CONFIRMADO]. Esta missão apenas documenta a evolução. | 02, seção 9; RNF-019 |
| Machine Learning / Power BI | [PARCIAL] Há estruturas para scores ML e exportação/especificação de BI; treino/inferência e dashboard `.pbix`/`.pbit` não foram encontrados. Não são capacidade completa nem dependência de decisão no MVP. | 02, seções 7/8 |

## 3. Compras públicas e unidade de análise

| Termo [PROPOSTO] | Definição operacional e distinção necessária | Referência |
|---|---|---|
| PNCP | Portal Nacional de Contratações Públicas, fonte oficial prevista para descoberta e atualização. Acesso à documentação não comprova cobertura integral nem execução de endpoints; contratos serão detalhados em 10. | RF-001/RF-004; 02, seção 11 |
| Compras.gov.br | Fonte oficial prevista para investigação de dados de compras e preços históricos. A integração de comparáveis é posterior, com contrato e qualidade a validar em 10. | RF-028; RN-005 |
| Contratação pública | Registro oficial que contextualiza objeto, órgão/unidade, itens, documentos, prazos e regras. Sua publicação não significa que exista contrato firmado com o operador. | RF-001/RF-003/RF-008; RN-007/RN-031 |
| Órgão / unidade contratante | Identificação institucional e unidade vinculada à contratação, preservadas conforme a fonte. Não inferir identidade única só pelo nome textual. | RF-002/RF-003 |
| Item | Componente de fornecimento com especificação, unidade e quantidade próprias. Pode ser julgável isoladamente ou integrar conjunto obrigatório. Não é sinônimo de produto de fornecedor. | RF-003/RF-017/RF-023; RN-007/RN-008 |
| Lote / grupo | Agregações de itens identificadas no instrumento/fonte. Preservar a designação e os vínculos originais; não presumir equivalência universal entre as duas denominações. | RF-003/RF-023; RN-007 a RN-009 |
| Unidade de julgamento / unidade efetiva de participação | Conjunto no qual o instrumento exige avaliar e apresentar o fornecimento: item, lote, grupo ou conjunto global. A avaliação precisa abranger todos os itens obrigatórios. Sua identidade técnica será especificada em 08. | RF-023; RN-007 a RN-009 |
| Julgamento global | Situação em que o conjunto global definido no instrumento governa a análise. Não significa somar seletivamente apenas itens favoráveis. | RF-023; RN-007/RN-008 |
| Modalidade / procedimento / critério de julgamento | Informações distintas do instrumento oficial: forma de procedimento e regra aplicável ao julgamento devem ser registradas como tais. Modalidade não determina, sozinha, agregação por item/lote/grupo/global nem comprova abertura. | RF-002/RF-008/RF-023; RN-007/RN-025 |
| Oportunidade comercial pública | Unidade candidata acompanhada internamente para análise e decisão humana, vinculada à contratação e à unidade de julgamento. Não é necessariamente uma contratação inteira ou uma promessa de receita. Modelo de identidade a fechar em 08. | RF-003/RF-023/RF-030 |
| Edital / instrumento aplicável | Fonte das exigências e condições concretas, considerada com anexos, versões e esclarecimentos pertinentes. O sistema preserva a origem e registra conflitos; não escolhe automaticamente o trecho mais favorável. | RF-006 a RF-009; RN-022/RN-023 |
| Habilitação | Conjunto de exigências a verificar por contratação, com aplicabilidade, evidências, validade e responsável. Checklist atendido pelo sistema não é certificação jurídica. | RF-008/RF-025; RN-022 a RN-025 |
| ME/EPP | Denominações microempresa/empresa de pequeno porte quando pertinentes à informação e às condições do caso. Não atribuem automaticamente benefício, prazo ou dispensa. | RN-025 |
| CATMAT/CATSER | Códigos de catálogo de material/serviço quando disponíveis na fonte, usados como elementos de comparação. Código igual ou descrição parecida não bastam para comprovar equivalência. | RF-028; RN-005/RN-015 |
| Retificação | Mudança oficial relevante que cria versão, preserva o anterior e exige revalidar análises afetadas. Não apaga fatos comerciais já ocorridos. | RF-004/RF-034; RN-003/RN-028 |
| Situação oficial | Informação da fonte sobre a contratação, distinta do estado interno. Abertura, encerramento, adiamento e cancelamento não são intercambiáveis; ausência ou falha de consulta não confirma situação. | RF-002/RF-004/RF-030; RN-001/RN-003 |
| Janela de participação | Prazo conhecido e atualizado de recebimento de propostas, interpretado com instante/fuso e tempo suficiente para preparo. Sessão eletrônica futura não comprova janela aberta. | RF-002/RF-025; RN-001 |
| Entrega parcelada / participação parcial | Entrega em etapas ou quantidades distribuídas no cronograma / participação em parte do conjunto. Uma não autoriza a outra; fracionamento depende de evidência aplicável. | RF-008/RF-023; RN-008/RN-010 |

## 4. Fornecedores, matching e natureza dos preços

| Termo [PROPOSTO] | Definição operacional e distinção necessária | Referência |
|---|---|---|
| Fornecedor / distribuidor | Parte comercial que informa produtos e condições. Cadastro não confirma disponibilidade, habilitação ou atendimento de uma contratação específica. | RF-013/RF-015; RN-006/RN-014 |
| Produto de fornecedor / SKU | Oferta identificada no catálogo de um fornecedor / identificador comercial no contexto desse fornecedor. SKUs iguais de fornecedores distintos não são fundidos automaticamente. | RF-014; RN-015/RN-028 |
| Unidade de fornecimento / conteúdo da embalagem | Base em que quantidade/preço são expressos / quantidade ou composição de cada embalagem. Conversão exige fator comprovado e origem preservada. | RF-016/RF-017; RN-005/RN-015 |
| Cotação comercial | Oferta datada e versionada, com preço, quantidade, validade e condições específicas. Não equivale a preço oficial, estoque reservado ou compromisso de capacidade não evidenciado. | RF-015/RF-016; RN-006 |
| Preço de catálogo | Valor divulgado no catálogo, útil como insumo comercial com data/origem. Não é convertido automaticamente em cotação firme. | RF-014/RF-015 |
| Estoque informado / confirmado / reservado | Informação recebida / disponibilidade verificada com evidência / reserva especificamente comprovada. São situações distintas; uma cotação não promove automaticamente estoque informado a reservado. | RF-015; RN-006 |
| Matching técnico | Comparação do par item–produto por atributos e condições de fornecimento. Similaridade textual recupera candidatos; a confirmação exige evidência e revisão suficiente. | RF-017 a RF-019; RN-015 |
| Compatível confirmado após verificação suficiente | Todos os atributos críticos atendidos, sem conflito e com verificação humana registrada. Vale somente para o par e as versões examinadas. | RF-018; RN-015 |
| Possivelmente compatível | Candidato com indícios suficientes para estudo e verificação não crítica ainda incompleta. Não é confirmação de atendimento. | RF-018; RN-015 |
| Incompatível / informação insuficiente | Divergência obrigatória conhecida / evidência crítica ausente. Divergência conhecida permanece incompatibilidade mesmo se existirem outras lacunas. | RF-018; RN-015 |
| Preço estimado oficial / valor de referência | Valor informado oficialmente com essa natureza, usado como referência. Ausente, sigiloso ou zero na origem mantém valor econômico utilizável desconhecido; o dado bruto é preservado. | RN-004/RN-005 |
| Preço homologado | Valor cuja natureza é identificada na fonte como homologada, com contexto, data e unidade. Não preenche automaticamente preço contratado ou receita recebida. | RF-028; RN-005 |
| Preço contratado | Valor identificado no instrumento/fonte contratual aplicável. Deve permanecer distinto de valor estimado, homologado e efetivamente recebido. | RF-028/RF-032; RN-005 |
| Preço hipotético de proposta / de disputa | Premissa de cenário fornecida explicitamente para avaliar custos e retorno. Não é lance enviado, previsão de vitória ou receita garantida. | RF-020/RF-024; RN-004/RN-005 |
| Preço efetivamente informado | Valor da participação/proposta externa que o operador comprova e registra manualmente. Registrar não envia nem altera a proposta no portal. | RF-031; RN-031 |
| Receita efetivamente recebida | Entrada financeira realizada, comprovada e conciliada, distinta de cenário, contratação, nota fiscal ou saldo a receber. | RF-032/RF-033; RN-005/RN-016 |
| Comparável histórico | Observação histórica com natureza de preço, especificação, unidade, quantidade, época, região e condições suficientemente equivalentes e justificadas. Amostra incompatível não produz referência confiável. | RF-028; RN-005/RN-015 |

## 5. Logística e operação sem deslocamento

| Termo [PROPOSTO] | Definição operacional e distinção necessária | Referência |
|---|---|---|
| Participação eletrônica | Realização externa de atos de participação por meio eletrônico, registrada manualmente no Radar. Não comprova execução remota do fornecimento. | RF-010/RF-031; RN-010/RN-031 |
| Execução remota / sem deslocamento do operador | Modelo em que o operador pode atender às obrigações sem viajar ao local. Obrigações presenciais de terceiros continuam explicitadas, custeadas e verificadas. | RF-010/RF-012; RN-010/RN-013 |
| Envio convencional por transportadora | Transporte físico do produto ao destino, condicionado a características, prazos e recebimento. Não elimina instalação, assistência ou pós-venda. | RF-010/RF-012; RN-010 |
| Envio direto pelo fornecedor | Modelo em que o fornecedor envia ao destino da contratação. Exige evidências de capacidade e validação contratual/fiscal específica; cotação ou promessa genérica não bastam. | RF-012; RN-014 |
| Operação local/presencial | Instalação, montagem, treinamento, equipe, manutenção, assistência ou outra presença relevante. Classe C pode ter alternativa local comprovada sem deslocamento do operador. | RN-010/RN-013 |
| Prazo de preparação / cronograma de fornecimento | Tempo requerido antes do despacho / sequência completa de cotação, compra, preparo, transporte, agendamento, parcelas e obrigações. O gatilho e o calendário vêm da evidência aplicável. | RF-015/RF-022; RN-002 |
| Classe A | Aparentemente compatível com envio convencional, sem presença/restrição relevante não resolvida, sujeita à confirmação do edital. Não é aprovação automática. | RF-010; RN-011 |
| Classe B | Atendimento potencialmente remoto com restrições relevantes e condições documentadas. | RF-010; RN-011 |
| Classe C | Execução com operação local ou presencial relevante suficientemente identificada. Não determina descarte obrigatório. | RF-010; RN-011/RN-013 |
| Classe D | Informação crítica insuficiente ou conflitante para concluir o conjunto; conserva alertas de presença já conhecidos. | RF-010; RN-011 |

A precedência logística é **D → C → B → A**, conforme evidência e criticidade. Não se calcula a classe a partir de um limiar numérico; ela é independente da elegibilidade e do estado comercial.

### 5.1 `remote_delivery_score`, cobertura e intervalo

**[PROPOSTO] Versão `v0.1-logistica`.** Indicador de prontidão logística documentada, em escala 0–100. Pesos iniciais: envio convencional `0,35`; preparação/prazo `0,25`; destinos/parcelas `0,15`; pós-venda `0,15`; envio direto `0,10`. São hipóteses calibráveis; pesos válidos são finitos, não negativos e somam 1, sem dispensar verificação obrigatória.

Cada dimensão tem `x_i` igual a `0`, `0,5`, `1` ou `null`: incompatibilidade/restrição não resolvida; atendimento condicionado documentado; atendimento suficiente para análise preliminar; desconhecido, respectivamente. Para `K`, conjunto de dimensões conhecidas, e `w_i`, peso da dimensão:

```text
coverage = soma(w_i, i em K)
remote_delivery_score = lower = 100 × soma(w_i × x_i, i em K)
upper = min(100, lower + 100 × (1 − coverage))
```

- **Cobertura (`coverage`):** fração 0–1 da soma dos pesos conhecidos, apresentada também em percentual. `coverage=0,75` equivale a 75%.
- **Limite inferior (`lower`):** o próprio score conservador; desconhecidos não contribuem favoravelmente e continuam registrados como `null`.
- **Limite superior (`upper`):** limite de incerteza do indicador, não pontuação recomendada, probabilidade ou promessa de viabilidade.
- **Não aplicável justificado:** envio direto pode assumir `x=1` no modelo operador + transportadora, com justificativa/evidência. Isso evita penalizar a alternativa legítima e não declara autorização para envio direto.

Não dividir o score pela cobertura. Com preparação/prazo desconhecido e as demais dimensões iguais a 1: score 75, cobertura 75%, intervalo `[75;100]`; lacuna crítica mantém classe D e impede recomendação favorável. Com tudo desconhecido: score 0, cobertura 0%, intervalo `[0;100]`, sem atribuir incompatibilidade a cada dimensão. Fonte normativa: RF-011, documento 04/seção 9 e RN-012. A fórmula global de ranking continua pendente do documento 09.

## 6. Evidência, confiança e decisão

| Termo [PROPOSTO] | Definição operacional e distinção necessária | Referência |
|---|---|---|
| Evidência / proveniência | Conteúdo que sustenta a conclusão / vínculo à origem oficial, comercial ou manual, documento/hash/versão, campo ou página/trecho, data e responsável quando aplicável. | RF-007/RF-027; RN-026; RNF-005 |
| Extração / interpretação / validação | Conteúdo obtido por parser / entendimento humano / verificação objetiva ou especializada registrada. Preservar cada origem sem reescrever o documento bruto. | RF-007 a RF-009; RN-024/RN-026 |
| Confiança | Qualidade da evidência/interpretação, identificada com fundamento e limitações. Não é a cobertura, o score ou uma probabilidade calibrada de vitória/lucro. Rubricas serão detalhadas na Execução 2. | RN-027; RNF-016 |
| Cobertura documental | Extensão de dados/documentos efetivamente obtidos, extraídos ou verificados, com denominador explícito. Não confundir com a cobertura ponderada logística nem com MN-07, que mede decisões positivas com trilha completa. | RF-007; RN-027; 03, MN-07 |
| Atualidade | Validade temporal das informações frente à fonte e à política versionada. Última tentativa e última confirmação são datas distintas. | RF-004/RF-005; RN-003; RNF-018 |
| Desconhecido / não aplicável / não atendido | Informação insuficiente / exigência inaplicável com justificativa / falha conhecida. Desconhecido não vira zero, dispensa ou atendimento. | RN-027 |
| Bloqueio | Falha impeditiva conhecida que impede recomendação favorável. Não é penalização compensável por score alto. | RF-025; RN-029 |
| Pendência obrigatória | Lacuna ou conflito que exige resolução/evidência antes de recomendar favoravelmente; pode coexistir com bloqueios conhecidos. | RF-009/RF-025; RN-023/RN-027 |
| Elegibilidade / resultado analítico | Resultado dos controles obrigatórios: **bloqueada** quando há falha impeditiva; na ausência dela, **pendente** quando há lacuna obrigatória; **viável para priorização** após atendimento suficiente. Preservar todas as razões. | 06, seção 1; RF-025; RN-029 |
| Viabilidade | Avaliação conjunta de produto, capacidade, logística, prazo e finanças na unidade exigida. Lucro estimado isolado não demonstra viabilidade. | RF-017 a RF-025; RN-007 a RN-021 |
| Priorização / ranking global | Ordenação explicável dos candidatos que passaram pelos controles. Bloqueados/pendentes ficam identificados em grupos próprios. Fórmula global e exemplos serão definidos em 09. | RF-026; RN-029 |
| Revisada / revisão humana | Registro de intervenção e decisão do operador. Revisada não substitui nem remove o resultado de elegibilidade; decisão desfavorável também pode estar revisada. | 06, seção 1; RF-029; RN-024/RN-030 |
| Reanálise / invalidação | Nova avaliação fundamentada / perda de atualidade ou substituição auditada de resultado anterior. Não apagam decisão, participação, entrega ou recebimento comprovados. | RF-034; RN-028/RN-030/RN-032 |
| Validação especializada | Manifestação externa sobre matéria jurídica, contábil, fiscal ou técnica com responsável, data, escopo e fundamento. Não é executada por um botão de aprovação do sistema. | RF-009/RF-020; RN-024 |
| Identidade canônica / idempotência | Identidade estável rastreável à origem / reprocessamento igual sem criar duplicação lógica. Chaves exatas dependem do modelo 08 e do contrato 10. | RF-003; RN-028; RNF-008 |

## 7. Finanças e caixa

As definições abaixo [PROPOSTO] delimitam as bases dos indicadores, sem antecipar as equações e os limites que serão fechados em 12. Nenhuma taxa, capital pessoal, meta de margem ou prazo de pagamento foi presumido.

| Termo | Definição operacional | Referência |
|---|---|---|
| Cenário financeiro | Conjunto versionado de cotações, modelo de fornecimento, quantidades, custos, preço hipotético, cronograma, regras e parâmetros. Pode ser salvo incompleto, com pendências; não sustenta positivo enquanto faltar dado obrigatório. | RF-020; RN-020 |
| Custo total / unitário | Custos atribuídos à unidade de julgamento / custo na unidade e quantidade explicitadas. Rateios devem reconciliar o conjunto e não duplicar despesas compartilhadas. | RF-021/RF-023; RN-009/RN-020 |
| Base de incidência | Grandeza sobre a qual uma taxa/custo incide, registrada com origem, unidade e período. Não aplicar todo tributo sobre receita por conveniência. | RF-020/RF-021; RN-017/RN-020 |
| Lucro estimado | Resultado econômico projetado com a receita hipotética e os custos/despesas definidos no cenário. Não é entrada de caixa ou resultado realizado. | RF-021; RN-021 |
| Margem de contribuição / margem líquida estimada | Indicadores de resultado com conjuntos de custos explicitamente definidos; quando percentuais, usam receita como base. A composição e a memória serão formalizadas em 12. | RF-021; RN-021 |
| ROI | Retorno sobre capital empregado, cujo denominador deve estar definido no cenário. Não é margem sobre receita; capital denominador nulo produz “não calculável”. | RF-021; RN-021 |
| Preço mínimo economicamente sustentável | Preço calculado sob as premissas e custos do cenário. Não representa preço aceito pelo órgão, mínimo legal ou garantia de vitória. | RF-021; RN-021 |
| Preço por meta de margem | Hipótese de proposta que atende à meta parametrizada, se existir solução válida considerando custos incidentes sobre receita. | RF-021/RF-024; RN-021 |
| Ponto de equilíbrio | Limite econômico no qual o resultado se equilibra sob as variáveis e bases declaradas. A formulação concreta será fechada em 12; não presume viabilidade logística ou caixa suficiente. | RF-021; RN-021 |
| Fluxo de caixa / desembolso | Sequência datada de entradas e saídas / saída financeira efetiva ou prevista. Faturar não equivale a receber. | RF-022/RF-032; RN-016 |
| Capital de giro necessário / pico de capital | Necessidade de financiar a operação ao longo do cronograma / maior déficit acumulado a cobrir. Picos de itens em datas diferentes não são somados mecanicamente. | RF-022/RF-023; RN-009/RN-017 |
| Capital disponível / limite de exposição | Recurso demonstrado para financiar o cenário / teto configurado de comprometimento. Ausência de informação gera pendência; insuficiência conhecida bloqueia. | RF-022/RF-025; RN-018 |
| Custo financeiro | Custo parametrizado do capital no período de exposição. Taxa, base, calendário e atraso devem permanecer explícitos. | RF-022/RF-024; RN-017 |
| Garantia recuperável / reserva de risco | Montante que pode imobilizar caixa e ser liberado posteriormente / provisão de cenário para incerteza. Não equivalem automaticamente a despesa definitiva ou desembolso realizado. | RN-016/RN-020/RN-021 |
| Sensibilidade | Comparação de cenários ao variar premissas identificadas, como frete, aquisição, preço de disputa e câmbio quando aplicável. Não é previsão de resultado. | RF-024; RN-021 |
| `Decimal` / quantização / rateio | Representação decimal para cálculo monetário / ajuste a uma precisão explícita / distribuição documentada de custo compartilhado. Política deve ter versão, reconciliação e memória. | RF-021; RN-019; RNF-010 |
| Não calculável | Resultado sem base matemática válida, como denominador nulo ou equação inviável, acompanhado do motivo. Não se converte em zero nem em pontuação favorável. | RN-021/RN-027; RNF-010 |

Política inicial preservada: preços/custos unitários e quantidades aceitam até seis casas decimais; intermediários conservam pelo menos oito casas; totais BRL são apresentados e reconciliados em duas casas com `ROUND_HALF_UP`, sem passagem por `float`. Distribuição de resíduos, contexto e limites serão detalhados em 12. Essa política [PROPOSTO] não descreve a aritmética atual do motor SILLO.

## 8. Estados comerciais e fatos externos

Os 16 nomes e códigos lógicos abaixo [PROPOSTO] reproduzem RN-032. Códigos `E01`–`E16` são identificadores documentais; enumeração física e persistência ainda serão definidas em 08. Transições, guardas, exceções e eventos sem avanço estão em [06, RN-032](06-regras-de-negocio.md#rn-032--máquina-de-estados-comerciais) e no [D10](diagramas/D10-estados.md); esta tabela não cria transições.

| Código | Nome fixado | Sentido resumido |
|---|---|---|
| E01 | oportunidade descoberta | Unidade candidata identificada. |
| E02 | em enriquecimento | Coleta/revisão de dados e exigências. |
| E03 | aguardando cotação | Confirmação comercial pendente. |
| E04 | aguardando revisão | Pacote ou pendência requer decisão humana. |
| E05 | pré-aprovada para estudo | Autorização para aprofundar estudo, sem aptidão final. |
| E06 | descartada | Decisão de não prosseguir, com motivo. |
| E07 | apta para decisão humana de participação | Controles suficientes e revisão favorável atual. |
| E08 | participação registrada manualmente | Ato externo comprovado, não enviado pelo sistema. |
| E09 | em disputa | Evento externo de disputa registrado. |
| E10 | resultado conhecido | Resultado disponível, ainda sujeito à confirmação/interpretação pertinente. |
| E11 | vencida | Resultado comercial vencedor; nunca significa prazo expirado. |
| E12 | perdida | Resultado comercial não vencedor. |
| E13 | contrato em execução | Fundamento contratual aplicável e início registrados. |
| E14 | entrega concluída | Parcelas e aceite/requisitos de conclusão comprovados. |
| E15 | recebimento financeiro | Total devido recebido e conciliado. |
| E16 | encerrada | Fechamento normal ou excepcional justificado. |

**Adjudicação, contratação, entrega, aceite, faturamento e recebimento** são fatos com evidências próprias, não sinônimos nem avanços inferidos a partir da vitória. “Vencida” é apenas o nome fixado do estado vencedor; expiração do prazo pertence à situação/prazo e aos bloqueios.

Entrega parcial e recebimento parcial são eventos com saldos, sem declarar conclusão total. Retificação após E08 abre revisão em paralelo e preserva o estágio factual. **Reconciliação excepcional** permite registrar fatos externos comprovados fora da sequência normal, com autor, datas real/de registro, motivo e evidência; não fabrica aprovação E07 nem remove bloqueios. **Encerramento** não apaga obrigações residuais: exige tratamento ou acompanhamento formal, conforme RN-032.

## 9. Limites da versão inicial e continuidade

- Entidades aqui nomeadas são conceitos de domínio. Tabelas, schemas novos, campos, chaves e cardinalidades permanecem para o documento 08; nomes exemplificativos do prompt não foram adotados como contrato físico.
- A fórmula fixada é apenas `v0.1-logistica`. Score global, normalizações, rubricas de confiança/matching e fórmulas financeiras detalhadas serão definidos em 09/11/12, preservando bloqueios e desconhecidos.
- Não há limiar universal de aprovação, regra legal específica, alíquota ou prazo normativo criado neste glossário. Regime e aplicabilidade exigem fonte e análise pertinente, conforme RN-025.
- Na Execução 2, incorporar os nomes efetivamente escolhidos e distinguir decisões aceitas de alternativas. Na Execução 3, consolidar CP/RT e rastreabilidade sem renomear silenciosamente os termos fixados.

## 10. Complemento da Execução 2 — 2026-10-11

O texto da seção 9 registra o limite da versão inicial da Execução 1. Os detalhamentos ali previstos estão agora em 08/09/11/12, com nomes e políticas abaixo [PROPOSTO]. Datas e definições anteriores foram preservadas; CP/RT permanecem para a Execução 3.

| Termo/nome fixado | Definição nesta versão | Fonte normativa |
|---|---|---|
| procurement | Schema do domínio oficial normalizado, com identidades, versões, itens, julgamento, documentos e evidências; mesmo banco atual | 08 |
| sourcing | Schema de fornecedores, versões de produtos/cotações e matching | 08 |
| commercial | Schema transacional de oportunidades, cenários, análises, scores, revisões, eventos, parâmetros e histórico comparável futuro | 08 |
| judgment_units | Unidade de julgamento de tipo item/lote/grupo/global; substitui a necessidade de tabela redundante procurement_lots | 08 |
| judgment_unit_items | Associação que preserva a composição obrigatória da unidade em uma versão | 08 |
| Versão de conteúdo / observação | Hash identifica conteúdo imutável; recebimento posterior e retorno a conteúdo antigo são observações que podem mudar atualidade sem duplicar conteúdo | 08/10 |
| Manifesto de análise | Lista congelada de identidades/versões, parâmetros, fontes, software e instante que sustentam o resultado | 08/09 |
| analysis_invalidations | Eventos de perda de atualidade separados do resultado imutável da análise | 08 |
| Lease / fencing token | Concessão temporária de job e número monotônico que impede confirmação por trabalhador cujo direito expirou | 08/10 |
| Spool documental | Volumes de entrada/saída entre coordenador e parser; não é broker novo nem permite acesso do parser a credenciais | 07/21 |
| Gold público | Projeções reconstruíveis de leitura; registros comerciais e auditoria permanecem em commercial | 08/D11 |
| v0.1-financeiro | Política Decimal com contexto 50 dígitos, quantização e rateio conciliados, custos e caixa separados | 12 |
| G_base / G | Maior déficit acumulado / capital necessário acrescido da reserva de liquidez pertinente | 12 |
| ROI operacional ajustado | Lucro estimado ajustado dividido por G; não anualizado, não retorno alavancado do equity; G=0 não calculável | 12 |
| R_l / Z | Reserva de liquidez retida / reserva econômica de risco; mesma contingência não vira duas saídas | 12 |
| v0.1-ranking | Prioridade para candidatos viáveis: 0,30F+0,20L+0,20T+0,15C+0,15E menos penalidades versionadas | 09 |
| F / L / T / C / E | Scores financeiro / logístico / técnico / confiabilidade / exposição a capital e prazo, todos 0–100 | 09 |
| Score NULL | Resultado não publicado para bloqueio, pendência ou base não calculável; não equivale a score zero de candidato viável | 09 |
| Fuso pendente | Data/hora da origem sem interpretação de offset confirmada; texto preservado, sem UTC presumido nem aptidão fundada em prazo ambíguo | 10 |

Inventário físico normativo, incluindo tabelas auxiliares: 08, seções 3–5. Estados E01–E16 mantêm nomes/transições de RN-032/D10. Pesos logísticos v0.1 permanecem 0,35/0,25/0,15/0,15/0,10. Não há limiar universal de aprovação nem conversão de classe A em autorização automática.

## 11. Consolidação da Execução 3 — 2026-10-12

As menções anteriores a decisões futuras preservam o histórico da Execução 1; seus detalhamentos foram concluídos em 07–17/21. PRD/CP e TRD/RT agora estão em 00-prd e 00-trd. Últimos IDs: CP-10, RT-016, ADR-017, TST-055, EP-10, HU-034 e RSK-18. RF-034, RNF-021, RN-032, MN-10 e E16 permanecem sem renumeração.

| Termo | Definição final | Fonte |
|---|---|---|
| CP | Capacidade de produto, CP-01–10; conjunto de RF com prioridade/fase | 00-prd/17 |
| RT | Requisito técnico com origem e verificação, RT-001–016 | 00-trd/17 |
| TST | Caso de teste planejado, não evidência de execução | 14 |
| EP / HU / TEC | Épico, história e habilitador técnico do backlog | 15 |
| Gate G0–G7 | Condição verificável para avançar migração/piloto | 16 |
| RPO / RTO | Perda temporal máxima pretendida / tempo pretendido de recuperação, conforme ponto inicial definido em RNF-014 | 05/16 |
| Retenção operacional | Política proposta de conservação/cópias, distinta de prazo legal do caso | 13/16/ADR-016 |
| Score persistido | Decimal exato em NUMERIC sem escala fixa; quatro casas somente na apresentação | 08/ADR-017 |

Fórmulas v0.1-logistica/financeiro/ranking, schemas e estados continuam fixados em 08/09/11/12 e RN-032. O mesmo símbolo F significa custo fixo em 12 e componente financeiro em 09; a unidade/contexto deve sempre acompanhar o símbolo.

## 12. Descoberta de fornecedores — revisado em R1

As definições desta seção atualizam o escopo histórico antes tratado integralmente como fase 6. [22](22-inteligencia-de-mercado-fornecedores.md), RN-033–037 e ADR-018/019 são normativos. Demais termos/fórmulas permanecem.

| Termo | Definição [PROPOSTO] |
|---|---|
| Histórico de contratação | Observações públicas com etapa explicitada; não sinônimo de entrega/execução comprovada |
| Resultado homologado | Resultado oficial identificado por item/seq/situação/data/versão; pode ser cancelado ou retificado |
| Preço registrado | Condição de ata de registro; não contratação obrigatória nem receita |
| Preço contratado | Valor de instrumento contratual no grão comprovado; global não é unitário nem custo do operador |
| Referência histórica comparável | Observação com natureza e atributos/unidade/contexto suficientes para comparação revisada; não substitui cotação |
| Lance | Valor proposto em disputa, separado de homologado, registrado, contratado e cotação comercial |
| Estabelecimento | Unidade identificada pelo CNPJ completo, quando aplicável; identidade interna independente do documento |
| Raiz do CNPJ | Agrupamento analítico de estabelecimentos; não autoriza fusão de condições nem prova grupo econômico mais amplo |
| CNPJ numérico/alfanumérico | Formatos coexistentes tratados como texto, preservando letras/zeros/original e validação oficial versionada; cadastro válido não comprova capacidade |
| Fornecedor candidato | Estabelecimento ou identidade provisória associado a evidência que justifica investigação; não fornecedor aprovado |
| Disposição de venda | Resposta comercial sobre vender ao operador sob condições/escopo; desconhecida, aceita sob condições ou recusada |
| Cotação vigente | Versão de proposta comercial com validade verificada e escopo explícito de estabelecimento/produto/quantidade/condições |
| Disponibilidade informada | Quantidade/prazo declarados com fonte/data; ainda não confirmação suficiente nem reserva |
| Disponibilidade confirmada | Evidência atual suficiente para escopo definido; não se propaga para outro produto/filial e não equivale automaticamente a reserva |
| Correspondência entre fontes | Relação evidenciada de identidades/eventos equivalentes; conserva fontes e resolução por atributo |
| Evento relacionado | Etapa distinta vinculada a outra, como resultado e contrato; vinculação não autoriza somar valores como negócios independentes |
| Possível duplicidade | Correspondência ainda não comprovada; preservada para revisão, fora do agregado padrão, com exclusão explícita |
| Papel inferido | Hipótese fabricante/distribuidor/revendedor etc. por método/evidência limitados; não fato comercial nem critério universal de inaptidão |
| Cobertura documental/amostral | Contrato descreve recurso / resposta observada em execução; são dimensões independentes, nenhuma prova implementação do Radar |
| Marco 2-H | Incremento Should de comparáveis/candidatos após identidade/ingestão, com contato/cotação na fase 3; não renumera fases |

Estado de contato não substitui E01–E16. Frequência de resultados vencedores não é taxa de vitória. UF de órgão não é UF de entrega. “Experiência” é descrição de histórico observado com etapa, nunca capacidade atual presumida.

## Histórico de revisões

2026-10-12 — 0.2-R1: vocabulário de descoberta, identidade, evidências e escopo antecipado; definições anteriores preservadas como histórico.

## 13. Consolidação R1-E2 — revisado em R1

Os detalhes antes pendentes da E2 estão em 08 §9, 16 §7 e 23; preservam os termos da seção 12. `public_events` guarda etapas versionadas; `event_links` distingue equivalência/relacionamento; `historical_comparisons` revisa item×observação; `supplier_candidates` associa estabelecimento ao item sem segunda identidade; `candidate_interactions` registra manualmente as dimensões de relacionamento. `historical_prices` conserva preço/natureza, cotação permanece autoridade em supplier_quotes. `registration_stage` é maturidade cadastral, não aprovação de fornecimento.

**Coorte de pesquisa:** todos os itens admitidos segundo regra prévia do ciclo, inclusive insucessos. **Episódio de contato:** estabelecimento+item/escopo no ciclo, tentativas contadas separadamente. **Censura:** observação ainda sem cotação no corte, com duração explícita; não é resposta negativa nem caso descartável da métrica. Definições operacionais em 23.

Últimos IDs atuais: RF-038, RNF-021, RN-037, ADR-020, CP-10, RT-019, UC-09, TST-071, MN-18, EP-10, HU-038, TEC-06, RSK-23, D15. Referências a fases/IDs anteriores nas consolidações datadas permanecem históricas; marco 2-H antecipa somente básico Should. Estados RN-032/D10 e fórmulas intactos.

2026-10-13 — 0.3-R1-E2, Histórico de revisões: vocabulário físico e métrico consolidado, IDs e fontes normativas atualizados.

