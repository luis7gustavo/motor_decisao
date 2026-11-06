# Radar Licita — requisitos funcionais

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-10 (America/Sao_Paulo).  
Revisão: 0.2-R1, 2026-10-12 (America/Sao_Paulo). Revisado em R1-E1 nas seções indicadas; propagação documental E2 em 00-prd/15/17.
Commit base do código: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: [PROPOSTO] — especificação para revisão do operador; nenhuma funcionalidade implementada nesta missão.

## Escopo e convenções

Este documento traduz a [visão de produto](01-visao-produto.md) e o [DAN](03-analise-negocio-dan.md) em comportamentos verificáveis para o Radar Licita. A base técnica foi inspecionada no [diagnóstico AS-IS](02-diagnostico-as-is.md). Os 38 requisitos abaixo pertencem ao novo domínio de compras públicas; capacidade semelhante existente no SILLO não significa requisito licitatório já atendido. R1 mantém 32 Must e acrescenta quatro Should condicionados; 00-prd/15/17 receberam propagação na R1-E2.

Os campos de cada requisito são: identificador, nome, descrição, ator interessado, entradas, regras de processamento, saídas, dependências, prioridade MoSCoW, fase e critérios de aceitação. Acrescentam-se referências às regras RN do [documento 06](06-regras-de-negocio.md). Os critérios são propostas de verificação futura; não representam testes executados nesta missão.

- **Must:** comportamento necessário para considerar concluída a fase indicada. O MVP compreende os Must das fases 1 a 5, entregues incrementalmente; a primeira fase isolada não equivale ao MVP completo.
- **Should:** incremento relevante que pode ser postergado sem omitir controles obrigatórios de decisão.
- **Fases:** 1, fundação pública; 2, documentos e logística; 3, fornecedores e matching; 4, finanças e ranking; 5, operação comercial; 6, inteligência avançada. A fase 0 é esta análise documental. O roadmap definitivo será consolidado na Execução 3.
- Revisão humana é transversal às fases 2–4 (RF-009, RF-018, RF-019 e verificação de premissas); RF-029 consolida a fila comercial na fase 5, sem postergar essa validação.
- A operação inicial é individual. “Revisor”, “analista” e “responsável pelos parâmetros” representam responsabilidades que podem caber ao mesmo operador, sem exigir multiusuário complexo na V1.
- Fornecedor e assessor fornecem informações/validações registradas pelo operador; não se presume acesso deles ao sistema.
- Nenhum RF autoriza envio automático de lance ou proposta, consulta a área privada sem autorização, compra, contratação, mensagem a fornecedor ou parecer jurídico, contábil ou fiscal.
- Referências aos documentos 08, 09, 10, 11 e 12 indicam detalhamento previsto na Execução 2. Não representam artefatos já concluídos nem contratos de API inventados.

## Índice e cobertura

| ID | Nome | Prioridade | Fase |
|---|---|---|---|
| RF-001 | Consultar contratações públicas nacionais | Must | 1 |
| RF-002 | Pesquisar, filtrar e identificar abertura | Must | 1 |
| RF-003 | Normalizar identidade e impedir duplicação | Must | 1 |
| RF-004 | Sincronizar alterações e invalidar análises afetadas | Must | 1 |
| RF-005 | Manter consulta local e sinalizar indisponibilidade | Must | 1 |
| RF-006 | Obter e versionar documentos oficiais | Must | 2 |
| RF-007 | Extrair texto e localizar evidências | Must | 2 |
| RF-008 | Estruturar exigências do edital | Must | 2 |
| RF-009 | Revisar ambiguidade e conflito documental | Must | 2 |
| RF-010 | Classificar viabilidade de execução remota | Must | 2 |
| RF-011 | Calcular score de prontidão logística explicável | Must | 2 |
| RF-012 | Comparar modelos de fornecimento e envio direto | Must | 2 |
| RF-013 | Cadastrar fornecedor e condições comerciais | Must | 3 |
| RF-014 | Importar catálogo CSV e manter atributos de produto | Must | 3 |
| RF-015 | Registrar cotação, validade e disponibilidade | Must | 3 |
| RF-016 | Consultar histórico e alternativas de cotação | Must | 3 |
| RF-017 | Comparar atributos técnicos e fornecimento | Must | 3 |
| RF-018 | Classificar compatibilidade técnica | Must | 3 |
| RF-019 | Revisar e versionar o matching | Must | 3 |
| RF-020 | Cadastrar cenários financeiros completos | Must | 4 |
| RF-021 | Calcular custos, preços, margem e retorno com Decimal | Must | 4 |
| RF-022 | Simular desembolsos e capital de giro | Must | 4 |
| RF-023 | Consolidar viabilidade no nível de julgamento | Must | 4 |
| RF-024 | Comparar sensibilidade e metas de margem | Should | 4 |
| RF-025 | Aplicar bloqueios e pendências antes de recomendar | Must | 4 |
| RF-026 | Produzir ranking configurável e transparente | Must | 4 |
| RF-027 | Explicar e reconstruir cada análise | Must | 4 |
| RF-028 | Contextualizar preços históricos oficiais | Should | 2-H básico; 6 ampliado |
| RF-029 | Operar fila de revisão comercial | Must | 5 |
| RF-030 | Controlar estados e transições comerciais | Must | 5 |
| RF-031 | Registrar participação, disputa e resultado manualmente | Must | 5 |
| RF-032 | Acompanhar contratação, execução e recebimento | Must | 5 |
| RF-033 | Visualizar carteira e indicadores comerciais | Must | 5 |
| RF-034 | Preservar decisões e reabrir revisão | Must | 5 |
| RF-035 | Ingerir resultados e vínculos históricos delimitados | Should | 2-H |
| RF-036 | Pesquisar e revisar resultados comparáveis | Should | 2-H |
| RF-037 | Descobrir candidatos pelo histórico observado | Should | 2-H |
| RF-038 | Registrar relacionamento e vincular cotação de candidato | Should | 3 |

## 1. Coleta de oportunidades — domínio 7.1

[PROPOSTO] Novo domínio público. [CONFIRMADO] Há contratos de coleta e registro de fontes comerciais reutilizáveis, porém sem adapter PNCP (`pipelines/collection/registry.py:17`, `pipelines/collection/collectors/factory.py:7`).

### RF-001 — Consultar contratações públicas nacionais

Status: [PROPOSTO].

- **Identificador:** RF-001.
- **Nome:** Consultar contratações públicas nacionais.
- **Descrição:** Descobrir contratações publicadas nas fontes oficiais para compor a base local do Radar Licita.
- **Ator interessado:** Operador; responsável pela coleta.
- **Entradas:** Período de busca, fonte oficial e parâmetros de consulta validados no contrato da fonte.
- **Regras de processamento:** Consultar a API pública aplicável, percorrer páginas e registrar escopo, horário, origem, falhas e contagens. A disponibilidade nacional da fonte não equivale a cobertura garantida de todas as contratações do país. Não usar serviços de manutenção para consulta nem inventar campos ausentes.
- **Saídas:** Registros brutos preservados e contratações disponíveis para normalização.
- **Dependências:** Contrato oficial a detalhar em 10, persistência proposta em 08; nenhuma dependência de marketplaces.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 1.
- **Regras de negócio associadas:** RN-026, RN-027.
- **Critérios de aceitação verificáveis:** Dada uma fixture oficial sintética com duas páginas, a coleta processa ambas e preserva fonte e payload de cada registro. Se a segunda página falhar, a execução fica parcial, com ponto de retomada e sem afirmar completude.

### RF-002 — Pesquisar, filtrar e identificar abertura

Status: [PROPOSTO].

- **Identificador:** RF-002.
- **Nome:** Pesquisar, filtrar e identificar abertura.
- **Descrição:** Permitir que o operador encontre oportunidades pertinentes e identifique a situação de recebimento de propostas.
- **Ator interessado:** Operador.
- **Entradas:** Contratações normalizadas; UF, município, órgão, modalidade, categoria, período e situação de recebimento.
- **Regras de processamento:** Aplicar filtros combináveis e indicar fonte/data da situação. Diferenciar modalidade, procedimento e prazo de recebimento; informação ausente deve permanecer desconhecida. Registrar particularidades de fluxos que não suportem a mesma jornada. Abertura operacional não prova habilitação do fornecedor.
- **Saídas:** Lista pesquisável com situação aberta, encerrada, futura ou desconhecida e condições específicas.
- **Dependências:** RF-001, RF-003, RF-004.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 1.
- **Regras de negócio associadas:** RN-001, RN-003, RN-022 a RN-025.
- **Critérios de aceitação verificáveis:** Uma consulta UF+modalidade retorna apenas os registros que satisfazem os dois filtros. Um registro sem prazo não aparece como aberto confirmado; um prazo vencido deixa de permitir recomendação positiva.

### RF-003 — Normalizar identidade e impedir duplicação

**Extensão — revisado em R1:** identidade de estabelecimento preserva CNPJ completo textual, original e canônico numérico/alfanumérico, com DV oficial e UUID independente; raiz só agrupa (RN-033). Deduplicação de evento por identidade/grão comprovados mantém fonte por atributo; etapas relacionadas não são duplicatas (RN-034). Critério adicional: mesmo resultado nas duas fontes conta uma vez e conserva proveniências; matriz/filial continuam separadas. Aplica-se aos dados de RF-035 quando habilitado, sem converter esse Should em dependência da fase 1.

Status: [PROPOSTO].

- **Identificador:** RF-003.
- **Nome:** Normalizar identidade e impedir duplicação.
- **Descrição:** Manter entidades distintas e rastreáveis, mesmo em reprocessamentos, retificações e cruzamentos de fontes.
- **Ator interessado:** Responsável pela coleta; operador.
- **Entradas:** Identificadores originais de contratação, item, lote/grupo e órgão; origem e payload.
- **Regras de processamento:** Definir chave canônica rastreável ao identificador oficial, mantendo valores originais e vínculos entre entidades. Reprocessamento idêntico não cria nova oportunidade comercial; sem identificador suficiente, manter pendência de resolução sem unir registros por título parecido.
- **Saídas:** Identidades estáveis, mapa de origem e resultado de deduplicação.
- **Dependências:** RF-001; modelo de identidade a fechar em 08 e contrato 10.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 1.
- **Regras de negócio associadas:** RN-026, RN-028.
- **Critérios de aceitação verificáveis:** Reprocessar o mesmo identificador oficial e conteúdo mantém uma contratação e o número de itens esperado. Duas contratações com títulos iguais e IDs oficiais distintos permanecem separadas; item sem identidade confiável fica sinalizado.

### RF-004 — Sincronizar alterações e invalidar análises afetadas

**Extensão — revisado em R1:** incluir resultados/atas/contratos de RF-035 quando habilitado; versão/situação/consulta distintas. Cancelamento retira a referência atual, preserva original e invalida dependentes; falha não infere cancelamento. Mesmo payload consultado novamente não cria outra versão de negócio (RN-028).

Status: [PROPOSTO].

- **Identificador:** RF-004.
- **Nome:** Sincronizar alterações e invalidar análises afetadas.
- **Descrição:** Reconhecer mudanças oficiais e tornar visível seu efeito nas análises e decisões já registradas.
- **Ator interessado:** Operador; responsável pela coleta.
- **Entradas:** Novas publicações, atualizações, retificações, prazos e situações da fonte; versão armazenada.
- **Regras de processamento:** Sincronizar incrementalmente, comparar versão/hash/campos relevantes e preservar versões anteriores. Distinguir cancelamento, encerramento, adiamento e retificação quando a fonte os informar. Mudança de prazo, item, documento ou regra de julgamento invalida a atualidade de conclusões dependentes; não apaga participação já registrada.
- **Saídas:** Versão nova, diferenças, eventos de atualização e análises pendentes de revalidação.
- **Dependências:** RF-001, RF-003; RF-027 e RF-034 quando implementados.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 1.
- **Regras de negócio associadas:** RN-003, RN-028.
- **Critérios de aceitação verificáveis:** Ao receber retificação com quantidade e prazo alterados, ambas as versões continuam consultáveis e análises vinculadas à anterior são marcadas desatualizadas. Payload repetido sem mudança não cria nova versão comercial.

### RF-005 — Manter consulta local e sinalizar indisponibilidade

**Extensão — revisado em R1:** falha da pesquisa histórica não apaga candidato/cadastro/cotação já validada nem bloqueia análise com insumos suficientes. Guardas atuais de cotação/prazo continuam aplicáveis; ausência de histórico não é pendência obrigatória nova (RN-036).

Status: [PROPOSTO].

- **Identificador:** RF-005.
- **Nome:** Manter consulta local e sinalizar indisponibilidade.
- **Descrição:** Preservar utilidade operacional da base local sem ocultar falhas ou envelhecimento da informação.
- **Ator interessado:** Operador; responsável pela coleta.
- **Entradas:** Último conjunto persistido, resultados das tentativas, horário da última confirmação e política de atualidade.
- **Regras de processamento:** Continuar mostrando dados locais durante falha da fonte. Exibir diferença entre última tentativa e última confirmação; nunca transformar falha em encerramento nem preservar sinal positivo como se atualizado. Permitir retomar sincronização sem eliminar registros históricos.
- **Saídas:** Consulta local, indicador de atualização, aviso de falha e pendência de reconfirmação.
- **Dependências:** RF-001 a RF-004; metas operacionais do documento 05.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 1.
- **Regras de negócio associadas:** RN-026 a RN-029.
- **Critérios de aceitação verificáveis:** Simulada indisponibilidade após coleta bem-sucedida, as oportunidades continuam consultáveis com indicação de desatualização. Após retomada, registros reconciliados ganham nova confirmação e os demais não desaparecem silenciosamente.

## 2. Documentos e editais — domínio 7.2

[PROPOSTO] Novo fluxo documental de contratação. [CONFIRMADO] A interface atual mostra evidências comerciais; não foi localizado pipeline de editais com trechos/páginas (`app/services/review_service.py:236`).

### RF-006 — Obter e versionar documentos oficiais

Status: [PROPOSTO].

- **Identificador:** RF-006.
- **Nome:** Obter e versionar documentos oficiais.
- **Descrição:** Conservar os documentos que fundamentam a análise com versão e procedência verificáveis.
- **Ator interessado:** Operador; responsável pela coleta.
- **Entradas:** Links oficiais associados à contratação, metadados da origem e política de download.
- **Regras de processamento:** Localizar e baixar arquivos autorizados; verificar destino, tamanho e tipo antes de processar; preservar hash, URL, nome, data, versão e associação à contratação. Conteúdo deve ser tratado como não confiável. Revisões novas não sobrescrevem o arquivo oficial anterior.
- **Saídas:** Documento preservado, metadados e estado baixado, indisponível ou em quarentena.
- **Dependências:** RF-001, RF-003, RF-004; segurança documental do documento 05.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 2.
- **Regras de negócio associadas:** RN-026, RN-028.
- **Critérios de aceitação verificáveis:** Dois downloads do mesmo conteúdo não duplicam a versão. Novo conteúdo no mesmo endereço cria versão; arquivo rejeitado por política não é aberto/executado e a análise fica pendente com motivo.

### RF-007 — Extrair texto e localizar evidências

Status: [PROPOSTO].

- **Identificador:** RF-007.
- **Nome:** Extrair texto e localizar evidências.
- **Descrição:** Transformar PDFs textuais em conteúdo rastreável, sem atribuir certeza a extrações incompletas.
- **Ator interessado:** Operador; revisor documental.
- **Entradas:** PDF textual validado, versão do extrator e parâmetros.
- **Regras de processamento:** Extrair texto por página, preservando trecho, referência ao documento/hash e coordenada ou posição quando disponível. Registrar falhas e cobertura. PDF sem camada textual não produz conclusão baseada em texto inexistente; OCR e LLM são extensões posteriores, fora do caminho obrigatório inicial.
- **Saídas:** Extração versionada, páginas/trechos consultáveis e status de sucesso, parcial ou revisão necessária.
- **Dependências:** RF-006.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 2.
- **Regras de negócio associadas:** RN-026, RN-027.
- **Critérios de aceitação verificáveis:** PDF sintético com condição de entrega na página 3 gera evidência vinculada à página 3 e versão correta. PDF escaneado sem texto é identificado como insuficiente e segue à revisão, sem texto inventado.

### RF-008 — Estruturar exigências do edital

Status: [PROPOSTO].

- **Identificador:** RF-008.
- **Nome:** Estruturar exigências do edital.
- **Descrição:** Converter texto e campos oficiais em requisitos de fornecimento e execução que possam ser avaliados.
- **Ator interessado:** Operador; revisor documental; assessor quando necessário.
- **Entradas:** Texto extraído, dados oficiais estruturados e complementos manuais com evidência.
- **Regras de processamento:** Identificar especificação de item, unidade/quantidade/embalagem, entrega/execução, garantia, habilitação, julgamento, prazos e endereços. Para habilitação, discriminar regularidade, qualificação, capacidade financeira, cadastros, certificações, garantias, subcontratação e condições ME/EPP quando pertinentes. Separar campo extraído, interpretação humana e verificação objetiva; desconhecido permanece explícito.
- **Saídas:** Checklist de exigências, evidências, campos pendentes e divergências.
- **Dependências:** RF-007; regras do documento 06 e validação especializada quando exigida.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 2.
- **Regras de negócio associadas:** RN-002, RN-007, RN-010, RN-022 a RN-027.
- **Critérios de aceitação verificáveis:** Fixture com garantia de 24 meses e entrega parcelada em dois endereços produz os três requisitos com suas origens. Ausência de regra sobre instalação gera pendência, não conclusão de dispensa; não há declaração automática de conformidade jurídica.

### RF-009 — Revisar ambiguidade e conflito documental

Status: [PROPOSTO].

- **Identificador:** RF-009.
- **Nome:** Revisar ambiguidade e conflito documental.
- **Descrição:** Permitir resolver dúvidas e divergências documentais por revisão humana fundamentada.
- **Ator interessado:** Operador; revisor documental.
- **Entradas:** Campos extraídos, páginas, documentos/retificações e conflitos detectados.
- **Regras de processamento:** Abrir pendências para texto ambíguo, tabela ilegível, cláusulas conflitantes ou requisito não conclusivo. Registrar interpretação, responsável, justificativa e evidência; preservar extração original. Pendências críticas impedem conclusão positiva até resolução suficiente.
- **Saídas:** Pendência, decisão de revisão ou solicitação de esclarecimento registrada manualmente.
- **Dependências:** RF-006 a RF-008.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 2.
- **Regras de negócio associadas:** RN-023, RN-024, RN-027, RN-030.
- **Critérios de aceitação verificáveis:** Dois documentos com prazos diferentes produzem conflito visível e bloqueiam conclusão até o operador registrar a evidência que resolve a precedência. Corrigir uma extração preserva o valor anterior e o autor da revisão.

## 3. Classificação logística — domínio 7.3

[PROPOSTO] Classes A/B/C/D e score logístico são novos. Taxas genéricas de frete do SILLO não demonstram execução remota de contrato público.

### RF-010 — Classificar viabilidade de execução remota

Status: [PROPOSTO].

- **Identificador:** RF-010.
- **Nome:** Classificar viabilidade de execução remota.
- **Descrição:** Distinguir oportunidades potencialmente atendíveis a distância de contratos com exigência presencial ou informação insuficiente.
- **Ator interessado:** Operador; revisor logístico.
- **Entradas:** Exigências documentais de entrega, instalação, montagem, treinamento, equipe/assistência local, manutenção, parcelamento, endereços e pós-venda.
- **Regras de processamento:** Classificar A: aparentemente compatível com envio convencional, sujeita à confirmação; B: potencialmente remota com restrições; C: operação local/presencial relevante; D: informação crítica insuficiente ou conflitante. Aplicar D primeiro quando falta evidência crítica para avaliar o conjunto; depois C se presença relevante confirmada, B se restrições remotas resolúveis e A com evidência suficiente sem presença/restrições relevantes. Presença já identificada continua destacada mesmo quando outra lacuna crítica determina D. Participação eletrônica não comprova entrega remota.
- **Saídas:** Classe com justificativa, condições, evidências e pendências.
- **Dependências:** RF-008, RF-009; classificação detalhada em 06 e 11.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 2.
- **Regras de negócio associadas:** RN-010, RN-011, RN-013, RN-014.
- **Critérios de aceitação verificáveis:** Caso sem instalação e com transporte/recebimento documentados pode resultar A, com ressalvas preservadas. Instalação presencial obrigatória resulta C; local de entrega desconhecido resulta D. Nenhuma classe libera participação automaticamente.

### RF-011 — Calcular score de prontidão logística explicável

Status: [PROPOSTO].

- **Identificador:** RF-011.
- **Nome:** Calcular score de prontidão logística explicável.
- **Descrição:** Expressar prontidão logística por dimensões verificáveis e evitar que uma nota esconda informação ausente.
- **Ator interessado:** Operador; responsável pelos parâmetros.
- **Entradas:** Critérios logísticos, evidências, avaliação por dimensão e pesos versionados.
- **Regras de processamento:** Calcular remote_delivery_score conforme proposta v0.1-logistica deste documento; manter cobertura e intervalo separados e não substituir classe ou bloqueios pelo score. Permitir configurar pesos com validação e registrar versão. Um campo desconhecido não pode gerar evidência favorável; não aplicabilidade exige justificativa.
- **Saídas:** Score 0–100, contribuição por critério, cobertura, versão e motivos.
- **Dependências:** RF-010; detalhamento/calibração em 09 e 11.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 2.
- **Regras de negócio associadas:** RN-012, RN-013, RN-027.
- **Critérios de aceitação verificáveis:** Com todas as dimensões evidenciadas e avaliação 1, score=100 e cobertura=100% (`coverage=1`). Se preparação/prazo (peso 0,25) é desconhecida e as demais valem 1, limite inferior=75, cobertura=75% (`coverage=0,75`) e limite superior=100; a dimensão permanece desconhecida e classe/elegibilidade não são automaticamente favoráveis.

### RF-012 — Comparar modelos de fornecimento e envio direto

Status: [PROPOSTO].

- **Identificador:** RF-012.
- **Nome:** Comparar modelos de fornecimento e envio direto.
- **Descrição:** Comparar formas de atendimento compatíveis com as obrigações, incluindo cenários com estoque e entrega por terceiros.
- **Ator interessado:** Operador; fornecedor; assessor quando necessário.
- **Entradas:** Condições de estoque próprio, aquisição posterior, transporte, fornecedor local/regional e envio direto; evidências contratuais, fiscais e operacionais.
- **Regras de processamento:** Registrar modelo considerado, responsável por compra, transporte, faturamento, garantia e recebimento. Envio direto exige confirmação específica das condições aplicáveis e do fornecedor; existência de cotação não confirma capacidade. Simular alternativa local quando necessário, mantendo custos e obrigações.
- **Saídas:** Alternativas operacionais, responsáveis, permissões confirmadas e pendências.
- **Dependências:** RF-008 a RF-010; RF-013 a RF-016 na fase 3; RF-020 na fase 4.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 2.
- **Regras de negócio associadas:** RN-010, RN-013, RN-014.
- **Critérios de aceitação verificáveis:** Oferta de envio direto sem evidência contratual/fiscal suficiente permanece pendente. Duas alternativas podem coexistir sem misturar custos ou obrigações. Nesta fase aceita cadastro manual; integração com catálogo entra após RF-013.

## 4. Fornecedores e cotações — domínio 7.4

[CONFIRMADO] Há catálogos brutos/normalizados e importadores comerciais. [PARCIAL] Essa base não cobre cotação pública negociada, validade e confirmação de fornecimento; adaptação deve preservar IDs e histórico.

### RF-013 — Cadastrar fornecedor e condições comerciais

**Revisado em R1:** estabelecimento completo, compatibilidade alfanumérica e reutilização de cadastro seguem RN-033/037; candidato identificado pode anteceder cotação. Não criar fornecedor duplicado ao converter candidato; conflito fiscal exige revisão. RF-038 é consumidor opcional deste cadastro, nunca pré-requisito dele.

Status: [PROPOSTO].

- **Identificador:** RF-013.
- **Nome:** Cadastrar fornecedor e condições comerciais.
- **Descrição:** Manter dados mínimos de fornecedores e condições aplicáveis sem presumir capacidade de execução.
- **Ator interessado:** Operador; fornecedor como fonte das informações.
- **Entradas:** Identificação empresarial, região atendida, pedido mínimo, prazo de preparação, condições de pagamento, entrega direta, garantia, devolução e contatos necessários.
- **Regras de processamento:** Validar campos, registrar procedência/data e permitir atualização versionada. Separar condições gerais de condições específicas da cotação; desconhecido não vira padrão favorável. Cadastro não significa homologação ou capacidade de atendimento confirmada.
- **Saídas:** Fornecedor e condições com histórico e pendências.
- **Dependências:** RF-003 para identidade; modelo 08 e política de acesso/retenção. RF-012 consome condições comerciais depois de cadastradas, não é pré-requisito do cadastro básico (revisado em R1 para remover ciclo RF-012↔RF-013).
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 3.
- **Regras de negócio associadas:** RN-006, RN-014, RN-026.
- **Critérios de aceitação verificáveis:** Fornecedor sem informação de entrega direta mantém esse campo desconhecido. Alteração de pedido mínimo preserva condição anterior e não reescreve análise financeira passada.

### RF-014 — Importar catálogo CSV e manter atributos de produto

Status: [PROPOSTO].

- **Identificador:** RF-014.
- **Nome:** Importar catálogo CSV e manter atributos de produto.
- **Descrição:** Reaproveitar catálogos comerciais e permitir importação controlada de ofertas e atributos.
- **Ator interessado:** Operador; responsável pelo catálogo.
- **Entradas:** CSV com mapa de colunas, fornecedor, SKU, descrição, atributos, unidade, embalagem, preço e quantidade informada.
- **Regras de processamento:** Pré-validar estrutura, tipos, unidades, duplicidades e identificadores; informar rejeições por linha. Associar SKU ao fornecedor e preservar payload/arquivo e data. Normalizar sem assumir equivalência entre unidades/embalagens e sem tratar preço de catálogo como cotação firme.
- **Saídas:** Produtos versionados, resumo de importação e linhas rejeitadas com motivo.
- **Dependências:** RF-013; preservação dos catálogos SILLO existentes.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 3.
- **Regras de negócio associadas:** RN-015, RN-026.
- **Critérios de aceitação verificáveis:** CSV de 10 linhas com duas inválidas gera oito aceitas e duas rejeitadas identificáveis, sem sucesso integral indevido. Mesmo SKU de fornecedores distintos não é fundido; catálogo repetido não duplica produtos.

### RF-015 — Registrar cotação, validade e disponibilidade

**Extensão — revisado em R1:** cotação vincula estabelecimento, produto/versão, quantidade, prazo e condições; não confirma outra filial/produto nem reserva estoque. RF-038 registra sua associação ao candidato, sem duplicar a autoridade da cotação de RF-015. Critério adicional: cotação E1 para 10 unidades não confirma E2 ou 20 unidades.

Status: [PROPOSTO].

- **Identificador:** RF-015.
- **Nome:** Registrar cotação, validade e disponibilidade.
- **Descrição:** Registrar propostas comerciais datadas com quantidade e disponibilidade comprováveis.
- **Ator interessado:** Operador; fornecedor.
- **Entradas:** Produto, preço unitário, moeda, quantidade, validade, pedido mínimo, preparação, pagamento, garantia/devolução e evidência de estoque/disponibilidade.
- **Regras de processamento:** Manter preço ofertado separado do preço de catálogo; vincular cotação a fornecedor e versão de produto. Registrar horário, validade e condições. Estoque informado, reservado e confirmado são estados distintos; preço/estoque vencidos exigem confirmação antes de conclusão favorável.
- **Saídas:** Cotação rastreável com quantidade atendível, validade e pendências.
- **Dependências:** RF-013, RF-014.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 3.
- **Regras de negócio associadas:** RN-006, RN-014, RN-026.
- **Critérios de aceitação verificáveis:** Cotação vencida continua no histórico, mas não sustenta decisão positiva sem reconfirmação. Quantidade ofertada de 10 para demanda de 20 registra insuficiência; uma página de catálogo não vira reserva de estoque.

### RF-016 — Consultar histórico e alternativas de cotação

Status: [PROPOSTO].

- **Identificador:** RF-016.
- **Nome:** Consultar histórico e alternativas de cotação.
- **Descrição:** Preservar evolução dos preços e condições e apoiar escolha entre alternativas comparáveis.
- **Ator interessado:** Operador.
- **Entradas:** Cotações e versões de fornecedor/produto; item de contratação; período.
- **Regras de processamento:** Comparar condições em bases explicitamente equivalentes, mantendo fornecedor, moeda, unidade, embalagem, validade e quantidade. Revisão de preço gera versão; seleção comercial não apaga alternativas. Conversões exigem fator e fonte.
- **Saídas:** Histórico de cotações e alternativas comparáveis ou sinalizadas como não comparáveis.
- **Dependências:** RF-014, RF-015, RF-017 quando disponível.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 3.
- **Regras de negócio associadas:** RN-005, RN-006, RN-015.
- **Critérios de aceitação verificáveis:** Revisar preço de uma cotação cria versão e permite reconstruir cenário anterior. Caixa de 10 e unidade avulsa só são comparadas após fator documentado, nunca pelo número bruto do preço.

## 5. Matching técnico — domínio 7.5

[CONFIRMADO] SILLO compara títulos e alguns atributos de produtos (`app/services/review_service.py:109–160`). [PARCIAL] Isso não confirma atendimento de especificação licitatória; os níveis abaixo são novos.

### RF-017 — Comparar atributos técnicos e fornecimento

Status: [PROPOSTO].

- **Identificador:** RF-017.
- **Nome:** Comparar atributos técnicos e fornecimento.
- **Descrição:** Avaliar atendimento técnico e quantitativo de cada produto ao item requerido.
- **Ator interessado:** Operador; revisor técnico.
- **Entradas:** Exigências de item/lote, produto e documentos do fabricante/fornecedor; atributos técnicos.
- **Regras de processamento:** Comparar categoria, descrição técnica, marca/modelo quando aplicáveis, dimensões, capacidade, potência, material, compatibilidade, certificações, quantidade, unidade, conteúdo da embalagem e garantia. Identificar atributos obrigatórios, correspondentes, divergentes e desconhecidos. Similaridade textual serve para recuperar candidatos, não comprovar atendimento.
- **Saídas:** Matriz requisito×produto com evidência e pendências por atributo.
- **Dependências:** RF-008, RF-014, RF-015.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 3.
- **Regras de negócio associadas:** RN-007, RN-015.
- **Critérios de aceitação verificáveis:** Produto com título muito semelhante e potência inferior à exigida resulta em incompatibilidade explícita no atributo. Quantidade em caixas é convertida somente com conteúdo comprovado, preservando origem e fator.

### RF-018 — Classificar compatibilidade técnica

Status: [PROPOSTO].

- **Identificador:** RF-018.
- **Nome:** Classificar compatibilidade técnica.
- **Descrição:** Comunicar o grau real de compatibilidade, distinguindo confirmação humana de candidato possível.
- **Ator interessado:** Operador; revisor técnico.
- **Entradas:** Matriz comparativa, critérios obrigatórios, evidências e julgamento humano.
- **Regras de processamento:** Classificar como compatível confirmado após verificação suficiente, possivelmente compatível, incompatível ou informação insuficiente. Confirmação exige todos os atributos críticos atendidos com evidência, sem conflito pendente e com registro da verificação humana. Similaridade, marca ou score isolado não substituem essa verificação.
- **Saídas:** Nível de compatibilidade, fundamentos, autor e data.
- **Dependências:** RF-017, RF-009.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 3.
- **Regras de negócio associadas:** RN-015, RN-024, RN-027.
- **Critérios de aceitação verificáveis:** Um atributo obrigatório não atendido resulta incompatível; atributo crítico sem evidência impede confirmado. Produto completo e revisado pode ser confirmado, vinculado às versões do item e da ficha técnica.

### RF-019 — Revisar e versionar o matching

Status: [PROPOSTO].

- **Identificador:** RF-019.
- **Nome:** Revisar e versionar o matching.
- **Descrição:** Manter rastreabilidade de revisões técnicas e da validade de correspondências após mudanças.
- **Ator interessado:** Operador; revisor técnico.
- **Entradas:** Resultado do matching, evidências complementares, versões novas de item/produto/cotação.
- **Regras de processamento:** Permitir corrigir correspondência e registrar motivo sem substituir resultado anterior. Mudança relevante invalida confirmação dependente e pede nova revisão. Registrar fontes, critérios/versões e responsável; não atribuir conformidade automaticamente a produtos relacionados.
- **Saídas:** Histórico de avaliações, correspondência atual e pendências de revalidação.
- **Dependências:** RF-004, RF-017, RF-018.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 3.
- **Regras de negócio associadas:** RN-015, RN-028, RN-030.
- **Critérios de aceitação verificáveis:** Retificação que altera capacidade exigida marca matching anterior como desatualizado. Correção manual preserva algoritmo/resultado original e registra avaliação que o substituiu.

## 6. Simulação financeira — domínio 7.6

[CONFIRMADO] O motor SILLO e a revisão exibem margem/taxas para revenda (`app/services/review_service.py:260–287`). [PROPOSTO] Fluxo de caixa, lotes, garantias e preços de proposta precisam de modelo próprio.

### RF-020 — Cadastrar cenários financeiros completos

Status: [PROPOSTO].

- **Identificador:** RF-020.
- **Nome:** Cadastrar cenários financeiros completos.
- **Descrição:** Reunir todas as premissas econômicas necessárias para avaliar uma unidade de participação.
- **Ator interessado:** Operador; contador/assessor como fonte validada de parâmetros.
- **Entradas:** Preço de aquisição, quantidade, frete, seguro, embalagem, tributos/encargos, operação, garantias/despesas contratuais, custo financeiro, prazos, capital, reserva de risco e adicionais.
- **Regras de processamento:** Exigir moeda, unidade, base de incidência, periodicidade, origem, data e validade. Separar custo fixo, variável, estimado e confirmado. Desconhecido não é zero: custo zero exige justificativa. Permitir selecionar alternativa operacional/cotação sem alterar cenários anteriores.
- **Saídas:** Cenário versionado, premissas e lista de valores faltantes.
- **Dependências:** RF-012, RF-015, RF-018; regra financeira do documento 06.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 4.
- **Regras de negócio associadas:** RN-004, RN-005, RN-019, RN-020.
- **Critérios de aceitação verificáveis:** Cenário com frete desconhecido é marcado incompleto, sem lucro confirmado. Valor zero explicitamente justificado pode ser usado e permanece auditável; simulação nova não reescreve a anterior.

### RF-021 — Calcular custos, preços, margem e retorno com Decimal

Status: [PROPOSTO].

- **Identificador:** RF-021.
- **Nome:** Calcular custos, preços, margem e retorno com Decimal.
- **Descrição:** Produzir indicadores financeiros coerentes, verificáveis e reproduzíveis.
- **Ator interessado:** Operador.
- **Entradas:** Cenário completo, preço de proposta hipotético, meta de margem e política financeira versionada.
- **Regras de processamento:** Usar Decimal e precisão/arredondamento explícitos. Produzir custo total/unitário, preço mínimo economicamente sustentável, margem de contribuição, margem líquida estimada, lucro, ROI e ponto de equilíbrio, com bases/denominadores definidos. Resolver preço por meta de margem considerando custos incidentes sobre receita; identificar equações inviáveis. Preço estimado oficial é referência, nunca venda garantida.
- **Saídas:** Memória de cálculo e indicadores estimados por cenário; indicação não calculável quando necessário.
- **Dependências:** RF-020; fórmulas e validação numérica a detalhar em 12.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 4.
- **Regras de negócio associadas:** RN-004, RN-005, RN-019 a RN-021.
- **Critérios de aceitação verificáveis:** Fixture Decimal com quantidades fracionárias e custos rateados reconcilia total e linhas pela política declarada. Receita zero, denominador nulo ou taxa inviável gera indicador não calculável/motivo, sem divisão por zero ou rentabilidade artificial.

### RF-022 — Simular desembolsos e capital de giro

Status: [PROPOSTO].

- **Identificador:** RF-022.
- **Nome:** Simular desembolsos e capital de giro.
- **Descrição:** Verificar se a operação pode ser financiada ao longo do tempo, além de parecer lucrativa.
- **Ator interessado:** Operador.
- **Entradas:** Datas/intervalos de compra, tributos, frete, garantia, entrega, pagamento esperado, custo do dinheiro e capital disponível.
- **Regras de processamento:** Construir cronograma de entradas e saídas; calcular maior necessidade acumulada de financiamento e capital de giro. Não presumir antecipação pelo órgão. Considerar atrasos em cenários e distinguir lucro econômico de capacidade de financiar a operação; capital desconhecido gera pendência.
- **Saídas:** Fluxo de caixa, pico de exposição, prazo de recuperação e insuficiência de capital.
- **Dependências:** RF-020, RF-021.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 4.
- **Regras de negócio associadas:** RN-016 a RN-018, RN-021.
- **Critérios de aceitação verificáveis:** Operação lucrativa com desembolso anterior superior ao capital disponível fica impedida pela regra de capital. Adiar o recebimento aumenta exposição/custo quando aplicável e preserva o cenário original.

### RF-023 — Consolidar viabilidade no nível de julgamento

Status: [PROPOSTO].

- **Identificador:** RF-023.
- **Nome:** Consolidar viabilidade no nível de julgamento.
- **Descrição:** Evitar análise parcial quando o julgamento exige participação conjunta em vários itens.
- **Ator interessado:** Operador.
- **Entradas:** Itens e quantidades, vínculo lote/grupo/global, critério de julgamento, cotações e cenários.
- **Regras de processamento:** Avaliar todos os itens exigidos na unidade de participação; ratear custos compartilhados por política documentada sem dupla contagem. Item isolado lucrativo não autoriza conclusão favorável de lote inviável; critério de julgamento desconhecido impede consolidação favorável.
- **Saídas:** Custo, lucro, capital e elegibilidade consolidados por unidade de julgamento, com decomposição.
- **Dependências:** RF-008, RF-020 a RF-022.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 4.
- **Regras de negócio associadas:** RN-007 a RN-009, RN-019, RN-020.
- **Critérios de aceitação verificáveis:** Lote com um item lucrativo e outro deficitário exibe resultado agregado e não recomenda participar apenas pelo primeiro. Frete compartilhado é somado uma vez e sua alocação reconcilia ao total.

### RF-024 — Comparar sensibilidade e metas de margem

Status: [PROPOSTO].

- **Identificador:** RF-024.
- **Nome:** Comparar sensibilidade e metas de margem.
- **Descrição:** Mostrar quais premissas mais afetam a viabilidade e quais preços atendem às metas propostas.
- **Ator interessado:** Operador.
- **Entradas:** Cenário base, variações de frete, câmbio quando aplicável, aquisição e preço hipotético de disputa; metas de margem.
- **Regras de processamento:** Produzir cenários base/conservador/otimista com premissas explícitas e variar fatores selecionados. Preço de disputa é hipótese, não previsão de vitória. Câmbio só se aplica a custo exposto; informar data/base da taxa e não buscar taxa arbitrária silenciosamente.
- **Saídas:** Tabela/gráfico de sensibilidade, preços por meta e limites de viabilidade.
- **Dependências:** RF-020 a RF-023.
- **Prioridade MoSCoW:** Should.
- **Fase de implementação:** 4.
- **Regras de negócio associadas:** RN-005, RN-016 a RN-021.
- **Critérios de aceitação verificáveis:** Aumentar apenas frete não melhora lucro, mantidas demais entradas. Cenário em BRL sem exposição cambial mostra câmbio não aplicável; todos os cenários preservam entradas e versão.

## 7. Elegibilidade e ranking — domínio 7.7

[CONFIRMADO] Há ranking heurístico comercial e evidências versionadas. [PROPOSTO] Elegibilidade e score licitatórios não herdam pesos/limiares do motor de revenda.

### RF-025 — Aplicar bloqueios e pendências antes de recomendar

Status: [PROPOSTO].

- **Identificador:** RF-025.
- **Nome:** Aplicar bloqueios e pendências antes de recomendar.
- **Descrição:** Separar oportunidades inviáveis ou pendentes de candidatas à decisão humana.
- **Ator interessado:** Operador; revisor.
- **Entradas:** Prazos, habilitação, matching, disponibilidade, logística, capital, julgamento e qualidade/atualidade dos dados.
- **Regras de processamento:** Aplicar regras eliminatórias antes do ranking positivo. Distinguir impedimento objetivo, pendência verificável e análise especializada; informação crítica desconhecida impede recomendação favorável. Verificar janela de participação e tempo de cotação/preparação/transporte; não certificar habilitação jurídica automaticamente.
- **Saídas:** Resultado de elegibilidade com bloqueios, pendências, responsáveis e evidências.
- **Dependências:** RF-002, RF-008, RF-010, RF-015, RF-018, RF-022, RF-023; regras 06.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 4.
- **Regras de negócio associadas:** RN-001 a RN-003, RN-006 a RN-011, RN-015, RN-018, RN-022 a RN-029.
- **Critérios de aceitação verificáveis:** Prazo vencido impede recomendação positiva mesmo com margem alta. Pendência documental crítica permanece pendência, e não elegível; score alto não remove um bloqueio.

### RF-026 — Produzir ranking configurável e transparente

Status: [PROPOSTO].

- **Identificador:** RF-026.
- **Nome:** Produzir ranking configurável e transparente.
- **Descrição:** Priorizar oportunidades reais por retorno, viabilidade e qualidade de evidência.
- **Ator interessado:** Operador; responsável pelos parâmetros.
- **Entradas:** Candidatos, resultado de elegibilidade, dados financeiros, logísticos, técnicos, confiabilidade e risco de capital/prazo.
- **Regras de processamento:** Calcular componentes separados, combinar apenas conforme versão de pesos/normalização e apresentar candidatos bloqueados/pendentes em grupos próprios. Não privilegiar valor total estimado por si só. Registrar ajustes de configuração; ausência de dado não gera nota favorável e exige cobertura explícita.
- **Saídas:** Ranking explicável, subscores, classe/pendências e versão de cálculo.
- **Dependências:** RF-011, RF-018, RF-021 a RF-023 e RF-025; modelo matemático em 09. RF-024 é incremento opcional quando disponível, sem impedir o ranking obrigatório do MVP.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 4.
- **Regras de negócio associadas:** RN-027, RN-029.
- **Critérios de aceitação verificáveis:** Oportunidade de valor estimado maior mas bloqueada não supera candidato elegível no ranking positivo. Alterar peso válido cria versão e explica mudança; peso inválido não é aceito.

### RF-027 — Explicar e reconstruir cada análise

Status: [PROPOSTO].

- **Identificador:** RF-027.
- **Nome:** Explicar e reconstruir cada análise.
- **Descrição:** Permitir auditoria e reconstrução das conclusões de cada rodada de análise.
- **Ator interessado:** Operador; revisor/auditor.
- **Entradas:** Snapshot de contratação/itens/documentos, cotações, matching, parâmetros, regras, cenários e resultados.
- **Regras de processamento:** Preservar fontes/campos/trechos, obtenção, atualidade, confiança, versões e pendências. Reproduzir análise histórica com seus próprios insumos; separar avaliação humana de saída automática. Análise sobre dados novos é nova versão, não alteração retroativa.
- **Saídas:** Dossiê de análise, decomposição de cálculos, razões de decisão e histórico reproduzível.
- **Dependências:** RF-004, RF-007, RF-016, RF-019, RF-020, RF-025, RF-026.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 4.
- **Regras de negócio associadas:** RN-020, RN-026 a RN-030.
- **Critérios de aceitação verificáveis:** Após alterar cotação e pesos, a análise anterior continua mostrando valores/regras originais. Todo bloqueio e recomendação aponta para fonte ou premissa explícita; ausência de evidência fica identificada.

### RF-028 — Contextualizar preços históricos oficiais

Status: [PROPOSTO]. Revisado em R1; ID preservado. Versão original: Should integralmente fase 6. ADR-018 antecipa somente apresentação básica; RF-035/036/037/038 são donos de ingestão, comparação, descoberta e relacionamento, respectivamente.

- **Identificador:** RF-028.
- **Nome:** Contextualizar preços históricos oficiais.
- **Descrição:** Apresentar preços históricos por natureza e contexto de comparáveis validados, sem inferir custo de aquisição ou capacidade atual.
- **Ator interessado:** Operador; analista.
- **Entradas:** Conjunto de RF-036 com identidades/versões, natureza comprovada, atributos, unidade/fator, quantidade, datas distintas, região/condições e qualidade; política versionada de 22.
- **Regras de processamento:** Manter lance, estimado, homologado, registrado, contratado e cotação separados. Exibir observações qualificadas; agregados somente com suficiência de RN-035, denominadores e exclusões explícitos. Proibir total global transformado em unitário arbitrário. Seleção manual como hipótese de preço de venda registra contexto e sensibilidade; nunca preenche custo. Fase 6 acrescentará dispersão/concentração/tendências amplas, com universo observado e método próprios, sem obrigação no piloto.
- **Saídas:** Referências individuais e, se sustentadas, mínimo/mediana/máximo por coorte, n/janela/qualidade/atualidade; “não calculável” com motivo quando insuficiente.
- **Dependências:** RF-003, RF-035 e RF-036, contratos/gates de 22 e matriz R1. RF-017/027 consomem contexto posteriormente quando pertinente, não bloqueiam pesquisa básica; nenhum RF financeiro depende obrigatoriamente de histórico.
- **Prioridade MoSCoW:** Should.
- **Fase de implementação:** 2-H para apresentação básica em uma categoria; 6 para inteligência ampliada; não renumerar fases.
- **Regras de negócio associadas:** RN-004, RN-005, RN-015, RN-026, RN-028, RN-034, RN-035, RN-036.
- **Critérios de aceitação verificáveis:** Preço homologado por caixa não entra em série unitária sem fator comprovado. Duas observações mostram amostra insuficiente para agregado. Resultado replicado não duplica n; resultado e contrato permanecem naturezas separadas. Selecionar referência nunca preenche aquisição/cotação nem altera score. Fase 6 exige coorte/etapa/denominador explícitos, sem prometer concentração nacional.

## 8. Pipeline comercial — domínio 7.8

[CONFIRMADO] Existe fila de revisão humana de oportunidades de revenda, decisão transacional e desfazimento (`app/api/review.py:163`, `app/repositories/review_repository.py:290`). [PROPOSTO] Estados comerciais públicos serão novos, sem reinterpretar registros antigos.

### RF-029 — Operar fila de revisão comercial

Status: [PROPOSTO].

- **Identificador:** RF-029.
- **Nome:** Operar fila de revisão comercial.
- **Descrição:** Adaptar o padrão de revisão humana do SILLO à oportunidade comercial pública.
- **Ator interessado:** Operador; revisor.
- **Entradas:** Análises, bloqueios/pendências, documentos, matching e cenários; filtros de prioridade.
- **Regras de processamento:** Apresentar evidências, atualidade, versões e decisões anteriores em uma tela de revisão. Permitir solicitar complementação, descartar com motivo e registrar avaliação humana sem alterar score original. Pré-aprovação para estudo e aptidão para decisão são avaliações de apoio, não autorização automática de participação.
- **Saídas:** Decisão humana auditável e próximo estado permitido.
- **Dependências:** RF-025 a RF-027; padrão de revisão SILLO adaptado.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 5.
- **Regras de negócio associadas:** RN-024, RN-029 a RN-032.
- **Critérios de aceitação verificáveis:** Descartar exige motivo; pedir complemento identifica pendência/responsável. Dupla submissão não cria decisões ativas conflitantes. Revisão favorável não envia lance/proposta nem supera bloqueio crítico.

### RF-030 — Controlar estados e transições comerciais

Status: [PROPOSTO].

- **Identificador:** RF-030.
- **Nome:** Controlar estados e transições comerciais.
- **Descrição:** Manter um ciclo comercial consistente, com transições explícitas e separadas da situação oficial.
- **Ator interessado:** Operador.
- **Entradas:** Estado atual, ação manual ou evento de atualização, pendências e evidências.
- **Regras de processamento:** Aplicar a máquina de estados definida no documento 06 e D10; registrar origem/destino, autor, data, motivo e versão da análise. Separar situação oficial da contratação de estado interno da oportunidade. Impedir saltos que omitam verificação necessária; retificações geram revalidação sem apagar fatos comerciais já ocorridos.
- **Saídas:** Linha do tempo comercial e estado atual consistente.
- **Dependências:** RF-004, RF-025, RF-029; estados 06/D10.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 5.
- **Regras de negócio associadas:** RN-028, RN-030 a RN-032.
- **Critérios de aceitação verificáveis:** Uma transição não autorizada é rejeitada sem modificar o estado. Encerramento oficial afeta elegibilidade, mas não converte participação registrada em inexistente; cada mudança aparece na linha do tempo.

### RF-031 — Registrar participação, disputa e resultado manualmente

Status: [PROPOSTO].

- **Identificador:** RF-031.
- **Nome:** Registrar participação, disputa e resultado manualmente.
- **Descrição:** Registrar atos externos realizados pelo operador, sem executá-los automaticamente.
- **Ator interessado:** Operador.
- **Entradas:** Comprovantes/links, data, unidade de participação, preço efetivamente informado e resultado publicado.
- **Regras de processamento:** Registrar somente ações já realizadas manualmente no portal competente. Diferenciar participação registrada, disputa, resultado conhecido, vencedora ou perdida; resultado provisório não é contrato. Permitir corrigir registro com justificativa e histórico; não realizar comunicação, lance ou proposta automaticamente.
- **Saídas:** Eventos comerciais e evidências de participação/resultado.
- **Dependências:** RF-029, RF-030; validação de transição em 06.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 5.
- **Regras de negócio associadas:** RN-030 a RN-032.
- **Critérios de aceitação verificáveis:** Salvar participação exige referência verificável e responsável. Registrar resultado vencedor não cria contrato automaticamente; nenhum evento dispara envio externo de lance ou proposta.

### RF-032 — Acompanhar contratação, execução e recebimento

Status: [PROPOSTO].

- **Identificador:** RF-032.
- **Nome:** Acompanhar contratação, execução e recebimento.
- **Descrição:** Acompanhar a diferença entre expectativa comercial e fatos da execução e recebimento.
- **Ator interessado:** Operador.
- **Entradas:** Documento de contratação quando existente, obrigações, marcos de compra, entrega/aceite, valores e datas recebidos.
- **Regras de processamento:** Registrar execução apenas com fundamento contratual aplicável e preservar entregas parceladas, recusas/pendências, recebimentos parciais e saldo. Separar vencer de contratar, entregar de aceitar e faturar de receber. Encerramento exige motivo e tratamento dos saldos/pendências conforme regras.
- **Saídas:** Cronograma real, entregas, recebimentos, resultado realizado e fechamento auditável.
- **Dependências:** RF-022, RF-023, RF-030, RF-031.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 5.
- **Regras de negócio associadas:** RN-002, RN-007 a RN-009, RN-016, RN-030 a RN-032.
- **Critérios de aceitação verificáveis:** Entrega parcial não marca entrega total. Recebimento parcial mantém saldo; vencedor sem contratação não inicia execução por inferência. Encerramento excepcional registra motivo e pendências residuais.

### RF-033 — Visualizar carteira e indicadores comerciais

Status: [PROPOSTO].

- **Identificador:** RF-033.
- **Nome:** Visualizar carteira e indicadores comerciais.
- **Descrição:** Dar visão operacional da carteira com acesso às origens dos indicadores.
- **Ator interessado:** Operador.
- **Entradas:** Oportunidades, estados, prazos, bloqueios, classes, análises, resultados e atualidade.
- **Regras de processamento:** Disponibilizar filtros e indicadores de carteira, próximos prazos, revisão pendente, situação logística e financeira. Distinguir projeção de valor realizado e dados incompletos. Permitir navegar do indicador à evidência; exportação analítica reaproveita mecanismo existente após redefinir fatos/dimensões.
- **Saídas:** Dashboard local e listagens rastreáveis; exportações coerentes com filtros quando previstas.
- **Dependências:** RF-002, RF-025 a RF-027 e RF-029 a RF-032; metas e métricas do DAN. RF-028 é extensão opcional da fase 6, sem impedir o dashboard obrigatório da fase 5.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 5.
- **Regras de negócio associadas:** RN-004, RN-005, RN-026, RN-027, RN-032.
- **Critérios de aceitação verificáveis:** Contador de pendências coincide com a lista filtrada de origem. Valor estimado, proposta hipotética e receita efetivamente recebida são identificados separadamente; dados desatualizados aparecem sinalizados.

### RF-034 — Preservar decisões e reabrir revisão

Status: [PROPOSTO].

- **Identificador:** RF-034.
- **Nome:** Preservar decisões e reabrir revisão.
- **Descrição:** Permitir correção auditada sem apagar decisões ou fatos comerciais.
- **Ator interessado:** Operador; revisor/auditor.
- **Entradas:** Decisão atual, snapshot, motivo de correção/desfazimento ou mudança de dados.
- **Regras de processamento:** Preservar decisão original, registrar invalidação/substituição e nova avaliação, sem apagar eventos. Reabertura deve seguir máquina de estados: corrigir análise não desfaz automaticamente participação, disputa, entrega ou recebimento já ocorridos. Não alterar decisões históricas de revenda para estados licitatórios.
- **Saídas:** Histórico de decisões e reversões com vínculo à versão e revisão pendente quando aplicável.
- **Dependências:** RF-004, RF-027, RF-029, RF-030.
- **Prioridade MoSCoW:** Must.
- **Fase de implementação:** 5.
- **Regras de negócio associadas:** RN-028, RN-030 a RN-032.
- **Critérios de aceitação verificáveis:** Desfazer pré-aprovação mantém autor/motivo/valores originais e registra novo evento. Retificação após participação abre revisão operacional sem excluir o comprovante de participação; histórico SILLO permanece com seu significado original.

## 9. Proposta inicial de remote_delivery_score

[PROPOSTO] Versão **v0.1-logistica**, alinhada a RN-012. O score mede prontidão logística documentada; a classe A/B/C/D e os bloqueios permanecem independentes. Os pesos são hipóteses iniciais calibráveis, não resultado de validação empírica.

| Dimensão | Peso | Evidência esperada |
|---|---:|---|
| Envio convencional | 0,35 | Produto transportável, restrições conhecidas e forma de recebimento compatível |
| Preparação e prazo | 0,25 | Tempo de fornecedor, transporte, recebimento e margem operacional compatíveis com a obrigação |
| Destinos e parcelas | 0,15 | Endereços, abrangência, frequência, quantidades e parcelamento suficientemente conhecidos |
| Pós-venda | 0,15 | Garantia, manutenção, assistência e eventual presença local compreendidas |
| Envio direto | 0,10 | Compatibilidade contratual/fiscal/operacional evidenciada quando esse modelo for adotado |

Cada dimensão mantém avaliação `x` em `0`, `0,5`, `1` ou desconhecido (`null`): 0 indica incompatibilidade/restrição não resolvida para aquela dimensão; 0,5, atendimento condicionado com condições documentadas; 1, atendimento suficiente para análise preliminar. A avaliação aponta para evidências e não constitui confirmação jurídica.

Seja `K` o conjunto de dimensões conhecidas e `w_i` seu peso normalizado:

- `coverage = soma(w_i, i em K)`;
- `remote_delivery_score = lower = 100 × soma(w_i × x_i, i em K)`;
- `upper = min(100, lower + 100 × (1 − coverage))`.

O limite superior representa incerteza possível, não pontuação recomendada nem promessa de viabilidade. Desconhecido permanece `null`; sua contribuição conservadora é zero, sem registrá-lo como incompatível ou como evidência favorável. Não dividir o score pela cobertura para esconder lacunas.

No modelo operador + transportadora, envio direto pode ser **não aplicável justificado**: registra-se essa condição, evidência do modelo escolhido e `x=1` para não penalizar alternativa legítima. Não aplicável sem justificativa não é valor conhecido. Para modelo que dependa de envio direto, a ausência de confirmação é pendência crítica.

Exemplo: quatro dimensões conhecidas com `x=1`, mas preparação/prazo desconhecido, produzem `coverage=0,75`, `lower=75`, `upper=100`. Se prazo é crítico, a classe é D e a recomendação favorável permanece impedida.

A classe segue evidência e criticidade, nesta ordem: D se há lacuna/conflito crítico; C se operação presencial relevante está suficientemente demonstrada; B se atendimento potencialmente remoto depende de restrições resolúveis; A se há evidência suficiente de envio convencional sem presença ou restrição relevante não resolvida. D não apaga fatos já conhecidos, inclusive presença local. Classe A continua sujeita à confirmação do edital e revisão humana. A Execução 2 detalhará rubricas, validação e parametrização; não se fixa nesta etapa um limiar universal de aprovação.

## 10. Contrato funcional da simulação

[PROPOSTO] RF-020 a RF-024 seguem RN-019 a RN-021. A implementação deverá usar `Decimal`, aceitar custos/preços unitários e quantidades com até seis casas decimais, manter ao menos oito casas nos intermediários e apresentar totais BRL com duas casas usando `ROUND_HALF_UP`. A política deve ser versionada, com validação de entradas e regra explícita de rateio/reconciliação; quantização apenas na apresentação não pode mascarar divergência entre linhas e total.

O detalhamento matemático pertence ao documento 12. A distinção funcional já é obrigatória: margem usa receita como base; ROI usa capital empregado definido no cenário; capital de giro decorre do cronograma de caixa. Indicadores de bases diferentes devem ser nomeados e demonstrados. Divisor nulo produz “não calculável” com motivo. Preço oficial estimado, preço hipotético de proposta e receita efetivamente recebida são grandezas distintas.

Um cenário incompleto pode ser salvo para estudo com pendências visíveis. Ele não sustenta recomendação favorável. Custos/despesas/tributos não se transformam em zero por omissão e parâmetros fiscais dependem de fonte/validação aplicável ao operador.

## 11. Ciclo comercial coberto

[PROPOSTO] RF-030 a RF-034 cobrem os seguintes estados de RN-032/D10: **oportunidade descoberta; em enriquecimento; aguardando cotação; aguardando revisão; pré-aprovada para estudo; descartada; apta para decisão humana de participação; participação registrada manualmente; em disputa; resultado conhecido; vencida; perdida; contrato em execução; entrega concluída; recebimento financeiro; encerrada**.

A fonte editável da máquina de estados está em [D10 — estados](diagramas/D10-estados.md), prevista nesta mesma execução junto ao documento 06.

Os nomes “vencida” e “perdida” expressam resultado comercial da disputa; prazo expirado é atributo/situação da contratação e bloqueio de elegibilidade, não sinônimo do estado vencida. Pré-aprovação permite aprofundar estudo; aptidão para decisão humana não representa envio de proposta. Resultado vencedor não pressupõe adjudicação/contratação concluída. As transições válidas, retornos, guardas e exceções estão centralizadas em RN-032 e D10 para impedir duas máquinas de estados divergentes.

O ciclo deve admitir recebimentos e entregas parciais como eventos, conservando o estado e pendências adequados, sem forçar sequência que declare conclusão inexistente. Atualização oficial reabre análises afetadas; fatos externos comprovados continuam registrados.

## 12. Dependências e verificação de completude

| Domínio do prompt | RF | Resultado esperado |
|---|---|---|
| 7.1 Coleta | RF-001 a RF-005 | Busca nacional, filtros, abertura, identidade, sincronização, alterações e consulta local |
| 7.2 Editais | RF-006 a RF-009 | Arquivos, extração textual, exigências, evidências e revisão |
| 7.3 Logística | RF-010 a RF-012 | Classes A/B/C/D, score explicável e modelos de fornecimento |
| 7.4 Fornecedores | RF-013 a RF-016 | Cadastro, catálogo, cotação/estoque, condições e histórico |
| 7.5 Matching | RF-017 a RF-019 | Atributos técnicos, níveis de compatibilidade e revisão versionada |
| 7.6 Finanças | RF-020 a RF-024 | Custos/preço/retorno, caixa, julgamento e sensibilidade |
| 7.7 Elegibilidade/ranking | RF-025 a RF-028 | Bloqueios, componentes, configuração, explicação e histórico oficial |
| 7.8 Pipeline | RF-029 a RF-034 | Fila, estados, participação manual, execução, painel e auditoria |

A implementação deve preservar o PostgreSQL e a arquitetura útil existentes e seguir Docker-first. Esses aspectos de qualidade/operação constam dos RNF; os RF não exigem novo banco, novo cache ou troca automática de orquestrador/interface.

Na Execução 2, resolver contratos oficiais, modelos de dados, rubricas de matching, parâmetros financeiros e algoritmo global antes de implementar. Na Execução 3, relacionar cada Must a pelo menos uma capacidade CP e um requisito técnico RT, além de componentes, entidades, casos de uso, testes e fases. A existência desta especificação não comprova funcionamento do Radar Licita.

## 13. Descoberta antecipada — revisado em R1

As execuções originais acima já foram concluídas. R1-E2 propagou CP/RT/UC/TST/HU e fases. Os Should abaixo não são dependência oculta de um Must; suas guardas são obrigatórias quando o incremento for habilitado. Marco 2-H: após identidade/ingestão e antes ou junto da fase 3, condicionado aos gates de [22](22-inteligencia-de-mercado-fornecedores.md).

### RF-035 — Ingerir resultados e vínculos históricos delimitados

Status: [PROPOSTO], revisado em R1.

- **Identificador:** RF-035.
- **Nome:** Ingerir resultados e vínculos históricos delimitados.
- **Descrição:** Obter incrementalmente resultados por item e relações oficiais com atas/contratos no recorte validado, sem presumir execução.
- **Ator interessado:** Operador e responsável pela integração.
- **Entradas:** Contrato público validado/versionado, janela/filtros reais, quota do produto, checkpoint e escopo da categoria.
- **Regras de processamento:** GET de leitura validado por recurso; limites e atualização conforme contrato; classificar categoria localmente quando filtro ausente e declarar custo/cobertura. Preservar origem/natureza/identidade/situação/consulta usando RF-003/004; vincular etapas sem somar. Campo ausente não é fato favorável; contrato global não produz unitário. Sem suporte a vínculo, registrar pendência e não fabricar correspondência.
- **Saídas:** Observações versionadas por recurso, vínculos comprovados ou pendentes, contagens/qualidade e checkpoint durável.
- **Dependências:** RF-001, RF-003, RF-004, RF-005; QA-R1-01–04 e RNF de integração. Não depende de cadastro, cotação ou motor financeiro.
- **Prioridade MoSCoW:** Should no MVP; necessário ao incremento de descoberta quando ativado.
- **Fase de implementação:** 2-H, após fundação da fase 1.
- **Regras de negócio associadas:** RN-005, RN-026, RN-028, RN-033, RN-034.
- **Critérios de aceitação verificáveis:** Duas páginas com repetição geram observações sem duplicata lógica; interrupção mantém checkpoint parcial. Resultado replicado mantém duas fontes e uma contagem. Contrato relacionado não soma quantidade entregue; dado sem unidade/identidade suficiente permanece pendente. Fonte não validada não é chamada.

### RF-036 — Pesquisar e revisar resultados comparáveis

Status: [PROPOSTO], revisado em R1.

- **Identificador:** RF-036.
- **Nome:** Pesquisar e revisar resultados comparáveis.
- **Descrição:** Recuperar resultados candidatos e registrar equivalência por atributo para um item de oportunidade.
- **Ator interessado:** Operador/revisor técnico.
- **Entradas:** Item versionado, atributos/unidade/embalagem/quantidade/contexto, observações RF-035 e taxonomia quando disponível.
- **Regras de processamento:** Código/texto recupera; revisão compara especificação, unidade/fator, natureza, datas, região pertinente e condições. Classificar comparável/incompatível/pendente com evidência e política de 22; sem fator comprovado não converter. Retificação invalida vínculo dependente. Reutilizar critérios técnicos RN-015, sem exigir produto/cotação cadastrados.
- **Saídas:** Relação item→resultado versionada, atributos comparados, conclusão/motivo/revisor e lacunas; conjunto para RF-028.
- **Dependências:** RF-003 e RF-035, item/requisitos suficientemente descritos; RF-008 pode fornecer atributos, mas entrada manual evidenciada é admitida. RF-017 não é pré-requisito.
- **Prioridade MoSCoW:** Should.
- **Fase de implementação:** 2-H.
- **Regras de negócio associadas:** RN-005, RN-015, RN-024, RN-026, RN-028, RN-035.
- **Critérios de aceitação verificáveis:** Código igual com especificação incompatível é rejeitado; código ausente com atributos comprovados pode ser comparável. Caixa sem fator fica pendente, nunca unidade presumida. Conclusão registra autor/versões e nova especificação exige revalidação. Não calcular preços/agregados aqui: apresentação pertence a RF-028.

### RF-037 — Descobrir candidatos pelo histórico observado

Status: [PROPOSTO], revisado em R1.

- **Identificador:** RF-037.
- **Nome:** Descobrir candidatos pelo histórico observado.
- **Descrição:** Apresentar estabelecimentos que justificam investigação comercial para um item, com perfil histórico descritivo.
- **Ator interessado:** Operador.
- **Entradas:** Comparáveis RF-036, identidade/qualidade/situação de fonte e correspondências revisadas.
- **Regras de processamento:** Agrupar observações pelo estabelecimento completo, com raiz em dimensão analítica separada; mostrar etapa, n, recência e cobertura. Lista ordena por comparabilidade/qualidade documentadas com desempate estável, sem probabilidade de fornecimento. Identidade provisória fica claramente pendente; papel inferido/CNAE é opcional. Não alegar entrega/capacidade nem taxa de vitória sem universo de participação.
- **Saídas:** Candidatos por item com justificativa, histórico por etapa, fontes e pendências; descoberta identificado/em avaliação/descartado com motivo.
- **Dependências:** RF-003, RF-035, RF-036; regras de privacidade. RF-028 complementa contexto de preço, não bloqueia listagem quando agregado é insuficiente; cadastro/cotação são posteriores.
- **Prioridade MoSCoW:** Should.
- **Fase de implementação:** 2-H.
- **Regras de negócio associadas:** RN-026, RN-027, RN-033, RN-034, RN-035, RN-036, RN-037.
- **Critérios de aceitação verificáveis:** Matriz e filial aparecem separadas com agrupamento indicado; ausência de histórico mostra cobertura insuficiente, não “inapto”. Candidato concorrente não vira fornecedor validado. Histórico sem cotação mantém análise pendente; histórico ausente com insumos suficientes não altera elegibilidade ou fórmula global.

### RF-038 — Registrar relacionamento e vincular cotação de candidato

Status: [PROPOSTO], revisado em R1.

- **Identificador:** RF-038.
- **Nome:** Registrar relacionamento e vincular cotação de candidato.
- **Descrição:** Registrar manualmente contato/disposição e aproveitar candidato no cadastro e nas cotações existentes, preservando escopos.
- **Ator interessado:** Operador; fornecedor como fonte externa, sem conta exigida.
- **Entradas:** Candidato RF-037, evidência do contato externo/resposta, estabelecimento e cadastro reconciliado, produto/quantidade/prazo/condições, referências de cotação/disponibilidade.
- **Regras de processamento:** Aplicar dimensões e transições de 22 §9, autoria/instante/evidência; registro não envia comunicação. Reutilizar identidade RF-013 ou abrir reconciliação; RF-015 cria/versiona cotação, este RF só vincula sua jornada. Não copiar confirmação entre filiais/produtos. Sem resposta não é recusa nem incapacidade; expiração perde atualidade com histórico.
- **Saídas:** Trilha de contato/disposição, vínculo candidato→supplier_id→cotação, disponibilidade por escopo e dados para métricas futuras.
- **Dependências:** RF-037; RF-013 para cadastro; RF-015 para cotação. Contato básico pode ser registrado antes da cotação, sem depender de RF-020–027. Nenhuma dependência inversa RF-013/015→RF-038.
- **Prioridade MoSCoW:** Should.
- **Fase de implementação:** 3, junto à consolidação de fornecedores.
- **Regras de negócio associadas:** RN-006, RN-024, RN-026, RN-030, RN-031, RN-033, RN-036, RN-037.
- **Critérios de aceitação verificáveis:** Mesmo CNPJ completo reconciliado reutiliza cadastro. Resposta aceitando cotar não confirma estoque. Cotação de E1 para 10 unidades não valida E2/20 unidades. Contato pendente permanece separado de E01–E16; registro não gera requisição de envio. Cotação expirada fica na trilha e fora das métricas de vigentes.

## Histórico de revisões

2026-10-12 — 0.2-R1: RF-035–038; decomposição de RF-028 sem exclusão/renumeração; extensões 003/004/005/013/015 e correção do ciclo 012↔013. Total 38 RF, 32 Must e 6 Should. Score, estados e demais obrigações preservados; nenhuma capacidade implementada.


2026-10-13 — 0.3-R1-E2: propagação referenciada e correção editorial da seção duplicada 11 para 13; IDs/requisitos/aceites e prioridades preservados.

