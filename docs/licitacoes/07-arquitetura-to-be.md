# 07 — Arquitetura TO-BE

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: [PROPOSTO] — especificação; nenhum componente novo implementado.

## 1. Fundamentos e alternativas

O [diagnóstico](02-diagnostico-as-is.md) confirma FastAPI, Jinja2, SQLAlchemy, Alembic, PostgreSQL, Prefect e coletores. O novo domínio não pode herdar a semântica de compra para revenda. A arquitetura selecionada é um monólito modular com processos de trabalho separados para ingestão e isolamento de documentos, compartilhando contratos de domínio e o banco existente (ADR-002/003/004/013).

| Alternativa | Benefício | Custo/risco | Decisão |
|---|---|---|---|
| Acrescentar PNCP ao motor heurístico atual | Menos arquivos inicialmente | Mistura produto, item, lote e estados; aritmética float; regressões silenciosas | Rejeitada |
| Monólito modular FastAPI + trabalhadores Prefect | Reusa transações, UI, operação e histórico; fronteiras testáveis | Exige disciplina contra SQL cruzado e side effects | Recomendada |
| Microsserviços com bancos/fila novos | Deploy independente | Operação distribuída, consistência e infraestrutura desnecessárias para um operador | Rejeitada para MVP |
| Reescrita total | Liberdade de desenho | Perda de padrões úteis e maior migração | Rejeitada |

Não há benchmark que prove uma opção mais rápida. Os custos são relativos; a escolha reduz mudanças operacionais e preserva os investimentos demonstrados no código.

## 2. Interface: comparação explícita

| Critério | Jinja2 com melhoria progressiva | React/TypeScript |
|---|---|---|
| Base existente | [CONFIRMADO] templates, filtros, formulários e CSRF | [PROPOSTO] aplicação, build e container novos |
| Lista e revisão | Paginação no servidor, formulários e links de evidência | Estado no cliente e API dedicada; boa composição de painéis |
| Simulação | Submissão de cenário e atualização parcial via JS simples | Interação extensa favorecida; exige equivalência cliente/servidor |
| Precisão financeira | Backend único calcula e formata | Backend deve continuar autoridade Decimal; evitar Number como cálculo decisório |
| Operação Docker | Mesmo container API | Container dev Node e build estático/servidor local em produção |
| Teste/manutenção | Uma stack principal; JS limitado | Duas stacks, dependências, contratos e testes de estado |
| Custo relativo | Médio para adaptar fluxo público | Alto para migração e paridade funcional |

**Decisão:** manter Jinja2 no MVP, com tabelas paginadas, formulários acessíveis e JS progressivo sem replicar regras financeiras. Templates não consultam banco: recebem DTOs preparados pelos serviços. Não é necessário container frontend adicional. Reavaliar React se o piloto falhar em RNF-015 por estado interativo que não seja resolvido por melhoria localizada, se edição simultânea de múltiplos cenários exigir estado complexo ou se multiusuário ampliar o produto. Registrar evidência, protótipo e custo antes de novo ADR; latência de SQL não é motivo isolado para trocar frontend (ADR-005/013).

## 3. Módulos e contratos internos

Nomes abaixo são fronteiras propostas, não arquivos existentes. APIs internas são chamadas Python tipadas, sem obrigar HTTP entre módulos. Instantes normalizados, dinheiro Decimal, identificadores UUID e dados ausentes explícitos. Toda mutação recebe ator, correlation_id e versão esperada; não confiar em campos ocultos da UI.

| Módulo | Entradas → saídas | Dono dos dados / referências |
|---|---|---|
| Ingestão oficial | Janela/filtros → payload bruto, checkpoint e versões | Bronze + control; RF-001–005, documento 10 |
| Contratações | Versão oficial/documental → itens e unidades de julgamento | procurement; RF-003/004/008, documento 08 |
| Documentos | URL autorizada → hash, extração, página/trecho e falhas | procurement; RF-006–009, RNF-003/004 |
| Fornecimento | Cadastro/CSV/cotação → produtos e ofertas versionadas | sourcing; RF-013–016, documento 11 |
| Matching | Item versionado + produto/cotação → comparação por atributo | sourcing; RF-017–019; não confirma por similaridade |
| Logística | Exigências, destinos e plano → A/B/C/D, cobertura e score | commercial; RF-010–012, documento 11 |
| Finanças | Unidade completa + cotações + parâmetros → cenário e caixa | commercial; RF-020–024, documento 12 |
| Decisão | Snapshot coerente → bloqueios, pendências e score condicionado | commercial + projeção Gold; RF-025–027, documento 09 |
| Revisão/comercial | Decisão humana/fato comprovado → revisão e evento | commercial; RF-029–034, RN-032 |
| Consulta/UI | Filtros/cursor → carteira, evidência e histórico | Leitura autorizada; RF-002/033; sem coletar em GET |

Os módulos de ingestão não conhecem templates. O motor recebe dados congelados e devolve resultado puro; não faz HTTP, não grava decisões humanas e não envia propostas. O serviço de aplicação coordena persistência em transação. Repositórios delimitam SQL; PostgreSQL continua autoridade transacional, e Gold é projeção recuperável. Catálogos legados só viram fornecedores/cotações após validação, sem criar certeza artificial.

## 4. Processos e fluxo transacional

1. Prefect agenda uma janela por escopo; o trabalhador HTTP recebe concessão exclusiva do job e consulta páginas. Retry HTTP e checkpoints pertencem ao adapter. Prefect pode retomar o job, mas não multiplica indiscriminadamente retentativas por página.
2. Página validada é persistida com bytes originais/hash, recebimento e normalização atômica dos registros aceitos; registros defeituosos ficam em quarentena lógica, sem concluir o checkpoint como completo.
3. Nova versão marca análises dependentes como desatualizadas. Um job de enriquecimento separado obtém documentos. Descoberta não depende de sucesso do PDF.
4. Download controlado fica no trabalhador HTTP. Parser documental executa em container sem rede, banco, segredos ou socket Docker; lê somente arquivos aprovados e escreve resultados em volume restrito. O trabalhador importa a saída validada. Jobs/polling no PostgreSQL e diretórios de spool bastam; não há novo broker.
5. Matching, logística e finanças usam versões explícitas. Antes de publicar análise/revisão positiva, transação verifica que as versões ainda são atuais e que as guardas temporais continuam válidas. Mudança concorrente cancela publicação positiva e solicita reanálise.
6. Revisão humana grava evento e transição comercial atomicamente. Após E08, retificação altera atualidade analítica, sem apagar participação. RN-032 governa transições; não há inferência automática de contrato, entrega ou pagamento.

Processamento de documento no container isolado não é um microsserviço de negócio: é fronteira de contenção. O parser não lança containers nem acessa o daemon. Workers existentes poderão acompanhar o spool com leasing e idempotência; a especificação de concessão e crash recovery está em 08/21.

## 5. Segurança, observabilidade e falhas

Aplicação local deve escutar somente no loopback do host; acesso remoto exige projeto próprio de autenticação, não simples publicação de portas. Reusar proteção CSRF com revisão de origens e autorização nas mutações. Logs estruturados usam job_id, procurement_id, analysis_id, duração e erro sanitizado; URLs/headers e texto documental não são logs irrestritos. Métricas: atraso da fonte, janela incompleta, documentos rejeitados, análises desatualizadas, fila e falhas por etapa.

Readiness da API deve retornar falha quando DB essencial falhar: o `/health` atual informa degradação com HTTP 200 [CONFIRMADO]. PNCP indisponível não derruba a consulta local; apresenta frescor e suspende novos positivos quando guardas exigirem atualização. Jobs expiram concessões e retomam sem duplicar efeitos. Limites mensuráveis permanecem em RNF-001–021; não criar SLA para PNCP.

## 6. Evolução e critérios de aceitação arquitetural

Entregar fundação/dados/PNCP antes de enriquecimento; revisão mínima acompanha cada módulo. Depois fornecimento, finanças/motor e carteira completa. Descoberta básica opcional entra no marco 2-H; inteligência ampla e ML permanecem posteriores (ADR-011/018; revisado em R1). Não alterar rotas de revisão legadas para reinterpretar seus registros. Novos prefixos internos/UI públicos serão definidos na implementação sem colidir com `/review` existente; nenhum endpoint local novo é alegado como implementado.

Aceite futuro: indisponibilidade externa mantém carteira; retificação concorrente impede positivo obsoleto; parser sem rede/DB; reinício retoma job; simulação reproduz snapshot; legado permanece consultável. [D02](diagramas/D02-containers.md), [D03](diagramas/D03-motor.md), [D11](diagramas/D11-dados.md) e [D12](diagramas/D12-implantacao.md) representam essas fronteiras. Infraestrutura em [21](21-infraestrutura-docker.md), dados em [08](08-modelo-de-dados.md). Nenhum teste de execução foi realizado nesta missão.


## 7. Fronteiras do histórico público — revisado em R1

Revisão 0.3-R1-E2, 2026-10-13. [PROPOSTO]. Reutilizar monólito, banco de domínio, Prefect e UI; nenhuma amostra/ferramenta auxiliar é adapter de produção. O registro de fontes/factory atual continua sem adapter público demonstrado. Bronze oficial mantém bytes/JSON no PostgreSQL em pncp_responses multirrecurso (08 §9); não criar volume de JSON, novo banco/cache/serviço/agendador. Arquivos só serão avaliados em ADR posterior se medição justificar mudar armazenamento; PDFs continuam no desenho anterior.

| Fronteira / RT | Entrada → saída / autoridade | Falha e revisão |
|---|---|---|
| Coleta de resultados — RT-017 | Contrato habilitado, janela, quota/versionamento → Bronze e ingestion_jobs | Worker HTTP existente; scheduler Prefect único. Contrato desconhecido desabilita recurso; página parcial não confirma checkpoint |
| Identidade/normalização — RT-017 | Resposta → public_events, suppliers provisórios/validados, historical_prices | Identidade por estabelecimento e evento; divergências/retificações preservadas. event_links diferencia equivalência de etapa relacionada; sem merge fuzzy |
| Comparabilidade/contexto — RT-018/009 | Item versionado + histórico + política → historical_comparisons e projeção de preços | Comparação técnica e qualidade revisadas; estatística insuficiente fica não calculável. Não depende de catálogo do operador; CATMAT não prova equivalência |
| Candidatos — RT-018 | Comparações → supplier_candidates com razões e ordenação explicável | Identidade/proveniência/recência, sem probabilidade de vender. Lista não escreve score, capacidade ou disponibilidade |
| Relacionamento — RT-019/004 | Ato externo registrado pelo operador → candidate_interactions, cadastro/produto/cotação | Sem conector de envio/outbox; RF-015 é autoridade de cotação. Guardas de estabelecimento/produto/quantidade/validade |
| Decisão — RT-006/008 | Insumos comerciais atuais + hipótese histórica opcional selecionada → cenário/guardas atuais | Nenhuma chamada HTTP durante cálculo; nenhum bônus de histórico. Fonte histórica falha não impede análise suficiente; insumos obrigatórios faltantes mantêm pendência |

Cada política recebe ID/versão no manifesto; serviço transacional valida referências JSON e versão esperada. Atualização oficial invalida comparações e somente análises que utilizaram referência afetada, conservando atos/cotações independentes. Observabilidade por fonte/recurso: contagem recebida, utilizável/rejeitada/pendente, duplicidade pendente, tentativas/bytes, janela, exclusões e último sucesso. Não mostrar dado velho como atual nem taxa de vitória sem universo observado.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: marco opcional 2-H e fronteiras coleta→identidade→comparáveis→candidatos→relação/decisão; sem serviços ou autoridades redundantes.

