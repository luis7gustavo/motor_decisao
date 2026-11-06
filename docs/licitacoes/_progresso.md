# Radar Licita — controle das três execuções

> Revisão atual 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo), prompt v2.0 conciliado. Três execuções originais, R1-E1 e R1-E2 concluídas documentalmente. Piloto [PROPOSTO] condicionado aos gates. Amostragem 0/30; validação documental e Mermaid offline executadas na E2, com limitações explícitas.

Data da inspeção inicial: 2026-10-10 (America/Sao_Paulo). Retomada: 2026-10-11. Versão: 0.1.
Commit base do código: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: Execuções 1 e 2 concluídas em 2026-10-11; Execução 3 concluída em 2026-10-12; especificação [PROPOSTO], não implementação.

## 1. Linha de base

| Campo | Registro |
|---|---|
| Repositório | https://github.com/luis7gustavo/motor_decisao |
| Remote observado | `origin`, URL acima com sufixo `.git` |
| Caminho | `<checkout-local-motor_decisao>` |
| Branch de origem | `main` |
| Branch de documentação | `docs/radar-licita-especificacao` |
| Commit base | `261b32107ff7b053c68c1df98e88de329c41c0f5` |
| Data de inspeção | 2026-10-10, America/Sao_Paulo |
| Working tree inicial do repositório alvo | Limpo; `git status --short` sem saída |
| Clone nesta execução | Não; checkout existente preservado, sem atualização remota |
| Documentação anterior em `docs/licitacoes/` | Diretório inexistente |
| Instruções locais | Nenhum `AGENTS.md` encontrado no repositório nem nos ancestrais inspecionados |

O diretório pai também é um repositório Git e contém arquivos não rastreados preexistentes. Ele não é o alvo desta missão. Todos os comandos Git de validação usam a raiz `motor_decisao`; nenhuma alteração no pai é autorizada pelo escopo documental.

## 2. Execuções

| Execução | Estado | Início | Fim | Commit local |
|---|---|---|---|---|
| 1 — diagnóstico, negócio e requisitos | concluída | 2026-10-10 | 2026-10-11 | Não necessário para persistência no mesmo checkout |
| 2 — arquitetura, dados e decisão | concluída | 2026-10-11 | 2026-10-11 | Não necessário no mesmo checkout |
| 3 — qualidade e consolidação | concluída | 2026-10-11 | 2026-10-12 | Não necessário no mesmo checkout |

## 3. Checklist de entregáveis

| Arquivo/atividade | Execução | Estado | Observações |
|---|---|---|---|
| Etapa 0 e `_progresso.md` | 1 | concluído | Baseline, branch, fontes oficiais e configuração Compose verificados |
| `02-diagnostico-as-is.md` | 1 | concluído | Inventário, evidências, Docker, fontes oficiais e matriz de reaproveitamento |
| `01-visao-produto.md` | 1 | concluído | Visão, modelos comerciais, MVP e limites |
| `03-analise-negocio-dan.md` | 1 | concluído | Personas, fluxos, riscos e dez indicadores com baseline desconhecida |
| `04-requisitos-funcionais.md` | 1 | concluído | 34 RF, onze campos verificados em cada um; oito domínios cobertos; score logístico v0.1 |
| `05-requisitos-nao-funcionais.md` | 1 | concluído | 21 RNF mensuráveis; 18 temas, metas calibráveis e critérios de validação |
| `06-regras-de-negocio.md` | 1 | concluído | RN-001 a RN-032, sete domínios, exemplos verificáveis e 16 estados com transições |
| `diagramas/D01-contexto.md` | 1 | concluído | Fonte preexistente revisada em 2026-10-11; conteúdo preservado |
| `diagramas/D04-casos-de-uso.puml` | 1 | concluído | Fonte preexistente revisada em 2026-10-11; UC-01 a UC-08; explicação no arquivo |
| `diagramas/D07-atividades.md` | 1 | concluído | Jornada editável, bloqueios/pendências e decisão humana; produção em 2026-10-11 |
| `diagramas/D10-estados.md` | 1 | concluído | 16 estados em três vistas; 45 transições conferidas com RN-032, sem ausentes/excedentes |
| `18-glossario.md` | 1 | concluído | Vocabulário de domínio, preços, logística, finanças, score e E01–E16; produção em 2026-10-11 |
| `19-decisoes-arquiteturais.md` | 1 | concluído | ADR-001 a ADR-012; conflitos, fundamentos, alternativas e consequências; decisões finais da etapa 2 sinalizadas |
| Conferência final e escopo Git da Execução 1 | 1 | concluído | 13 arquivos; 34 RF completos, 21 RNF, 32 RN, 12 ADR; links locais válidos; RN-032/D10 com 45 transições coincidentes |
| `07-arquitetura-to-be.md` | 2 | concluído | Monólito modular, Jinja2 e fronteira de parser isolado |
| `21-infraestrutura-docker.md` | 2 | concluído | Onze itens, isolamento, volumes, backup, testes propostos e Windows |
| `08-modelo-de-dados.md` | 2 | concluído | Modelo conceitual/lógico/físico, integridade e migração aditiva |
| `10-integracoes-pncp.md` | 2 | concluído | Contrato de consulta verificado; itens/documentos pendentes por acesso; histórico planejado |
| `11-logistica-e-fornecimento.md` | 2 | concluído | Classes, rubricas v0.1, cotações e matching por atributo |
| `12-simulacao-financeira.md` | 2 | concluído | Decimal, rateio, margem, caixa, garantia e três cenários |
| `09-motor-de-decisao.md` | 2 | concluído | Fórmula v0.1, rubricas, bloqueios e três casos numéricos |
| Diagramas D02, D03, D05, D06, D08, D11, D12 | 2 | concluído | Fontes concluídas: D02, D03, D05, D06, D08, D11, D12; validação formal na Execução 3 |
| Atualização de `18` e `19` | 2 | concluído | Vocabulário físico/numérico e ADR-013–015; ADR-005 fechado |
| `13-seguranca-riscos-conformidade.md` | 3 | concluído | 18 riscos; três fontes legais oficiais consultadas; retenção e responsabilidades |
| `14-plano-de-testes.md` | 3 | concluído | TST-001–055; fixtures, isolamento, comandos propostos e aceites |
| `16-plano-de-migracao.md` | 3 | concluído | Gates G0–G7, backfill, preservação, backup e rollback |
| `15-roadmap-backlog.md` | 3 | concluído | Sete fases, dez épicos, 34 histórias e seis habilitadores |
| `17-matriz-rastreabilidade.md` | 3 | concluído | 34 linhas RF e 21 RNF; CP/RT reservados para consolidação |
| Diagramas D09, D13, D14 | 3 | concluído | Fontes Mermaid e explicações; produzidas em 2026-10-12 |
| `00-prd.md` | 3 | concluído | CP-01–10; 13 tópicos; métricas e Must cobertos |
| `00-trd.md` | 3 | concluído | RT-001–016, origens explícitas, 15 tópicos técnicos |
| `20-resumo-executivo.md` | 3 | concluído | Problema, negócio, arquitetura, fases e riscos sem projeções inventadas |
| `18` e `19` finais | 3 | concluído | Vocabulário final e ADR-016/017; precisão persistida corrigida |
| `README.md` e `diagramas/README.md` | 3 | concluído | Índices, fontes, leitura e limites da validação |
| Validação da sintaxe e coerência final | 3 | concluído | 179 links locais; IDs/cobertura; 45 arcos incluindo início; 14 blocos Mermaid válidos; limitação PlantUML registrada |

## 4. Convenções fixadas

- Rótulos: [CONFIRMADO] é evidência no código, não comprovação de operação; [NÃO-CONFIRMADO] é alegação documental; [PARCIAL] tem implementação incompleta; [PROPOSTO] pertence ao Radar Licita; [LEGADO] é candidato a sair do fluxo principal, sem exclusão nesta missão.
- Últimos IDs: RF-034, RNF-021, RN-032, ADR-017, CP-10, RT-016; TST-055, EP-10, HU-034 e RSK-18. Casos de uso: UC-01 a UC-08. Métricas de negócio: MN-01 a MN-10.
- Schemas novos fixados: procurement, sourcing e commercial no banco de domínio existente. Tabelas/PK/FK/índices em 08; bronze.pncp_responses e control.ingestion_jobs integram infraestrutura atual. judgment_units representa item/lote/grupo/global; sem procurement_lots redundante. Preservar nomes e semântica do legado.
- Score logístico: `v0.1-logistica` (04, seção 9; RN-012). Pesos: envio convencional 0,35; preparação/prazo 0,25; destinos/parcelas 0,15; pós-venda 0,15; envio direto 0,10. Para dimensões conhecidas K: `coverage = soma(w_i, i em K)`; `remote_delivery_score = lower = 100 × soma(w_i × x_i, i em K)`; `upper = min(100, lower + 100 × (1 − coverage))`. Avaliações: 0, 0,5, 1 ou null. Não renormalizar por cobertura; classe A/B/C/D e bloqueios são independentes. Fórmula global v0.1-ranking definida em 09: clamp de 0,30F+0,20L+0,20T+0,15C+0,15E−P em 0..100; P de 0..6 na configuração inicial. Global NULL para bloqueada/pendente; nunca renormalizar ausências. Pesos não herdados da revenda.
- Glossário fixado em 18: contratação/contrato; item/produto do fornecedor; lote/grupo/unidade de julgamento; naturezas de preço; participação eletrônica/entrega remota/envio direto; classes A/B/C/D; cobertura/confiança/score; caixa/capital/margens/ROI; evidência/versão/snapshot. Nomes físicos fixados em 08 e complemento da seção 10 de 18.
- Estados comerciais: E01–E16, nomes e transições em RN-032/D10. “Vencida” significa resultado comercial vencedor, nunca expiração de prazo. Elegibilidade: bloqueada, pendente ou viável para priorização; revisão humana é dimensão independente.
- Política numérica inicial: Decimal ponta a ponta; unitários e quantidades até seis casas; intermediários com pelo menos oito casas e contexto suficiente; totais BRL em centavos com ROUND_HALF_UP; contexto Decimal de 50 dígitos, quantização por linha/obrigação e rateio por maiores restos fixados em v0.1-financeiro (12). Meta de margem verificada após conciliação; ROI ajustado usa G.

## 5. Decisões vigentes

Todos os ADRs descrevem decisões da especificação, sem implantação. Detalhes, alternativas e consequências em `19-decisoes-arquiteturais.md`.

| ID | Título | Estado |
|---|---|---|
| ADR-001 | Execução documental, retomável e restrita à etapa pendente | Aceita para especificação |
| ADR-002 | Evolução incremental de FastAPI e camadas existentes | Aceita para especificação |
| ADR-003 | Banco de domínio existente e metadados Prefect preservados | Aceita para especificação |
| ADR-004 | Prefect, Redis e Crawlee com responsabilidades justificadas | Aceita para especificação |
| ADR-005 | Comparação Jinja2/React sem migração pressuposta | Fechada: Jinja2 no MVP; ADR-013 |
| ADR-006 | Bloqueios, incerteza, logística, ranking e revisão independentes | Aceita para especificação |
| ADR-007 | Simulação decimal por unidade de julgamento e caixa | Aceita para especificação |
| ADR-008 | Histórico preservado e revalidação de análises | Aceita para especificação |
| ADR-009 | Progresso como índice provisório até o README final | Aceita para especificação |
| ADR-010 | Contratos oficiais verificados e acessibilidade explícita | Aceita para especificação |
| ADR-011 | MVP independente de ML, BI externo e histórico futuro | Aceita para especificação |
| ADR-012 | Documentos externos como entrada não confiável | Aceita para especificação |
| ADR-013 | Jinja2 no monólito modular e contenção documental | Aceita para especificação |
| ADR-014 | Identidades, versões e autoridade transacional | Aceita para especificação |
| ADR-015 | Políticas financeiras e ranking condicionado | Aceita para especificação |
| ADR-016 | Preservação histórica com retenção por finalidade | Aceita para especificação |
| ADR-017 | Precisão interna do score persistido | Aceita para especificação |

## 6. Questões em aberto

| ID | Questão | Como resolver | Execução |
|---|---|---|---|
| QA-001 | Parcialmente resolvida: consulta oficial inspecionada; itens/documentos/resultados, fuso, modalidades e semântica de datas ainda pendentes | Reconsultar OpenAPI principal/manual e validar contratos antes da implementação; limitações detalhadas em 10 | 3 e implementação |
| QA-002 | Infraestrutura declarada versus efetivamente operacional | Diagnóstico estático concluído; documentar validação futura em 21/14. Runtime somente em missão posterior de implementação | 2/3 e missão posterior |
| QA-003 | `README.md` exigido na leitura da Execução 2 mas entregue na 3 | Resolvida documentalmente por ADR-009: ler README principal e usar progresso como índice provisório | 1 — resolvida |
| QA-004 | Categorias/UFs piloto, capital, fornecedores e tolerância de risco | Definir parâmetros com operador antes de piloto; conservar desconhecido enquanto não validado | 2/3 |
| QA-005 | Resolvida: Jinja2 com melhoria progressiva no MVP | Comparação em 07, topologia em 21 e ADR-005/013; reavaliar com evidência de usabilidade | 2 — resolvida |
| QA-006 | Resolvida documentalmente: procedimento de migração em 16 | Gates G0–G7, preservação, backfill, backup/restore e rollback definidos; ensaio operacional fica para implementação | 3 — resolvida na especificação |
| QA-007 | Resolvida para especificação: fórmulas v0.1, rubricas, Decimal, rateio e três casos | Documentos 09/11/12; exemplos conferidos aritmeticamente; calibração empírica permanece futura | 2 — resolvida |
| QA-008 | Fontes legais verificadas em 13; aplicabilidade contratual/fiscal por caso permanece aberta | Operador/especialista confirma regime, instrumento, bases fiscais e envio direto antes do piloto | Piloto posterior |
| QA-009 | Mermaid validado/renderizado; PlantUML com limitação declarada | D04/D05 revisados estruturalmente, sem Java/PlantUML no PATH; render formal futuro quando disponível | 3 — validação disponível concluída; limitação residual |
| QA-010 | Metas e capacidade de referência ainda hipotéticas | Medir baseline e amostras do DAN/RNF no piloto, reportando tamanho e incerteza; não tratar metas como resultados | 3 e piloto posterior |

## 7. Divergências entre prompt e repositório real

Execução 3, iniciada em 2026-10-11 e concluída em 2026-10-12: checkout existente, remote/branch/HEAD iguais à linha de base; git diff do código contra a base sem diferenças. Estado inicial e final resumido: `?? docs/licitacoes/`. Sem clone, pull, push, commit, build, coleta, teste de aplicação, mudança de branch, banco ou volume. A data mudou durante o trabalho; datas dos entregáveis anteriores foram preservadas. Documentos 13/14/16/15/17, D09/D13/D14, PRD/TRD/resumo/índices e consolidação de 18/19 concluídos. Correção pontual em 08 e D05 registrada abaixo. O status anterior das Execuções 1/2 nos parágrafos seguintes é histórico.


Execução 2 em 2026-10-11: checkout existente, remote/branch/HEAD reconfirmados; commit igual à base e comparação de código sem diferenças. Início com 13 arquivos não rastreados em docs/licitacoes/. Nenhum clone, pull, alteração de branch ou alteração operacional. O usuário pediu explicitamente iniciar a Execução 2 nesta mesma conversa após a conclusão da 1; essa autorização atual prevaleceu sobre a orientação original de abrir nova sessão, preservando a separação dos entregáveis. Execução 3 não foi iniciada.

Documentos 07/21/08/10/11/12/09 e sete fontes de diagramas foram produzidos. Novas decisões mantêm Jinja2 e infraestrutura útil; parser isolado e schemas públicos são propostos, não existentes. OpenAPI de consulta inclui rota de atualização global; APIs detalhadas de itens/documentos continuam pendentes. A falta de acesso foi registrada, sem inventar endpoints ou declarar integração completa.

Retomada em 2026-10-11: remote, branch e HEAD reconferidos no repositório alvo; mesma base `261b32107ff7b053c68c1df98e88de329c41c0f5`, sem diferenças de código em relação à base e sem alterações rastreadas. Status inicial: `?? docs/licitacoes/`. D01 e D04 já existiam, mas o checklist ainda indicava pendência; foram lidos e validados quanto ao conteúdo antes de marcar conclusão. Nenhum clone, troca de branch ou atualização remota nesta retomada. As datas originais dos documentos foram preservadas como histórico.

Retomada em 2026-10-10: repositório e branch já existiam, HEAD igual ao commit base; status inicial `?? docs/licitacoes/`. Os documentos 01, 02 e 03 estavam concluídos e foram preservados. O arquivo 04 existia completo em aparência, mas ainda não estava concluído no controle. Não houve novo clone, troca de branch ou atualização remota. A comparação de código com a base não mostrou diferenças; a auditoria usa o repositório filho, preservando os arquivos preexistentes do pai.

Divergências técnicas consolidadas do diagnóstico, seção 13:

- Docker/Compose existem [CONFIRMADO], mas operação saudável, dados e volumes atuais seguem [NÃO-CONFIRMADO]. Validação de configuração não valida runtime (ADR-001).
- Há dois serviços PostgreSQL: domínio e metadados Prefect; preservar ambos e usar o banco de domínio atual para Radar, sem criar outro desnecessário (ADR-003).
- Prefect e Crawlee são reais; retries encontrados nos clientes/Crawlee não devem ser atribuídos a decorators Prefect que não os declaram. Redis tem necessidade atual de mensageria Prefect, sem cache de negócio demonstrado (ADR-004).
- ML é [PARCIAL], sem treino/inferência completos; Power BI é [PARCIAL], com exportador/spec mas sem dashboard localizado (ADR-011).
- Jinja2 existe; React não está no repositório alvo. O projeto separado no diretório pai não compõe a base auditada (ADR-005).
- Matching lexical, sinais de marketplace e dinheiro em float não satisfazem verificação técnica/financeira de licitações; motor específico e Decimal são [PROPOSTO] (ADR-006/007).
- Coleta V2 e operação legada coexistem; revisão humana atual não possui os estados públicos. PNCP, evidências de edital e logística A/B/C/D não foram encontrados implementados (ADR-002/008/010/012).
- Leitura do README na Execução 2 versus sua entrega final na 3 foi conciliada em ADR-009. Nenhum README principal foi modificado.

## 8. Correções retroativas

| Documento | Correção | Motivo |
|---|---|---|
| 04, RF-023 | Substituído “modalidade desconhecida” por “critério de julgamento desconhecido” | Modalidade do procedimento e agregação do julgamento são conceitos distintos; ajuste na revisão do arquivo preexistente |
| 04, RF-011 | Cobertura expressa como percentual e fração (`75%`, `0,75`) | Eliminar ambiguidade entre a escala do score 0–100 e a cobertura 0–1 |
| 01, seção 5 (2026-10-11) | Três resultados de elegibilidade; revisão como dimensão independente | Alinhar com 06 e impedir que “revisada” substitua bloqueio/pendência |
| 04, RF-026 e RF-033 (2026-10-11) | RF-024 e RF-028 explicitados como incrementos opcionais | Requisitos Should/fase posterior não devem ser pré-requisitos ocultos do MVP Must |
| 02, seção 11 (2026-10-11) | Acrescentada reconsulta das cinco fontes, preservando histórico | Acessibilidade mudou entre inspeções; não apresentar acesso anterior como validação atual |
| 03, seção 6 (2026-10-11) | Definidos o denominador de MN-07/MN-08 e o tratamento de amostra vazia | Pré-aprovação E05 admite lacunas; não confundir com aptidão final E07 nem declarar percentual sem casos |

| 18/19, complemento da Execução 2 (2026-10-11) | Glossário físico/financeiro e ADR-013–015 acrescentados; ADR-005 e índice fechados para Jinja2 | Atualização prevista pelo prompt; decisões anteriores preservadas com contexto histórico |

| 08, global_score (2026-10-12) | NUMERIC sem escala fixa em vez de NUMERIC(8,4) | Preservar precisão interna da ordenação de 09/12 após persistência; ADR-017 e TST-054 |

| D05 (2026-10-12) | Cardinalidade contratação→versões passou de 1..* para 0..* | Alinhar criação inicial sem snapshot com 08/D06; publicação continua exigindo versão coerente |

## 9. Limitações de validação

- Inspeção estática; aplicação, testes, coletores e migrations não executados.
- Banco, volumes e contagens atuais não consultados.
- Nenhum segredo ou valor de variável de ambiente deve ser reproduzido.
- Versões das ferramentas locais consultadas: Docker 29.8.2 e Compose v5.5.1; isso não confirma saúde dos serviços.
- Validação silenciosa da configuração Compose: código de saída 0. Nenhuma configuração expandida foi exibida; não houve build ou inicialização de serviços.
- Conectividade em 2026-10-10: as cinco URLs oficiais do prompt responderam HTTP 200 via GET direto. Swagger: HTML; OpenAPI consulta: JSON 3.0.1, versão informada 1.0, 12 caminhos; OpenAPI principal: JSON 3.0.1, versão informada 1.0, 124 caminhos; manual e portal Compras.gov.br: HTML. Isso confirma acesso e leitura da identificação dos contratos, não valida parâmetros/endpoints operacionais. Detalhamento na Execução 2. O navegador de pesquisa apresentou erro intermediário no contrato principal/manual, resolvido pela consulta HTTP direta.
- O prompt permite consultas locais de versão, mas exige rotular todo comando Docker documentado como proposto. Registram-se aqui os resultados, sem apresentar esses comandos como procedimentos executáveis.
- Reconsulta nesta retomada, em 2026-10-10: Swagger, contratos OpenAPI (12/124 caminhos) e portal Compras.gov.br responderam HTTP 200 por GET direto. O manual PNCP falhou no envio HTTP e também no intermediário de pesquisa; a consulta anterior bem-sucedida é preservada como histórico, mas a acessibilidade atual do manual está pendente de nova validação na Execução 2. Nenhuma coleta de oportunidades foi executada.
- Reconsulta em 2026-10-11: Swagger, OpenAPI de consulta (12 caminhos) e portal Compras.gov.br responderam HTTP 200 via GET direto. OpenAPI principal: timeout local de 20 segundos; manual: falha no envio HTTP. Intermediário de pesquisa também falhou para os dois. Acesso atual e contratos detalhados desses endereços pendentes; não se conclui indisponibilidade global nem se substitui contrato por conhecimento presumido.

### Conferência de encerramento em 2026-10-11

Retomada final da Execução 1, sem iniciar a 2. Os entregáveis já existentes foram preservados. Conferidos automaticamente: 34 definições RF com os onze campos obrigatórios; 21 RNF, 32 RN e 12 ADR; destinos dos links locais sem arquivos ausentes; commit base presente nos 13 arquivos; 45 transições de RN-032 coincidentes com D10, sem pares ausentes ou excedentes. Revisão estática de `app/main.py`, `app/api/health.py` e `pipelines/collection/prefect_flows.py` reconfirmou routers, limitação da resposta de saúde e ausência de retries nos decorators. HEAD permanece igual à base; comparação de código sem diferenças. Escopo de `git status --porcelain --untracked-files=all`: 13 arquivos novos exclusivamente sob `docs/licitacoes/`; nenhum arquivo rastreado alterado. Nesta retomada de encerramento, somente este controle foi atualizado.

A reconsulta pelo navegador de pesquisa acessou o [Swagger](https://pncp.gov.br/api/consulta/swagger-ui/index.html), o [OpenAPI de consulta](https://pncp.gov.br/api/consulta/v3/api-docs) e o [portal Compras.gov.br](https://www.gov.br/compras/pt-br/cidadao/portal-de-dados-abertos). O [OpenAPI principal](https://pncp.gov.br/api/pncp/v3/api-docs) e o [manual](https://pncp.gov.br/manual/pt-br/latest/) retornaram erro do intermediário. Não se infere indisponibilidade global; contratos detalhados continuam pendentes para a Execução 2. Nenhum endpoint operacional foi acionado.

`mmdc`, `plantuml` e `java` não foram encontrados no PATH nesta conferência. As verificações documentais acima não equivalem a validação formal de sintaxe, renderização ou testes da aplicação. Permanecem as limitações de runtime já registradas. Não foi necessário commit local; o mesmo checkout mantém os arquivos, ainda não rastreados.

### Conferência de encerramento da Execução 2 — 2026-10-11

- Sete documentos e sete fontes de diagramas novos; glossário, ADR e progresso atualizados. Total no diretório: 27 arquivos. Todos possuem commit base; links locais para arquivos existentes, sem destinos ausentes; cercas Markdown equilibradas.
- Referências explícitas RF/RNF/RN/ADR conferidas: 34/21/32/15 definições, nenhum ID citado sem definição. Diagramas conferidos textualmente com módulos/entidades; parser/renderização Mermaid/PlantUML não executados. Ferramentas mmdc/plantuml/java não localizadas no PATH.
- Exemplos validados com aritmética decimal em PowerShell, sem executar código da aplicação: lucro base 1.900; score global 82,98 após penalidade 3; capital conservador 11.254 e otimista 10.305; lucro de lote 480 e falta de capital 4.020; rateio 100,00 conciliado. A meta de 15% requer ajuste do preço após arredondamento; exemplo final em 12 preserva esse achado.
- Revisão do modelo explicitou versão rascunho/publicada, manifesto dos recursos e unicidade parcial de conteúdo, impedindo tratar cabeçalho de lista como conjunto completo de itens/documentos. Cotação tem chave composta por produto/chave/versão. Não há DDL aplicado.
- Fontes oficiais reconsultadas: Swagger, OpenAPI de consulta e portal Compras.gov.br acessíveis; contrato principal excedeu timeout direto de 20 s e falhou no intermediário; manual falhou por envio HTTP e intermediário. Contrato de consulta analisado em detalhe, sem chamar endpoints de contratações. Resultados/links em 10.
- Nenhum comando Docker, build, pipeline, teste de aplicação, migration ou consulta ao banco foi executado na Execução 2. Aritmética documental não comprova um simulador implementado nem valida performance.
- Git final: 27 entradas não rastreadas exclusivamente em docs/licitacoes/; nenhuma alteração rastreada e nenhuma diferença de código frente à base. git diff --check sem erros nos arquivos rastreados; para novos documentos a verificação foi feita por leitura/checagens próprias, sem apresentar esse comando como validação dos não rastreados. Sem commit/push; persistência no mesmo checkout.

### Validação final da Execução 3 — 2026-10-12

- Verificados 40 arquivos Markdown/PlantUML, 179 links locais com arquivos/âncoras válidos, 34 RF (32 Must), 21 RNF, 10 CP, 16 RT com origens e 55 casos planejados. Todo Must aparece em CP/RT e cada RNF possui RT/teste. Relatório: validacao-documental.json.
- RN-032/D10 coincidem: 44 transições entre E01–E16 mais o arco inicial Início→E01 = 45 arcos documentados. Nenhum ausente/excedente. Contagem de 45 nas execuções anteriores inclui o arco inicial.
- 14 blocos Mermaid em 12 fontes passaram em mermaid.parse e mermaid.render locais, versão 11.12.0 em Chromium headless. D09 precisou trocar separador textual antes do aceite. Biblioteca obtida do CDN; fontes não enviadas a serviço de renderização. Resultado em diagramas/validacao-mermaid.json.
- D04/D05 revisados estruturalmente; Java/PlantUML não disponíveis no PATH, portanto sem parse/render formal. Interface GitHub não foi inspecionada. Essa limitação não foi apresentada como teste aprovado.
- Fontes legais oficiais de Lei 14.133, LC 123 e LGPD consultadas no documento 13; aplicabilidade fiscal/jurídica do piloto ainda depende do caso. Lacunas contratuais PNCP de 10 mantidas explicitamente, sem endpoints inventados.
- Inspeção Git final: 42 arquivos não rastreados exclusivamente sob docs/licitacoes/, incluindo dois relatórios JSON; zero alterações rastreadas. git diff --check é apenas complementar: os não rastreados foram lidos/validados por verificação própria. Nenhuma implementação ou operação foi testada.

## 10. Notas para a próxima execução

1. As três execuções documentais estão concluídas; não há Execução 4 prevista.
2. Começar leitura por README, PRD, TRD e resumo executivo.
3. Documentos permanecem no mesmo checkout/branch, sem commit/push automático.
4. A próxima missão é implementação conforme fases/gates, somente quando solicitada.
5. Resolver QA-001 por recurso antes de declarar integração detalhada completa.
6. Definir QA-004/008 com operador/especialista e medir QA-002/010 no piloto.
7. Executar restore/migration/testes somente em missão futura, com destinos isolados e evidência.
8. PlantUML D04/D05 ainda requer renderização formal quando ferramenta estiver disponível.
9. Manter fórmulas v0.1, precisão persistida ADR-017, estados e legado.
10. Não interpretar conclusão da especificação como MVP implementado ou autorização de participação.

## 11. Revisão R1 — revisado em R1

Versão do controle: 0.2-R1. Data: 2026-10-12, America/Sao_Paulo. Prompt aplicado: `prompt-codex-revisao-inteligencia-fornecedores-v2-docker.md`, versão 2.0 de 2026-10-12. Nenhuma R1 anterior encontrada; não houve conciliação com versão anterior.

### Baseline e preparação

- Checkout: `<checkout-local-motor_decisao>`; remote `origin https://github.com/luis7gustavo/motor_decisao.git`.
- Branch preservada: `docs/radar-licita-especificacao`; HEAD/base `261b32107ff7b053c68c1df98e88de329c41c0f5`.
- Estado inicial: 42 arquivos não rastreados em `docs/licitacoes/`, zero alterações rastreadas. Lista literal em [git-inicial.txt](amostras/r1/git-inicial.txt). Documentação anterior sem commit preservada.
- Comparações Git base→HEAD, working tree e index, excluindo `docs/licitacoes/`: sem diferenças. Nenhum valor de ambiente/segredo foi lido. Repositório pai não é o alvo.
- Nenhum AGENTS.md encontrado no checkout, diretório pai ou ancestrais inspecionados até `C:/`.
- Código inspecionado: `pipelines/collection/registry.py` e `collectors/factory.py`; busca em app/pipelines/alembic/entity_resolution não localizou PNCP, Compras.gov.br, CNPJ ou historical_prices implementados. Modelo de 08 é proposta. Busca inicial em src/migrations falhou porque esses diretórios não existem; corrigida para os caminhos reais, sem tomar a falha como evidência de ausência.
- Docker CLI 29.8.2; Compose v5.5.1. Engine inacessível: pipe `dockerDesktopLinuxEngine` inexistente. Nenhum container/pull/build/stack iniciado. Amostragem e validação automatizada não executadas; permitida conclusão documental condicionada pela seção 0.5 do prompt.
- IDs conferidos: RF-034, RNF-021, RN-032, ADR-017, CP-10, RT-016, UC-08, TST-055, MN-10, EP-10, HU-034, TEC-06, RSK-18, D14. Reservas R1-E1: RF-035–038, RN-033–037, ADR-018–020. Extensões dos RNF existentes, sem IDs novos.

### Execuções e checklist

| Execução | Estado | Início | Fim |
|---|---|---|---|
| R1-E1 — fontes, capacidade e regras | concluída | 2026-10-12 | 2026-10-12 |
| R1-E2 — propagação e piloto | concluída | 2026-10-13 02:04 America/Sao_Paulo | 2026-10-13 07:56 America/Sao_Paulo |

| Entregável E1 | Estado |
|---|---|
| T1 baseline/leitura/limites Docker | concluído |
| T2 matriz e fontes oficiais | concluído; contrato detalhado/gates pendentes explicitados |
| T3 manifesto/ledger/perfil/limitações e ferramentas | concluído documentalmente; amostra não executada |
| T4 documento 22 | concluído |
| T5 ADR/glossário | concluído; ADR-018–020 e extensões, glossário R1 |
| T6 RN | concluído; RN-033–037 e extensões 005/006/026/028 |
| T7 RF | concluído; RF-035–038, RF-028 decomposto, extensões e ciclo 012/013 corrigido |
| T8 RNF | concluído; 21 IDs preservados, perfil R1-P1 e extensões |
| T9 conferência/encerramento | concluído; revisão textual/Git, sem validação automatizada |

### Orçamento e evidência

Requisições a endpoints de dados: **0/30**, cumulativo R1-E1, incluindo retomadas. Nenhuma tentativa incerta. Consultas humanas a documentação oficial via ferramenta de pesquisa são registradas separadamente na matriz. Sem denominador amostral; nenhuma fonte aprovada em amostra.

### Decisões, correções e entregáveis finais da E1

- Produzidos [22](22-inteligencia-de-mercado-fornecedores.md), matriz/manifesto/ledger/perfil/relatório em `amostras/r1/` e roteiro `ferramentas-r1/README.md`. Não há script de processamento executável nem imagem/digest obtidos; o roteiro identifica pré-condições e passos ainda necessários.
- Revisados 04/05/06/18/19, README desta pasta e este controle. IDs usados: RF-035–038, RN-033–037, ADR-018–020; nenhum CP/RT/TST/UC/MN/EP/HU/TEC/RSK/diagrama novo na E1. Totais parciais R1: 38 RF (32 Must/6 Should), 21 RNF, 37 RN, 20 ADR.
- RF-028 conserva ID: apresentação básica antecipada ao marco 2-H, Should; ingestão/comparáveis/perfil/candidatos/contato com donos explícitos; inteligência ampla fica fase 6. RF-013/015/cenário não dependem do histórico. Score global, finanças e estados RN-032/D10 preservados.
- Correção retroativa de dependência em RF-013: removido ciclo RF-012↔RF-013 do cadastro básico; RF-012 consome condições quando disponíveis. RF-028 básico deixa de exigir RF-017/027 como pré-requisitos. E2 deve refletir isso em HU-013/HU-028, CP/RT e rastreabilidade.
- Correção identificada e **ainda não propagada**: 08 limita CNPJ a dígitos; R1 exige texto numérico/alfanumérico, original/canônico, DV oficial e estabelecimentos distintos. Não afirmar que há validador fiscal implementado com esse defeito; é divergência da especificação física a corrigir na E2.
- ADR-018 antecipa escopo condicionado; ADR-019 separa identidade/evento/relação comercial; ADR-020 especifica exceção Docker auxiliar. Estendidos ADR-010/011/014/016 e conciliado ADR-001, sem reescrever decisões originais.
- Fonte Compras: manual 2.0 legível descreve recursos úteis; sem contrato executável integral/hash e sem dados observados. PNCP consulta tem evidência histórica de 11/10; principal/manual falharam no intermediário nesta sessão. Receita/Lei/ANPD fundamentam regras documentais. Matriz preserva limites por recurso.
- Categoria comercial não escolhida; papel A4 é hipótese exploratória provisória por atributos comparáveis, sem diversidade/utilidade observadas. Pendências QA-R1-01–05 no relatório: ambiente, contrato, termos/quota, amostra futura e decisões do operador. Não bloqueiam conclusão documental E1, bloqueiam alegação de piloto operacional.

### Verificações e escopo Git no encerramento

Inspeção textual das seções novas e referências: campos completos dos quatro RF novos/RF-028, exemplos positivos/negativos de RN, metas/perfil/baseline/método/fase dos RNF, separação de etapas/cotação, IDs preservados, versões/históricos e rótulos. Revisão de caminhos dos links novos contra arquivos criados/lidos; sem teste automatizado de âncoras. Não houve parser/render de Mermaid/PlantUML, suíte da aplicação, benchmark, cálculo programático ou coleta de dados. Relatórios originais não foram reemitidos nem atribuídos à R1; README/relatório R1 delimitam seu escopo anterior.

Comparações Git finais: HEAD/base iguais; zero diferenças rastreadas fora de `docs/licitacoes/` entre base/HEAD, index e working tree. Lista final em [git-final.txt](amostras/r1/git-final.txt); lista inicial preservada. As 42 entradas documentais originais continuam presentes, com acréscimos somente nesta pasta. Como são arquivos não rastreados, `git diff` sozinho não mede suas alterações; o conjunto autorado está enumerado acima. Nenhuma mudança no código, README principal, Compose/Dockerfiles, ambiente, migrations, dados ou volumes; sem commit/push/pull/reset/clean/stash/troca de branch. Nenhum contato/ato comercial.

### Propagação pendente e notas para R1-E2 (12 linhas)

1. Executar somente E2 em nova sessão, mesmo checkout/branch e prompt v2.0; E1 está concluída.
2. Ler 22, matriz/relatório, RF/RNF/RN revisados e ADR-018–020 antes de propagar.
3. Atualizar 08/16: identidade alfanumérica, eventos/vínculos/candidatos, migração aditiva sem fusão de filiais.
4. Atualizar 07/00-trd/21: fronteiras, Docker de produto, RT e distinção da ferramenta auxiliar.
5. Preservar fórmulas e exemplos em 09/12; acrescentar os dois casos sintéticos pedidos pelo prompt.
6. Propagar evidências e jornada em 10/11; ampliar riscos 13 e fixtures/testes 14 em Docker futuro.
7. Atualizar 15: marco 2-H e fase 3, dependências/aceites; preservar fases/EP/HU/TEC.
8. Criar 23: plano de piloto, métricas, categoria a decidir pelo operador e gates; não executar contato.
9. Atualizar 01/03/00-prd/17/18/19/20/README, todos Must revisados ligados a RT/testes.
10. Criar D15; revisar D01–09/D11/D12 conforme impacto, preservar RN-032/D10 e índices.
11. Sem nova amostragem em E2; 0/30 cumulativo. Nova coleta exige missão posterior e gates resolvidos.
12. Validar documentos/diagramas só em Docker isolado; se indisponível, registrar limitação, sem aprovação fictícia.

### Histórico de revisões

| Data | Versão | Alteração |
|---|---|---|
| 2026-10-12 | 0.2-R1 | Acrescentado controle R1 sem apagar resultados originais; baseline e limitação Docker verificadas |
| 2026-10-12 | 0.2-R1 E1 | Entregáveis documentais concluídos, zero amostras, correções/decisões/gates e notas E2 registrados |


### Retomada R1-E2 — 2026-10-13, America/Sao_Paulo

Prompt: conteúdo integral recebido no agendamento, conciliado com v2.0 da E1; mesmas restrições, sem reiniciar E1. Três execuções originais e E1 confirmadas concluídas. Checkout/remote/branch/HEAD iguais à baseline R1. Estado inicial literal em `amostras/r1/e2/git-inicial.txt`; a própria entrada desse arquivo aparece como novo arquivo de registro. Todos os documentos anteriores não rastreados preservados. Comparações base→HEAD, index e working tree fora de docs/licitacoes sem diferenças. Nenhum segredo lido.

Instruções: nenhum AGENTS no checkout ou ancestrais aplicáveis. Uma busca excessivamente ampla em outros chats encontrou erros de acesso em dependências antigas; esses diretórios não são ancestrais nem escopo do checkout, e não sustentam conclusões sobre instruções locais. Registro/collector factory/health inspecionados sem execução; integração pública permanece [PROPOSTO].

Docker CLI/Engine 29.8.2 e Compose v5.5.1 acessíveis agora: mudança de disponibilidade da ferramenta, sem reabrir amostragem E1. Aquisição de Python oficial e Mermaid CLI do projeto upstream, separada de dados. Zero GET de dados na E2; cumulativo 0/30. Digest e validações efetivas serão registrados em ferramentas-r1 e relatório E2.

IDs conferidos no conjunto: RF-038, RNF-021, RN-037, ADR-020, CP-10, RT-016, UC-08, TST-055, MN-10, EP-10, HU-034, TEC-06, RSK-18, D14. Reservados para E2: RT-017–019, UC-09, TST-056–071, MN-11–18, HU-035–038, RSK-19–23 e D15. CP/EP existentes serão estendidos, sem novos IDs nessas famílias.

| Entregável E2 | Estado |
|---|---|
| T1 modelo/migração | concluído; 08/16, sem DDL |
| T2 arquitetura/TRD/Docker | concluído; 07/00-trd/21, RT-017–019 |
| T3 motor/finanças | concluído; 09/12, exemplos anteriores preservados |
| T4 integrações/fornecimento/riscos/testes | concluído; 10/11/13/14, 16 testes apenas especificados |
| T5 roadmap/backlog | concluído; marco 2-H, HU-035–038 e dependências conciliadas |
| T6 documento 23 | concluído; piloto condicionado, categoria pendente, MN-11–18 |
| T7 sínteses/rastreabilidade/diagramas | concluído; Mermaid renderizado, PlantUML textual |
| T8 coerência/encerramento | concluído; validação offline/Git, limitações registradas |







### Encerramento R1-E2 — revisado em R1

2026-10-13 07:56 America/Sao_Paulo. Entregues 23 e D15; propagação em 00-prd/00-trd/01/03–22, README/índices e diagramas afetados. 02, D10, D13 e D14 preservados. Relatórios originais receberam apenas adendo de escopo, sem atribuir resultados antigos à R1. Relatório E1 ganhou nota temporal. Inventário exato de fontes alteradas/novas e hashes em amostras/r1/e2/validate.json; ferramentas e saídas enumeradas no README auxiliar.

IDs efetivamente usados: RT-017–019, UC-09, TST-056–071, MN-11–18, HU-035–038, RSK-19–23, D15. Nenhum novo RF/RNF/RN/ADR/CP/EP/TEC. Totais: 38 RF (32 Must/6 Should), 21 RNF, 37 RN, 20 ADR, 10 CP, 19 RT, 9 UC, 71 testes planejados, 18 MN, 10 EP, 38 HU, 6 TEC, 23 riscos e 15 diagramas. ADR-018–020 propagados; sem alterar fórmula/pesos, estados comerciais ou somar histórico ao score.

Correções retroativas: CNPJ exclusivamente numérico da especificação 08 substituído por identidade textual com original/DV e pendência explícita, sem afirmar validador implementado; dependências HU-013/RF-028 conciliadas; contagens antigas no TRD atualizadas; piloto de pesquisa separado do piloto completo do MVP. Nenhuma fusão automática de filial, histórico como custo/capacidade ou escolha comercial presumida.

Verificação Docker final: 237 links locais, campos/IDs/referências e 32 Must conferidos pelo auxiliar; zero erros, nenhum arquivo da baseline ausente, corpos originais de 09/12/D10 preservados. Quinze blocos Mermaid renderizados com sucesso e hashes reconfirmados sem mudança de fonte; D04/D05 PlantUML somente revisão textual. Ferramentas Python/Mermaid fixadas por digest, scripts/limites/comandos sanitizados e falhas reais em ferramentas-r1/README.md e amostras/r1/e2/relatorio-validacao.md. Validação não equivale à suíte de aplicação, negócio, DV ou aprovação de fonte. Retomada após intervalo manteve baseline; conferência final não refez amostra.

Git final em amostras/r1/e2/git-final.txt. Branch e HEAD permanecem os da baseline; comparações base→HEAD, index e working tree fora de docs/licitacoes vazias. Todas as entradas iniciais continuam presentes e alterações desta missão ficam nessa pasta; documentos preexistentes não rastreados mantidos, sem commit. Nenhuma alteração operacional, contato, escrita de banco, teste da aplicação, coleta ou mudança de branch. Orçamento cumulativo E1 0/30; E2 zero chamadas de dados.

### Notas posteriores à R1 (7 linhas)

1. R1-E1 e R1-E2 concluídas; não repetir a missão documental automaticamente.
2. Categoria/UF/janela e recursos comerciais dependem do operador; papel A4 é só hipótese exploratória.
3. Fechar contratos por recurso/atributo, termos/quota e suporte alfanumérico; QA-R1-02–05 seguem abertas.
4. QA-R1-01 parcialmente resolvida para ferramenta documental; runner de amostragem e produto não preparados.
5. Nova amostra exige missão posterior delimitada; nenhuma observação ou conversão foi obtida aqui.
6. Implementar em ordem: isolamento/contratos, identidade/fonte, deduplicação/qualidade, comparáveis, candidatos, contato/cotação, medição.
7. Gates técnicos/comerciais de 23 e testes futuros permanecem obrigatórios antes de operação/ampliação.

### Histórico de revisões da retomada

2026-10-13 — 0.3-R1-E2: propagação e piloto documentais concluídos, evidências de validação auxiliar, escopo Git preservado e pendências de implementação explícitas.
