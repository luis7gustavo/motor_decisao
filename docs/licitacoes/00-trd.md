# 00 — TRD: requisitos técnicos do Radar Licita

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-12 (America/Sao_Paulo).  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: rascunho para revisão do operador; requisitos [PROPOSTO], sem implantação nesta missão.

## 1. Escopo e vínculo com o produto

Este documento consolida a solução para CP-01–CP-10 do [PRD](00-prd.md). Os contratos detalhados permanecem em 07–12/21; RT não substitui RF/RNF/RN. Evolução incremental, monólito modular e persistência existente; sem microsserviços prematuros. Rastreabilidade por RF, entidade, UC, teste e fase em [17](17-matriz-rastreabilidade.md).

## 2. Linha de base AS-IS

[CONFIRMADO] FastAPI/Jinja2, PostgreSQL/SQLAlchemy/Alembic, Bronze/Silver/Gold, Prefect/Crawlee, Redis no controle, revisão transacional e snapshots. [PARCIAL] ML/BI e matching para o novo uso. [LEGADO] ranking de revenda como modelo principal do domínio público. [PROPOSTO] PNCP, edital/evidência, classes A/B/C/D, Decimal/caixa e estados comerciais públicos. [NÃO-CONFIRMADO] saúde operacional, volumes instalados, revisão Alembic aplicada e desempenho. Evidências e matriz de reaproveitamento em [02](02-diagnostico-as-is.md), base reconfirmada sem mudança de código na execução 3.

## 3. Arquitetura-alvo

Módulos: ingestão oficial, contratações, documentos, fornecimento, matching, logística, finanças, decisão, revisão/comercial e consulta/UI. FastAPI recebe mutações com ator/versão; serviços coordenam transações, núcleo de cálculo recebe snapshot e não faz HTTP. Worker Prefect consulta fontes e coordena spool; parser isolado processa arquivos sem rede/DB/segredos. PostgreSQL é autoridade; Gold é projeção reconstruível. [D02](diagramas/D02-containers.md), [D03](diagramas/D03-motor.md), [D12](diagramas/D12-implantacao.md), ADR-002/013/014.

## 4. Stack e alternativas

Manter FastAPI, PostgreSQL, SQLAlchemy e Alembic para preservar contratos/transações; metadados Alembic incompletos exigem revisão manual de migration. Prefect permanece orquestrador, Redis apenas enquanto necessário à mensageria; PNCP por HTTP, Crawlee/browser preservados para fontes comerciais pertinentes. Jinja2 com melhoria progressiva mantém uma stack e cálculo Decimal no servidor. React/TypeScript foi comparado em 07 e rejeitado para o MVP: reavaliar com evidência de estado interativo/usabilidade que melhoria localizada não resolva, então novo ADR e container próprio. Reescrita total, banco adicional e APScheduler paralelo foram rejeitados por custo/duplicação, sem benchmark alegado.

## 5. Requisitos técnicos

Todos os RT são [PROPOSTO]; cada origem é explícita. Testes citados são casos planejados de 14. RT-007/009 suportam Should; demais são necessários ao escopo Must conforme fase.

| ID | Descrição verificável | Origem | Verificação / aceitação | Fase |
|---|---|---|---|---|
| RT-001 | Adapter de consulta com contrato versionado, Bronze, identidade, paginação, checkpoint durável e frescor | RF-001, RF-002, RF-003, RF-004, RF-005; RNF-007, RNF-008, RNF-018; RN-026, RN-028 | TST-001–005/041/042/052; replay não duplica, erro não apaga, atualização invalida | 1 |
| RT-002 | Downloader restrito, documentos por hash, parser isolado e evidências revisáveis | RF-006, RF-007, RF-008, RF-009; RNF-003, RNF-004; RN-023, RN-026, RN-027 | TST-006–009/037/038; nenhuma conexão proibida, ambiguidade permanece pendente | 2 |
| RT-003 | Classificação D→C→B→A e score logístico sem renormalizar ausências | RF-010, RF-011, RF-012; RN-010, RN-011, RN-012, RN-013, RN-014 | TST-010–012; L=75 e cobertura 0,75 com prazo desconhecido; D13 | 2 |
| RT-004 | Cadastro/CSV idempotentes e produtos/cotações versionados por fornecedor | RF-013, RF-014, RF-015, RF-016; RN-005, RN-006, RN-014 | TST-013–016; validade/quantidade/unidade explícitas; estoque não é reserva | 3 |
| RT-005 | Matching por atributo com unidade/evidência e revisão ligada às versões | RF-017, RF-018, RF-019; RN-015, RN-024, RN-028 | TST-017–019; conflito obrigatório impede confirmação, versão nova exige revisão | 3 |
| RT-006 | Simulador Decimal contexto 50 dígitos, limites, ROUND_HALF_UP por linha/obrigação e rateio por maiores restos; caixa da unidade completa | RF-020, RF-021, RF-022, RF-023; RNF-010; RN-007, RN-008, RN-009, RN-016, RN-017, RN-018, RN-019, RN-020, RN-021 | TST-020–023/044; soma/rateio exatos, garantia separada, meta verificada após quantização; contrato 12 | 4 |
| RT-007 | Sensibilidades adicionais e metas comparadas com recálculo integral | RF-024; RN-021 | TST-024; três cenários de 12; recurso opcional, sem bloquear Must | 4 |
| RT-008 | Checks antes de score; global NULL para bloqueada/pendente, versão de parâmetros e replay; preservar precisão interna ao persistir | RF-025, RF-026, RF-027; RNF-016, RNF-020; RN-027, RN-028, RN-029 | TST-025–027/050/054; caso hardware 82,98, lote bloqueado, presença pendente; ordenação estável após gravar | 4; RNF transversal desde 2 |
| RT-009 | Apresentar preços separados por natureza, qualidade e contexto; agregado somente suficiente (revisado em R1) | RF-028; RN-005, RN-015, RN-035; RNF-010, RNF-016, RNF-020 | TST-028/061–064; n, exclusões, janela e versão; nenhuma imputação de custo | 2-H básico; 6 ampliado |
| RT-010 | UI/revisão e eventos comerciais atômicos com estado, motivo, ator, versão e fatos externos separados | RF-029, RF-030, RF-031, RF-032, RF-033, RF-034; RNF-005, RNF-015; RN-030, RN-031, RN-032 | TST-029–034/039/049; 45 transições e pares proibidos, evidência em duas ações; pré-E07 revisado nos módulos | 5; controles transversais desde 1/2 |
| RT-011 | Docker-first com perfis, healthchecks, LF, volumes persistentes e nenhuma dependência de runtime/caminho pessoal do host | RNF-019, RNF-017; RF-001, RF-006 | TST-053; topologia e onze tópicos de 21; testes/restore sem volumes globais operacionais | 1–5 |
| RT-012 | Loopback, mutações protegidas, minimização, redaction, controle de destinos e contenção documental | RNF-001, RNF-002, RNF-003, RNF-004; RN-024, RN-026 | TST-035–038; zero efeito não autorizado/vazamento/egress do parser; política de 13 | 1–2 |
| RT-013 | Readiness real e observabilidade de jobs, correlação, falhas, frescor e filas | RNF-006, RNF-013; RF-005, RF-027 | TST-040/047; DB falho sem prontidão enganosa, alerta em até 60 s; rede externa não derruba leitura local | 1–5 |
| RT-014 | Migração aditiva, FK restritiva, backup coerente banco/arquivos, ensaio isolado e retorno de imagem compatível | RNF-014, RNF-017, RNF-005, RNF-008; RN-028, RN-030 | TST-039/048/051; G0–G7 de 16; preservar 100% baseline; RPO<=24 h/RTO<=4 h sujeitos a medição | 1–5 |
| RT-015 | Contratos modulares e testes determinísticos em container com isolamento verificado antes de escrita | RNF-011, RNF-012; RF-003, RF-021, RF-025, RF-030 | TST-045/046 mais todos aplicáveis; destino operacional rejeitado antes da suíte | 1–5 |
| RT-016 | Consulta paginada, processamento por lote, orçamento de recursos e recusa controlada sob saturação | RNF-009, RNF-021; RF-002, RF-020, RF-033 | TST-043/055; perfil 05 seção 1.1 e metas abaixo, sem erro funcional mascarado por resposta rápida | 1–5 |

## 6. Infraestrutura Docker-first

[21](21-infraestrutura-docker.md) é a fonte dos onze tópicos: containers/responsabilidades, comunicação, redes/portas, volumes, variáveis, migrations, backup/restore, testes, implantação incremental e legado. API/worker acessam banco de domínio; Prefect usa banco próprio/Redis; parser sem rede recebe spool limitado. API/admin em loopback, bancos sem publicação no uso normal. Preservar volumes globais existentes, criar documentos/spool e destinos de teste independentes. Configuração lista apenas nomes de variáveis; segredos não entram em docs/imagens/logs.

Dev pode usar reload/bind relativo; produção local usa imagem imutável. Migration é tarefa única com lock/gate, separada de bootstrap; healthcheck de dependência não dispensa revisão instalada. Backups coerentes com arquivos, cópia independente, restore isolado e reconciliação de jobs. Não alterar Compose atual nesta missão. Comandos em 14/21 são propostos, não executados.

## 7. Dados e evolução

Schemas novos procurement/sourcing/commercial no mesmo PostgreSQL. Bronze preserva original, domínio mantém versões e eventos, Gold projeta. UUID/PK/FK restritivas, NUMERIC para dinheiro, JSON decimal em strings e snapshots com schema versionado. Publicação exige manifesto completo e atual; dados ausentes são NULL com motivo. lock_version, lease e fencing impedem publicação obsoleta. Modelo físico em [08](08-modelo-de-dados.md); global_score conserva precisão interna e arredonda somente na exibição (ADR-017). Não converter revisão de compra para participação. Expansão/backfill/validação/corte em [16](16-plano-de-migracao.md).

## 8. Integração PNCP

Verificação de contrato registrada em 2026-10-11 no [10](10-integracoes-pncp.md): Swagger, OpenAPI de consulta e portal de dados acessíveis; OpenAPI principal/manual falharam no ambiente. Rotas detalhadas de itens/anexos/resultados e semânticas de datas continuam pendentes, sem contrato inventado. Política proposta de atualização a cada seis horas, sobreposição e reconciliação; quota local não é limite oficial. Erros permanentes sem retry cego, transitórios com orçamento total de até três tentativas e Retry-After respeitado. Leitura pública não autoriza manutenção autenticada.

## 9. Segurança e conformidade

Aplicar RT-012 e matriz de 23 riscos de [13](13-seguranca-riscos-conformidade.md). Documentos não são instruções; parser não pode executar conteúdo nem promover saída hostil a regra. PDFs/cotações/dados pessoais têm acesso/retenção por finalidade e revisão; histórico referenciado não é excluído automaticamente. Regime, habilitação e parâmetros fiscais são evidências do caso; lei consultada não equivale a parecer. Auditoria tem autor/motivo/instante/versões; nenhuma credencial no snapshot.

## 10. Desempenho, capacidade e observabilidade

Metas iniciais calibráveis de 05: p95 busca<=2 s, painel<=3 s, simulação até 100 itens/três cenários<=5 s; 500 requisições após 20 de aquecimento e três rodadas no perfil hipotético de 4 vCPU/8 GiB Docker. Base: 10 mil contratações, 100 mil itens, 50 mil produtos, 10 mil análises e até 10 GiB documentos. Não extrapolar para limite máximo do simulador. Disponibilidade local>=99% na janela declarada; alerta de falha<=60 s, 95% das alterações visíveis<=30 min da obtenção. Fonte externa medida separadamente.

Download: 50 MiB, três redirects, 10 s conexão/120 s total; parser: 1 vCPU, 1 GiB, 120 s, 1.000 páginas. Armazenamento alerta>80%, suspende novas tarefas cuja projeção exceda 90%; fila limitada, nenhum descarte automático de histórico. Registrar p50/p95/p99, erros, memória, fila, backlog e frescor. Baseline operacional desconhecida, nenhum benchmark executado.

## 11. Estratégia de testes

[14](14-plano-de-testes.md) define TST-001–071: unitários de domínio, contratos sintéticos, integração PostgreSQL isolada e jornadas UI. Testes via container com credenciais/volumes exclusivos, guardas pré-escrita e relógio fixo. TST-024/028 acompanham Should; demais são condições por fase. Comparar centavos com oráculo independente, todos os bloqueios/estados, falhas de rede, PDF adverso, concorrência, restore e legado. Não usar PNCP ao vivo na suíte determinística. Relatório deve distinguir planejado de executado.

## 12. Migração e rollback

G0 inventário real → G1 backup/restore → G2 expansão em cópia → G3 backfill → G4 aprovação da release → G5 corte → G6 ativação gradual → G7 piloto. Preferir retornar imagem compatível com schema aditivo; incompatibilidade exige escrita suspensa e recuperação isolada com reconciliação de eventos pós-backup. Sem downgrade destrutivo automático, exclusão de tabelas ou recriação dos volumes. Schedules suspensos até reconciliação. Preservação e gates em 16.

## 13. Índice de ADRs

[19](19-decisoes-arquiteturais.md) é o registro normativo: ADR-001 escopo documental; 002 evolução modular; 003 bancos preservados; 004 Prefect/Redis/Crawlee; 005 comparação da UI; 006 guardas independentes; 007 Decimal/caixa; 008 histórico; 009 índice provisório; 010 contratos oficiais; 011 MVP sem ML obrigatório; 012 documentos não confiáveis; 013 Jinja2/parser isolado; 014 identidades/autoridade; 015 políticas numéricas; 016 retenção/preservação; 017 precisão persistida do ranking. Alternativas rejeitadas e consequências ficam em cada ADR; detalhes atuais prevalecem sobre planejamento histórico das execuções.

## 14. Riscos técnicos e dívida

Saúde ainda não comprovada; /health atual pode responder 200 degradado; dependências/imagens precisam reprodução; teste legado usa TRUNCATE com guarda nominal fraca; volumes globais impedem isolamento apenas pelo project name. Contratos detalhados PNCP e fuso pendentes; parser/ingestão públicos inexistentes; float atual não atende finanças. Controle de riscos e testes são propostos, não soluções já instaladas. Contratos incertos não devem ser preenchidos por conhecimento presumido.

## 15. Questões abertas

QA-001 contrato por recurso e datas; QA-002 runtime/restore real; QA-004 recorte/capital e fornecedores; QA-008 condições jurídicas/fiscais do caso; QA-010 capacidade/metas e calibração. Resolver com fonte oficial, ensaio isolado e decisão do operador/especialista conforme natureza. QA-006 tem procedimento definido em 16, não execução de migração. Limites de validação documental/diagramas no progresso e índices.


## 16. Requisitos técnicos do incremento condicionado — revisado em R1

Revisão 0.3-R1-E2, 2026-10-13. [PROPOSTO]. CP-09 passa a abranger RF-028/035–037; CP-04 inclui RF-038 opcional. Não ampliar a obrigação universal do MVP. Os controles são necessários quando o incremento é habilitado. RT-009 permanece dono da apresentação; os três RT novos têm responsabilidades distintas.

| ID | Descrição verificável | Origem | Verificação / aceitação | Fase |
|---|---|---|---|---|
| RT-017 | Adapter por recurso validado, Bronze multirrecurso/jobs/checkpoints; identidade fiscal textual, evento/versionamento, correspondências e precedência por atributo | RF-003, RF-004, RF-005, RF-035; RN-026, RN-028, RN-033, RN-034; RNF-005, RNF-007, RNF-008, RNF-010, RNF-011, RNF-013, RNF-018, RNF-021 | TST-056–060/065/069; replay conserva IDs, equivalência conta uma vez por etapa, relacionado não soma; erro não perde último válido, nenhum recurso desconhecido é chamado | 1 nas extensões Must; 2-H no histórico |
| RT-018 | Comparação versionada item×preço por atributos/unidade, candidatos por estabelecimento com razões; projeções descartáveis, sem interferência no ranking | RF-036, RF-037, RF-028; RN-005, RN-015, RN-024, RN-027, RN-033–037; RNF-009, RNF-016, RNF-020 | TST-059/061–066/068/071 e TST-043 no perfil R1-P1; nenhum fator inventado, sem agregado insuficiente, sem capacidade inferida; p95<=2 s é meta não medida | 2-H |
| RT-019 | Ledger manual de contato/disposição/disponibilidade, vínculo ao supplier_id e cotação versionada por escopo, métricas sem viés de só contar sucessos | RF-013, RF-015, RF-038; RN-006, RN-024, RN-026, RN-030, RN-031, RN-033, RN-036, RN-037; RNF-002, RNF-005, RNF-012, RNF-015, RNF-016, RNF-020 | TST-059/066–068/071; cotação de outra filial/quantidade expirada não confirma; zero envio externo; estados não alteram RN-032, denominador zero não aprova meta | 3 |

Extensões dos RT existentes: RT-001 aplica quotas/checkpoints também a RT-017; RT-004 usa identidade por estabelecimento e disponibilidade por escopo (TST-059/060/067); RT-006 e RT-008 devem passar TST-068, preservando fórmulas e preço público fora de custo. RT-011/015 exigem imagens/digests/locks e guarda pré-escrita (TST-070/071); RT-012 aplica minimização a histórico/contato (TST-071); RT-013 mede qualidade com categorias exclusivas (TST-063/069); RT-014 preserva todos os novos vínculos na expansão e restore (TST-051/056/059); RT-016 usa R1-P1 no benchmark TST-043/055. Nenhum teste do produto foi executado nesta revisão.

Produção futura segue 21 §12; imagens/dependências do produto ainda precisam lock/digest e ensaio, não herdam a imagem Python auxiliar. Novo egress apenas no worker HTTP para hosts/rotas oficiais validados, sem portas novas. Readiness local depende de DB/schema, não de fonte histórica; métricas distinguem fonte indisponível, dado parcial e serviço local degradado. Teste/restore têm rede, banco, volume e credenciais próprios; guarda por identidade/destino falha antes da primeira escrita. Backup/retorno aditivos em 16 §7; nenhum bootstrap/serviço operacional executado na R1.

Documentação oficial reutilizada da E1 mantém datas/escopo em matriz-fontes; nenhuma evidência amostral nova. ADR-018/019/020 estendem índice da seção 13. Total atual: 19 RT; casos planejados TST-001–071, com controles transversais dos RNF, sem confundir validação documental e aprovação de aplicação.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: RT-017–019 e extensões dos RT existentes, fase 2-H de RT-009, Docker/identidade/qualidade/jornada e independência do score.

