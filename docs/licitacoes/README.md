# Radar Licita — índice e guia de leitura

Versão: 0.1. Data: 2026-10-12 (America/Sao_Paulo).  
Revisão: 0.2-R1, 2026-10-12 (America/Sao_Paulo). Revisado em R1-E1.
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: especificação [PROPOSTO], rascunho para revisão do operador.

Esta pasta documenta a evolução proposta do SILLO para inteligência comercial em licitações públicas. Não é anúncio de migração implementada. O README principal e a aplicação permanecem preservados.

> **Revisão R1-E2:** propagação documental e plano em [23](23-especificacao-piloto-fornecedores.md), com [D15](diagramas/D15-descoberta-fornecedores.md). Histórico básico/candidatos antecipados opcionalmente ao marco 2-H; contato junto da fase 3. Gates de contratos, dados, termos/quota, categoria e ambiente do produto permanecem. E1: amostra não executada, 0/30 chamadas. E2: nenhuma nova amostra; validação auxiliar offline discriminada no [relatório](amostras/r1/e2/relatorio-validacao.md). Conclusão documental não habilita piloto.

## Comece por aqui

1. [PRD — produto, capacidades e aceite do MVP](00-prd.md).
2. [TRD — arquitetura e requisitos técnicos](00-trd.md).
3. [Resumo executivo — diagnóstico, negócio, riscos e entrega](20-resumo-executivo.md).

Para implementação, seguir [roadmap/backlog](15-roadmap-backlog.md), [migração](16-plano-de-migracao.md), [testes](14-plano-de-testes.md) e [rastreabilidade](17-matriz-rastreabilidade.md). Antes de usar contrato de fonte ou regra fiscal/jurídica, resolver as pendências pertinentes.

## Documentos

| Arquivo | Conteúdo |
|---|---|
| [00-prd.md](00-prd.md) | 00 — PRD: requisitos de produto do Radar Licita |
| [00-trd.md](00-trd.md) | 00 — TRD: requisitos técnicos do Radar Licita |
| [01-visao-produto.md](01-visao-produto.md) | 01 — Visão do produto Radar Licita |
| [02-diagnostico-as-is.md](02-diagnostico-as-is.md) | 02 — Diagnóstico técnico AS-IS do SILLO |
| [03-analise-negocio-dan.md](03-analise-negocio-dan.md) | 03 — Documento de Análise de Negócio |
| [04-requisitos-funcionais.md](04-requisitos-funcionais.md) | Radar Licita — requisitos funcionais |
| [05-requisitos-nao-funcionais.md](05-requisitos-nao-funcionais.md) | 05 — Requisitos não funcionais |
| [06-regras-de-negocio.md](06-regras-de-negocio.md) | 06 — Regras de negócio |
| [07-arquitetura-to-be.md](07-arquitetura-to-be.md) | 07 — Arquitetura TO-BE |
| [08-modelo-de-dados.md](08-modelo-de-dados.md) | 08 — Modelo de dados e evolução inicial |
| [09-motor-de-decisao.md](09-motor-de-decisao.md) | 09 — Motor determinístico de decisão e ranking |
| [10-integracoes-pncp.md](10-integracoes-pncp.md) | 10 — Integração PNCP e preços históricos |
| [11-logistica-e-fornecimento.md](11-logistica-e-fornecimento.md) | 11 — Logística, fornecedores e matching técnico |
| [12-simulacao-financeira.md](12-simulacao-financeira.md) | 12 — Simulação financeira e capital |
| [13-seguranca-riscos-conformidade.md](13-seguranca-riscos-conformidade.md) | 13 — Segurança, riscos e conformidade |
| [14-plano-de-testes.md](14-plano-de-testes.md) | 14 — Plano de testes |
| [15-roadmap-backlog.md](15-roadmap-backlog.md) | 15 — Roadmap e backlog priorizado |
| [16-plano-de-migracao.md](16-plano-de-migracao.md) | 16 — Plano de migração, implantação e recuperação |
| [17-matriz-rastreabilidade.md](17-matriz-rastreabilidade.md) | 17 — Matriz de rastreabilidade |
| [18-glossario.md](18-glossario.md) | 18 — Glossário do Radar Licita |
| [19-decisoes-arquiteturais.md](19-decisoes-arquiteturais.md) | 19 — Decisões arquiteturais — consolidação final |
| [20-resumo-executivo.md](20-resumo-executivo.md) | 20 — Resumo executivo |
| [21-infraestrutura-docker.md](21-infraestrutura-docker.md) | 21 — Infraestrutura Docker-first |
| [22-inteligencia-de-mercado-fornecedores.md](22-inteligencia-de-mercado-fornecedores.md) | R1-E1: escopo antecipado, identidade, comparação, candidatos e gates |
| [23-especificacao-piloto-fornecedores.md](23-especificacao-piloto-fornecedores.md) | R1-E2: categoria pendente, gates, jornada, métricas e plano de piloto |
| [Matriz R1](amostras/r1/matriz-fontes.md) | Evidência documental separada de amostra/suporte por recurso |
| [Ferramentas R1](ferramentas-r1/README.md) | Procedimento Docker auxiliar proposto, não executado |
| [_progresso.md](_progresso.md) | Linha de base, execuções, correções e questões abertas |
| [Diagramas](diagramas/README.md) | D01–D15, fontes editáveis, visualização e limites |

## Convenções e cobertura

[CONFIRMADO] significa evidência no código, sem prova de operação saudável; [PARCIAL] indica implementação incompleta; [NÃO-CONFIRMADO] identifica alegação não demonstrada; [PROPOSTO] descreve evolução; [LEGADO] preserva semântica antiga. Glossário em 18, decisões em 19. Conjunto original: 34 RF (32 Must), 21 RNF, 32 RN, 10 CP, 16 RT, 17 ADRs, 14 diagramas e 55 testes planejados. R1-E1: 38 RF (32 Must, 6 Should), 21 RNF estendidos, 37 RN e 20 ADRs; R1-E2: 19 RT, 9 UC, 71 TST planejados, 18 MN, 38 HU, 23 RSK e 15 diagramas; CP-01–10/EP-01–10 e TEC-01–06 reutilizados. IDs preservados, sem renumeração.

## Validação e limitações

Execução documental: inventário/reconferência de Git, vínculos locais, IDs, cobertura Must→CP/RT e RNF→RT, revisão de regras/fases, estados e fórmulas. Os 14 blocos Mermaid distribuídos em 12 diagramas foram analisados e renderizados localmente com Mermaid 11.12.0; D10 contém três vistas. D04/D05 são PlantUML e receberam revisão estrutural; Java/PlantUML não foram localizados no PATH, sem validação formal desses dois. Não houve teste de renderização na interface do GitHub. Evidências em [relatório Mermaid](diagramas/validacao-mermaid.json) e [relatório documental](validacao-documental.json).

O parágrafo anterior e os dois relatórios referem-se somente à missão original, antes da R1. Não abrangem novos requisitos/documentos. R1-E1 recebeu revisão textual e inspeção Git; sem checagem automatizada/renderização em razão do Engine inacessível naquela sessão. R1-E2 teve ambiente auxiliar offline, com escopo/resultados no relatório E2; isso não valida TST da aplicação. O [relatório R1](amostras/r1/relatorio.md) preserva essa distinção.

Nenhuma suíte de aplicação, coleta, build, migration ou restauração foi executada. Banco/volumes/saúde e metas seguem não medidos. PNCP detalhado ainda tem lacunas registradas em 10; fontes legais consultadas em 13 não substituem análise do caso. Correções na consolidação: precisão persistida do score em 08 e cardinalidade inicial em D05, registradas em ADR-017/progresso.

## Linha de base e próxima etapa

Checkout existente de luis7gustavo/motor_decisao, branch docs/radar-licita-especificacao, commit indicado no cabeçalho. Não houve clone, pull, push ou alteração de arquivos operacionais. Persistência no mesmo checkout, sem commit automático. Próxima etapa somente em missão posterior autorizada: resolver gates e implementar incrementalmente conforme 23/15/16. E2 consolida a especificação; não habilita operação.

Convenção de contagem de estados: os 45 arcos de RN-032/D10 incluem a entrada inicial Início→E01; são 44 transições entre os 16 estados. Eventos sem avanço não acrescentam transições.

## Histórico de revisões

2026-10-12 — 0.2-R1: índice E1, aviso de revisão parcial, novas contagens e separação entre validação original e R1; próximo passo E2.

2026-10-13 — 0.3-R1-E2: índice 23/D15, propagação e contagens consolidadas, evidência auxiliar E2 separada dos resultados originais; piloto condicionado.
