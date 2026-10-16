# Diagramas Radar Licita — índice e visualização

Versão: 0.1. Data: 2026-10-12 (America/Sao_Paulo).  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: especificação [PROPOSTO], rascunho para revisão do operador.

Todos os modelos representam a solução [PROPOSTO]. Fontes normativas: 04/05/06 para requisitos/regras, 07/08/21 para arquitetura/dados e 09/11/12 para decisão. Diagramas sintetizam essas fontes; não criam endpoints, estados ou autorizações.

| ID / fonte | Descrição e leitura |
|---|---|
| [D01 — D01-contexto.md](D01-contexto.md) | Contexto C4: operador, sistema e fontes externas |
| [D02 — D02-containers.md](D02-containers.md) | Containers C4: aplicação, workers, parser e persistência |
| [D03 — D03-motor.md](D03-motor.md) | Componentes: guardas, matching, logística, finanças e revisão |
| [D04 — D04-casos-de-uso.puml](D04-casos-de-uso.puml) | UML: UC-01–UC-08 e atores |
| [D05 — D05-classes.puml](D05-classes.puml) | UML: classes e associações conceituais |
| [D06 — D06-entidades.md](D06-entidades.md) | ER: PK, FK e cardinalidades principais |
| [D07 — D07-atividades.md](D07-atividades.md) | Atividades: descoberta até decisão e acompanhamento |
| [D08 — D08-coleta-pncp.md](D08-coleta-pncp.md) | Sequência: paginação, checkpoint, retry e idempotência PNCP |
| [D09 — D09-analise-oportunidade.md](D09-analise-oportunidade.md) | Sequência: itens/documentos, fornecedor, matching, simulação e publicação |
| [D10 — D10-estados.md](D10-estados.md) | Estados: três vistas, 16 estados e 45 transições de RN-032 |
| [D11 — D11-dados.md](D11-dados.md) | Dados: Bronze, domínio transacional e Gold |
| [D12 — D12-implantacao.md](D12-implantacao.md) | Implantação: Docker, redes, volumes e isolamento |
| [D13 — D13-entrega-remota.md](D13-entrega-remota.md) | Decisão remota: D/C/B/A, evidências e revisão |
| [D14 — D14-fluxo-financeiro.md](D14-fluxo-financeiro.md) | Finanças: custos, precificação, caixa e cenários |

## Visualização e validação

Mermaid: abrir o Markdown no GitHub ou visualizador compatível; cada bloco é fonte editável. Pode-se copiar somente o bloco para renderer local. PlantUML: abrir D04/D05 em ferramenta compatível com PlantUML e Java localmente disponíveis; GitHub não renderiza .puml nativamente. Evitar enviar documentos privados a renderizadores públicos.

Validação realizada em 2026-10-12: Mermaid 11.12.0 carregado do CDN e executado localmente em Chromium headless, usando mermaid.parse e mermaid.render. Fontes não foram enviadas a serviço externo de renderização. Todos os 14 blocos dos 12 arquivos passaram após correção de separador em D09. SVGs foram gerados em memória; aprovação de sintaxe/renderização não substitui inspeção visual na versão do renderer do GitHub. [Resultado por bloco](validacao-mermaid.json).

D04/D05: revisão manual de atores/classes, associações, cardinalidades, abertura/fechamento e referências. Java/PlantUML não localizados no PATH; não alegamos parse/render formal. D05 foi alinhado a 08/D06 para permitir identidade inicial com zero versões; publicação apta exige versão coerente. Registro desta limitação em _progresso.md.

Legendas estão junto às fontes. Setas variam por notação: fluxo, chamada, associação e transição têm sentidos distintos. Classe logística, elegibilidade e estado comercial nunca são equivalentes. D14 usa F para custo fixo; D03/09 usam F como componente financeiro do ranking.

Convenção de contagem de estados: os 45 arcos de RN-032/D10 incluem a entrada inicial Início→E01; são 44 transições entre os 16 estados. Eventos sem avanço não acrescentam transições.

## Revisão R1-E2 e checklist de impacto

Versão atual 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Revisado em R1. [D15 — Descoberta de fornecedores](D15-descoberta-fornecedores.md) inclui qualidade, comparabilidade, identidade/deduplicação, candidato, contato manual, cotação por escopo e verificação anterior à análise. UC-09 acrescentado em D04, sem renumerar UC-01–08. Fontes editáveis mantidas.

| Diagrama | Revisão de impacto E2 |
|---|---|
| D01 | Alterado: fonte histórica opcional 2-H e RF novos |
| D02 | Alterado rótulo de responsabilidades/fontes; sem containers novos |
| D03 | Sem impacto na fórmula/guardas; nota de invariância do contexto histórico |
| D04 | Alterado: UC-09 pesquisa/registro manual; UC-07 preserva decisão |
| D05 | Alterado: eventos, correspondência, preço, comparação, candidato/interação e estabelecimento |
| D06 | Alterado: relações adicionais e unicidade fiscal composta/parcial conforme 08 |
| D07 | Alterado: ramo opcional de pesquisa D15 junto da cotação |
| D08 | Alterado: contrato por recurso, etapa e quota total de tentativas |
| D09 | Sem impacto na sequência obrigatória; nota de referência opcional no manifesto |
| D10 | Sem impacto; fonte integral preservada, estados RN-032 são outro domínio |
| D11 | Alterado: histórico e relacionamento no domínio, projeções recuperáveis e hipótese de venda |
| D12 | Sem impacto na topologia; rótulo das fontes ampliado, sem volume de JSON |
| D13 | Sem impacto: classe logística e verificação atual permanecem |
| D14 | Sem impacto: cálculo/capital e fontes de custo preservados |
| D15 | Novo fluxo de descoberta, independente de estados E01–E16 |

Validação original de 12/10 descrita acima não abrange alterações R1. Renderização E2 usa imagem Mermaid CLI fixada por digest, container não root/RO/offline e saídas próprias, sem renderizador público; resultado efetivo em [relatório E2](../amostras/r1/e2/relatorio-validacao.md). D04/D05 recebem revisão textual de atores/cardinalidades; renderização PlantUML permanece **não executada** por ausência de ferramenta PlantUML fixada para esta execução. Não instalar Java no host: procedimento futuro em ferramentas-r1, com imagem por digest e rede desativada.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: D15/UC-09, checklist de todos os diagramas potencialmente afetados e separação de evidência original/E2. Ver relatório para resultados de validação disponíveis e limitações.
