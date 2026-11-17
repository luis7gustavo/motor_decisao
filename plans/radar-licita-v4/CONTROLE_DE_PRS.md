# Controle de PRs — Radar Licita

**Atualizado em:** 17/11/2026

**Estado:** [P30 integrado pelo PR #4](https://github.com/luis7gustavo/motor_decisao/pull/4) no commit `b46aec77a3448f51ac212676216c675822fb12f3`; [P00 em revisão no PR #5](https://github.com/luis7gustavo/motor_decisao/pull/5); G0 pendente.

**Fonte da fila:** [backlog da proposta V4](PROPOSTA_V4_20261117.md), reordenado pelas dependências e pelo método aprovado por Luis.

## Resumo da execução

- **Método confirmado por Luis em 17/11/2026:** uma entrega curta por PR, worktree exclusivo, sessões distintas de autoria e verificação, aprovação humana antes de integrar e avançar para PR dependente. Os oito agentes da V4 são papéis de sessão.
- **P30 integrado:** [PR #4](https://github.com/luis7gustavo/motor_decisao/pull/4), commit `b46aec77a3448f51ac212676216c675822fb12f3` na branch de produto, confirmado por consulta ao remoto em 17/11/2026. A integração não aprova G0 nem a V4 integral.
- **Local dos documentos vivos:** este repositório de produto; o cofre `RADAR LICITA` preexistente permanece biblioteca de referência. O checkout de produto original tem alterações locais preexistentes e não foi usado para editar P30.
- **Publicação:** o repositório de produto é público. Luis autorizou em 17/11/2026 a publicação da V4 integral e dos documentos de execução (PUB-01). Isso não aprova merge nem gates.
- **Verificação de P30:** 36 cartões com IDs únicos, 36 entradas no controle, dependências de implementação em ordem, links Markdown locais resolvidos e `git diff --check` sem falhas. Uma sessão independente revisou o conteúdo e apontou o risco de publicação pública e um caminho ambíguo; o caminho foi corrigido e a autorização de publicação foi registrada.
- **P00 aguardando validação dos dados e G0:** [PR #5 pronto para revisão](https://github.com/luis7gustavo/motor_decisao/pull/5), branch `codex/radar-p00-baseline-20261117`, worktree exclusivo `motor_decisao_p00_20261117`, base no merge do P30. Entrega documental em [linha de base](LINHA_DE_BASE_P00.md), [políticas](POLITICAS_E_CRITERIOS_P00.md) e [pacote](PACOTE_REVISAO_P00.md). Luis aprovou o método P00-01 a P00-07; verificação técnica do contrato anterior registrada. Validação do responsável pelos dados, aprovação final, G0 e merge pendentes.

## Fila proposta de PRs

A ordem abaixo é uma **fila de execução**, não um cronograma. Um cartão é detalhado ao iniciar o PR; se a entrega for grande, subdivide-se antes de implementar. PRs independentes podem ser preparados sem consumir um gate de dados reais, mas o próximo PR dependente espera a aprovação aplicável. `P01` depende do acesso aos manifestos existentes; `P34` e `P35` dependem da entrega autorizada de Carlos. P24 nesta fila significa **piloto técnico offline e candidato a release**, não aprovação da versão 1.0.

| Ordem | PR | Entrega proposta | Dependência para implementar | Estado |
| ---: | --- | --- | --- | --- |
| 1 | [P30](prs/P30.md) | Inicializar Vault, índices, instruções dos agentes, templates, links e controle de permissão | — | [PR #4 integrado](https://github.com/luis7gustavo/motor_decisao/pull/4), `b46aec77` |
| 2 | [P00](prs/P00.md) | Baseline, políticas, limites, metas futuras, contrato sem agente no runtime e status no Vault | P30 | [PR #5 em revisão](https://github.com/luis7gustavo/motor_decisao/pull/5) |
| 3 | [P32](prs/P32.md) | CLI/testes para lint de notas, links, frontmatter, IDs, decisão e referências a commits/dados | P30 | Fila |
| 4 | [P31](prs/P31.md) | Inventário das descrições/exemplos, schema observado, campos ausentes, fixtures/manifestos E0/E1/E2 com hash | P30 | Fila |
| 5 | [P01](prs/P01.md) | Fechamento de manifestos **existentes**, se acessíveis; registrar pendência da janela 12–13/11 | P31 | Condicional: manifestos |
| 6 | [P02](prs/P02.md) | Ledger offline de páginas, falhas, versões, hash e idade; simular casos de falha | P31 | Fila |
| 7 | [P03](prs/P03.md) | Contrato, importador e mecanismo de rotulagem para futura exportação de Carlos | P31 | Fila |
| 8 | [P04](prs/P04.md) | Modelo de contratação, versão e item; migração/restauração em banco descartável | P31 | Fila |
| 9 | [P33](prs/P33.md) | Adaptador formal de exportação de Carlos: contract tests, validação e mapeamento de divergências | P03 + P04 | Fila |
| 10 | [P06](prs/P06.md) | Importar MegaMix/Coletek e reconciliar 3.095 registros de origem (se fontes disponíveis) | P00; arquivos de catálogo | Fila |
| 11 | [P12](prs/P12.md) | Contrato de cenário e casos de ouro financeiros propostos | P00 + P31 | Fila |
| 12 | [P05](prs/P05.md) | Documento, unidade de julgamento e resultado com estados desconhecidos e versão | P04 | Fila |
| 13 | [P07](prs/P07.md) | Cotação, estoque, validade e distinção catálogo × confirmado | P06 | Fila |
| 14 | [P13](prs/P13.md) | Lucro `Decimal`, fluxo de caixa datado e preço mínimo de lance | P12 | Fila |
| 15 | [P10](prs/P10.md) | Busca lexical (BM25/TF-IDF), regras de contexto/categoria, avaliação estrutural | P04 + P31 | Fila |
| 16 | [P08](prs/P08.md) | Parser PDF, OCR quando necessário, trechos e proveniência por página | P05; PDFs existentes | Fila |
| 17 | [P14](prs/P14.md) | Carteira, simultaneidade, sensibilidade e segunda implementação independente | P13 | Fila |
| 18 | [P27](prs/P27.md) | Ficha assistida **offline** com marcação de fonte e classe de evidência | P05 + P07 + P13 | Fila |
| 19 | [P25](prs/P25.md) | **I05V-E:** estudos preliminares com exemplos E0/E1/E2 e avisos visíveis | P07 + P13 | Fila |
| 20 | [P09](prs/P09.md) | NLP convencional de requisitos e matriz de revisão/habilitação ME/EPP | P08 | Fila |
| 21 | [P11](prs/P11.md) | Matching de atributos e lote inteiro; avaliação contra exemplos rotulados disponíveis | P07 + P09 + P10 | Fila |
| 22 | [P17](prs/P17.md) | Esquema de resultado por item/lote e histórico; fixture de cancelamento/retificação | P05 | Fila |
| 23 | [P26](prs/P26.md) | Checklist contratual/jurídico para parceiros e intermediação | P09 | Fila |
| 24 | [P15](prs/P15.md) | Regras de entrega direta/reenvio, prazo e frete por perna | P09 + P14 | Fila |
| 25 | [P16](prs/P16.md) | Responsabilidades e habilitação da rota com parceiro | P15 | Fila |
| 26 | [P18](prs/P18.md) | Economia de comissão/intermediação e confirmação manual de parceiro | P14 + P17 + P26 | Fila |
| 27 | [P21](prs/P21.md) | Idade, atraso e relatório de SLA com datasets de teste | P02 | Fila |
| 28 | [P19](prs/P19.md) | API da ficha integrada, riscos e proveniência para todas as rotas como capacidade parcial | P11 + P14 + P16 + P18 | Fila |
| 29 | [P20](prs/P20.md) | Interface completa, fila humana, reabertura, RBAC e dossiê | P19 | Fila |
| 30 | [P28](prs/P28.md) | Testar deploy, dependências e rede: **runtime sem agentes/LLM** | P10 + P13 + P27 | Fila |
| 31 | [P29](prs/P29.md) | Versionamento de regras/modelos, reprodutibilidade, avaliação e rollback | P11 + P21 | Fila |
| 32 | [P22](prs/P22.md) | Jobs incrementais idempotentes, reprocessamento, propagação de retificação e alertas simulados | P20 + P21 | Fila |
| 33 | [P23](prs/P23.md) | Telemetria, orçamentos, pausa, backup/restauração e rollback em sandbox | P22 | Fila |
| 34 | [P24](prs/P24.md) | Piloto técnico, correções, manual e release candidato; **release 1.0 condicionado** | P20, P23, P28, P29; para 1.0 também P34/P35 e G5/G6 | Fila |
| 35 | [P34](prs/P34.md) | Integrar snapshot real autorizado, auditar contagem/versão/itens/documentos | P33 + entrega de Carlos | Aguarda Carlos |
| 36 | [P35](prs/P35.md) | **I05V-R:** estudo de oportunidades reais e validação comercial por recorte | P34 + P11 + P14 | Aguarda P34 |

## Como atualizar este controle

Ao fechar cada PR, registrar: branch e base, commit examinado, link do PR, resumo do que entrou, testes e evidências, verificador independente, decisão humana com autor/data, commit integrado, pendências e próximo PR liberado. Mudança de ordem deve registrar motivo e dependências; itens bloqueados continuam visíveis. Não marcar `aprovado` por silêncio ou por testes verdes.
