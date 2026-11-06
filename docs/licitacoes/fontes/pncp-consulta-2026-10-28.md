# PNCP — conferência documental de 28/10/2026

Escopo inicial: pesquisa autorizada de documentação oficial para a missão `pncp-history-2026-10-27`. **Nesta conferência documental não houve consulta de licitações, modalidades ou outros dados de produção.** A política da missão principal continua `enabled=false`, com orçamento externo de dados zero. A tentativa documental direta foi registrada antes de cada conexão em `backups/pncp-docs/20261028/`; buscas do mecanismo web não integram o contador da API PNCP. Sondas reais posteriores, em missão separada, estão registradas ao fim do documento.

## Fontes arquivadas

| Fonte oficial | Evidência local | Resultado |
|---|---|---|
| [OpenAPI da API PNCP Consulta](https://pncp.gov.br/pncp-consulta/v3/api-docs) | `config/contracts/pncp-consulta-20261028.json`; SHA-256 `b2c1fb8874861f63ebf7c065756f82c04dc4a84c8def63e5b4aa641d5edb4430`; recibo `backups/pncp-docs/20261028/openapi-receipt.json` | HTTP 200, 37.943 bytes, OpenAPI 3.0.1, 12 caminhos. O diff semântico com 13/10 está em `openapi-comparison.json`: caminhos, operações, parâmetros e schemas operacionais iguais; 35 diferenças apenas em valores `example` de datas. |
| [OpenAPI da API PNCP Integração](https://pncp.gov.br/pncp-api/v3/api-docs) | `config/contracts/pncp-integracao-20261028.json`; SHA-256 `d721ad1c59a82f5dea3ced7ce4e92dc51da91096c2041382a567aa5773a57aa2`; recibo `backups/pncp-docs/20261028/integration-openapi-receipt.json` | HTTP 200, 406.779 bytes, OpenAPI 3.0.1, 124 caminhos. A existência do contrato de leitura não demonstra acesso real, estabilidade ou completude do recurso. |
| [Manual de Integração PNCP v2.6](https://pncp.gov.br/manual/pt-br/latest/singlehtml/) | Página oficial consultada pela ferramenta web, não copiada como snapshot | Confirma a base `https://pncp.gov.br/api/pncp` e descreve as rotas GET de modalidades, contratação, itens, resultados, documentos e histórico. |
| [Perguntas e Respostas do PNCP](https://www.gov.br/pncp/pt-br/pncp/perguntas-e-respostas) | Página oficial consultada pela ferramenta web, não copiada como snapshot | Confirma acesso público e gratuito aos dados publicados, sem login para visualização. Não informa quota numérica da API nem a data mais antiga recuperável por rota. |
| [Manual — API de Consultas v1.0](https://www.gov.br/pncp/pt-br/pncp/manuais) | `manual-attempt.json` registra uma tentativa direta; **nenhum PDF/recibo foi salvo** | O portal lista a versão 1.0. O download direto do PDF falhou; a ferramenta web recebeu 401 ao abrir o arquivo. Não foi tratado como manual local validado. |

Os snapshots são de documentação pública, não respostas de coleta. `config/contracts/pncp-consulta-20261013.json` e o C2 permanecem preservados. O hash da política da missão histórica aponta ao novo snapshot de consulta; o coletor C2 continua usando seu contrato original.

## O que o contrato confirma

- As rotas de consulta `/v1/contratacoes/publicacao` e `/v1/contratacoes/atualizacao` existem no OpenAPI atual. Ambas exigem `dataInicial`, `dataFinal` em `yyyyMMdd`, `codigoModalidadeContratacao` e `pagina` a partir de 1; `tamanhoPagina` opcional aceita 10–50. Os filtros de UF, município, CNPJ, unidade, usuário e modo de disputa são opcionais e não entram na aquisição do universo nacional.
- A consulta de detalhe `/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}` aparece na API Consulta e na API Integração. Na API Integração, existem GETs para `/itens` (paginado), `/itens/{numeroItem}/resultados`, `/arquivos` (paginado), `/historico` (paginado) e `/v1/modalidades` com `statusAtivo` opcional. As rotas de documentos retornam referências/URLs de arquivos; retenção de bytes de anexos requer validação própria.
- O manual de integração descreve acesso público às consultas e autenticação para manutenção. Isso não substitui teste controlado de cada rota de leitura nem estabelece quota numérica.
- O manual de integração documenta `/v1/modalidades` com `statusAtivo` opcional; para o catálogo completo histórico, a futura consulta deve omitir esse filtro e registrar também as modalidades inativas. A existência da rota não substitui o catálogo observado.

## Pendências que continuam bloqueando dados reais

| Questão | Estado em 28/10/2026 |
|---|---|
| Cobertura inicial por recurso/modalidade e catálogo completo de modalidades, inclusive inativas | Não identificados nos snapshots. Existe GET de catálogo, mas ele não foi chamado; nenhuma data inicial foi inferida da criação do portal. |
| Inclusão das bordas de `dataInicial`/`dataFinal`, fuso e amplitude máxima de janela | OpenAPI informa formato, semântica não identificada. Não usar como prova de inclusividade nem tomar limite de um terceiro como limite oficial PNCP. |
| Taxa, concorrência e quota periódica oficiais | Não identificadas nos dois OpenAPI nem na página oficial consultada. Ausência de número não é autorização ilimitada. A política local proposta de 1 segundo/1 conexão continua desativada. |
| Retenção/volume nacional, total de páginas e orçamento de dados | Sem estimativa verificável. O teto C2 de 50 chamadas é somente do ensaio e não foi reaproveitado. |
| Exportação oficial integral/snapshot independente | Não identificado nos contratos arquivados. Exportação de resultados filtrados no portal não foi validada como snapshot completo. |
| Enriquecimento real | Rotas documentadas, mas parser, construção de URLs, paginação, anexos e recibos precisam de teste de ponta a ponta por recurso antes de habilitar os respectivos gates. |
| Prefect compartilhado | Redis AOF ainda falha; deployment da missão ausente conforme inventário S06. |

**Decisão:** `contract_currently_revalidated=true` apenas para a estrutura de consulta da PNCP recuperada em 28/10. Os demais gates permanecem bloqueados; missão desabilitada e orçamento externo zero. Próximo passo técnico é planejar um pequeno conjunto de sondas de dados por recurso com reserva persistente e escopo explícito, depois resolver os gates de fonte e infraestrutura antes da prova real S06. Não alterar o job/ledger C2 nem ativar S07.

## Sondas posteriores nesta data

Após autorização específica do usuário para até 12 tentativas reais, foram feitas quatro em missão separada, com orçamento persistente: catálogo 200, publicação 200, atualização 200 e detalhe sob `/api/pncp` 301. A resposta de detalhe apontou uma porta não prevista; a execução parou, sem seguir o endereço. Após nova autorização, a mesma missão retomou pelo detalhe da API de Consulta documentada e testou itens, lista de arquivos e histórico. Encerrou pausada em **11/12**, com três janelas antigas 204 e uma tentativa de contingência sem uso. Ver `plans/coleta-historica-periodica-2026-10-27/S06_SONDAS_RESULTADO.md`. Os códigos de modalidade 1–19 foram observados no catálogo, mas cobertura histórica, resultado por item, bytes dos anexos e semântica das bordas permanecem pendentes.
