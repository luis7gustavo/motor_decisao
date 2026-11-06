# 10 — Integração PNCP e preços históricos

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Consulta das fontes: 2026-10-11 (America/Sao_Paulo).
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: adapter [PROPOSTO]; contrato de consulta lido [CONFIRMADO na fonte], não implementado no código. Rotas de itens/documentos: pendentes de validação.

## 1. Evidência e disponibilidade das fontes

| Fonte oficial | Resultado desta execução | Consequência |
|---|---|---|
| [Swagger de consulta](https://pncp.gov.br/api/consulta/swagger-ui/index.html) | HTML acessível pelo navegador de pesquisa | Interface disponível não prova operações |
| [OpenAPI de consulta](https://pncp.gov.br/api/consulta/v3/api-docs) | JSON lido por navegador e GET direto; OpenAPI 3.0.1, info 1.0, 12 caminhos | Rotas/parâmetros/schemas abaixo inspecionados; nenhuma coleta executada |
| [OpenAPI principal](https://pncp.gov.br/api/pncp/v3/api-docs) | Erro do intermediário e timeout de 20 s no GET direto | Não inventar operações de itens/arquivos/resultados; revalidar antes da implementação |
| [Manual PNCP](https://pncp.gov.br/manual/pt-br/latest/) | Erro do intermediário e falha de envio HTTP direto | Semânticas não descritas no OpenAPI permanecem pendentes |
| [Dados abertos Compras.gov.br](https://www.gov.br/compras/pt-br/cidadao/portal-de-dados-abertos) | HTML acessível | Catálogo oficial como ponto de descoberta; não valida contrato de API de itens históricos |

Falhas locais não demonstram indisponibilidade global. A consulta histórica de 10/10 não substitui a validação atual. Contrato pode mudar sem mudança de `info.version`; registrar hash do contrato na implementação e executar diff de schemas. Não armazenamos credenciais nem usamos endpoints autenticados. Ausência de security global no OpenAPI consultado não autoriza usar rotas de manutenção de outro serviço.

## 2. Operações inspecionadas

Base pública usada para compor URLs: `https://pncp.gov.br/api/consulta`. O contrato declara server relativo `/api/consulta` e uma alternativa localhost de desenvolvimento, que não será usada. Todas as operações abaixo são GET. Parâmetros são query, salvo os três identificadores no caminho de detalhe. Os tipos e limites são os observados, sem converter a proposta de política local em limite oficial.

| Caminho relativo | Finalidade / obrigatórios | Opcionais observados |
|---|---|---|
| `/v1/contratacoes/publicacao` | Descoberta por publicação; `dataInicial`, `dataFinal` (strings yyyyMMdd), `codigoModalidadeContratacao` (int64), `pagina` (int32 >=1) | `codigoModoDisputa` int32, `uf`, `codigoMunicipioIbge`, `cnpj`, `codigoUnidadeAdministrativa` strings, `idUsuario` int64, `tamanhoPagina` int32 10..50 |
| `/v1/contratacoes/atualizacao` | Alterações por atualização global; mesmos obrigatórios de publicação | Mesmos opcionais de publicação |
| `/v1/contratacoes/proposta` | Recebimento de propostas aberto; `dataFinal` string e `pagina` int32 >=1 | `codigoModalidadeContratacao` int64, `uf`, `codigoMunicipioIbge`, `cnpj`, `codigoUnidadeAdministrativa` string tamanho 1..30, `idUsuario` int64, `tamanhoPagina` 10..50 |
| `/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}` | Detalhe por identidade; `cnpj` string, `ano` int32, `sequencial` int32 >=1 no path | Nenhum parâmetro opcional observado |
| `/v1/contratos` | Contratos, candidato condicionado ao 2-H (revisado em R1); `dataInicial`, `dataFinal` yyyyMMdd e `pagina` >=1 | `cnpjOrgao`, `codigoUnidadeAdministrativa` strings; `usuarioId` int64; `tamanhoPagina` 10..500 |
| `/v1/contratos/atualizacao` | Alterações de contratos; mesmos parâmetros de contratos | Mesmos opcionais de contratos |

O contrato de proposta não explicita formato/semântica temporal de `dataFinal` na descrição inspecionada. Não transplantar silenciosamente formato/intervalo de outra operação: validar manual/fixture antes de habilitá-la. Publicação/atualização exigem modalidade; domínio completo dos códigos não foi confirmado, portanto uma execução nacional precisa de catálogo validado de modalidades e escopos declarados. Categoria será filtro local inicialmente; não há parâmetro de categoria nessas rotas. Não presumir máximo de dias por janela nem inclusividade das bordas a partir de nomes.

## 3. Respostas, campos e identidade

As três listas de contratações retornam `PaginaRetornoRecuperarCompraPublicacaoDTO`: `data` (array), `totalRegistros`, `totalPaginas`, `numeroPagina`, `paginasRestantes` (int64), `empty` (boolean). Detalhe retorna `RecuperarCompraDTO`. Envelope de contratos usa os mesmos campos e `RecuperarContratoDTO` em data. Content type contratual é `*/*`, embora o documento seja JSON; validar conteúdo real, não confiar só no header.

Mapeamento interno inicial, com fonte no OpenAPI consultado:

| Campo oficial observado | Destino/uso interno [PROPOSTO] | Limite |
|---|---|---|
| `numeroControlePNCP` | procurement.source_id, preservar texto | Não criar chave por título; não confundir capitalização com campo de contrato vinculado |
| `orgaoEntidade.cnpj`, `anoCompra`, `sequencialCompra` | Chave alternativa e chamada de detalhe | CNPJ string; zeros preservados; divergência entre chaves exige reconciliação |
| `objetoCompra`, `informacaoComplementar` | Snapshot descritivo | Texto não determina atributos técnicos sozinho |
| `modalidadeId/Nome`, `modoDisputaId/Nome` | Procedimento/fluxo | Modalidade não é unidade de julgamento |
| `unidadeOrgao.ufSigla`, `codigoIbge`, `codigoUnidade` | Filtros e órgão | Endereço da unidade não comprova local de entrega |
| `dataPublicacaoPncp`, `dataAtualizacao`, `dataAtualizacaoGlobal` | Instantes de origem + texto original | Exemplos sem offset; confirmar timezone, sem assumir UTC |
| `dataAberturaProposta`, `dataEncerramentoProposta` | Janela para participação | Ambiguidade temporal gera pendência; verificar retificações |
| `situacaoCompraId`, `situacaoCompraNome` | Situação oficial separada de E01–E16 | Enum ID observado 1..4; significado por ID não hardcoded sem fonte |
| `valorTotalEstimado`, `valorTotalHomologado` | Naturezas de preço distintas | Number JSON parseado em Decimal; zero/ausente não é receita garantida |
| `orcamentoSigilosoCodigo/Descricao` no detalhe | Motivo do desconhecido | `indicadorOrcamentoSigiloso` está writeOnly no schema; não esperar recebê-lo |
| `linkSistemaOrigem`, `linkProcessoEletronico` | Referência externa | Não são automaticamente URL segura de PDF |
| `justificativaPresencial` | Sinal a revisar | Não conclui obrigação de instalação/entrega presencial |

Os schemas inspecionados não declaram lista required desses campos: tolerar omissão, sem criar conclusões favoráveis. Campos desconhecidos permanecem no bruto. O uso de data/hora oficial exige confirmação do fuso; manter prazo pendente até resolver quando isso afetar aptidão.

## 4. Paginação e sincronização proposta

Política inicial: a cada seis horas (RNF-018), por fonte/modalidade/filtros explícitos. Separar jobs de publicação, atualização e reconciliação de oportunidades acompanhadas. Janela inicial proposta de um dia, com sobreposição de dois dias e revisão semanal de janela ampliada de 30 dias; são hipóteses locais a calibrar após validar limites/semântica da fonte. Não se declara completude histórica/nacional com esse recorte.

Fixar janela e parâmetros antes da primeira página; começar em 1 e tamanho 50 para contratações, dentro do contrato. Persistir página e checkpoint em transação; só avançar após confirmação durável. Conferir numeroPagina, contagens e páginas restantes. Repetição de página, metadados contraditórios, lista inesperadamente vazia ou mudança de total durante paginação sinalizam instabilidade; preservar dados e reprocessar janela, sem declarar completude. 204 é ausência de conteúdo daquela consulta, não exclusão de todos os registros locais. Página final requer consistência do envelope e ausência de erro/quarentena pendente.

Datas de atualização e hashes são complementares: mesmo timestamp pode acompanhar conteúdo diferente. Identidade estável + hash canônico do conteúdo de negócio evita duplicação; bruto preserva bytes originais. Campos voláteis de transporte não criam nova versão de contratação. Contrato de canonicalização versionado; não descartar campos oficiais novos silenciosamente. Retorno a conteúdo antigo atualiza observação/ponteiro e invalida análises, mesmo reutilizando versão de conteúdo.

Ao retomar após crash, repetir última unidade não confirmada; UNIQUE/lease/fencing de 08 impedem efeitos duplicados. Revisão periódica por detalhe das identidades acompanhadas detecta mudanças fora de recorte. Não inferir cancelamento pela ausência numa lista de propostas: consultar situação e guardar evidência. Antes de E07/decisão de participação, atualizar fonte e documentos pertinentes conforme guardas; falha deixa pendência de atualidade. Acesso ao cabeçalho sozinho não confirma atualização dos anexos.

## 5. Erros, limites e observabilidade

Respostas declaradas nas rotas inspecionadas: 200, 204, 400, 401, 422 e 500. Especificação permite formas distintas de erro (string, objeto/mapa, DTO de validação). Adapter trata também timeout, DNS/TLS, JSON inválido, 404/429/5xx como comportamento defensivo [PROPOSTO], sem alegar que o contrato os enumera.

400/422: falha de contrato/parâmetros, sem retry cego. 401/403: bloquear integração e investigar, sem tentar credenciais ou API de manutenção. 404: marcar consulta não resolvida; não apagar. 429/5xx/transporte: até três tentativas totais, conexão 10 s/leitura 60 s, backoff base 2 s com jitter e teto local 30 s; respeitar Retry-After válido, reagendando quando exceder a janela do job. Limite inicial de uma requisição por segundo e concorrência 1 por fonte compartilhados entre jobs; não são quotas oficiais. Limites oficiais mais restritivos prevalecem; quota oficial não localizada nesta consulta.

Após falhas persistentes, suspender novas tentativas pelo intervalo configurado e mostrar alerta/último sucesso; consulta local continua. Não repetir camada HTTP três vezes em cada retry Prefect sem orçamento global. Logs: scope, janela, página, attempts, latência, código sanitizado e contagens; sem header sensível ou corpo indiscriminado. Falha não regrava lista antiga como vazia.

## 6. Itens, documentos e retificações: contrato ainda aberto

Necessidades: lista/detalhe de itens, composição de lotes/grupos, anexos e versões, download, resultados por item e vínculos contratuais. **Rotas, parâmetros, paginação e campos desses recursos não foram confirmados**, porque o OpenAPI principal/manual não ficaram acessíveis. Não usar rotas lembradas de treinamento nem confundir POST/PUT/DELETE de manutenção com consulta pública.

Gate de implementação: obter contrato oficial acessível; registrar hash/data/base/rota/método/schemas/security; classificar operação pública de leitura; criar fixtures sintéticas derivadas do contrato e conferir uma amostra controlada em missão autorizada de implementação. Até lá, o adapter detalhado permanece incompleto. Pode-se projetar registro manual de evidências, com origem e revisão, mas não declarar RF-006–009 integralmente implementados. URLs recebidas passam por RNF-003: host aprovado, DNS/IP/redirect/HTTPS e limite de bytes; link oficial não dispensa proteção SSRF. Documento retificado invalida todos os dependentes afetados.

## 7. Estratégia de testes futura

Fixtures sintéticas: duas páginas e replay dez vezes; página 2 falha e retomada; 204 isolado; 400 versus timeout; 429 com espera superior à janela; chave oficial conflitante; mesmo ID/hash; mesmo ID com alteração intradiária; retorno ao conteúdo antigo; prazo sem offset; estimativa zero/sigilosa; campo adicional; remoção de campo essencial; link redirecionado para rede privada. Aceites: nenhuma perda do último dado válido, nenhuma duplicação lógica, checkpoint só confirmado após persistência, invalidação preserva revisão/ato externo, incerteza impede positivo. [D08](diagramas/D08-coleta-pncp.md) representa persistência/retry. Nenhum teste operacional foi executado.

## 8. Planejamento de preços históricos — 2-H básico e fase 6 ampliada (revisado em R1)

Separar estimado, homologado, contratado e cotação (RN-005). O contrato de consulta expõe `valorTotalHomologado` e, para contratos, `valorInicial`, `valorGlobal`, `valorAcumulado`, `numeroControlePncpCompra`, `dataAssinatura` e vigência. São valores de contratação/contrato; não representam automaticamente preço unitário comparável. `numeroControlePncpCompra` tem capitalização diferente de `numeroControlePNCP`; mapear explicitamente.

Usar os endpoints de contratos acima para localizar candidatos e vínculos; preço por item exige contrato oficial de itens/resultados ainda pendente. Compras.gov.br é fonte complementar a investigar por catálogo oficial, sem URL inventada. `commercial.historical_prices` guarda natureza, evidência e versão; nunca estimar preço unitário dividindo total global por quantidade de um único item.

Pipeline proposto: preservar bruto → identificar contratação/resultado/item → normalizar moeda/unidade/embalagem → recuperar candidatos por CATMAT/CATSER quando disponível → comparar especificação, quantidade, data/região, garantia, condições de fornecimento e tributação documentada → validar comparabilidade → calcular estatística descritiva. Código catalográfico ou título igual não confirma equivalência.

Metadados obrigatórios para comparação: fonte/data/natureza, descrição/atributos, unidade e fator de embalagem comprovado, quantidade e condições. Ausência de CATMAT não impede busca por atributos, mas sua ausência é explícita. Valores de épocas distintas só são corrigidos por índice escolhido e fonte/metodologia registrada, nunca implicitamente. Mediana/faixa/número de observações e exclusões visíveis; amostra insuficiente não recebe precisão artificial. Comparação não substitui cotação vigente nem confirma preço de disputa. Aceite futuro: pares de mesmo nome/unidades distintas rejeitados; total global não vira unitário; natureza preservada; retificação conserva observação anterior.

## 9. Pendências e rastreabilidade

QA-001 continua parcialmente aberta: itens/documentos/resultados, fuso das datas, semântica temporal de proposta, catálogo de modalidades e limites de janela/taxa precisam de fonte/validação. Fonte acessível não é integração implementada. Relaciona RF-001–009/028, RN-001–009/026–028, RNF-003/005/007/008/013/018, ADR-010/014. [08](08-modelo-de-dados.md) define o modelo interno; [21](21-infraestrutura-docker.md) define limites operacionais.


## 10. Evidências R1 e reconciliação por atributo — revisado em R1

Revisão 0.3-R1-E2, 2026-10-13. A evidência das seções 1–3/8 continua datada de 11/10, não revalidada por esta E2. [Matriz da E1](amostras/r1/matriz-fontes.md), consultada em 12/10, e [dicionário de 22](22-inteligencia-de-mercado-fornecedores.md) discriminam documentação/amostra/suporte. **Nenhuma nova chamada de dados na E2; nenhuma amostra observada na E1, cumulativo 0/30.** Este quadro não afirma disponibilidade operacional nem adapter implementado.

| Recurso/atributo | Evidência documental reutilizada / preferência condicionada | Amostra / desconhecido / gate |
|---|---|---|
| Identidade da compra, publicação/atualização e contrato global PNCP | OpenAPI consulta 3.0.1/info 1.0 inspecionado em 11/10; preferir identificador e atributo originador PNCP, preservando Compras quando relacionado | Não executada; revalidar contrato/hash, fuso e cobertura. Valor global não é preço unitário |
| Item e resultado homologado Compras | Manual 2.0, §§10.6/10.7 lido em 12/10: primeira fonte candidata à validação de resultado, identificação e preço por item | Não executada; contrato completo, paginação/tipos/datas/vínculos e alfanumérico pendentes. Divergência Inteiro/Decimal bloqueia parser presumido |
| Contrato/item Compras | Manual §12.2 descreve recurso; usar valor contratado somente no grão demonstrado pelo item contratual | Não executada; correlação com resultado não demonstrada; nunca juntar por descrição ou dividir global por item |
| Ata / catálogo | Manual §§11/4 descreve recursos; preço registrado separado; CATMAT/CATSER auxilia localização | Não executada; versão taxonômica/preenchimento/unidade/hierarquia e rotas detalhadas pendentes; ata não prova entrega |
| Cotação/disponibilidade para operador | Evidência comercial inserida manualmente, RF-013/015/038 | Não deriva das APIs públicas; validade/quantidade/prazo/filial próprios |
| Lances/perdedores/desertos, execução, aceite e pagamento | Não há universo ou evidência suficiente nos recursos examinados | Suporte desconhecido; não apresentar taxa de vitória, capacidade, execução ou cobertura nacional |

Preferência é por atributo/recurso, jamais ranking universal de portais. Mesma natureza/identidade com valores conflitantes: registrar ambas origens em event_links, manter pendência, revisão com método/autor/evidência decide valor usado por projeção. Versão mais recente só prevalece com semântica temporal demonstrada; observed_at é consulta, não homologação. Duplicata comprovada conta uma vez na mesma etapa; resultado/ata/contrato são eventos relacionados e não somáveis como faturamento. Fuzzy cria sugestão, nunca fusão.

Retificação/cancelamento gera nova versão e invalida referência/comparação dependente, sem apagar versão anterior nem cotação independente. Origem preferida ausente não elimina último dado válido; expor frescor/limitação. Recurso pode ficar desabilitado sem bloquear cadastro/cotação manual ou análise com insumos suficientes (RN-036).

Filtragem de categoria é local onde não houver filtro no contrato: orçamento precisa contemplar linhas de outras categorias, vínculo ao item e custo de revisão. Não anunciar API de categoria nem janela por data de resultado em rota cuja janela é vigência. Termos, quota oficial, cobertura temporal/regional e formatos alfanuméricos seguem QA-R1-02/03/04. Antes de habilitar: fechar contrato/hash, fixtures sintéticas, limites e amostra futura autorizada; alternativa manual com evidência. Nenhuma estatística de mercado decorre desta revisão.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: propagada evidência E1, precedência por atributo, revisão de conflitos/retificações e marco 2-H; datas históricas preservadas e zero novas amostras.

