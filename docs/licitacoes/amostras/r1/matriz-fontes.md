# R1 — Matriz de fontes e evidências

Versão 0.2-R1; consulta em 2026-10-12, America/Sao_Paulo. Revisado em R1-E1.

Esta matriz classifica recursos externos, não implementação do Radar. Em todas as linhas, evidência amostral = **não executada**, 0 requisições de dados. “Suportado” significa descrito no documento indicado, dentro daquele escopo; não confirma disponibilidade prática, cobertura ou qualidade. Ausência de campo na leitura não é evidência de não suporte. Nenhuma linha recebeu “não suportado com evidência explícita”.

## Registro documental separado do ledger de dados

| Ref. | URL oficial / versão e seção | Resultado da consulta humana |
|---|---|---|
| F1 | [PNCP Swagger](https://pncp.gov.br/api/consulta/swagger-ui/index.html) e [OpenAPI consulta](https://pncp.gov.br/api/consulta/v3/api-docs) | Swagger HTML e JSON acessíveis; ferramenta não expôs conteúdo estruturado legível do JSON nesta revisão. Detalhes de 10 são evidência histórica de 11/10, não revalidação integral de hoje |
| F2 | [OpenAPI principal](https://pncp.gov.br/api/pncp/v3/api-docs), [manual PNCP](https://pncp.gov.br/manual/pt-br/latest/) | Erro do intermediário em ambos; não prova indisponibilidade global |
| F3 | [Portal Compras](https://www.gov.br/compras/pt-br/cidadao/portal-de-dados-abertos), [Swagger produção](https://dadosabertos.compras.gov.br/swagger-ui/index.html), [OpenAPI](https://dadosabertos.compras.gov.br/v3/api-docs) | Portal/HTML/JSON acessíveis, schema JSON não exposto de forma legível pela ferramenta. Página duplicada do portal, terminada em `/portal-de-dados-abertos/portal-de-dados-abertos`, retornou conteúdo restrito; não houve autenticação. Resultado de busca em domínio `dadosabertos-hml` não usado como contrato de produção |
| F4 | [Manual API Compras](https://www.gov.br/compras/pt-br/acesso-a-informacao/manuais/manual-dados-abertos/manual-api-compras.pdf), 2.0, capa Fev/26, histórico Jan/26 | PDF oficial de documentação lido no navegador: seções 3, 4, 10.5–10.7, 11, 12.2. Diferença entre datas da capa/histórico preservada. Sem download de anexos de compras |
| F5 | [CNPJ alfanumérico](https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/acoes-e-programas/programas-e-atividades/cnpj-alfanumerico), [documentação DV](https://www.gov.br/receitafederal/pt-br/centrais-de-conteudo/publicacoes/documentos-tecnicos/cnpj) | Receita confirma coexistência com inscrições numéricas anteriores e fornece documentação técnica; algoritmo/fixtures de DV ainda devem ser conferidos na implementação. Não validar identidade apenas por regex |
| F6 | [Lei 14.133, art. 83](https://www.planalto.gov.br/ccivil_03/_ato2019-2022/2021/lei/l14133.htm) | Registro de preços não obriga contratação pela Administração. A aplicação ao procedimento concreto requer identificar o regime/instrumento |
| F7 | [ANPD — perguntas frequentes](https://www.gov.br/anpd/pt-br/acesso-a-informacao/perguntas-frequentes) | Dados públicos continuam sujeitos aos princípios e direitos da LGPD; finalidade, necessidade e retenção precisam de definição |

Consultas: sete grupos documentais, mais pesquisas e leituras de trechos de F4; não são sete requisições HTTP medidas. A ferramenta não expõe ledger HTTP interno, redirects nem duração por página. Não atribuir esses acessos ao orçamento de dados. Contratos não foram arquivados nem tiveram hash calculado; versão/hash binário = pendente. Links e localizadores acima permitem revisão humana, não reprodução byte a byte.

## Fonte × recurso × dado × evidência × suporte

| Fonte/recurso | Dado e localizador | Evidência documental | Evidência amostral | Suporte e limite |
|---|---|---|---|---|
| PNCP consulta — compras/publicação/atualização | Identidade, órgão, totais e datas; F1 e 10 §§2–3 | confirmada em 11/10; revalidação detalhada pendente | não executada | suportado no contrato histórico; cobertura atual desconhecida |
| PNCP consulta — contratos | Vínculo de compra, valor global e assinatura; 10 §§2/8 | confirmada em 11/10; revalidação detalhada pendente | não executada | suportado no escopo histórico; global não é preço por item |
| PNCP principal — item/resultado/ata | Estabelecimento, unitário, unidade, situação | pendente, F2 inacessível | não executada | desconhecido; nenhuma rota detalhada inventada |
| Compras — resultados PNCP 14133 | Identificação fiscal/tipo de pessoa, sequência, situação, quantidade e preços homologados, datas; F4 §10.7 | confirmada no manual | não executada | suportado documentalmente; ligação ao item e semântica de tipos a conferir |
| Compras — itens PNCP 14133 | Descrição e dados de item; F4 §10.6 | confirmada no manual | não executada | suportado documentalmente; equivalência técnica não decorre do código |
| Compras — contratos/itens | Identificadores e valores por item; F4 §12.2 | confirmada no manual | não executada | suportado documentalmente; vínculo resultado→item contratual depende de reconciliação |
| Compras — ARP | Ata e recursos de itens/unidades/empenhos; F4 §11 | confirmada quanto à existência dos recursos | não executada | suportado nesse escopo; preenchimento e encadeamento desconhecidos |
| Compras — CATMAT/CATSER | Catálogos e unidades; F4 §4 e índice §5 | confirmada quanto ao catálogo | não executada | suportado documentalmente; versão taxonômica efetiva/preenchimento desconhecidos |
| Ambas — entrega/aceite/pagamento | Comprovação de execução | não localizada nos recursos examinados | não executada | desconhecido; contrato/empenho não substituem execução |
| Ambas — lances/perdedores/desertos | Universo de participações e fracassos | pendente por recurso | não executada | desconhecido; não calcular taxa de vitória |
| Ambas — CNPJ alfanumérico | Aceitação em filtros/payload | pendente; tipo texto não basta | não executada | desconhecido; F5 define normalização interna, não suporte das APIs |
| Ambas — quota/licença/atualização/cobertura | Taxa oficial, termos específicos, atraso e completude regional/temporal | pendente; portal não é SLA | não executada | desconhecido; não assumir uso ilimitado nem cobertura nacional |

## Contrato candidato e gates

F4 §10.7 descreve GET na base `https://dadosabertos.compras.gov.br`, caminho `/modulo-contratacoes/3_consultarResultadoItensContratacoes_PNCP_14133`; paginação por `pagina`/`tamanhoPagina`, datas `dataResultadoPncpInicial`/`dataResultadoPncpFinal` obrigatórias. O formato exato e as bordas temporais permanecem pendentes. Não foi localizado filtro de categoria nesse recurso. A futura seleção por categoria será local e dependerá de vínculo com descrição/unidade do item. Tamanho 10 é candidato inicial; limite documental 500 não autoriza ultrapassar o limite da investigação.

O manual chama quantidades e valores homologados de Inteiro no retorno, mas filtros de valores são Decimal. Não truncar: confrontar OpenAPI e resposta antes de fixar parser. `niFornecedor` não é exclusivamente CNPJ; discriminar pessoa/país sem gravar CPF na amostra. Data de resultado não substitui assinatura/publicação/atualização; UF do órgão não indica destino de entrega. Campos de exemplo não asseguram presença nem nulabilidade.

F4 §12.2 descreve GET `/modulo-contratos/2_consultarContratosItem`, com janela obrigatória de início de vigência; não confundir essa janela com data do resultado. Se o vínculo necessário não puder ser resolvido dentro dos limites, registrar inconclusivo. Atas e catálogo só recebem chamadas após contrato específico completo, sem aproveitar parâmetros de outra rota.

## Precedência proposta por atributo

1. Resultado por item: Compras é primeiro candidato à validação porque o manual foi legível; PNCP principal continua candidato quando contrato puder ser lido. Isso não é avaliação de superioridade nacional.
2. Identidade de compra e retificação: preservar chave oficial PNCP e identificadores Compras; preferir versão oficial do recurso que originou o atributo, comparando instantes de atualização somente com semântica confirmada.
3. Preço contratado: recurso contratual/item explícito; homologado: resultado; ata: recurso da ata. Nunca sobrepor um ao outro por ser mais recente.
4. Especificação/unidade: item e evidência técnica aplicável; catálogo auxilia recuperação, não substitui especificação. Cotação atual: evidência comercial registrada pelo operador, nunca API pública histórica.
5. Conflito na mesma natureza/identidade abre revisão com os dois valores/fontes. Ausência na fonte preferida não apaga dado anterior; troca de precedência requer justificativa, autor e versão. Correspondência incerta fica fora do agregado padrão.

## Histórico de revisões

| Data | Versão | Alteração |
|---|---|---|
| 2026-10-12 | 0.2-R1 | Matriz documental inicial; zero amostras, preferências condicionadas e lacunas explícitas |
