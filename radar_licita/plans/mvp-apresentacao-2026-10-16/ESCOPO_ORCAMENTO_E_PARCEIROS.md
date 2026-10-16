# Orçamento e busca inicial de parceiros

Revisão 1.1 de 15/10/2026. Estes dois recursos passaram a obrigatórios por solicitação do usuário. Implementação futura nas sessões S04 e S05, integrada à interface em S06 e validada em S07.

## Experiência principal

O operador informa Quanto posso investir e o sistema atualiza as oportunidades por item que cabem nesse capital, com razões e premissas. Ao abrir um item, usa Buscar possíveis parceiros para localizar empresas em resultados anteriores de pregões pertinentes. Seleciona uma candidata, confere a evidência e a inclui no cadastro para obter cotação atual por um canal escolhido por ele.

Seleção por orçamento e descoberta de empresas são funções diferentes. O histórico público ajuda a encontrar quem investigar; a elegibilidade financeira depende do custo atual suficientemente sustentado. Uma candidata pode ser concorrente ou não ter interesse em fornecimento.

## Seleção por orçamento

Contrato proposto: POST /api/v1/recommendations/search.

~~~json
{
  "available_capital": "5000.00",
  "q": "notebook",
  "uf": null,
  "page": 1,
  "page_size": 25
}
~~~

Requisição somente de leitura e avaliação contextual. Não alterar SystemSetting nem persistir um score a cada digitação. Registrar no retorno o capital aplicado, evaluated_at, versão de regras, origem dos custos e snapshot identificado de item/cenário/cotação.

Resposta: available_capital, evaluated_at, rules_version, recommendations, needs_analysis, outside_budget, blocked e paginação com totais distintos por item/grupo. Cada registro contém procurement_id, item_id, scenario_id, supplier_quote_id, eligibility, score, required_capital, budget_remaining, estimated_result, margin_rate, proposal_close_at, pending_checks, blockers, explanation e cost_basis.

Prioridade dos grupos: impeditivo conhecido não financeiro vai para blocked com razão; incompletude crítica sem impeditivo conhecido vai para needs_analysis; análise suficiente que excede capital vai para outside_budget; análise elegível dentro do limite vai para recommendations. Não chamar incompatibilidade ou lote incompleto de excesso de orçamento. Prazo encerrado fica fora das abertas e tem exclusão contabilizada por motivo.

cost_basis distingue quoted, manual_estimate e unknown. Apenas custos suficientes e cotação atual rastreável sustentam a lista elegível. Em needs_analysis, score decisório permanece null e a ordenação é por relevância de pesquisa e prazo, sem fingir que o item cabe no orçamento.

Capital requerido do protótipo: aquisição de toda a quantidade obrigatória do item + frete + embalagem + seguro + tributos sobre receita simulada + custo financeiro + instalação + contingência. Não comparar orçamento ao valor global do edital. Lote/grupo fica fora da indicação por item isolado.

Usar cenário base mais recente por item e a cotação correspondente, com desempate por data e id. Reavaliar validade, bloqueios e capital usando available_capital da busca; não reutilizar o bloqueio de capital de uma análise salva com outro orçamento. As outras premissas e o vínculo da cotação vêm do cenário identificado. A busca não muda essa análise persistida.

Ausente/null exige informar orçamento para haver confirmação de encaixe. Zero é orçamento informado e não cabe custo positivo. Negativo ou formato inválido gera 422 legível. Igual ao capital requerido cabe; um centavo abaixo não cabe. Dinheiro usa Decimal e transporte textual.

Ordenação dos elegíveis: score decrescente; resultado estimado decrescente; prazo crescente; procurement_id e item_id crescentes. Explicar os principais componentes sem afirmar que o score é probabilidade de vitória. O detalhe aberto a partir da lista conserva o mesmo contexto de orçamento.

Os itens são alternativas individuais. Não somar resultados como carteira financiável. Seleção combinada e otimização de carteira ficam fora do recorte.

Na interface, validar formato brasileiro, converter para decimal canônico e aplicar debounce curto com AbortController ou número de sequência. Resposta antiga não substitui busca nova. Não reescrever a configuração global de capital por mudar o campo do radar.

## Busca por pregão eletrônico

Contrato proposto: POST /api/v1/items/{item_id}/partner-searches, com query, uf, published_from, published_to e max_results. Resposta retorna search_id, status, complete, request_count, consulted_window, queried_at, candidates, exclusions e limitações de cobertura. GET /api/v1/items/{item_id}/partner-candidates consulta o cache da busca. POST /api/v1/partner-candidates/{candidate_id}/promote cadastra ou associa fornecedor existente e conserva origem.

POST faz consulta limitada, com prazo máximo e resposta parcial explícitos; não criar um novo agendador ou fila distribuída. Se o recorte estourar o prazo, devolver parcial e ponto de parada. Falha da fonte não apaga o cache. Diferenciar success_empty, partial, unavailable e success. O tipo HTTP e o schema finais serão congelados em S01.

Fonte mínima: API pública do PNCP para localizar pregões eletrônicos, recuperar itens e consultar resultados por item. O manual oficial descreve GET /v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numeroItem}/resultados. A consulta de publicação tem filtro de modalidade; não presumir busca textual/categoria na API, filtrar localmente quando o contrato não oferecer esse filtro.

Resolver modalidades pelo catálogo ou pela evidência de domínio confirmada. A verificação atual observou modalidade 6 como Pregão - Eletrônico. O CNPJ do órgão serve à URL da compra e não identifica o fornecedor. Usar identificador da empresa do resultado, preservando texto completo.

A verificação pontual desta revisão fez 11 GET diretos bem-sucedidos, sendo quatro leituras de OpenAPI e sete consultas de dados/catálogo. Ela é independente da missão documental R1, cuja amostragem histórica continua 0/30. Não reescrever esse registro histórico.

Formatos confirmados no acesso direto: publicação usa envelope com data e paginação; itens retornam array e aceitam pagina/tamanhoPagina; resultados por item retornam array sem query de paginação declarada. Não reaproveitar o parser do envelope em todos os recursos. IDs de situação declarados como strings enum no OpenAPI vieram como inteiros na amostra; normalizar explicitamente, preservando o original.

Amostra real de contrato: compra 91984492000152-1-000082/2026, item 1, arado subsolador, unidade un; resultado PJ de COMERCIAL AGRO VOGEL E ZIMMERMANN LTDA, CNPJ 49552222000106, data 26/08/2026, valor unitário homologado R$ 9.600. Esse valor é observação histórica pública, sem representar custo ou cotação atual. A amostra comprova a cadeia técnica desta categoria; não valida notebook, papel A4 ou outra categoria da demonstração financeira.

Dados mínimos por observação: origem e URL, instante da consulta, hash do payload ou evidência sanitizada, identificador da compra, item, sequência do resultado, situação, identificação PJ, razão social, descrição, unidade, quantidade, data e valores com a natureza homologado quando presentes. Ausência de valor não elimina a identificação se o restante sustentar o candidato; preço continua indisponível.

Escopo inicial para pessoas jurídicas brasileiras identificadas. Não coletar CPF, sócios ou contatos pessoais. Outros tipos ficam fora do perfil com razão de exclusão registrada. CNPJ completo identifica estabelecimento; raiz/nome não autorizam fusão. Preservar o texto para não perder letras ou zeros.

Pipeline: recuperar pregões no recorte → obter itens → filtrar por termo/atributos e unidade → recuperar resultados pertinentes → excluir cancelados e situações não utilizáveis com justificativa → normalizar empresa → vincular evidências → ordenar candidatos para investigação.

Recuperação por nome ou código encontra candidatos, mas não comprova equivalência técnica. Exibir matched_terms, comparability como comparable/pending/incompatible e reasons. Somente comparabilidade suficiente ocupa o primeiro grupo; informações insuficientes permanecem pendentes. Incompatíveis ficam fora da recomendação de pesquisa padrão.

Identidade do evento: compra + item + sequência de resultado + versão/hash. Identidade da empresa: documento completo validado. A mesma empresa em vários resultados tem um perfil com várias evidências. Republicação não multiplica eventos. Resultados cancelados e correções invalidam a evidência correspondente sem apagar o histórico.

Campos de candidato: id, legal_name, tax_id, discovery_status, comparability, reasons, evidence_count, observations, last_observed_at e supplier_id opcional. Cada observação preserva a referência completa do pregão/item/resultado. Não chamar número de vitórias de taxa de sucesso ou atribuir capacidade por frequência.

Cadastro por promote é idempotente por estabelecimento e liga candidate_id a supplier_id. Não cria cotação, estoque, disponibilidade, confirmação de parceria nem envio direto. Contato e cotação atuais continuam manuais no MVP. Não enviar mensagem a nenhuma empresa por causa desta implementação.

## Limites locais de consulta

Valores de projeto, não quotas oficiais: concorrência 1; intervalo mínimo de 1 segundo; até 30 tentativas HTTP por busca, incluindo retries e redirects; no máximo 2 páginas de descoberta e 10 candidatos exibidos; limite de 2 MiB por resposta e prazo total de 90 segundos. Aplicar o limite oficial mais restritivo se houver. Respeitar Retry-After; pausar em 401/403/429 persistente, sem contorno.

Janela inicial de publicação e filtro do operador precisam aparecer no resultado. Não confundir data de publicação com data do resultado. Recorte sem cobertura suficiente pode retornar zero ou parcial. O sistema não afirma que não existem fornecedores fora dele.

Cache persistido por parâmetros, versões e instante observado; exibir idade e permitir Atualizar busca. No ensaio offline, reproduzir payloads oficiais reais previamente capturados e identificados. Nas suítes automatizadas, negar rede e usar fixtures identificadas.

## Aceite dos dois recursos

1. Itens sintéticos com capital requerido R$ 4.000, R$ 7.000 e R$ 11.000: orçamento R$ 5.000 retorna somente o primeiro; R$ 10.000 retorna os dois primeiros; R$ 12.000 retorna os três, na ordem da regra. São exemplos de teste, sem preços de mercado.
2. Reduzir de R$ 12.000 a R$ 5.000 remove os demais após a resposta atual; resposta atrasada não os restaura.
3. Cenário antigo barato e atual caro não fazem o item caber; cotação expirada e premissas incompletas vão para needs_analysis.
4. Valor do edital R$ 100.000 e capital conhecido R$ 4.000 podem caber em R$ 5.000; edital R$ 4.000 com capital R$ 6.000 não cabe.
5. Limite exato, um centavo abaixo, zero, null, negativo e taxa percentual têm testes próprios.
6. Adapter executável lê documentação/recursos oficiais e processa ao menos uma cadeia real de pregão eletrônico, item e resultado PJ. Uma lista fixa ou fixture apenas sintética não cumpre o aceite.
7. A amostra oficial do pacote atravessa parser, persistência, deduplicação e UI, com evidências; pode ser usada como replay offline após uma tentativa real do adapter documentada.
8. No recorte da cadeia real deve haver candidato com identificação e ligação verificável. Em recorte sem resultado, o sistema retorna vazio correto com limites. Falha externa retorna indisponível, preserva cache e não inventa empresas.
9. Busca repetida conserva um candidato por estabelecimento e um evento por chave/versão. Mesmo nome com documentos distintos não é fundido.
10. Cadastrar candidato preserva a origem e não preenche automaticamente custo de aquisição com preço homologado.

## Fonte e evidência

Manual oficial PNCP, seção Consultar Resultados de Item de uma Contratação:
https://pncp.gov.br/manual/pt-br/latest/singlehtml/

Contratos públicos:
https://pncp.gov.br/api/consulta/v3/api-docs
https://pncp.gov.br/api/pncp/v3/api-docs

Evidência de verificação limitada atual em evidencias/PNCP_VERIFICACAO_2026-10-15.json. Documentação lida e amostra verificada não significam módulo implementado. Não inferir homologação como entrega executada nem usar valor homologado como cotação atual.
