# 22 — Descoberta de fornecedores por histórico público

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão 0.2-R1. Data 2026-10-12 (America/Sao_Paulo). Revisado em R1-E1.
Status: [PROPOSTO], especificação condicionada; não implementada. Propagação documental E2 em 23 e documentos dependentes; gates de operação permanecem.
Base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.

## 1. Objetivo, usuário e decisões apoiadas

Demonstrar futuramente, em uma categoria delimitada, se resultados públicos ajudam o operador a localizar candidatos relevantes e obter cotações atuais, medindo cobertura, esforço e conversão. A E1 comprova somente evidência documental; não há amostra executada, viabilidade econômica ou capacidade de fornecer demonstrada.

Jornada: item da oportunidade → resultados potencialmente comparáveis → revisão de qualidade/atributos → estabelecimentos identificados → candidatos explicados → contato realizado externamente e registrado manualmente → disposição para vender → cotação vigente por escopo → verificação de produto, quantidade, disponibilidade, logística e capital → análise pelas regras atuais. Sem cotação/insumos obrigatórios, análise permanece pendente, sem margem afirmada. Com esses insumos e sem histórico, análise segue normalmente.

Operador decide quem investigar e quais hipóteses de preço de venda estudar. Não objetivos: certificar capacidade, garantir estoque, revelar margem do vencedor, prever vitória, provar entrega a partir de homologação, automatizar mensagens/participação ou mapear mercado nacional. Um vencedor pode ser concorrente, fabricante, distribuidor, revendedor ou empresa sem interesse em revenda.

## 2. Escopo antecipado e decomposição de RF-028

Marco **2-H**, após identidade/ingestão da fase 1 e junto da preparação da fase 3, sem renumerar fases. Should no MVP geral; seus controles são obrigatórios se o incremento/piloto for habilitado. Não cria dependência do ranking/cadastro para esse histórico.

| Responsabilidade | Dono normativo e fase |
|---|---|
| Ingerir resultados e vínculos no recorte validado | RF-035, 2-H, Should |
| Identidade/proveniência/replay/retificação | Extensões RF-003/004/005 e RNF existentes; não outro RF duplicado |
| Selecionar e revisar comparáveis por atributos | RF-036, 2-H, Should; reaproveita política RN-005/015, sem depender de catálogo do operador |
| Exibir preços históricos com natureza/qualidade | RF-028 revisado: parcela básica 2-H, Should; obrigação de apresentação continua nele |
| Perfil de histórico observado e candidatos explicáveis | RF-037, 2-H, Should |
| Registrar contato/disposição e vincular ao cadastro/cotação | RF-038, fase 3, Should; cadastro RF-013 e cotação RF-015 continuam autoridades |
| Mercado amplo/concentração/tendências; lances completos; CNAE em escala; previsão/ML | Parcela ampliada RF-028 fase 6, Should; sem dependência do piloto |

RF-028 conserva ID e histórico. Sua antiga dependência de RF-017/027 deixa de valer para pesquisa básica: matching de produto do operador e snapshot financeiro pertencem a usos posteriores. RF-013/015 não dependem de RF-035–038; o cadastro pode continuar totalmente manual. Marco exige revisão humana desde o primeiro incremento. Propagação física/CP/RT/UC/TST/HU realizada em R1-E2; plano de piloto em 23.

## 3. Etapas, preços e quantidades

| Etapa comprovada | Natureza de preço | Quantidade permitida | Conclusão vedada |
|---|---|---|---|
| Planejamento/estimativa | estimado | estimada | Receita, venda ou aquisição efetiva |
| Proposta/disputa | lance/proposta, segregado | ofertada quando documentada | Contratação ou preço final |
| Resultado | homologado, com situação/versão | homologada | Entregue, paga ou capacidade atual |
| Ata de registro | registrado | registrada | Obrigação de comprar todo o volume |
| Instrumento contratual | contratado | contratada quando explícita | Execução/aceite/pagamento |
| Execução/aceite/pagamento | fato próprio, se comprovado | entregue/recebida separadamente | Completar etapa ausente por inferência |
| Proposta comercial ao operador | cotação comercial | ofertada/disponível por escopo | Reserva de estoque ou condição de outra filial |

O art. 83 da [Lei 14.133](https://www.planalto.gov.br/ccivil_03/_ato2019-2022/2021/lei/l14133.htm) impede interpretar a mera existência do registro de preços como obrigação de contratação pela Administração. Identificar regime/instrumento antes de aplicar a regra ao caso. Resultado homologado pode ser retificado/cancelado; manter consulta, situação, versões e invalidações.

Preço público é referência contextual de venda/disputa, nunca custo de aquisição. Seleção pelo operador como hipótese exige natureza, justificativa, comparabilidade e sensibilidade; não preenche cotação/custo obrigatório. Não calcular preço unitário a partir de contrato global dividido pela quantidade de um único item.

## 4. Dicionário documental e mapeamento interno

[Matriz](amostras/r1/matriz-fontes.md) fixa URLs, versões, datas e estados independentes. Tipos abaixo são documentais ou internos propostos, **nenhum observado em amostra**. O dicionário não constitui contrato executável. Strings vazias, chave ausente, null, inválido e não aplicável têm razões distintas. Toda transformação guarda fonte, recurso, chave original, campo, data de consulta, versão/contrato e regra de conversão; na produção futura, referência ao Bronze. Nesta revisão não há bruto persistido.

| Fonte/recurso e campo documental | Tipo documental → interno proposto | Ausência e proveniência |
|---|---|---|
| Compras/resultado: `idCompraItem`, `idCompra`, `idContratacaoPNCP`, `numeroItemPncp`, `sequencialResultado` | Texto/inteiros de identificação → strings de fonte + chave técnica UUID | Chave incompleta não vira identidade oficial inventada; F4 §10.7 |
| Compras/resultado: `niFornecedor`, `tipoPessoa`, `codigoPais` | Texto → identificação original/canônica, tipo/país separados | Pessoa física não persistida na amostra; ausência/invalidade abre pendência, estrangeiro sem CNPJ não recebe CNPJ fictício |
| Compras/resultado: `valorUnitarioHomologado`, `valorTotalHomologado`, `quantidadeHomologada` | Manual indica Inteiro, filtros monetários Decimal → Decimal somente após contrato reconciliado | Não truncar fração nem inferir unidade; guardar natureza homologada, não custo |
| Compras/resultado: `situacaoCompraItemResultadoId/Nome`, `dataResultadoPncp`, `dataAtualizacaoPncp`, `dataCancelamentoPncp` | ID/texto/data → situação de fonte e datas distintas | Null não confirma vigência; não inferir fuso por um exemplo; cancelamento invalida referência |
| Compras/item: descrição/unidade/catálogo e contexto | Campos exatos/tipos ainda a fechar → texto/atributos/unidade/fator + evidência | F4 §10.6 descreve recurso; vínculo necessário deve ser provado por IDs, nunca por posição da lista |
| Compras/item contratual: `numeroControlePncpContrato`, `idCompra`, `numeroItem`, `quantidadeItem`, `valorUnitarioItem`, `valorTotalItem` | Texto/inteiro/decimal → IDs e Decimal, natureza contratada | F4 §12.2; unidade/ligação ao resultado pendentes. Não usar `valorGlobal` como unitário |
| Compras/ata e catálogo | Recurso documental existente; mapeamento de campo pendente → evento de ata/taxonomia versionada | Sem fechar contrato não criar adapter, unidade, hierarquia ou estatística |
| PNCP/compra: `numeroControlePNCP`, `orgaoEntidade.cnpj`, datas e totais | Tipos lidos historicamente em 10 → identidade e campos separados | Evidência de 11/10; precisa revalidação atual. CNPJ do órgão não identifica fornecedor |
| PNCP/contrato: `numeroControlePncpCompra`, `valorGlobal`, `dataAssinatura` | Texto/número/data → vínculo, Decimal total, data própria | Capitalização distinta preservada; sem preço de item inferido; fonte histórica em 10 |
| Evidência manual de contato/cotação | Sem campo de API pública → autor, instante, escopo, referência, validade e termos | Responsabilidade RF-013/015/038; informação comercial não deriva do histórico oficial |

As seções indicadas pertencem ao [manual Compras 2.0](https://www.gov.br/compras/pt-br/acesso-a-informacao/manuais/manual-dados-abertos/manual-api-compras.pdf). A lista é mapeamento técnico mínimo, não transcrição integral do manual. Não há suporte alfanumérico confirmado nas APIs apenas porque um campo é texto. Informações sem campo verificado permanecem pendentes no adapter.

## 5. Identidade de estabelecimento

Identificador fiscal é string; guardar representação original e canônica, tipo, país, versão do validador e resultado da validação. Remover apenas pontuação de apresentação conhecida, normalizar letras segundo regra oficial e nunca eliminar letras ou zeros. Usar algoritmo de DV da Receita vigente e fixtures oficiais verificadas em Docker; regex não prova validade cadastral nem capacidade. [Receita](https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/acoes-e-programas/programas-e-atividades/cnpj-alfanumerico) confirma que CNPJs numéricos existentes permanecem válidos no modelo alfanumérico.

UUID interno independe do documento. CNPJ completo identifica estabelecimento; raiz é agrupamento analítico, sem unir contratos, cotações, disponibilidade, contatos ou capacidade de matriz/filial. Raiz comum não prova grupo econômico mais amplo. Dado sem tipo/identidade válida permite candidato provisório com pendência e chave interna; não permite deduplicação fiscal conclusiva. Não buscar sócios/CPF/contatos pessoais como enriquecimento padrão. Exceções de estrangeiro/pessoa física precisam política própria antes de operar.

Correção documental propagada na E2: `08` agora admite identificação fiscal textual numérica/alfanumérica; a restrição anterior só numérica não era validação implementada. Revisar também restrições, normalizadores, importadores e fixtures na implementação, sem fundir estabelecimentos ou alterar legado nesta missão.

## 6. Deduplicação e relações entre eventos

Três níveis independentes: (a) identidade do estabelecimento, (b) identidade de evento/item da fonte, (c) correspondência entre fontes. Mesmo estabelecimento vencedor em dois itens não transforma os itens no mesmo negócio.

Dentro de uma fonte, chave validada do recurso + item + sequência/versão identifica resultado; hash de conteúdo evita versão redundante, consulta nova continua observação. Entre fontes, exigir identificador oficial comum ou relação documentada com grão equivalente. Preservar ambas as proveniências e resolver conflitos por atributo/natureza, não por uma fonte universalmente vencedora. Correspondência registra método, evidências, autor, data e estado: proposta, confirmada, rejeitada ou invalidada.

Fuzzy/nome/raiz apenas sugere; nunca consolida automaticamente. Possível duplicidade fica em grupo pendente, visível individualmente, excluído dos agregados padrão até revisão; não apresentar sua quantidade como fornecedores distintos confirmados. Relatar exclusões e impacto na cobertura.

Resultado → ata → contrato são eventos relacionados, não cópias a apagar. Contar uma vez dentro da mesma etapa/identidade; nunca somar o valor homologado ao contratado como faturamento. Uma compra pode ter vários resultados/contratos; relação 1:1 não é pressuposta. Órgãos também exigem ID, não deduplicação só por nome.

## 7. Política `r1-comparabilidade-1` [PROPOSTO]

Para cada par item pesquisado/resultado, registrar atributo esperado, observado, unidade, fonte e conclusão humana: comparável, incompatível ou pendente. CATMAT/CATSER e termos recuperam candidatos; comprovação exige especificação, embalagem/fator, quantidade, moeda, natureza, datas e condições pertinentes. Código ausente não impede equivalência por atributos demonstrados. Caixa/unidade só convertem com fator provado; lote heterogêneo não gera unitário médio arbitrário.

Datas: resultado/homologação, assinatura, publicação, atualização e consulta separadas. Janela de busca não é data de competência inventada. Região: órgão, estabelecimento e entrega separadas; desconhecida fica explícita. Atualidade depende de consulta/retificação e política por recurso; não medir atraso da fonte apenas pelo tempo desde publicação. Valores nominais preservados; correção monetária futura exige índice, base, método e versão próprios.

Qualidade por registro (categorias exclusivas para contabilização): rejeitado por violação objetiva; pendente por lacuna/conflito/possível duplicidade; utilizável para o propósito após verificações. Dentro dos utilizáveis, observar as restrições de amostra. Amostra não coletada não tem taxa de rejeição zero. Valores anômalos são sinalizados; inclusão/exclusão exige justificativa e autor, mantendo original. Na primeira versão, revisão de anomalia é manual; não impor limiar estatístico aprendido sem dados.

Agregado exploratório proposto por natureza/unidade/atributos/janela/região: só com pelo menos cinco eventos comparáveis distintos e três estabelecimentos distintos, sem duplicidade pendente no conjunto. Limiares são heurísticos calibráveis, baseline desconhecida; não asseguram representatividade. Caso insuficiente: observações individuais qualificadas, agregado “não calculável/amostra insuficiente”. Se apto, mostrar n eventos, estabelecimentos, janela, exclusões, mínimo, mediana e máximo nominais; não chamar faixa de intervalo de confiança. Sem média misturando lances, homologação, ata, contrato e cotação.

## 8. Perfil e priorização de candidatos

Perfil apresenta exclusivamente histórico observado: eventos por etapa, atributos compatíveis, recência, órgão/região observada, qualidade e fontes. “Experiência” significa esse histórico descritivo; “entregou/atendeu/executou” só com evidência correspondente. Frequência vencedora não é taxa de vitória sem universo de participações/perdedores, cuja cobertura permanece desconhecida.

Lista para investigação: primeiro comparabilidade suficiente e identidade estabelecida; depois pendências explícitas; entre comparáveis, ordenar por qualidade/atualidade documentadas, mantendo desempate por chave interna estável. Exibir motivos, não probabilidade ou nota de capacidade. Preferências do operador podem mudar ordem com registro, sem tornar ausência de histórico fator de inaptidão. CNAE/nome podem sugerir papel com método/confiança e rótulo inferido; confirmação exige evidência direta. CNAE não é requisito do piloto. Empresa concorrente/sem resposta não é incapaz universalmente.

Nenhum sinal histórico soma pontos, subtrai pontos ou remove guarda da fórmula `v0.1-ranking`. Pesos, penalidades, exemplos numéricos e precisão ADR-017 permanecem intactos. Busca histórica indisponível não elimina fornecedor já validado.

## 9. Dimensões comerciais e transições

Todas as mudanças registram autor, instante do fato/registro, escopo, motivo, evidência, versão anterior/nova. São domínios independentes de E01–E16/RN-032/D10; não usar “fornecedor confirmado”.

| Dimensão | Transições propostas e guardas |
|---|---|
| Descoberta | identificado → em avaliação → descartado com motivo; descartado → em avaliação somente com justificativa nova. Cadastro pode existir sem cotação |
| Contato | não contatado → contato registrado → aguardando resposta → respondeu ou encerrado/sem interesse. Resposta imediata permite contato registrado → respondeu. Nova tentativa reabre ciclo com histórico; prazo sem resposta encerra contato, não prova recusa/capacidade |
| Disposição de venda | desconhecida → aceita sob condições ou recusada mediante resposta evidenciada; condições/decisão nova gera sucessor; não herdar aceite de outra filial/produto/período |
| Cotação | inexistente → solicitada → recebida → vigente somente com preço, escopo e validade suficientes; vigente → expirada por relógio ou substituída por versão nova. Cotação espontânea admite inexistente → recebida com evidência. Nova cotação não apaga anterior |
| Disponibilidade | desconhecida → informada → confirmada com evidência de quantidade/prazo/escopo, ou insuficiente se evidência negativa; evidência vencida/escopo alterado volta a desconhecida com histórico. Reserva é compromisso distinto, não consequência do preço |

Cotação refere estabelecimento + produto/versão + quantidade + prazo + local/condições + validade. Confirmação para 10 unidades não cobre 20, outro item ou outro estabelecimento. Reaproveitar supplier_id existente após identidade reconciliada; candidato não vira segundo cadastro comercial. Registro de contato não envia mensagem. Logs/evidências minimizados; dados públicos não dispensam finalidade/base e proteção, conforme [ANPD](https://www.gov.br/anpd/pt-br/acesso-a-informacao/perguntas-frequentes) e ADR-016.

## 10. Métricas propostas e limites de interpretação

Baseline de todas: desconhecida; nenhuma meta atingida nesta revisão. Operador mede por ciclo futuro de quatro semanas, registrando categoria/janela/fontes, todos os itens pesquisados e instante de inclusão. 23 fixa plano e MN-11–18 sem duplicar MN-01–10.

| Métrica mínima | Numerador/denominador, grão e observação |
|---|---|
| Cobertura comparável | Itens com >=1 resultado comparável / todos os itens pesquisados no ciclo, incluindo fracassos |
| Candidatos por item | Estabelecimentos distintos confirmados na identidade, por item; raízes em coluna separada, provisórios à parte |
| Qualidade | Utilizáveis, rejeitados e pendentes / registros processados por recurso, categorias exclusivas; possíveis duplicidades são subconjunto de pendentes |
| Resposta | Candidatos que responderam / candidatos contatados; contato conta estabelecimento+item/escopo uma vez, tentativas à parte |
| Aceite de cotar | Candidatos que aceitaram cotar / candidatos contatados; também reportar sobre respondentes, nomeando denominador |
| Cotações vigentes | Candidatos contatados com >=1 cotação vigente / contatados; itens com cotação vigente / pesquisados, separadamente |
| Tempo até cotação | Desde primeiro contato no escopo até primeira cotação válida; reportar durações dos sucessos e casos ainda sem resposta com tempo de observação, sem excluir censurados |
| Esforço | Minutos ativos registrados por item e por cotação válida; horas em insucessos incluídas no total, informar n e casos sem cotação |

Denominador zero = não calculável. Cotação recebida expirada não entra como vigente. Metas numéricas de utilidade dependerão de decisão/calibração em 23; não derivar meta comercial de 30 chamadas exploratórias. Integridade exige 100% dos vínculos com evidência e nenhuma inferência proibida, mesmo se conversão for baixa.

Evolução de mercado (fase 6): valor/volume por etapa e janela separados, somente unidades homogêneas; estabelecimentos e raízes distintos; órgãos por ID; dispersão em coorte comparável. Concentração pode usar participação do valor de uma etapa por estabelecimento no total observado da mesma etapa/coorte; informar exclusões/denominador, sem somar etapas nem afirmar participação nacional. Não calcular concentração com duplicidades pendentes ou total sem natureza. Essas métricas não são entregáveis implementados do piloto.

## 11. Autoridade e arquitetura

Preservar monólito FastAPI/Jinja2, Prefect e PostgreSQL existentes. Reutilizar contratos de run/job/evidência sem afirmar adapter implementado. Ingestão → normalização/identidade → comparabilidade → candidatos → fornecimento/decisão são fronteiras lógicas, não novos serviços.

Produção futura: Bronze guarda payload conforme retenção/proveniência aprovada; domínio transacional conserva eventos/versões e vínculos; Silver/Gold são projeções recuperáveis. `commercial.historical_prices` deve ser estendida/reavaliada para servir observação de preço; não criar outro histórico autoritativo concorrente. `sourcing.suppliers`, produtos e cotações continuam autoridades de identidade/condições comerciais. 08 §9 e 16 §7 detalham campos/PK/FK e migração aditiva; nenhum DDL aplicado. JSON/bytes em Bronze PostgreSQL já são proposta existente, sem novo volume obrigatório por haver JSON.

A amostra R1 tem finalidade diferente: perfil em memória, descarte de bruto, somente agregados minimizados. Esse descarte não é política de produção nem comprova ingestão, agendamento, migrations, readiness ou backup do Radar.

## 12. Docker e reprodução

Todos os processamentos futuros de amostra/fixtures/validação documental rodam em container auxiliar efêmero; [roteiro e restrições](ferramentas-r1/README.md). Na E1, Engine inacessível: amostra/validação automática não executadas. E2 executa somente validação documental offline com ambiente próprio registrado em ferramentas-r1; amostra segue não executada. Sem scripts do sistema, compose operacional, banco, volume existente, portas, credenciais ou socket. Runner/contratos/imagem precisam ser preparados e verificados antes de qualquer GET.

Futuro produto deve desenvolver, executar, testar e operar via Docker/Compose com imagens/dependências fixadas, segredos por nome, destinos de teste/restore independentes, logs sanitizados, quotas/checkpoints por recurso, readiness e gates de migração. Não transportar quota exploratória de 30 chamadas como limite permanente do produto. A E2 propagou os requisitos a 07/21/TRD; ambiente exploratório não prova esses componentes.

## 13. Gates, decisões e continuação

ADRs 018–020 e extensões 010/011/014/016 preservam escopo, identidade, evidência e isolamento. QA-R1-01–05 em [relatório](amostras/r1/relatorio.md): ambiente, contrato, termos/quota, amostra e decisões do operador. Sem esses gates, decisão é antecipar especificação, **não habilitar piloto**. Alternativa é cadastro/pesquisa/cotação manual com origem, mantendo análise independente do histórico.

E2 propagou em 00-prd/TRD, 01/03, 07–17/20/21, índices e diagramas; criou 23/D15, corrigiu restrições fiscais de 08 e estendeu rastreabilidade/testes/backlog. Nenhuma nova amostragem em E2. Os resultados da validação original não abrangem a R1. A escolha definitiva de categoria permanece do operador; papel A4 é somente hipótese exploratória sem amostra.

## Histórico de revisões

| Data | Versão | Mudança |
|---|---|---|
| 2026-10-12 | 0.2-R1 | Especificação central da E1, antecipação condicionada, dicionário documental, identidade, comparação, candidatos e métricas sem execução |

2026-10-13 — 0.3-R1-E2: referências de propagação concluídas e distinção entre não execução da amostra E1 e validação documental E2; sem nova evidência de fonte.

