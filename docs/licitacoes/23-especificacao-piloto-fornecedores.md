# 23 — Especificação do piloto de descoberta de fornecedores

Versão: 0.3-R1-E2. Data: 2026-10-13 (America/Sao_Paulo). Revisado em R1.
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: [PROPOSTO], plano implementável condicionado a gates; **piloto não executado nem pronto para operar**. Nenhuma meta comercial foi atingida nesta revisão.

## 1. Hipótese e decisões apoiadas

Hipótese: em uma categoria delimitada, resultados públicos permitem localizar candidatos relevantes e ajudar o operador a obter cotações atuais com esforço aceitável. Medir cobertura, esforço e conversão; não presumir disponibilidade, capacidade, margem ou interesse em vender para revenda. O vencedor histórico pode ser concorrente. Fontes de desenho: [22](22-inteligencia-de-mercado-fornecedores.md), [matriz E1](amostras/r1/matriz-fontes.md), ADR-018/019/020.

Decisões ao encerrar o ciclo: continuar com o mesmo recorte; ajustar fonte/atributos/janela; abandonar a hipótese para a categoria; ou preparar ampliação controlada. Não decidir investimento/escala nacional a partir deste piloto. Elegibilidade e finanças continuam pelas regras atuais; resultado homologado não é entrega, ata não é compra obrigatória e preço público não é custo de aquisição.

## 2. Categoria: critérios e escolha ainda pendente

A E1 leu documentação oficial, mas não amostrou dados (0/30 chamadas). Assim, **não há evidência suficiente para recomendar categoria comercial definitiva nem enumerar três candidatas como validadas**. Única hipótese exploratória preservada: papel A4, pois dimensões/formato, gramatura, cor e folhas por embalagem permitem explicitar atributos comparáveis. Isso é raciocínio de desenho, não observação de preenchimento, diversidade de empresas, preço/logística manejáveis ou propensão a cotar. Peso/frete, volume e requisitos adicionais podem torná-lo inadequado.

| Critério de escolha do operador | Evidência a obter antes de aprovar categoria |
|---|---|
| Comparabilidade técnica | Checklist de atributos obrigatórios, unidade/embalagem/fator e evidência do produto; código não basta |
| Taxonomia/identificadores | Código CATMAT quando disponível, versão/origem, estabilidade de vínculos e identificação de estabelecimento; sem código, método por atributos documentado |
| Disponibilidade de dados | Amostra futura por recurso mostrando identidade/item/preço/natureza/quantidade/unidade/datas e vínculos utilizáveis; separar ausente/null/inválido |
| Logística de produto físico | Região/destinos, peso/volume/frete/armazenagem/parcelas e prazo avaliados, sem presumir envio direto |
| Diversidade observada | Estabelecimentos distintos demonstrados e raízes à parte, evitando contar replicação como diversidade; nenhum número observado na R1 |
| Possibilidade comercial | Operador disposto a pesquisar e solicitar cotação manualmente no futuro; histórico não comprova disposição de venda |

Decisão registrada em `pilot_scope` versionado, autor/data, categoria/atributos e motivo. Se amostra não sustentar a hipótese A4, voltar aos critérios; não expandir categorias para fabricar sucesso. QA-R1-05 permanece aberta.

## 3. Recorte proposto, grão e limites

Proposta para decisão do operador: uma categoria, ciclo de quatro semanas, até 20 itens de oportunidades descritos de forma suficiente e inseridos consecutivamente segundo regra escrita antes da busca. Manter todos os itens do ciclo, inclusive sem resultado; se não houver 20, reportar n real e não afirmar eficácia. Este ciclo de pesquisa/cotação é distinto do piloto do MVP completo de 03 (50 unidades e operações realizadas); não altera MN-01–10 nem exige participação comercial para avaliar descoberta.

Janela histórica candidata: 12 meses completos anteriores à abertura do ciclo, fixada em `pilot_scope`; reduzir antes de executar se contratos/limites/custo não permitirem e registrar versão/motivo. Região de entrega: uma UF definida pelo operador, sem presumir São Paulo ou UF do órgão como destino; candidatos podem estar em outra UF, com frete/prazo a verificar. Se a fonte não filtrar categoria/região pertinente, consultar apenas janelas/escopos contratualmente admitidos e classificar localmente, contabilizando descarte/custo. Não prometer busca histórica completa nesse recorte.

Grão de pesquisa: item versionado da oportunidade. Grão histórico: resultado por item/seq/estabelecimento/versão e contrato por item comprovado, em conjuntos separados. Naturezas prioritárias homologado e contratado; contrato sem item fica contexto total, não comparável unitário. Ata somente contexto identificado se o recurso vier a ser validado; não soma volume executado. Lances completos, perdedores e execução não estão prometidos. CNAE, painel amplo/concentração/tendências e ML ficam fase 6.

Fontes condicionadas: Compras para item/resultado por manual legível da E1, PNCP para identidade/atualização da compra e vínculo contratual, recurso de contrato/item que provar cada atributo. [10](10-integracoes-pncp.md) e matriz definem preferência por atributo; não escolher uma fonte superior em tudo. Cotação atual sempre evidência comercial registrada manualmente.

## 4. Gates antes de implementar/habilitar

| Gate e responsável | Evidência necessária / alternativa |
|---|---|
| Técnico — ambiente e release | Imagens do produto por digest/plataforma, dependências com lock/hashes, teste/restore isolados, guarda antes da escrita; 21/16. Validação documental auxiliar da E2 não aprova ambiente produtivo |
| Integrador — contratos | QA-R1-02: base/método/rota/security/parâmetros/tipos/paginação/datas/vínculos/hash fechados por recurso, resolvendo Integer/Decimal do manual e suporte fiscal sem suposição |
| Operador/integrador — termos e limites | QA-R1-03: finalidade/reuso/termos/quota/atualização/cobertura registrados; sem quota publicada, documentar limite local conservador e condição de pausa |
| Integrador — dados observados | QA-R1-04: nova missão autorizada para amostra limitada, ambiente isolado e ledger cumulativo preservado se retomar R1; E2 não coleta. Sem dado suficiente, recurso desligado, sem inventar utilidade |
| Operador — recorte e esforço | QA-R1-05: categoria/UF/janela/coorte, orçamento de minutos, canais comerciais e responsabilidade de medição aprovados antes de contato real |
| Responsável de tratamento/especialista | Finalidade/base/minimização/retencão e condições do caso; excluir CPF/sócios/contatos pessoais desnecessários, confirmar regras aplicáveis quando usadas |

Alternativa enquanto gates abertos: pesquisa humana de referências com origem e cadastro/cotação manual usando RF-013/015; análise suficientemente fundamentada prossegue sem histórico. Isso não satisfaz aceite de ingestão automática do piloto. Não contatar fornecedor nem executar atos comerciais nesta missão documental.

## 5. Ambiente futuro Docker/Compose

Desenvolver, executar, testar e operar em Docker; host somente Git e Docker Engine/Desktop com Compose compatível. Banco de domínio e controle Prefect existentes preservados, sem novo banco/cache/agendador; JSON/bytes Bronze no PostgreSQL. Modelo/migração [08](08-modelo-de-dados.md)/[16](16-plano-de-migracao.md), arquitetura/recursos/egress [07](07-arquitetura-to-be.md)/[21](21-infraestrutura-docker.md).

Release futura deve registrar tag informativa, digest por plataforma, arquitetura, runtime/dependências, hashes de lock/configuração não sensível e revisão de código. **Versões/digests do produto ainda não selecionados nem validados; bloqueio explícito de habilitação.** Os digests auxiliares efetivamente usados em E2, em [ferramentas](ferramentas-r1/README.md), fixam somente validação de documentos, não API, worker, banco ou renderer do produto.

Limites iniciais propostos: perfil R1-P1 de 05, host Docker 4 vCPU/8 GiB, worker 1 CPU/512 MiB/128 PIDs e deadline 15 min/job, sem portas novas. Imagem não root, capabilities removidas, no-new-privileges, temporário limitado, caminhos relativos/LF. Ensaiar Windows em caminho com espaços/acentos; nenhum caminho pessoal requerido. Rede de teste sem fonte externa, volumes/credenciais independentes e nenhuma herança de nomes globais operacionais.

**Proposto; não executado nesta missão.** Os nomes de Compose/serviços abaixo são contrato futuro a implementar/revisar, não arquivos existentes. `tests-history` deve executar guarda de destino antes de qualquer escrita, usar relógio/fixtures sintéticos e negar rede externa. Não substituir pelo Compose operacional.

```text
docker compose -f compose.test.yml config --quiet
docker compose -f compose.test.yml run --rm tests-history
```

Runner de testes só será executável depois do gate de release/imagens/locks/destinos; migrations e ativação de schedules seguem 16. Backup/restore e retorno de imagem compatível devem ser ensaiados separadamente; nenhuma instrução acima autoriza execução nesta R1. Readiness depende de schema/DB; falha histórica apenas sinaliza recurso indisponível. Logs sanitizados por fonte/job, sem bruto/segredo.

## 6. Ingestão, quotas e identidade

Proposta produtiva inicial a calibrar frente ao contrato: concorrência 1 por fonte, intervalo >=1 s entre inícios, máximo 100 tentativas por job compartilhado com retries/redirects, até três tentativas totais por requisição transitória dentro desse teto, até 2 MiB/resposta, 50 MiB/job e 10 mil registros/job. São limites locais do produto proposto, **não quota oficial e não orçamento exploratório de 30 GET da E1**. Gate exige compatibilidade por recurso; oficial mais restritivo prevalece. Teto impede expansão silenciosa: pausa com janela parcial/checkpoint, operador revisa backlog/custo antes de retomar. Retry-After prevalece; 401/403 pausa, sem contorno/autenticação.

Janela/cursores contratados, checkpoint após commit, lease/fencing, fingerprint por fonte/recurso/parâmetros e hash canônico versionado. Repetição não duplica; retificação cria versão/invalida dependentes; falha mantém último válido. Quota é registrada antes de enviar tentativa. Schedule a cada seis horas é hipótese subordinada às quotas, inicialmente desativado. Reconciliação periódica de registros acompanhados não autoriza coleta massiva.

CNPJ completo original/canônico textual e DV oficial nos dois formatos, UUID independente; raiz somente agrupamento. Ausente/inválido fica provisório; pessoa física/estrangeiro requer tratamento específico antes de habilitar, sem CPF em perfis/logs. Equivalência entre fontes demanda mesma identidade/grão/etapa com evidência; resultado e contrato relacionados conservam eventos distintos. Conflito abre revisão e possível duplicidade fica fora do agregado padrão. Registros de observação não são apagados por preferência de fonte.

## 7. Jornada e plano de observação manual

Operador inclui item e inicia cronômetro de esforço → consulta resultados → revisa comparabilidade/qualidade/identidade → examina candidatos e razões → seleciona investigação e registra contato realizado externamente → registra resposta/disposição → vincula cotação recebida e seu escopo → verifica disponibilidade, produto, frete/prazo, capital e demais requisitos antes da análise. Histórico pode informar preço de venda hipotético explicitamente escolhido; aquisição permanece cotação/premissa admitida.

Dimensões de 22 §9 são independentes: descoberta, contato, disposição, cotação e disponibilidade. Contato sem resposta não é recusa, aceite de cotar não é capacidade, cotação vigente não reserva estoque. Reutilizar supplier_id de RF-013; cotação RF-015 segue filial/produto/quantidade/condições/validade. Estados E01–E16 não mudam. Lista ordenada explica pesquisa, não soma pontos na fórmula global.

Proposta de janela de observação por contato: dez dias corridos desde primeiro contato externo, com data de corte do ciclo e observação adicional se contato tardio. Operador fixa regra antes do ciclo, registra tentativas, tempo ativo e duração de espera separadamente. Não insistir após recusa; tentativas não multiplicam denominador. Itens sem contato continuam na cobertura/esforço por item. No encerramento, publicar datas/duração por caso ainda sem resposta/cotação; não excluir insucessos nem calcular mediana de sucessos como tempo de toda população.

## 8. Métricas MN-11–18 e medição

Baseline de **todas**: desconhecida. Responsável: operador registra eventos/tempo, técnico verifica contagens/versões e operador revisa coorte completa. Janela: ciclo de quatro semanas e seguimento de dez dias por contato, com corte e duração explicitados. População: todos os itens admitidos pela regra do ciclo, todos os registros processados por recurso e todos os contatos de candidatos no escopo. Não usar universo nacional. N=0 resulta não calculável; nenhuma meta atingida pela documentação.

Unidade de contato: estabelecimento+item/escopo do ciclo, contado uma vez mesmo com várias tentativas; relatório adicional por estabelecimento único para não confundir empresa com oportunidades de cotar. Mudança relevante de escopo cria novo episódio identificado e conserva vínculo, sem inflar retroativamente coorte inicial. Cotações vigentes avaliadas na data de corte; histórico de recebimento e expiração exibido à parte.

| ID | Medida e denominador | Fonte/grão | Meta provisória e uso |
|---|---|---|---|
| MN-11 | Itens com >=1 resultado comparável / todos os itens pesquisados, inclusive sem resultado | historical_comparisons + ledger da pesquisa, item versionado; janela histórica separada do ciclo | Hipótese >=50%, calibrável antes do ciclo; não estima cobertura da fonte ou mercado |
| MN-12 | Número de estabelecimentos identificados distintos por item; número de raízes distinto separado; provisórios à parte | supplier_candidates + suppliers por item; reportar distribuição, n de itens e itens com zero | Hipótese mediana >=2 estabelecimentos/item; raiz não é fornecedor único nem grupo econômico |
| MN-13 | Utilizáveis/N, rejeitados/N, pendentes/N; possíveis duplicidades/P e /N explicitados como subconjunto de pendentes | N=observações normalizadas de origem processadas por recurso no recorte, antes da consolidação entre fontes; replay idêntico é recepção repetida separada, não N novo | Reconciliação 100% das categorias exclusivas; hipótese utilizáveis>=60%, sem apagar exclusões. Contagem de eventos após equivalência relatada separadamente |
| MN-14 | Respondentes/contatados no escopo; reportar também estabelecimentos únicos e tentativas | Ledger de contato, primeira resposta substantiva até corte; silêncio incluído no denominador | Hipótese >=30%; resultado pequeno é descritivo, sem taxa de vitória/capacidade |
| MN-15 | Aceitaram cotar/contatados; adicional aceitaram cotar/respondentes com denominador rotulado | Evidência da resposta/disposição no mesmo escopo; promessa não é cotação | Hipótese >=20% sobre contatados; sobre respondentes sem limiar prévio, observar |
| MN-16 | Contatados com >=1 cotação vigente/contatados; itens com >=1 cotação vigente/pesquisados; número de cotações vigentes/contatados e /itens em coluna de razão, não percentual | quote_id/versão+escopo+relógio no corte; cotação espontânea de não contatado separada no primeiro denominador, elegível por item se válida | Hipótese >=10% dos contatados e >=20% dos itens; nenhuma extrapolação para margem. Zero vigente pode coexistir com recebidas depois expiradas |
| MN-17 | Duração desde primeiro contato até primeira cotação que atendia escopo/validade ao receber; todos os casos sem cotação têm duração observada e flag censurado | Episódio de contato, relógio; recebida inválida não encerra tempo. Reportar sucessos, censurados e razões; expiração no corte separada em MN-16 | Hipótese mediana dos sucessos <=5 dias, sempre com n/N e censura; jamais declarar meta global por excluir insucessos |
| MN-18 | Minutos ativos totais/n itens pesquisados; minutos ativos totais/n cotações válidas obtidas, incluindo trabalho dos insucessos; separação de espera | Cronômetro/registro manual por atividade/item/episódio; rateio de pesquisa compartilhada explicitado uma vez, amostra n informada | Hipótese <=30 min/item e <=120 min/cotação obtida; N cotação=0 não calculável, esforço total continua reportado |

Metas numéricas são hipóteses de planejamento, não critérios comprovados de viabilidade econômica. Ajustes antes/depois do ciclo devem manter a versão original e explicar alteração; não mudar denominador para atingir meta. Independente de conversão, exigir 100% de referências e mudanças com evidência/autor/escopo e zero inferência proibida. Sem lucro realizado, não calcular ROI do negócio; esforço humano medido não substitui custos completos de operação.

## 9. Aceite, pausa e decisão

Aceite técnico do incremento: contratos dos recursos habilitados fechados; TST-056–071 e testes herdados pertinentes passam em Docker isolado; igualdade de replay/deduplicação e integridade de escopo; nenhuma escrita operacional por teste; UI distingue dimensões e qualidade; readiness/backup/restore/aditividade aprovados na implementação. Produção não herda aprovação de ferramenta documental. Para analisar margem/ranking, exigir adicionalmente Must da fase 4; piloto de pesquisa pode terminar apenas em cotação e esforço, sem simular decisão incompleta.

Aceite comercial **exploratório**: concluir relatório do ciclo com coorte/corte, MN-11–18, recusas/silêncio, razões de exclusão e esforço; operador avalia se candidatos ajudam a obter cotações que de outro modo exigiriam mais trabalho. Não exige compra/participação/contrato para aprender. Contato real só em missão posterior autorizada pelo operador; não necessário para concluir esta revisão.

Pausar recurso em bloqueio/quota/termos incertos, schema essencial alterado, vazamento, conflito de identidade não contido, cobertura impossível dentro do teto, destino de teste inadequado ou perda de rastreabilidade. Preservar dados/histórico e relato parcial; não contornar falha nem trocar fonte silenciosamente. Pausar análise positiva se cotação/insumo obrigatório faltante; falha histórica isolada não cria bloqueio novo.

Ampliar apenas se integridade técnica passar, denominadores estiverem completos e operador julgar conversão/esforço úteis em pelo menos um ciclo, confirmando limites e riscos; preferir segundo ciclo independente antes de nova categoria/região. Rejeitar ou reformular hipótese no recorte se não houver comparáveis utilizáveis ou cotações apesar de esforço/observação acordados; resultado inconclusivo se coorte/exposição insuficientes. Não afrouxar qualidade ou chamar inconclusivo de aprovado para cumprir meta. Limites de cinco eventos/três estabelecimentos de 22 regem agregado de preço, não tamanho suficiente para conclusão comercial.

## 10. Backlog e decisões pendentes

Ordem: TEC-01/02/04 (ambiente/contratos) → HU-035 (identidade/fonte/replay) → HU-036 (qualidade/comparáveis) → HU-028 básico/037 (preços/candidatos) → RF-013/015 e HU-038 (cadastro/contato/cotação) → jornada/medição TEC-05/06. [15](15-roadmap-backlog.md) define aceites/origem/testes e revisão humana por incremento; [17](17-matriz-rastreabilidade.md) percorre CP→RF→RN→RT→componente→entidade→UC→teste→fase.

Riscos prioritários: preço histórico virar custo (RSK-19), etapas virarem execução (20), dupla contagem (21), viés/seleção (22), termos/quota (23), identidade/unidade (04), escopo/volatilidade (05) e falsa segurança (16). Mitigações/detecção/residual em 13. Dados oficiais/documentação não provam conversão, cobertura nacional nem capacidade de vender ao operador hoje.

Operador ainda precisa decidir categoria/atributos/UF, janela/coorte e orçamento de esforço, responsáveis/canais comerciais, critérios calibrados de utilidade e fundamento de tratamento. Integrador precisa resolver contratos e dados; técnico precisa implementar/testar release e isolamento do produto. Essas pendências bloqueiam implantação/piloto, **não foram declaradas resolvidas pela conclusão documental R1**.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: piloto delimitado, hipótese A4 não aprovada, gates/quotas produtivas propostas, jornada e MN-11–18 com denominadores/censura/esforço; execução futura condicionada.
