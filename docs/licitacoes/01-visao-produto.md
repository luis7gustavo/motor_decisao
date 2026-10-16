# 01 — Visão do produto Radar Licita

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-10 (America/Sao_Paulo).
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: rascunho para revisão do operador; capacidades futuras [PROPOSTO].

## 1. Problema e proposta de valor

O Radar Licita deve ajudar um operador comercial a responder: **quais oportunidades de fornecimento ao governo têm viabilidade técnica, operacional e financeira demonstrável, com preferência por execução sem deslocamento do operador?** O resultado é uma fila explicável para decisão humana, sustentada por fontes oficiais, documentos, cotações e cenários.

Encontrar publicação não é comprovar capacidade de participar. Preço de referência não é receita assegurada, cotação não é estoque reservado e sessão eletrônica não dispensa instalação, assistência técnica ou outras obrigações presenciais. O sistema deve tornar essas diferenças visíveis antes de recomendar o estudo ou a participação.

O SILLO [CONFIRMADO] oferece coleta comercial, infraestrutura, histórico e revisão humana. O [diagnóstico](02-diagnostico-as-is.md) mostra que integração PNCP, domínio público e simulador contratual ainda são [PROPOSTO]. A evolução preservará os componentes úteis e o histórico de compra para revenda.

## 2. Operador e partes interessadas

| Parte | Necessidade | Papel no produto |
|---|---|---|
| Operador comercial, inicialmente único | Triar oportunidades, comparar fornecedores, entender caixa e obrigações | Decide, revisa evidências e registra participação/resultado |
| Fornecedor/distribuidor | Esclarecer produto, estoque, prazo, frete e validade | Fonte de cotações/documentos, inseridos pelo operador no MVP |
| Contador/assessoria jurídica/técnica | Verificar matéria especializada aplicável ao caso | Parecer/validação externa registrada com responsável/data |
| Órgão/unidade contratante | Publica condições e atualizações oficiais | Entidade de domínio e origem documental, sem acesso ao app no MVP |
| Responsável técnico pela solução | Operar sincronização, backup, diagnóstico e evolução | Pode ser a mesma pessoa do operador; papéis lógicos distintos |

Não se pressupõe cadastro multiusuário, portal de fornecedor ou comunicação automática com órgãos.

## 3. Modelos de fornecimento

| Modelo [PROPOSTO] | Condições que a análise precisa demonstrar | Exposição principal |
|---|---|---|
| Estoque próprio/compra antecipada | Produto aderente, disponibilidade, armazenagem, capital e prazo | Imobilização e obsolescência |
| Aquisição após adjudicação/contratação | Cotação e disponibilidade suficientes para o prazo; gatilho de compra explicitado | Fornecedor alterar preço/estoque antes da compra |
| Distribuição por transportadora | Embalagem, seguro, prazo, destinos e comprovantes | Frete e recebimento |
| Envio direto pelo fornecedor | Capacidade logística e validação contratual/fiscal específica | Confundir viabilidade física com autorização |
| Fornecedor local/distribuidor regional | Cobertura, custos e atendimento de obrigações locais | Dependência de terceiro e pós-venda |

O modelo selecionado deve ser armazenado no cenário. Envio direto é uma alternativa, não condição universal para operação remota. Compra após adjudicação não autoriza presumir contratação, pedido, pagamento antecipado ou disponibilidade futura garantida.

## 4. Escopo proposto do MVP

MVP = entrega das capacidades Must das fases 1 a 5, com registro operacional básico após a decisão. MoSCoW qualifica necessidade na fase atribuída; o plano de entrega detalhado será consolidado na Execução 3. Nenhum desses recursos está implementado por esta missão.

| Domínio [PROPOSTO] | Entrega mínima | Resultado esperado |
|---|---|---|
| Descoberta oficial | Consulta nacional, filtros, identidade, sincronização e versões | Contratação e unidade de julgamento identificáveis |
| Documentos | Download controlado, extração de PDF textual e evidências | Página/trecho/campo acessível para conferência |
| Logística | Classe A/B/C/D e indicador explicável | Separação entre convencional, restrito, presencial e desconhecido |
| Fornecedores/matching | Cadastro, CSV, cotação versionada e atributos verificáveis | Produto/cotação ligados à exigência correta |
| Finanças | Custos, preço mínimo, margem, lucro, ROI, capital e caixa | Cenário com premissas explícitas, por item/lote/grupo/global |
| Elegibilidade/ranking | Bloqueios, pendências, subscores e versões | Apenas candidatos aptos entram na ordenação de decisão |
| Revisão/operação | Decisão humana, participação manual e acompanhamento | Histórico até execução, entrega, recebimento e encerramento |

Revisão de evidências é necessária já nas fases 2–4; a fase 5 integra a fila e o pipeline comercial. O uso de revisão básica antes da fase 5 é dependência transversal, não uma postergação da validação humana.

Revisado em R1: comparação histórica básica e candidatos são Should condicionados do marco 2-H; contato junto da fase 3. Posteriores: mercado amplo, CNAE em escala, lances completos, melhoria de sensibilidade, OCR/LLM, aprendizado e multiusuário. Dados insuficientes não serão compensados por IA nem por inferência otimista.

Fora de escopo: envio automático de proposta/lance, credenciais de participação em portais, parecer jurídico/contábil/fiscal, pagamento, compra automática ao fornecedor, garantia de vitória/lucro, ERP completo ou exploração de fontes com bloqueio de acesso.

## 5. Jornada e decisão

1. Sincronizar publicações e atualizações preservando o dado oficial bruto.
2. Identificar contratação, unidade de julgamento, itens e prazo para propostas.
3. Obter documentos, extrair condições e registrar evidências/pendências.
4. Classificar logística; uma classe A ainda depende da conferência do edital.
5. Consultar fornecedores, registrar cotações e verificar aderência técnica.
6. Simular a unidade completa de julgamento, caixa e sensibilidade.
7. Aplicar bloqueios e pendências antes da pontuação de priorização.
8. Submeter ao operador; registrar motivo, evidências, versão e decisão.
9. Registrar manualmente a participação e os eventos comerciais posteriores.

Ver [D01](diagramas/D01-contexto.md), [D07](diagramas/D07-atividades.md) e [D10](diagramas/D10-estados.md). Fontes desses diagramas são entregáveis desta execução.

Três resultados de elegibilidade [PROPOSTO], separados do estado comercial: **bloqueada** (falha impeditiva conhecida), **pendente** (falta verificação obrigatória), **viável para priorização** (passou verificações). A revisão é uma dimensão independente: registrar se houve revisão, a decisão humana e sua trilha, preservando bloqueios e pendências. “Revisada” não substitui o resultado de elegibilidade (RN-029 e RN-030). Nenhum score substitui essas condições.

## 6. Princípios de produto

- Fonte, data, versão e incerteza acompanham a recomendação.
- A unidade de análise respeita o julgamento; um item lucrativo não mascara outro obrigatório deficitário.
- O passado é preservado: retificação invalida a atualidade da análise, não apaga o que foi decidido.
- Operação individual local e Docker são restrições de entrega; a interface deve permitir conferir evidência sem consultar SQL/logs.
- Jinja2 é alternativa real [CONFIRMADO]; React é opção [PROPOSTO] a comparar na Execução 2, sem migração pressuposta.
- O PostgreSQL da aplicação continua como banco de domínio. A infraestrutura existente de metadados Prefect tem finalidade distinta.

## 7. Valor e hipóteses a validar

Benefícios esperados [PROPOSTO]: diminuir tempo de triagem, reduzir falsos positivos técnicos, detectar falta de capital antes da participação e tornar decisões reproduzíveis. Não há nesta inspeção evidência de demanda de mercado, disposição a pagar, tamanho de mercado ou rentabilidade real.

O modelo comercial inicial é uso próprio para decisões de fornecimento. Monetização da ferramenta, assinaturas e venda de leads não são premissas. A receita analisada é a receita eventual do fornecimento contratado; a ferramenta não gera essa receita automaticamente.

Hipóteses principais: o operador dispõe de produtos/categorias focais, fornecedores verificáveis e capital; as fontes oficiais oferecem os dados necessários; documentos contêm condições verificáveis; ganhos de tempo compensam custo operacional. O [DAN](03-analise-negocio-dan.md) define métricas e coleta de linha de base para testar essas hipóteses.

## 8. Critérios de valor e decisões pendentes

Antes de usar o MVP em decisão real, uma amostra revisada deve demonstrar trilha de evidência completa, tratamento conservador de desconhecidos, bloqueio de incompatibilidades, cálculo por unidade de julgamento e distinção entre cenário e resultado realizado. Metas quantitativas provisórias estão no DAN; não são resultados medidos.

Pendente com operador: categorias/UFs iniciais, capital e limite por oportunidade, disponibilidade de fornecedores, tolerância de risco, prazo máximo sem recebimento e responsável por validação especializada. A falta dessas escolhas não impede especificar parâmetros configuráveis, mas impede afirmar viabilidade de uma oportunidade real.


## 9. Descoberta por resultados públicos — revisado em R1

[PROPOSTO] Item → comparáveis → estabelecimentos/candidatos explicáveis → contato manual → cotação atual por escopo → disponibilidade/logística/capital → análise. Histórico informa quem pesquisar e preço contextualizado, não quem vende ao operador hoje. Preço público não vira custo, homologação/ata não provam entrega e raiz de CNPJ não funde filiais. Sem histórico, insumos suficientes permitem análise; sem cotação obrigatória, muitos candidatos não resolvem pendência. Ranking/fórmulas preservados.

[23](23-especificacao-piloto-fornecedores.md) define hipótese, escolha de categoria ainda pendente, gates e MN-11–18; não foi executado. E1 teve 0/30 chamadas, sem diversidade/conversão observadas. ADR-018 antecipa apenas RF-028 básico/035–037 em 2-H e RF-038 na fase 3, Should sem dependência reversa no cadastro. Mercado amplo permanece fase 6; D15 mostra a jornada. Revisão documental não autoriza operação.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: escopo antecipado condicionado e fluxo de descoberta, preservadas guardas e proposta central.

