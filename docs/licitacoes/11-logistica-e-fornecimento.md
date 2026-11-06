# 11 — Logística, fornecedores e matching técnico

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: [PROPOSTO] — detalhamento de RF-010–019 e RN-010–015, sem implementação.

## 1. Unidade de avaliação e evidência

Avaliar obrigações da contratação e de todos os itens da unidade de julgamento, incluindo destinos, parcelas, transporte, instalação, treinamento, assistência, garantia, devolução e aceite. Uma condição da contratação se propaga a todas as unidades às quais se aplica; condição de item não se generaliza sem fundamento. Modelo de fornecimento pertence ao cenário: estoque, compra posterior, transportadora, envio direto ou prestador regional. Participação eletrônica não prova execução remota.

Cada condição guarda documento/hash/página/trecho ou campo JSON, instante da consulta, versão, interpretação, autor, nível de confiança e pendência. Estados de informação: confirmado, estimado com fundamento, não atendido, desconhecido e não aplicável justificado. Documento extraído sem cláusula localizada não prova ausência da obrigação: revisão confirma cobertura de anexos/tabelas antes de marcar atendido.

## 2. Classificação A/B/C/D

Ordem normativa de RN-011, independente da pontuação:

| Classe | Condição | Exemplo e ação |
|---|---|---|
| D | Lacuna ou conflito crítico em obrigação/prazo/destino/modelo | Instalação citada e endereço desconhecido: D com alerta de presença; solicitar complemento |
| C | Presença local relevante suficientemente demonstrada, sem lacuna crítica classificatória | Instalação obrigatória no órgão; pode ser atendida por parceiro validado, custeado e permitido |
| B | Execução potencialmente remota com restrições relevantes conhecidas | Agendamento e várias entregas com transportadora confirmada; conferir compromissos |
| A | Evidência suficiente de envio convencional sem presença/restrição relevante não resolvida | Equipamento embalado, destino/prazo/garantia conferidos; ainda exige revisão final |

Uma unidade herda D se houver item/obrigação crítica desconhecida; depois C se qualquer obrigação exigir presença relevante; depois B se restrição relevante; caso contrário A. Guardar resultado por item e consolidado. Um problema técnico pode bloquear unidade A; classe C com prestador confirmado pode ser viável. Classe D não apaga fato presencial conhecido. Não transformar classe automaticamente em participação ou descarte.

## 3. Rubricas do score v0.1-logistica

Preservar fórmula de 04/RN-012. Para pesos w e dimensões conhecidas K:

```text
coverage = Σ(i∈K) w_i
L = lower = 100 × Σ(i∈K) w_i × x_i
upper = min(100, lower + 100 × (1 − coverage))
```

| Dimensão / peso | x=1 | x=0,5 | x=0 | null |
|---|---|---|---|---|
| Envio convencional / 0,35 | Transporte convencional suficiente e confirmado | Transporte especial viável e documentado | Transporte convencional não atende obrigação conhecida | Transportabilidade/obrigações sem evidência |
| Preparação/prazo / 0,25 | Preparação+transporte+buffer dentro do prazo, compromissos confirmados | Plano cabe com folga reduzida explicitada e aceitação de risco | Prazo conhecido inexequível pelo plano | Datas ou compromisso ausentes |
| Destinos/parcelas / 0,15 | Todos cobertos e orçamento completo | Múltiplos/agendados com plano confirmado e restrição relevante | Destino/parcela conhecido não atendido | Endereço/composição crítica ausente |
| Pós-venda / 0,15 | Garantia/retorno remoto suficientes e confirmados | Apoio local terceirizado confirmado ou condição remota especial | Obrigação conhecida sem solução viável | Cobertura/responsável não esclarecido |
| Envio direto / 0,10 | Modelo direto comprovado e validado, ou não aplicável justificado em operador+transportadora | Viável com condição operacional explícita resolvida; sem pendência contratual/fiscal | Vedação ou incapacidade comprovada no modelo selecionado | Confirmação/validação necessária faltante |

Não usar 0,5 para esconder condição crítica não resolvida: nesse caso null, D/pendência. Exigência presencial pode produzir baixa prontidão remota e continuar viável com plano local comprovado. Nota zero só gera bloqueio se a regra objetiva correspondente falhar; score não cria/remova bloqueios.

Pesos finitos >=0 e soma=1; nova combinação cria versão de parâmetros, mantendo verificações obrigatórias mesmo para dimensão com peso zero. Cobertura ponderada é acompanhada de contagem das condições críticas conhecidas, pois peso zero não resolve falta crítica. Não aplicável exige motivo/evidência; não é valor padrão. Mostrar intervalo, componentes, classe e motivos juntos.

Exemplos: todos x=1 → L=100, cobertura=1, [100;100]. Prazo null, demais 1 → L=75, cobertura=0,75, [75;100], D, sem ranking positivo. Envio convencional 1, prazo 1, destinos 0,5, pós-venda 0,5, direto não aplicável justificado 1 → L=85 e cobertura=1; B se restrições remotas, C se pós-venda presencial relevante. A classe depende da obrigação, não de 85.

## 4. Cadastro, catálogos e cotações

Fornecedor: identidade fiscal quando aplicável, razão/nome, contatos mínimos, regiões, condições e situação. Produto versionado: chave estável do fornecedor, SKU, categoria, marca/modelo, unidade, embalagem/conteúdo, atributos técnicos e ficha de origem. Importação CSV exige mapeamento explícito, codificação, separador, decimal e unidade; preview de válidas/rejeitadas; valores brutos preservados. Reimportação idêntica não duplica produto; SKU repetido com atributos divergentes exige revisão, não substituição cega.

Cotação versionada inclui produto/fornecedor, preço/moeda, quantidade disponível e data/evidência, validade, mínimo, preparação, região, condição de pagamento, frete incluído ou separado, envio direto, garantia/devolução e compromissos. Validade sem horário/fuso não vira instante favorável; guardar texto e obter confirmação. Estoque observado não é reservado. Reconferir validade e disponibilidade antes da decisão e gatilho real de compra, sem confundir adjudicação com contrato.

Envio direto exige confirmação específica do fornecedor e análise contratual/fiscal aplicável registrada pelo responsável; este documento não estabelece regra jurídica/fiscal. Se a opção estiver pendente, avaliar cenário independente operador+transportadora, com custos próprios, sem reaproveitar autorização fictícia. Garantia continua obrigação do modelo escolhido; não transferi-la implicitamente ao fabricante.

## 5. Matching por atributo

Pipeline: recuperação lexical/código → normalização de unidades → comparação técnica → avaliação de lacunas/conflitos → revisão humana. Reusar tokenização do SILLO somente para candidatos [PARCIAL]; a confirmação é nova. CATMAT/CATSER, EAN/SKU ou semelhança do nome não substituem especificação.

| Grupo | Comparação proposta | Evidência e tratamento |
|---|---|---|
| Categoria/descrição | Taxonomia e requisitos extraídos | Conflito de função é impeditivo |
| Marca/modelo | Exato ou equivalência permitida documentada | Não presumir que marca alternativa é aceita |
| Dimensão/capacidade/potência | Operador eq/min/max/faixa com unidade convertida | Tolerância só se explicitada; potência maior nem sempre melhor |
| Material/compatibilidade | Conjunto permitido/interfaces/ambiente | Ausência de conector obrigatório é conflito |
| Certificação | Emissor/escopo/validade e produto coberto | Nome parecido de certificado não confirma |
| Quantidade/unidade/embalagem | Fator comprovado e quantidade efetivamente fornecível | Caixa de 12 vs unidade só converte com conteúdo comprovado; sobras não viram receita |
| Garantia | Duração, condições, atendimento e responsável | Prazo sem cobertura pertinente não atende |

Status em ordem de prevalência: incompatível se há conflito obrigatório conhecido; informação insuficiente se falta atributo crítico; possivelmente compatível se há indícios e falta não crítica/revisão; compatível confirmado após verificação suficiente se todos críticos atendidos, sem conflito obrigatório e com revisão registrada. Conflito conhecido prevalece mesmo com outros dados ausentes. Registrar requisito, esperado, ofertado, unidade, resultado e evidência em cada linha. Mudança de produto/item invalida somente conclusões dependentes, de forma conservadora quando impacto não puder ser delimitado.

Quantidade pode exigir múltiplas cotações: alocar quantidades por fornecedor/item sem exceder disponibilidade confirmada; verificar mínimos, embalagem, frete, garantia e eventual heterogeneidade permitida. Não somar dois estoques que derivam do mesmo distribuidor sem confirmação independente. Matching técnico não certifica disponibilidade nem cumprimento de lote.

## 6. Aceites e dependências

Fixtures futuras: potência 500 W exigida vs 400 W → incompatível; embalagem desconhecida → insuficiente; título idêntico e certificado inválido → incompatível; garantia sem responsável → pendência; instalação obrigatória com parceiro/custo confirmados → C, avaliável; prazo null → D e sem positivo. Score de cobertura nunca renormaliza faltantes. Par técnico validado vale só para versões examinadas. Persistência em [08](08-modelo-de-dados.md), finanças em [12](12-simulacao-financeira.md), elegibilidade em [09](09-motor-de-decisao.md). RNF-005/010/016/020 governam evidência e parâmetros. Nada foi testado operacionalmente nesta missão.

## 7. Candidato até condição atual — revisado em R1

Revisão 0.3-R1-E2, 2026-10-13. [PROPOSTO], RF-037/038; [22 §9](22-inteligencia-de-mercado-fornecedores.md#9-dimensões-comerciais-e-transições) é fonte normativa das transições. Contato é registrado manualmente; o Radar não envia mensagens. Histórico demonstra apenas evento observado com etapa. Vencedor pode ser concorrente ou não vender para revenda; CNAE/papel inferido opcional não confirma oferta.

1. Item/versão recebe candidato com evidência comparável, supplier_id por estabelecimento completo, razões e pendências; sem identidade suficiente, candidato provisório claramente marcado. Cadastro pode existir antes de produto/cotação, sem aprovação para fornecimento.
2. Operador avalia razões, registra descarte justificado ou abre ciclo de contato no escopo; sistema não procura contatos pessoais. Canal comercial e evidência minimizados. Não contatado → contato registrado → aguardando resposta → respondeu ou encerrado/sem interesse; resposta imediata é admitida. Nova tentativa inicia ciclo mantendo anterior. Tempo esgotado sem resposta é silêncio, não recusa.
3. Resposta fixa disposição desconhecida/aceita sob condições/recusada, com produto/especificação, quantidade, prazo e estabelecimento. Aceite de cotar não significa preço nem estoque confirmado. Condição nova gera evento sucessor, sem rotular toda empresa como confirmada.
4. Cadastro comercial reutiliza supplier_id reconciliado; produto e quote são criados/versionados por RF-014/015. Inexistente/solicitada são jornada; recebida/vigente/expirada/substituída vêm da evidência/relógio/versões de cotação. Versão vigente exige escopo e validade suficientes, não apenas status digitado.
5. Disponibilidade permanece desconhecida, informada, confirmada com evidência ou insuficiente para a oportunidade. Expiração da evidência ou mudança de escopo torna desconhecida novamente, preservando histórico. Preço não reserva estoque; capacidade para 10 não valida 20, nem outra filial ou período.
6. Conferir matching, logística por destino/parcela/prazo, capital e demais guardas; somente então cenário/decisão atuais. Se cotação faltar, pesquisa histórica não preenche a lacuna. Histórico indisponível não elimina cotação/fornecedor já suficientes.

Cada evento tem autor, instante real e de registro, dimensão, estado anterior/novo, motivo, escopo, evidence_id e versão. RN-032/D10 não recebe nenhum estado novo por causa de contato; são domínios distintos. UI mostra as cinco dimensões lado a lado, origem e prazo de reconfirmação. TST-059/066/067/068/071 verificam essa jornada futuramente em Docker. Métricas e censura dos casos sem resposta em 23; não registrar mensagem como enviada sem evidência do ato externo.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: jornada de descoberta/contato/disposição/cotação/disponibilidade e aproveitamento de identidade, preservados matching e rubricas logísticas.

