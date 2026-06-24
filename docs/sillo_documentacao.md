# SILLO - Documentacao do Sistema

![Logo SILLO](assets/sillo-logo.png)

**SILLO - Intelligence for better buying**

SILLO e uma plataforma local-first de inteligencia de compras para transformar sinais de mercado, historico de preco e catalogos de fornecedores em decisoes de compra mais claras, auditaveis e orientadas por dados.

Esta documentacao consolida a identidade da marca, a arquitetura atual do MVP, o fluxo de dados e o modo de operacao do motor de decisao.

## Identidade

### Conceito

A SILLO deve parecer inteligente, acolhedora, organizada e confiavel. A marca comunica tecnologia humana: um sistema que reduz ruido, organiza evidencias e ajuda a decidir melhor sem parecer frio, agressivo ou generico.

### Direcao Visual

**Soft Tech + Intelligent Systems**

Usar:

- fundos claros e quentes;
- espacos vazios generosos;
- curvas e continuidade visual;
- cards suaves com bordas discretas;
- graficos limpos;
- conexoes, linhas e nos como metafora de sistema;
- contraste confortavel para leitura longa.

Evitar:

- cyberpunk;
- neon ou glow;
- excesso de preto;
- branco puro dominante;
- azul corporativo exagerado;
- visual de IA generica.

### Paleta Oficial

| Papel | Nome | Hex |
| --- | --- | --- |
| Fundo principal | Warm Ivory | `#F7F3ED` |
| Texto principal | Slate | `#2F3437` |
| Cor principal | Sage Teal | `#6FA79B` |
| Cor secundaria | Mist Blue | `#8EA4C8` |
| Destaque quente | Soft Terracotta | `#D69A7C` |
| Verde de apoio | Olive Sage | `#9AA27B` |

### Tipografia

Fonte oficial: **DM Sans**

Uso recomendado:

- titulos e cabecalhos em DM Sans Bold;
- subtitulos, corpo, legendas e citacoes em DM Sans Regular;
- numeros de dashboard com peso medio ou bold, sem exagero.

## Proposta do Produto

A SILLO apoia o ciclo de compra para revenda:

1. coleta sinais de mercado;
2. organiza dados brutos em camadas;
3. normaliza fornecedores e precos;
4. calcula margem, demanda, confianca e risco;
5. separa oportunidades em `comprar_teste`, `revisar` e `ignorar`;
6. preserva historico de cada rodada para auditoria.

O objetivo nao e comprar automaticamente. O objetivo do MVP e entregar uma triagem confiavel para compra experimental controlada.

## Arquitetura Atual

```text
Fontes externas
  Mercado Livre API
  Marketplaces por scraping
  Zoom / Buscape
  MegaMix
  Mirao

        |
        v

Bronze
  dados brutos, payloads e execucoes

        |
        v

Silver
  normalizacao, deduplicacao e produtos de fornecedor

        |
        v

Gold
  oportunidades atuais
  snapshots historicos
  runs versionadas do motor

        |
        v

API FastAPI
  /health
  /ops/*
  /decision-engine/*
```

## Camadas de Dados

### Control

Registra execucoes, fontes e checks de qualidade.

Tabelas principais:

- `control.pipeline_runs`
- `control.source_runs`
- `control.data_quality_checks`

### Bronze

Guarda os dados brutos, com payload original sempre que possivel.

Tabelas principais:

- `bronze.market_web_listings_raw`
- `bronze.price_history_raw`
- `bronze.mercado_livre_products_raw`
- `bronze.mercado_livre_items_raw`
- `bronze.supplier_products_raw`

### Silver

Normaliza os dados que ja podem ser comparados.

Tabelas principais:

- `silver.mercado_livre_product_prices`
- `silver.supplier_products_normalized`

### Gold

Entrega a camada de decisao.

Tabelas principais:

- `gold.decision_opportunities`: estado atual, uma linha por produto ativo;
- `gold.decision_engine_runs`: cada rodada versionada do motor;
- `gold.decision_opportunity_snapshots`: historico completo das oportunidades por rodada.
- `gold.ml_model_runs`: modelos treinados, metricas e artefatos versionados;
- `gold.ml_opportunity_scores_latest`: score hibrido atual e explicacao por produto.

## Fontes Atuais

### Marketplaces e comparadores

| Fonte | Uso | Observacao |
| --- | --- | --- |
| Mercado Livre | Catalogo e enriquecimento | API oficial, com limites em algumas buscas abertas. |
| Amazon | Benchmark de preco e demanda | Util, mas sensivel a anti-bot. |
| Kabum | Benchmark nacional | Boa fonte para perifericos e hardware. |
| Terabyte | Benchmark nacional | Boa fonte para componentes e perifericos. |
| Zoom | Comparacao e historico visivel | Historico completo ainda nao e extraido ponto a ponto. |
| Buscape | Comparacao e historico visivel | Mesmo limite do Zoom. |

### Fornecedores

| Fonte | Status | Observacao |
| --- | --- | --- |
| MegaMix | Carregado | Catalogo em `data/raw/megamix_catalog_raw.json`. |
| Mirao | Carregado | Scraper configurado; uma categoria retorna 404 e e registrada como execucao parcial. |
| Coletek | Carregado | Snapshot importado de planilha do fornecedor. |

## Motor de Decisao

Versao atual: `heuristic_v2_confidence_guard`

O motor calcula:

- preco do fornecedor;
- preco estimado de mercado;
- lucro liquido estimado;
- margem liquida;
- quantidade de evidencias de mercado;
- diversidade de fontes;
- score de demanda;
- confianca de match;
- score final de decisao;
- nivel de confianca;
- flags de risco.

### Recomendacoes

| Recomendacao | Significado | Acao sugerida |
| --- | --- | --- |
| `comprar_teste` | Oportunidade forte, com confianca alta | Comprar lote pequeno para validar giro. |
| `revisar` | Pode haver oportunidade, mas ainda precisa olhar humano | Conferir produto, frete, estoque, anuncio e concorrencia. |
| `ignorar` | Evidencia fraca, margem ruim ou risco alto | Nao comprar agora. |

### Niveis de Confianca

| Nivel | Criterio pratico |
| --- | --- |
| `alta` | Match forte, demanda alta, margem positiva e fontes suficientes. |
| `media` | Sinais promissores, mas ainda com risco ou incerteza. |
| `baixa` | Match fraco, demanda fraca, margem ruim ou evidencias insuficientes. |

### Guardrails Atuais

`comprar_teste` so e liberado quando nao ha flags bloqueantes, como:

- `sem_preco_mercado`;
- `match_fraco`;
- `match_revisar`;
- `demanda_fraca`;
- `demanda_incompleta`;
- `margem_baixa`;
- `fontes_insuficientes`;
- `poucas_ofertas_mercado`;
- `modelo_nao_confirmado`;
- `preco_mercado_muito_disperso`;
- `ticket_fornecedor_muito_baixo`.

## Resultado Validado em 01/06/2026

Ultima execucao validada:

| Campo | Valor |
| --- | --- |
| `decision_run_id` | `4b79565d-51da-4a2c-9c12-cacee5c64843` |
| `scoring_version` | `heuristic_v2_confidence_guard` |
| Status | `success` |
| Produtos pontuados | `5.776` |
| Evidencias usadas | `21.031` |
| `comprar_teste` | `2` |
| `revisar` | `38` |
| `ignorar` | `5.736` |

Distribuicao por confianca:

| Nivel | Produtos |
| --- | ---: |
| Alta | 2 |
| Media | 38 |
| Baixa | 5.736 |

Distribuicao por fornecedor:

| Fornecedor | Comprar teste | Revisar | Ignorar |
| --- | ---: | ---: | ---: |
| Coletek | 0 | 0 | 624 |
| MegaMix | 1 | 18 | 2.341 |
| Mirao | 1 | 20 | 2.771 |

Produtos classificados como `comprar_teste` na rodada:

| Produto | Fornecedor | Preco fornecedor | Preco mercado estimado | Margem liquida | Confianca |
| --- | --- | ---: | ---: | ---: | --- |
| Adaptador USB 3.0 para RJ45 10/100/1000 Mbps Dex - UR-03 | Mirao | 71,01 | 169,00 | 25,98% | Alta |
| ADAPTADOR WIRELESS 600MBPS | MegaMix | 34,99 | 81,99 | 25,32% | Alta |

## Como Operar

### Subir ambiente

```powershell
.\SetupMotor.cmd
```

### Verificar API

```powershell
Invoke-RestMethod http://127.0.0.1:8010/health
```

### Coletar fornecedor Mirao

```powershell
docker compose exec -T api python scripts/collect_suppliers.py --supplier mirao
```

### Importar MegaMix e rodar motor

```powershell
docker compose exec -T api python scripts/build_decision_engine.py --import-megamix
```

### Consultar resumo do motor

```powershell
Invoke-RestMethod http://127.0.0.1:8010/decision-engine/summary
```

### Atualizar camada hibrida de ML

```powershell
docker compose exec -T api python scripts/ml_run_all.py --predict-only
Invoke-RestMethod http://127.0.0.1:8010/ml-engine/summary
```

### Ver oportunidades

```powershell
Invoke-RestMethod "http://127.0.0.1:8010/decision-engine/opportunities?recommendation=comprar_teste"
```

### Ver historico de rodadas

```powershell
Invoke-RestMethod http://127.0.0.1:8010/decision-engine/runs
```

## Leitura de Seguranca das Decisoes

O motor esta pronto para apoiar decisao humana, mas ainda nao deve executar compra automatica. A recomendacao `comprar_teste` significa:

- existe margem estimada;
- existem sinais de demanda;
- o match textual e aceitavel;
- a oportunidade passou pelos guardrails atuais;
- ainda falta validacao comercial final.

Antes de comprar:

1. confirmar estoque e prazo do fornecedor;
2. validar frete e impostos reais;
3. revisar se o produto comparado e exatamente equivalente;
4. checar concorrencia e taxa do canal de venda;
5. comprar lote pequeno;
6. registrar giro, devolucao e margem real.

## Roadmap

### Proximo ciclo tecnico

- Criar entidade canonica de produto para unir fornecedor e mercado com menos ambiguidade.
- Melhorar matching com embeddings ou modelo estatistico treinado com pares validados.
- Separar margem por canal de venda.
- Incluir frete real por fornecedor.
- Criar painel de revisao humana para aprovar/reprovar oportunidades.
- Registrar decisao humana para gerar dataset supervisionado.

### Evolucao de ML

A primeira camada hibrida ja esta ativa. Ela compara Logistic Regression,
Random Forest, HistGradientBoosting e XGBoost CPU com validacao cruzada
estratificada, versiona o melhor modelo e preserva a heuristica como trava de
seguranca. Os rotulos atuais ainda sao proxy da heuristica, nao resultados
comerciais.

A evolucao atual compara treino somente real contra treino real com dados
sinteticos controlados. Os sinteticos entram apenas no treino, recebem peso
reduzido e so sao aceitos quando melhoram a avaliacao composta exclusivamente
por registros reais. O model card e o relatorio de augmentation ficam em
`reports/ml/`.

Quando houver dados historicos suficientes, substituir os rotulos proxy por
resultados reais e evoluir os modelos:

- modelo de probabilidade de venda;
- modelo de margem realizada;
- classificador de match de produto;
- estimador de risco de preco disperso;
- ranking learning-to-rank para priorizar oportunidades.

## Tom de Voz

A SILLO deve falar de forma clara, objetiva e acessivel:

- "2 oportunidades passaram pelos criterios de teste."
- "171 itens ficaram na fila hibrida de revisao antes de compra."
- "Este produto tem margem estimada, mas o match ainda precisa de confirmacao."
- "Nao recomendamos compra automatica sem validar estoque, frete e equivalencia."

Evitar:

- promessas absolutas;
- linguagem exagerada de IA;
- tecnicismo sem explicar impacto;
- tom alarmista.

## Arquivos Relacionados

- `docs/arquitetura_tecnica.md`: arquitetura tecnica, dados, motor, objetivos e proximos passos.
- `docs/uso_local_e_importacao.md`: runbook operacional local.
- `docs/sillo_documentacao.html`: versao visual da documentacao.
- `docs/assets/sillo-logo.png`: logo da SILLO.
- `README.md`: guia operacional principal do repositorio.
- `docs/mercado_livre_ngrok.md`: OAuth Mercado Livre com ngrok.
