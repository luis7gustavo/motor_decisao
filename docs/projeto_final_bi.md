# SILLO - Projeto Final de Business Intelligence

## Tema escolhido

O projeto SILLO analisa oportunidades de compra para revenda. O objetivo de negocio e reduzir o risco de decidir manualmente quais produtos comprar quando existem muitos fornecedores, sinais de mercado fragmentados e custos que afetam margem.

## Fonte dos dados

Os dados sao reais e vem de fontes operacionais do projeto:

| Origem | Uso no projeto | Camada original |
| --- | --- | --- |
| Catalogos de fornecedores MegaMix, Mirao e Coletek | Produtos, precos de compra, SKU/EAN e disponibilidade quando existente | `bronze.supplier_products_raw` |
| Mercado Livre | Evidencias de preco, oferta e sinais de mercado | `bronze.mercado_livre_items_raw`, `bronze.mercado_livre_products_raw` |
| Marketplaces e comparadores | Precificacao, concorrencia e sinais de demanda | `bronze.market_web_listings_raw`, `bronze.price_history_raw` |
| Execucoes de pipeline | Saude da coleta, volume carregado e falhas | `control.pipeline_runs`, `control.source_runs`, `control.data_quality_checks` |

O projeto nao usa dataset ficticio. As camadas brutas preservam payloads e hashes para auditoria.

## Problema de negocio

Pequenos revendedores precisam transformar catalogos grandes em uma lista curta de produtos que valem compra-teste. A decisao manual consome tempo e aumenta risco de estoque parado, margem insuficiente ou compra de produto mal comparado com o mercado.

A pergunta central e: entre os produtos disponiveis nos fornecedores, quais tem preco, demanda, evidencias e confianca suficientes para justificar uma compra-teste controlada?

## Processo de ETL

O fluxo do projeto segue arquitetura em camadas e foi adaptado ao formato pedido pela disciplina:

| Etapa | Implementacao |
| --- | --- |
| Staging / Repositorio | `repositorio.*`, criado por `scripts/setup_bi_final_project.sql`, copia os dados brutos de `bronze` e `control` sem transformacao relevante |
| ETL | Pipelines Python tratam tipos, nulos, padronizacao de titulos, precos e datas, alem de checks de qualidade em `control.data_quality_checks` |
| DW | `dw.*` cria dimensoes e fatos em modelo dimensional |
| Data Mart | `datamart.*` cria views analiticas para consumo direto no Power BI |
| Dashboard | `dashboard_motor_decisao.pbix` apresenta KPIs, filtros e visualizacoes analiticas |

## Modelo dimensional

### Fato principal

`dw.fato_oportunidade_compra`

Granularidade: uma oportunidade de compra por produto de fornecedor na rodada atual do motor de decisao.

Medidas principais:

- preco de fornecedor;
- preco estimado de mercado;
- lucro estimado;
- margem liquida;
- quantidade de ofertas de mercado;
- quantidade de fontes;
- score de demanda;
- confianca do match;
- score de decisao;
- quantidade de flags de risco.

### Outras fatos

| Fato | Granularidade | Finalidade |
| --- | --- | --- |
| `dw.fato_execucao_fonte` | uma execucao por fonte coletada | monitorar saude, volume e duracao do ETL |
| `dw.fato_score_ml` | um score hibrido ML por produto e modelo mais recente | comparar heuristica, ML e decisao final |

### Dimensoes

| Dimensao | Finalidade |
| --- | --- |
| `dw.dim_tempo` | analise temporal e role-playing de datas |
| `dw.dim_fornecedor` | fornecedor conformado entre fatos |
| `dw.dim_produto_fornecedor` | produto normalizado do fornecedor |
| `dw.dim_recomendacao` | classificacao: comprar_teste, revisar, ignorar |
| `dw.dim_confianca` | nivel de confianca da recomendacao |
| `dw.dim_execucao_motor` | versao, status e duracao da rodada do motor |
| `dw.dim_fonte_coleta` | fonte, tipo e tabela bruta de coleta |
| `dw.dim_modelo_ml` | versao e tipo do modelo treinado |

### Conceitos avancados usados

| Conceito | Onde aparece |
| --- | --- |
| Dimensao tempo | `dw.dim_tempo` |
| Role-playing dimension | `geracao_tempo_sk`, `atualizacao_tempo_sk`, `inicio_tempo_sk`, `fim_tempo_sk`, `score_tempo_sk` |
| Dimensao conformada | `dw.dim_fornecedor` reutilizada nas fatos de oportunidade e ML |
| Dimensao degenerada | UUIDs operacionais mantidos dentro das fatos, como `oportunidade_id` e `source_run_id` |
| Multiplas tabelas fato | oportunidade, execucao de fonte e score ML |

## Data Mart

O schema `datamart` tem views preparadas para o Power BI:

| View | Uso |
| --- | --- |
| `datamart.vw_resumo_executivo` | KPIs gerais do dashboard |
| `datamart.vw_dashboard_oportunidades` | ranking e detalhamento de oportunidades |
| `datamart.vw_evolucao_motor` | analise temporal por recomendacao |
| `datamart.vw_saude_coleta` | monitoramento das fontes e do ETL |
| `datamart.vw_heuristica_ml` | comparacao entre heuristica e ML |

## Camada semantica e KPIs no Power BI

O Power BI deve conectar ao PostgreSQL local:

```text
Host: localhost
Porta: 55432
Banco: motor_decisao
Usuario: motor
Senha: motor
```

Importar as views do schema `datamart`.

Medidas DAX recomendadas:

```DAX
Produtos Avaliados =
COUNTROWS(vw_dashboard_oportunidades)

Oportunidades Comprar Teste =
CALCULATE(
    COUNTROWS(vw_dashboard_oportunidades),
    vw_dashboard_oportunidades[recomendacao_codigo] = "comprar_teste"
)

Oportunidades Revisar =
CALCULATE(
    COUNTROWS(vw_dashboard_oportunidades),
    vw_dashboard_oportunidades[recomendacao_codigo] = "revisar"
)

Oportunidades Ignorar =
CALCULATE(
    COUNTROWS(vw_dashboard_oportunidades),
    vw_dashboard_oportunidades[recomendacao_codigo] = "ignorar"
)

Margem Liquida Media =
AVERAGE(vw_dashboard_oportunidades[net_margin_pct])

Lucro Estimado Comprar Teste =
CALCULATE(
    SUM(vw_dashboard_oportunidades[estimated_net_profit]),
    vw_dashboard_oportunidades[recomendacao_codigo] = "comprar_teste"
)

Score Medio de Decisao =
AVERAGE(vw_dashboard_oportunidades[decision_score])

Confianca Media do Match =
AVERAGE(vw_dashboard_oportunidades[match_confidence])

Fontes de Mercado =
SUM(vw_dashboard_oportunidades[market_source_count])

Evidencias de Marketplace =
SUM(vw_dashboard_oportunidades[market_offer_count])

Taxa Comprar Teste =
DIVIDE([Oportunidades Comprar Teste], [Produtos Avaliados])

Execucoes com Falha =
CALCULATE(
    SUM(vw_saude_coleta[execucoes]),
    vw_saude_coleta[status] = "failed"
)
```

## Dashboard

O arquivo Power BI entregue e:

```text
C:\Users\luisg\Downloads\dashboard_motor_decisao.pbix
```

Foi criado backup em:

```text
backups/power_bi/
```

Inspecao tecnica do PBIX:

| Pagina | Containers visuais |
| --- | ---: |
| Visao Geral | 12 |
| Produtos e Precos | 9 |
| Coletas e Qualidade | 13 |

Tipos obrigatorios encontrados no dashboard:

| Requisito | Evidencia no PBIX |
| --- | --- |
| Cards com KPIs | `cardVisual` |
| Grafico de linha | `lineChart` |
| Grafico de barras | `barChart` / `clusteredBarChart` |
| Grafico de participacao | `donutChart` |
| Tabela ou matriz | `tableEx` |
| Filtros | `slicer` |

## Principais decisoes tecnicas

- Manter a arquitetura original Bronze/Silver/Gold por ser adequada ao MVP.
- Criar a camada academica `repositorio`, `dw` e `datamart` por cima da base real, evitando duplicar regras de negocio.
- Usar modelo estrela com uma fato principal de oportunidade de compra e fatos complementares para observabilidade e ML.
- Preservar UUIDs operacionais como dimensoes degeneradas para rastreabilidade.
- Manter a decisao como apoio humano: `comprar_teste` nao significa compra automatica.

## Desafios enfrentados

- Integrar fontes heterogeneas com formatos diferentes de preco, titulo e disponibilidade.
- Comparar produtos similares sem confundir modelos ou especificacoes tecnicas.
- Lidar com coletas web sujeitas a bloqueio, mudanca de pagina e dados incompletos.
- Criar uma saida conservadora para evitar falso positivo de compra.
- Separar dados brutos, dados tratados e camada analitica de forma auditavel.

## Insights encontrados

- O motor e conservador: poucos itens passam como `comprar_teste`, o que reduz risco em um MVP.
- Fornecedores com preco atraente ainda precisam de evidencias de mercado e confianca de match para virar oportunidade.
- A observabilidade da coleta e tao importante quanto o ranking, porque fonte desatualizada distorce margem e demanda.
- A proxima evolucao deve registrar revisao humana e resultado real de venda para trocar rotulos proxy por aprendizado supervisionado.

## Como aplicar a entrega no banco

Com Docker Desktop aberto e a stack do projeto ativa:

```powershell
cd "C:\Users\luisg\revenda assistida\motor_decisao"
.\scripts\setup_bi_final_project.ps1
```

Depois, no Power BI, conecte ao PostgreSQL e importe as views do schema `datamart`.

## Checklist do enunciado

| Requisito | Status | Arquivo/evidencia |
| --- | --- | --- |
| Dados reais | Atendido | fontes reais em `bronze` e `repositorio` |
| PostgreSQL | Atendido | `docker-compose.yml` com PostgreSQL 15 |
| Schema repositorio | Atendido | `scripts/setup_bi_final_project.sql` |
| Processo ETL | Atendido | pipelines Python + SQL de DW |
| Schema DW | Atendido | `dw.*` |
| Minimo 1 fato | Atendido | 3 fatos |
| Minimo 6 dimensoes | Atendido | 8 dimensoes |
| Conceitos avancados | Atendido | tempo, role-playing, conformada, degenerada |
| Data Mart | Atendido | `datamart.*` |
| Pelo menos 1 view analitica | Atendido | 5 views |
| Camada semantica Power BI | Atendido | DAX neste documento |
| Minimo 10 KPIs | Atendido | 12 medidas sugeridas |
| Minimo 5 visualizacoes | Atendido | 34 containers visuais no PBIX |
| Filtros e layout organizado | Atendido | slicers em todas as paginas |
