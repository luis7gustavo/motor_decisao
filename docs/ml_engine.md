# SILLO - Camada Hibrida de Machine Learning

Data de referencia: 2026-06-01

## Objetivo

A camada de Machine Learning complementa o motor heuristico sem substitui-lo.
Ela compara produtos de todos os fornecedores carregados, aprende padroes a
partir das recomendacoes atuais e registra uma segunda avaliacao explicavel no
schema Gold.

O desenho inicial foi feito para permitir maturacao gradual:

1. preservar o score heuristico como baseline auditavel;
2. usar as decisoes heuristicas como rotulos proxy enquanto ainda nao existem
   vendas proprias;
3. comparar modelos simples antes de escolher o melhor;
4. aplicar uma regra hibrida com trava de seguranca;
5. substituir progressivamente os rotulos proxy por resultados reais de
   compra, venda, margem e giro.

## Limite Importante

O modelo inicial ainda nao preve vendas reais. Ele aprende a aproximar o
criterio heuristico usando as variaveis disponiveis. Por isso:

- `comprar_teste` continua exigindo apoio da heuristica;
- o ML nao promove sozinho um produto para compra;
- produtos com divergencia entram na fila `revisar`;
- todas as compras continuam dependendo de validacao humana.

Essa etapa e util para validar a esteira, comparar abordagens e preparar a
coleta de dados proprios para Projeto Integrador 2 e 3.

## Arquitetura

```text
Gold heuristico
  gold.decision_opportunities

        |
        v

Dataset proxy e features auditaveis
  data_processed/ml/training_dataset.csv

        |
        v

Treinamento comparativo
  Logistic Regression
  Random Forest
  HistGradientBoosting
  XGBoost CPU

        |
        v

Modelo selecionado e versionado
  artifacts/ml/models/
  gold.ml_model_runs

        |
        v

Score hibrido e explicacoes
  gold.ml_opportunity_scores
  gold.ml_opportunity_scores_latest

        |
        v

Power BI e API FastAPI
```

## Variaveis Usadas

O dataset inclui sinais de fornecedor, mercado, margem, demanda, match e
risco. O score final da heuristica nao entra como feature do modelo para evitar
vazamento direto do rotulo proxy. Ele continua sendo usado apenas na combinacao
hibrida posterior.

Os candidatos sao comparados com validacao cruzada estratificada out-of-fold.
Isso reduz a dependencia de uma unica divisao treino/teste quando a classe de
oportunidades ainda e rara. O XGBoost recebe peso automatico para a classe
positiva e usa `tree_method=hist` em CPU. Depois da selecao, o vencedor e
treinado novamente com todo o dataset antes de ser versionado.

As features sao registradas por versao em:

```text
artifacts/ml/feature_lists/
```

## Treinar e Executar

Aplicar migracao:

```powershell
docker compose exec -T api alembic upgrade head
```

Gerar apenas o dataset:

```powershell
docker compose exec -T api python scripts/ml_build_dataset.py
```

Treinar, pontuar e comparar:

```powershell
docker compose exec -T api python scripts/ml_run_all.py
```

Reutilizar o modelo mais recente depois de atualizar o Gold heuristico:

```powershell
docker compose exec -T api python scripts/ml_run_all.py --predict-only
```

Listar as melhores oportunidades:

```powershell
docker compose exec -T api python scripts/ml_top_opportunities.py --limit 30
```

## Saidas

Artefatos locais do treinamento:

```text
artifacts/ml/models/
artifacts/ml/metadata/
artifacts/ml/feature_lists/
```

Relatorios locais:

```text
reports/ml/metrics.json
reports/ml/classification_report.txt
reports/ml/feature_importance.csv
reports/ml/heuristic_vs_ml_comparison.csv
reports/ml/heuristic_vs_ml_summary.json
reports/ml/prediction_summary.json
```

Os artefatos e relatorios sao regeneraveis e nao devem ser versionados.

## Tabelas Gold

| Tabela ou view | Uso |
| --- | --- |
| `gold.ml_model_runs` | Historico de treinamento, modelo selecionado, metricas e artefato. |
| `gold.ml_opportunity_scores` | Scores por produto, explicacao e comparacao com a heuristica. |
| `gold.ml_opportunity_scores_latest` | Pontuacao mais recente pronta para API e Power BI. |

## Endpoints

| Rota | Metodo | Uso |
| --- | --- | --- |
| `/ml-engine/train` | POST | Treina e versiona um novo modelo. |
| `/ml-engine/predict` | POST | Reutiliza o ultimo modelo e recalcula scores hibridos. |
| `/ml-engine/summary` | GET | Resume recomendacoes e divergencias. |
| `/ml-engine/opportunities` | GET | Lista oportunidades com scores e explicacoes. |

## Estado Inicial Validado

Validado em 2026-06-01:

| Indicador | Valor |
| --- | ---: |
| Produtos avaliados | 5.776 |
| Rotulos proxy positivos | 40 |
| Modelo selecionado | `random_forest` |
| `comprar_teste` final | 2 |
| `revisar` final | 171 |
| `ignorar` final | 5.603 |
| Divergencias heuristica x ML | 3 |

As metricas de teste sao metricas de imitacao do baseline heuristico. Elas nao
devem ser interpretadas como validacao comercial.

Esse estado inicial foi registrado antes da inclusao do XGBoost e da validacao
cruzada estratificada. Novos treinamentos registram `evaluation_method`,
`cv_splits` e `positive_class_weight` no relatorio de metricas.

## Estado Com XGBoost Validado

Validado em 2026-06-02 com 5 folds estratificados:

| Indicador | Valor |
| --- | ---: |
| Produtos avaliados | 5.817 |
| Rotulos proxy positivos | 43 |
| Modelo selecionado | `xgboost` |
| Average precision XGBoost | 0,9862 |
| Average precision Random Forest | 0,9804 |
| Positivos encontrados pelo XGBoost | 41 de 43 |
| `comprar_teste` final | 2 |
| `revisar` final | 173 |
| `ignorar` final | 5.642 |
| Divergencias heuristica x ML | 7 |

O ganho ainda mede imitacao da heuristica. A proxima melhoria de qualidade nao
e apenas trocar o algoritmo: e registrar resultados reais de compra, venda,
margem e tempo de giro para criar rotulos comerciais.

## Evolucao Recomendada

Registrar manualmente cada compra teste e seu resultado:

- fornecedor;
- produto;
- custo;
- frete real;
- preco anunciado;
- preco vendido;
- dias ate a venda;
- taxa da plataforma;
- devolucao ou cancelamento;
- margem liquida real.

Com esse historico, o SILLO pode migrar de rotulos proxy para previsoes de
probabilidade de venda, margem esperada e tempo de giro.
