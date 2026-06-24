# FINAL REPORT - SILLO Motor de Decisao de Compra

Data de referencia: 2026-06-24

## 1. Resumo Executivo

O SILLO e um sistema local-first para apoiar decisoes de compra para revenda. A solucao coleta dados de fornecedores B2B e marketplaces, organiza os dados em camadas Bronze, Silver e Gold no PostgreSQL e calcula oportunidades com duas abordagens complementares:

- motor heuristico auditavel para decisao principal;
- camada de Machine Learning supervisionada proxy para ranking, comparacao e suporte a revisao.

O projeto foi reorganizado para uma estrutura production-ready local, com codigo fonte em `src/motor_decisao`, dados em `data/raw` e `data/processed`, scripts operacionais preservados em `scripts`, documentacao em `docs` e testes executaveis via Docker.

## 2. Estrategia de Modelagem e Arquitetura

### 2.1 Arquitetura de Dados

A arquitetura segue o padrao medalhao:

- Bronze: preserva registros brutos, payloads e snapshots de fontes externas.
- Silver: normaliza produtos, chaves e campos comparaveis.
- Gold: armazena recomendacoes, historico de runs, snapshots de decisoes e scores de ML.

Essa separacao foi escolhida porque as fontes possuem ruido, bloqueios, duplicidade e mudancas frequentes. Manter Bronze bruto reduz perda de informacao e permite reprocessar Silver/Gold quando a regra de negocio evolui.

Snapshot atual do banco:

| Tabela | Linhas |
| --- | ---: |
| `bronze.market_web_listings_raw` | 33.385 |
| `bronze.price_history_raw` | 9.562 |
| `bronze.supplier_products_raw` | 80.035 |
| `silver.supplier_products_normalized` | 80.035 |
| `gold.decision_opportunities` | 5.957 |
| `gold.decision_opportunity_snapshots` | 130.962 |
| `gold.ml_opportunity_scores_latest` | 24.519 |

### 2.2 Motor Heuristico

O motor heuristico e a fonte primaria de decisao. Ele estima margem, demanda, confianca textual e risco. A versao atual registrada no ultimo run e `heuristic_v3_attribute_guard`.

A principal escolha de engenharia foi manter a heuristica explicavel e conservadora. O sistema evita compras automaticas e classifica produtos em:

- `comprar_teste`;
- `revisar`;
- `ignorar`.

O ultimo run heuristico avaliou 5.957 produtos:

| Recomendacao | Confianca | Produtos |
| --- | --- | ---: |
| `revisar` | media | 55 |
| `ignorar` | baixa | 5.902 |
| `comprar_teste` | alta | 0 |

Esse resultado e conservador: nao houve promocao direta a compra no snapshot atual.

### 2.3 Camada de Machine Learning

O ML foi projetado como camada de apoio, nao como decisor autonomo. Como ainda nao existe historico robusto de vendas, margem realizada e giro, o modelo usa rotulos proxy derivados da heuristica:

- classe positiva: `revisar` ou `comprar_teste`;
- classe negativa: `ignorar`.

O objetivo atual e aprender a aproximar a heuristica com features intermediarias, produzir um score adicional e identificar divergencias para revisao.

Modelos comparados:

- Logistic Regression;
- Random Forest;
- HistGradientBoosting;
- XGBoost CPU.

Modelo selecionado:

```text
hist_gradient_boosting / real_plus_synthetic
```

Versao:

```text
ml_proxy_v3_20260608T195725Z_99008136
```

## 3. Feature Engineering

As features foram desenhadas para evitar vazamento direto do target. O `decision_score` final da heuristica nao entra como feature. O modelo usa sinais intermediarios:

- `supplier_price`;
- `estimated_market_price`;
- `estimated_net_profit`;
- `net_margin_pct`;
- `total_fee_pct`;
- `market_offer_count`;
- `market_source_count`;
- `price_history_count`;
- `mercado_livre_count`;
- `demand_score`;
- `match_confidence`;
- `risk_flag_count`;
- indicadores booleanos de preco, margem, multiplas fontes e sinal Mercado Livre.

Importancias registradas no ultimo treino:

| Feature | Importancia |
| --- | ---: |
| `match_confidence` | 0,7308 |
| `net_margin_pct` | 0,4866 |
| `demand_score` | 0,0789 |
| `market_source_count` | 0,0234 |
| `market_offer_count` | 0,0034 |

A leitura e coerente com o dominio: a confianca de match e a margem liquida sao os sinais mais fortes para separar candidatos de itens sem oportunidade.

## 4. Hiperparametros e Justificativas

Configuracao principal em `config/config.yaml`:

| Parametro | Valor | Justificativa |
| --- | ---: | --- |
| `heuristic_weight` | 0,70 | Mantem a heuristica como decisor principal. |
| `ml_weight` | 0,30 | Usa ML como apoio sem dominar o score final. |
| `opportunity_threshold` | 0,65 | Exige score alto para oportunidade. |
| `review_threshold` | 0,45 | Permite fila de revisao sem compra automatica. |
| `min_training_rows` | 50 | Evita treino com amostra insuficiente. |
| `cv_splits` | 5 | Balanceia estabilidade e custo computacional. |
| `test_size` | 0,25 | Mantem holdout util para importancia de features. |
| `model_type` | `best` | Seleciona algoritmo por metrica comparativa. |

Configuracao de augmentation:

| Parametro | Valor | Justificativa |
| --- | ---: | --- |
| `synthetic_multiplier` | 3 | Aumenta exemplos positivos raros com moderacao. |
| `max_synthetic_rows` | 5.000 | Limite absoluto contra explosao artificial. |
| `synthetic_sample_weight` | 0,30 | Reduz peso de linhas sinteticas. |
| `noise_level` | 0,08 | Pequena perturbacao controlada. |
| `min_average_precision_gain` | 0,001 | Exige ganho minimo para usar sinteticos. |

O dataset tem forte desbalanceamento: 5.946 linhas reais e 64 positivas proxy. Por isso, a avaliacao prioriza `average_precision`, precision/recall da classe positiva e metricas de ranking como `precision_at_10`.

## 5. Metricas de Avaliacao

Ultimo treino validado:

| Metrica | Valor |
| --- | ---: |
| Linhas reais | 5.946 |
| Positivos proxy | 64 |
| Linhas sinteticas finais | 192 |
| Accuracy | 0,9995 |
| Precision | 0,9552 |
| Recall | 1,0000 |
| F1 | 0,9771 |
| Average Precision | 0,9925 |
| ROC-AUC | 0,9999 |
| Precision@10 | 1,0000 |
| Recall@50 | 0,7656 |

Matriz de confusao do modelo selecionado:

```text
[[5879, 3],
 [0, 64]]
```

Comparacao de baseline:

| Modelo / Variante | Average Precision |
| --- | ---: |
| Logistic Regression real_only | 0,5723 |
| Random Forest real_only | 0,9896 |
| HistGradientBoosting real_plus_synthetic | 0,9925 |

O ganho do modelo selecionado e pequeno em relacao ao Random Forest real_only, mas ele manteve recall total da classe positiva e melhor average precision. Como o problema e encontrar poucos candidatos relevantes em uma massa grande de produtos, recall e ranking da classe positiva pesam mais do que accuracy global.

## 6. Analise de Performance

O sistema esta conservador no ultimo snapshot:

- 5.957 produtos avaliados pela heuristica;
- 55 produtos em `revisar`;
- 0 produtos em `comprar_teste`;
- 5 divergencias heuristica x ML no resumo hibrido;
- 710 produtos em revisao no universo de scores ML mais amplo.

Essa conservadorismo e desejavel nesta fase porque as metricas sao proxy. O maior risco do projeto nao e deixar passar alguma oportunidade marginal; e recomendar compra com match ruim, margem falsa ou evidencia insuficiente.

O endpoint `/decision-engine/summary` e o endpoint `/ml-engine/summary` estao funcionais e retornam os agregados atuais. A API foi validada em `http://localhost:8010/health` com banco conectado.

## 7. Validacao Tecnica

Validacoes executadas apos a refatoracao:

```text
AST_OK
docker compose config --quiet
docker compose run --rm api python -m pytest -q
14 passed
```

Tambem foi recriado o container `motor_api` com o novo entrypoint:

```text
uvicorn motor_decisao.app.main:app
```

O estado operacional antigo preso em `running` foi reparado via:

```text
POST /ops/repair-stale-runs?stale_after_hours=8
```

Depois da recriacao do servico, o daemon iniciou uma nova coleta ativa. Portanto, `running` pode aparecer novamente para runs recentes e saudaveis. A validacao relevante aqui e que o run antigo travado foi reparado e que a API voltou em estado `healthy`.

## 8. Limitacoes Tecnicas

1. Rotulos proxy

As metricas medem imitacao da heuristica, nao lucro real. O proximo salto de maturidade exige registrar compras, vendas, margem realizada, prazo de giro e devolucoes.

2. Scraping fragil

Marketplaces podem mudar DOM, aplicar anti-bot ou reduzir disponibilidade. O sistema ja registra falhas e status parcial, mas ainda precisa de monitoramento externo.

3. Tokens Mercado Livre

A coleta por API depende de OAuth e refresh token valido. Ambientes multiusuario exigem secrets manager e politica de renovacao.

4. Ausencia de CI/CD remoto

O projeto esta production-ready para uso local, mas ainda nao possui pipeline remoto de lint, testes, build de imagem e migracoes automatizadas.

5. Modelo nao calibrado comercialmente

O score hibrido ainda nao foi calibrado com retorno financeiro real. Thresholds devem ser revisados apos coletar resultados de operacao.

6. Observabilidade local

As tabelas `control` resolvem auditoria operacional local, mas producao real pediria logs centralizados, metricas, alertas e tracing.

## 9. Proximos Passos para Escalabilidade

- Criar tabela de feedback humano por oportunidade revisada.
- Registrar ciclo completo compra -> venda -> margem -> giro.
- Retreinar modelos com labels reais e comparar contra o proxy atual.
- Criar CI com `pytest`, `compileall`, `docker compose config` e smoke test de API.
- Separar profiles `dev`, `staging` e `prod`.
- Mover secrets para cofre externo.
- Criar jobs agendados fora do container local.
- Monitorar drift de fontes, volume por fonte e distribuicao de scores.
- Criar dashboard operacional com taxa de falha por fonte, tempo de coleta e status de tokens.

## 10. Conclusao

O projeto esta organizado, testado e documentado como um sistema local-first de decisao assistida. A arquitetura atual e adequada para evoluir de um motor heuristico auditavel para uma plataforma de decisao com aprendizado supervisionado real.

O ponto mais importante para a proxima fase e trocar proxy por evidencia comercial. Ate la, a trava hibrida e a revisao humana continuam corretas: o modelo ajuda a priorizar, mas nao decide compra sozinho.
