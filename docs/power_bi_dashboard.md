# Dashboard SILLO no Power BI

O Power BI e a interface analitica do SILLO para a entrega do Projeto
Integrador 1. O backend continua responsavel por coleta, normalizacao e
pontuacao. O dashboard consome apenas arquivos CSV gerados a partir das tabelas
Gold e das tabelas de observabilidade.

## Atualizar dados

Com a stack Docker ativa:

```powershell
docker compose exec -T api python scripts/export_power_bi.py
```

Os arquivos ficam em:

```text
data_processed/power_bi/
```

O ciclo completo de `scripts/collect_all.py` recalcula as recomendacoes Gold e
atualiza esses arquivos ao final da execucao.

## Arquivos exportados

| Arquivo | Uso |
| --- | --- |
| `fato_oportunidades_atuais.csv` | Ranking atual de oportunidades por fornecedor. |
| `fato_historico_oportunidades.csv` | Historico das recomendacoes por rodada do motor. |
| `fato_execucoes_motor.csv` | Duracao, status e versao de cada rodada do motor. |
| `fato_execucoes_fontes.csv` | Saude, volume e atualizacao das fontes coletadas. |
| `fato_qualidade_dados.csv` | Checks de qualidade e alertas dos pipelines. |
| `fato_ml_oportunidades_atuais.csv` | Scores hibridos, divergencias e explicacoes do modelo atual. |
| `fato_execucoes_ml.csv` | Modelos treinados, versoes e metricas registradas. |
| `dim_fornecedores.csv` | Pedido minimo e modo de atualizacao de cada fornecedor. |
| `dim_cenarios_frete.csv` | Cenarios de frete de entrada: otimista, base e conservador. |

Os CSVs usam UTF-8 com BOM e separador `;`.

## Modelo recomendado

Relacionamentos principais:

```text
dim_fornecedores[supplier_slug] 1 --- * fato_oportunidades_atuais[supplier_slug]
dim_fornecedores[supplier_slug] 1 --- * fato_historico_oportunidades[supplier_slug]
fato_execucoes_motor[decision_run_id] 1 --- * fato_historico_oportunidades[decision_run_id]
dim_fornecedores[supplier_slug] 1 --- * fato_ml_oportunidades_atuais[supplier_slug]
```

Use `dim_cenarios_frete` como tabela desconectada para alternar cenarios em um
segmentador.

## Paginas do dashboard

### 1. Resumo executivo

- quantidade de produtos avaliados;
- oportunidades `comprar_teste`, `revisar` e `ignorar`;
- margem liquida media;
- lucro estimado total das oportunidades fortes;
- fornecedor recomendado;
- data da ultima rodada do motor.

### 2. Oportunidades de compra

- ranking filtravel por fornecedor;
- preco de compra e preco estimado de mercado;
- lucro e margem liquida;
- confianca do match;
- score de decisao;
- flags de risco.

### 3. Carrinho simulado

- orcamento maximo de `R$ 1.000`;
- um fornecedor por pedido;
- pedido minimo por fornecedor;
- frete de entrada conforme o cenario selecionado;
- valor disponivel depois do frete;
- concentracao do carrinho por produto.

O otimizador de carrinho ainda sera implementado. Nesta etapa, a pagina pode
comecar com filtros e uma selecao manual das oportunidades aprovadas.

### 4. Saude da coleta

- ultima atualizacao por fonte;
- execucoes com sucesso, parcial ou falha;
- registros extraidos e carregados;
- duracao das coletas;
- checks de qualidade com alerta.

### 5. Evolucao do motor

- distribuicao das recomendacoes por rodada;
- evolucao da margem estimada;
- evolucao da confianca dos matches;
- comparacao entre versoes do scoring.

### 6. Heuristica x Machine Learning

- score heuristico, score ML e score final hibrido;
- quantidade de divergencias;
- recomendacao final por fornecedor;
- explicacao textual de cada oportunidade;
- versao do modelo selecionado;
- fila de revisao humana ampliada pelos sinais do ML.

## Medidas iniciais

Exemplos de medidas DAX:

```DAX
Produtos Avaliados =
COUNTROWS(fato_oportunidades_atuais)

Produtos Avaliados ML =
COUNTROWS(fato_ml_oportunidades_atuais)

Oportunidades Comprar Teste =
CALCULATE(
    COUNTROWS(fato_oportunidades_atuais),
    fato_oportunidades_atuais[recommendation] = "comprar_teste"
)

Margem Liquida Media =
AVERAGE(fato_oportunidades_atuais[net_margin_pct])

Frete Entrada Selecionado =
SELECTEDVALUE(dim_cenarios_frete[shipping_pct], 0.09)

Divergencias Heuristica ML =
CALCULATE(
    COUNTROWS(fato_ml_oportunidades_atuais),
    fato_ml_oportunidades_atuais[heuristic_decision] <> fato_ml_oportunidades_atuais[ml_decision]
)
```

## Regra de negocio registrada

- comparar MegaMix, Mirao, Coletek e futuros fornecedores;
- gerar carrinhos separados por fornecedor;
- permitir somente um fornecedor no carrinho final;
- usar pedido minimo de `R$ 0` para Mirao;
- usar pedido minimo de aproximadamente `R$ 300` para MegaMix e Coletek;
- usar frete de entrada de `7%`, `9%` e `14%` nos cenarios iniciais;
- exigir revisao humana antes de qualquer compra.
