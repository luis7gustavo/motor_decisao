-- SILLO - Projeto Final de Business Intelligence
-- Data Warehouse com PostgreSQL + Power BI
--
-- Objetivo:
--   Criar a camada academica exigida pelo projeto final da disciplina:
--   repositorio -> dados brutos sem transformacao relevante
--   dw          -> modelo dimensional em estrela
--   datamart    -> views analiticas prontas para Power BI
--
-- Pre-requisito:
--   As migracoes existentes do projeto SILLO ja devem ter criado os schemas
--   control, bronze, silver e gold.

BEGIN;

CREATE SCHEMA IF NOT EXISTS repositorio;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS datamart;

DROP VIEW IF EXISTS datamart.vw_resumo_executivo;
DROP VIEW IF EXISTS datamart.vw_dashboard_oportunidades;
DROP VIEW IF EXISTS datamart.vw_evolucao_motor;
DROP VIEW IF EXISTS datamart.vw_saude_coleta;
DROP VIEW IF EXISTS datamart.vw_heuristica_ml;

DROP TABLE IF EXISTS dw.fato_score_ml CASCADE;
DROP TABLE IF EXISTS dw.fato_execucao_fonte CASCADE;
DROP TABLE IF EXISTS dw.fato_oportunidade_compra CASCADE;
DROP TABLE IF EXISTS dw.dim_modelo_ml CASCADE;
DROP TABLE IF EXISTS dw.dim_fonte_coleta CASCADE;
DROP TABLE IF EXISTS dw.dim_execucao_motor CASCADE;
DROP TABLE IF EXISTS dw.dim_confianca CASCADE;
DROP TABLE IF EXISTS dw.dim_recomendacao CASCADE;
DROP TABLE IF EXISTS dw.dim_produto_fornecedor CASCADE;
DROP TABLE IF EXISTS dw.dim_fornecedor CASCADE;
DROP TABLE IF EXISTS dw.dim_tempo CASCADE;

DROP TABLE IF EXISTS repositorio.source_runs_raw CASCADE;
DROP TABLE IF EXISTS repositorio.pipeline_runs_raw CASCADE;
DROP TABLE IF EXISTS repositorio.supplier_products_raw CASCADE;
DROP TABLE IF EXISTS repositorio.market_web_listings_raw CASCADE;
DROP TABLE IF EXISTS repositorio.price_history_raw CASCADE;
DROP TABLE IF EXISTS repositorio.mercado_livre_items_raw CASCADE;
DROP TABLE IF EXISTS repositorio.mercado_livre_products_raw CASCADE;

-- 1. Repositorio: copia fiel dos dados brutos e de observabilidade.
CREATE TABLE repositorio.pipeline_runs_raw AS
SELECT *
FROM control.pipeline_runs;

CREATE TABLE repositorio.source_runs_raw AS
SELECT *
FROM control.source_runs;

CREATE TABLE repositorio.supplier_products_raw AS
SELECT *
FROM bronze.supplier_products_raw;

CREATE TABLE repositorio.market_web_listings_raw AS
SELECT *
FROM bronze.market_web_listings_raw;

CREATE TABLE repositorio.price_history_raw AS
SELECT *
FROM bronze.price_history_raw;

CREATE TABLE repositorio.mercado_livre_items_raw AS
SELECT *
FROM bronze.mercado_livre_items_raw;

CREATE TABLE repositorio.mercado_livre_products_raw AS
SELECT *
FROM bronze.mercado_livre_products_raw;

ALTER TABLE repositorio.pipeline_runs_raw ADD PRIMARY KEY (id);
ALTER TABLE repositorio.source_runs_raw ADD PRIMARY KEY (id);
ALTER TABLE repositorio.supplier_products_raw ADD PRIMARY KEY (id);
ALTER TABLE repositorio.market_web_listings_raw ADD PRIMARY KEY (id);
ALTER TABLE repositorio.price_history_raw ADD PRIMARY KEY (id);
ALTER TABLE repositorio.mercado_livre_items_raw ADD PRIMARY KEY (id);
ALTER TABLE repositorio.mercado_livre_products_raw ADD PRIMARY KEY (id);

-- 2. Dimensoes do DW.
-- Conceito avancado 1: dimensao tempo.
CREATE TABLE dw.dim_tempo AS
WITH date_values AS (
    SELECT generated_at::date AS data_referencia FROM gold.decision_opportunities
    UNION SELECT updated_at::date FROM gold.decision_opportunities
    UNION SELECT generated_at::date FROM gold.decision_opportunity_snapshots
    UNION SELECT started_at::date FROM gold.decision_engine_runs
    UNION SELECT finished_at::date FROM gold.decision_engine_runs WHERE finished_at IS NOT NULL
    UNION SELECT started_at::date FROM control.source_runs
    UNION SELECT finished_at::date FROM control.source_runs WHERE finished_at IS NOT NULL
    UNION SELECT scored_at::date FROM gold.ml_opportunity_scores
    UNION SELECT trained_at::date FROM gold.ml_model_runs WHERE trained_at IS NOT NULL
    UNION SELECT CURRENT_DATE
)
SELECT DISTINCT
    TO_CHAR(data_referencia, 'YYYYMMDD')::integer AS tempo_sk,
    data_referencia,
    EXTRACT(YEAR FROM data_referencia)::integer AS ano,
    EXTRACT(QUARTER FROM data_referencia)::integer AS trimestre,
    EXTRACT(MONTH FROM data_referencia)::integer AS mes,
    TO_CHAR(data_referencia, 'TMMonth') AS nome_mes,
    EXTRACT(DAY FROM data_referencia)::integer AS dia,
    EXTRACT(ISODOW FROM data_referencia)::integer AS dia_semana_iso,
    CASE WHEN EXTRACT(ISODOW FROM data_referencia)::integer IN (6, 7) THEN TRUE ELSE FALSE END AS fim_de_semana
FROM date_values
WHERE data_referencia IS NOT NULL;

ALTER TABLE dw.dim_tempo ADD PRIMARY KEY (tempo_sk);

CREATE TABLE dw.dim_fornecedor AS
WITH suppliers AS (
    SELECT supplier_slug FROM silver.supplier_products_normalized
    UNION SELECT supplier_slug FROM gold.decision_opportunities
    UNION SELECT supplier_slug FROM gold.decision_opportunity_snapshots
    UNION SELECT supplier_slug FROM gold.ml_opportunity_scores
)
SELECT
    ROW_NUMBER() OVER (ORDER BY supplier_slug)::integer AS fornecedor_sk,
    supplier_slug,
    CASE supplier_slug
        WHEN 'megamix' THEN 'MegaMix'
        WHEN 'mirao' THEN 'Mirao'
        WHEN 'coletek' THEN 'Coletek'
        ELSE INITCAP(REPLACE(supplier_slug, '_', ' '))
    END AS fornecedor_nome,
    CASE supplier_slug
        WHEN 'mirao' THEN 0::numeric(12,2)
        WHEN 'megamix' THEN 300::numeric(12,2)
        WHEN 'coletek' THEN 300::numeric(12,2)
        ELSE 0::numeric(12,2)
    END AS pedido_minimo_estimado
FROM suppliers
WHERE supplier_slug IS NOT NULL;

ALTER TABLE dw.dim_fornecedor ADD PRIMARY KEY (fornecedor_sk);
CREATE UNIQUE INDEX ux_dim_fornecedor_slug ON dw.dim_fornecedor(supplier_slug);

CREATE TABLE dw.dim_produto_fornecedor AS
SELECT
    ROW_NUMBER() OVER (ORDER BY product.supplier_slug, product.normalized_title, product.id)::integer AS produto_sk,
    supplier.fornecedor_sk,
    product.id AS supplier_product_id,
    product.supplier_raw_id,
    product.supplier_slug,
    product.source_product_key,
    product.raw_title,
    product.normalized_title,
    product.token_count,
    product.sku,
    product.ean,
    product.currency_id,
    product.fetched_date
FROM silver.supplier_products_normalized AS product
JOIN dw.dim_fornecedor AS supplier
    ON supplier.supplier_slug = product.supplier_slug;

ALTER TABLE dw.dim_produto_fornecedor ADD PRIMARY KEY (produto_sk);
ALTER TABLE dw.dim_produto_fornecedor
    ADD CONSTRAINT fk_dim_produto_fornecedor
    FOREIGN KEY (fornecedor_sk) REFERENCES dw.dim_fornecedor(fornecedor_sk);
CREATE UNIQUE INDEX ux_dim_produto_supplier_product ON dw.dim_produto_fornecedor(supplier_product_id);

CREATE TABLE dw.dim_recomendacao (
    recomendacao_sk integer PRIMARY KEY,
    recomendacao_codigo varchar(40) NOT NULL UNIQUE,
    recomendacao_rotulo varchar(80) NOT NULL,
    ordem_exibicao integer NOT NULL
);

INSERT INTO dw.dim_recomendacao (
    recomendacao_sk,
    recomendacao_codigo,
    recomendacao_rotulo,
    ordem_exibicao
) VALUES
    (1, 'comprar_teste', 'Comprar teste', 1),
    (2, 'revisar', 'Revisar', 2),
    (3, 'ignorar', 'Ignorar', 3),
    (99, 'desconhecida', 'Desconhecida', 99);

CREATE TABLE dw.dim_confianca (
    confianca_sk integer PRIMARY KEY,
    confianca_codigo varchar(20) NOT NULL UNIQUE,
    confianca_rotulo varchar(80) NOT NULL,
    ordem_exibicao integer NOT NULL
);

INSERT INTO dw.dim_confianca (
    confianca_sk,
    confianca_codigo,
    confianca_rotulo,
    ordem_exibicao
) VALUES
    (1, 'alta', 'Alta', 1),
    (2, 'media', 'Media', 2),
    (3, 'baixa', 'Baixa', 3),
    (99, 'desconhecida', 'Desconhecida', 99);

CREATE TABLE dw.dim_execucao_motor AS
SELECT
    ROW_NUMBER() OVER (ORDER BY run.started_at, run.id)::integer AS execucao_motor_sk,
    run.id AS decision_run_id,
    run.pipeline_run_id,
    run.scoring_version,
    run.status,
    run.started_at,
    run.finished_at,
    EXTRACT(EPOCH FROM (COALESCE(run.finished_at, NOW()) - run.started_at))::integer AS duracao_segundos,
    run.error_message
FROM gold.decision_engine_runs AS run;

ALTER TABLE dw.dim_execucao_motor ADD PRIMARY KEY (execucao_motor_sk);
CREATE UNIQUE INDEX ux_dim_execucao_motor_run ON dw.dim_execucao_motor(decision_run_id);

CREATE TABLE dw.dim_fonte_coleta AS
WITH sources AS (
    SELECT DISTINCT
        source_name,
        source_type,
        raw_table_name
    FROM control.source_runs
)
SELECT
    ROW_NUMBER() OVER (ORDER BY source_name, source_type, raw_table_name)::integer AS fonte_coleta_sk,
    source_name,
    source_type,
    raw_table_name
FROM sources
WHERE source_name IS NOT NULL;

ALTER TABLE dw.dim_fonte_coleta ADD PRIMARY KEY (fonte_coleta_sk);
CREATE UNIQUE INDEX ux_dim_fonte_coleta_natural
    ON dw.dim_fonte_coleta(source_name, source_type, COALESCE(raw_table_name, ''));

CREATE TABLE dw.dim_modelo_ml AS
SELECT
    ROW_NUMBER() OVER (ORDER BY model.created_at, model.id)::integer AS modelo_ml_sk,
    model.id AS model_run_id,
    model.model_version,
    model.model_type,
    model.status,
    model.training_rows,
    model.positive_rows,
    model.trained_at,
    model.artifact_path
FROM gold.ml_model_runs AS model;

ALTER TABLE dw.dim_modelo_ml ADD PRIMARY KEY (modelo_ml_sk);
CREATE UNIQUE INDEX ux_dim_modelo_ml_run ON dw.dim_modelo_ml(model_run_id);

-- 3. Fatos do DW.
-- Grao: uma oportunidade por produto de fornecedor por rodada do motor.
-- Conceitos avancados adicionais:
--   role-playing de tempo: geracao_tempo_sk e atualizacao_tempo_sk
--   dimensao conformada: dim_fornecedor usada por mais de uma fato
--   dimensao degenerada: UUIDs operacionais mantidos dentro das fatos
CREATE TABLE dw.fato_oportunidade_compra AS
SELECT
    ROW_NUMBER() OVER (ORDER BY opportunity.generated_at, opportunity.id)::bigint AS oportunidade_sk,
    opportunity.id AS oportunidade_id,
    run.execucao_motor_sk,
    supplier.fornecedor_sk,
    product.produto_sk,
    COALESCE(recommendation.recomendacao_sk, 99) AS recomendacao_sk,
    COALESCE(confidence.confianca_sk, 99) AS confianca_sk,
    TO_CHAR(opportunity.generated_at::date, 'YYYYMMDD')::integer AS geracao_tempo_sk,
    TO_CHAR(opportunity.updated_at::date, 'YYYYMMDD')::integer AS atualizacao_tempo_sk,
    opportunity.supplier_price,
    opportunity.estimated_market_price,
    opportunity.estimated_net_profit,
    opportunity.net_margin_pct,
    opportunity.total_fee_pct,
    opportunity.market_offer_count,
    opportunity.market_source_count,
    opportunity.price_history_count,
    opportunity.mercado_livre_count,
    opportunity.demand_score,
    opportunity.match_confidence,
    opportunity.decision_score,
    CARDINALITY(opportunity.risk_flags) AS risk_flag_count
FROM gold.decision_opportunities AS opportunity
LEFT JOIN dw.dim_execucao_motor AS run
    ON run.decision_run_id = opportunity.decision_run_id
LEFT JOIN dw.dim_fornecedor AS supplier
    ON supplier.supplier_slug = opportunity.supplier_slug
LEFT JOIN dw.dim_produto_fornecedor AS product
    ON product.supplier_product_id = opportunity.supplier_product_id
LEFT JOIN dw.dim_recomendacao AS recommendation
    ON recommendation.recomendacao_codigo = opportunity.recommendation
LEFT JOIN dw.dim_confianca AS confidence
    ON confidence.confianca_codigo = opportunity.confidence_level;

ALTER TABLE dw.fato_oportunidade_compra ADD PRIMARY KEY (oportunidade_sk);
ALTER TABLE dw.fato_oportunidade_compra
    ADD CONSTRAINT fk_fato_oportunidade_execucao
    FOREIGN KEY (execucao_motor_sk) REFERENCES dw.dim_execucao_motor(execucao_motor_sk);
ALTER TABLE dw.fato_oportunidade_compra
    ADD CONSTRAINT fk_fato_oportunidade_fornecedor
    FOREIGN KEY (fornecedor_sk) REFERENCES dw.dim_fornecedor(fornecedor_sk);
ALTER TABLE dw.fato_oportunidade_compra
    ADD CONSTRAINT fk_fato_oportunidade_produto
    FOREIGN KEY (produto_sk) REFERENCES dw.dim_produto_fornecedor(produto_sk);
ALTER TABLE dw.fato_oportunidade_compra
    ADD CONSTRAINT fk_fato_oportunidade_recomendacao
    FOREIGN KEY (recomendacao_sk) REFERENCES dw.dim_recomendacao(recomendacao_sk);
ALTER TABLE dw.fato_oportunidade_compra
    ADD CONSTRAINT fk_fato_oportunidade_confianca
    FOREIGN KEY (confianca_sk) REFERENCES dw.dim_confianca(confianca_sk);
ALTER TABLE dw.fato_oportunidade_compra
    ADD CONSTRAINT fk_fato_oportunidade_geracao_tempo
    FOREIGN KEY (geracao_tempo_sk) REFERENCES dw.dim_tempo(tempo_sk);
ALTER TABLE dw.fato_oportunidade_compra
    ADD CONSTRAINT fk_fato_oportunidade_atualizacao_tempo
    FOREIGN KEY (atualizacao_tempo_sk) REFERENCES dw.dim_tempo(tempo_sk);

CREATE INDEX idx_fato_oportunidade_rank
    ON dw.fato_oportunidade_compra(recomendacao_sk, decision_score DESC, net_margin_pct DESC);
CREATE INDEX idx_fato_oportunidade_fornecedor
    ON dw.fato_oportunidade_compra(fornecedor_sk, decision_score DESC);

-- Grao: uma execucao por fonte coletada.
CREATE TABLE dw.fato_execucao_fonte AS
SELECT
    ROW_NUMBER() OVER (ORDER BY source_run.started_at, source_run.id)::bigint AS execucao_fonte_sk,
    source_run.id AS source_run_id,
    pipeline.id AS pipeline_run_id,
    source.fonte_coleta_sk,
    TO_CHAR(source_run.started_at::date, 'YYYYMMDD')::integer AS inicio_tempo_sk,
    CASE
        WHEN source_run.finished_at IS NULL THEN NULL
        ELSE TO_CHAR(source_run.finished_at::date, 'YYYYMMDD')::integer
    END AS fim_tempo_sk,
    pipeline.pipeline_name,
    source_run.status,
    source_run.records_extracted,
    source_run.records_loaded,
    source_run.records_skipped,
    EXTRACT(EPOCH FROM (COALESCE(source_run.finished_at, NOW()) - source_run.started_at))::integer AS duracao_segundos,
    source_run.error_message
FROM control.source_runs AS source_run
LEFT JOIN control.pipeline_runs AS pipeline
    ON pipeline.id = source_run.pipeline_run_id
LEFT JOIN dw.dim_fonte_coleta AS source
    ON source.source_name = source_run.source_name
    AND source.source_type = source_run.source_type
    AND COALESCE(source.raw_table_name, '') = COALESCE(source_run.raw_table_name, '');

ALTER TABLE dw.fato_execucao_fonte ADD PRIMARY KEY (execucao_fonte_sk);
ALTER TABLE dw.fato_execucao_fonte
    ADD CONSTRAINT fk_fato_execucao_fonte_source
    FOREIGN KEY (fonte_coleta_sk) REFERENCES dw.dim_fonte_coleta(fonte_coleta_sk);
ALTER TABLE dw.fato_execucao_fonte
    ADD CONSTRAINT fk_fato_execucao_fonte_inicio_tempo
    FOREIGN KEY (inicio_tempo_sk) REFERENCES dw.dim_tempo(tempo_sk);
ALTER TABLE dw.fato_execucao_fonte
    ADD CONSTRAINT fk_fato_execucao_fonte_fim_tempo
    FOREIGN KEY (fim_tempo_sk) REFERENCES dw.dim_tempo(tempo_sk);

CREATE INDEX idx_fato_execucao_fonte_status
    ON dw.fato_execucao_fonte(status, inicio_tempo_sk DESC);

-- Grao: um score hibrido ML por produto de fornecedor no modelo mais recente.
CREATE TABLE dw.fato_score_ml AS
SELECT
    ROW_NUMBER() OVER (ORDER BY score.scored_at, score.id)::bigint AS score_ml_sk,
    score.id AS ml_score_id,
    model.modelo_ml_sk,
    run.execucao_motor_sk,
    supplier.fornecedor_sk,
    product.produto_sk,
    COALESCE(recommendation.recomendacao_sk, 99) AS recomendacao_final_sk,
    COALESCE(confidence.confianca_sk, 99) AS confianca_sk,
    TO_CHAR(score.scored_at::date, 'YYYYMMDD')::integer AS score_tempo_sk,
    score.heuristic_score,
    score.ml_score,
    score.final_score,
    score.score_difference,
    score.ml_prediction,
    score.decision_source
FROM gold.ml_opportunity_scores_latest AS score
LEFT JOIN dw.dim_modelo_ml AS model
    ON model.model_run_id = score.model_run_id
LEFT JOIN dw.dim_execucao_motor AS run
    ON run.decision_run_id = score.decision_run_id
LEFT JOIN dw.dim_fornecedor AS supplier
    ON supplier.supplier_slug = score.supplier_slug
LEFT JOIN dw.dim_produto_fornecedor AS product
    ON product.supplier_product_id = score.supplier_product_id
LEFT JOIN dw.dim_recomendacao AS recommendation
    ON recommendation.recomendacao_codigo = score.final_decision
LEFT JOIN dw.dim_confianca AS confidence
    ON confidence.confianca_codigo = score.confidence_level;

ALTER TABLE dw.fato_score_ml ADD PRIMARY KEY (score_ml_sk);
ALTER TABLE dw.fato_score_ml
    ADD CONSTRAINT fk_fato_score_ml_modelo
    FOREIGN KEY (modelo_ml_sk) REFERENCES dw.dim_modelo_ml(modelo_ml_sk);
ALTER TABLE dw.fato_score_ml
    ADD CONSTRAINT fk_fato_score_ml_execucao
    FOREIGN KEY (execucao_motor_sk) REFERENCES dw.dim_execucao_motor(execucao_motor_sk);
ALTER TABLE dw.fato_score_ml
    ADD CONSTRAINT fk_fato_score_ml_fornecedor
    FOREIGN KEY (fornecedor_sk) REFERENCES dw.dim_fornecedor(fornecedor_sk);
ALTER TABLE dw.fato_score_ml
    ADD CONSTRAINT fk_fato_score_ml_produto
    FOREIGN KEY (produto_sk) REFERENCES dw.dim_produto_fornecedor(produto_sk);
ALTER TABLE dw.fato_score_ml
    ADD CONSTRAINT fk_fato_score_ml_recomendacao
    FOREIGN KEY (recomendacao_final_sk) REFERENCES dw.dim_recomendacao(recomendacao_sk);
ALTER TABLE dw.fato_score_ml
    ADD CONSTRAINT fk_fato_score_ml_confianca
    FOREIGN KEY (confianca_sk) REFERENCES dw.dim_confianca(confianca_sk);
ALTER TABLE dw.fato_score_ml
    ADD CONSTRAINT fk_fato_score_ml_tempo
    FOREIGN KEY (score_tempo_sk) REFERENCES dw.dim_tempo(tempo_sk);

CREATE INDEX idx_fato_score_ml_rank
    ON dw.fato_score_ml(recomendacao_final_sk, final_score DESC);

-- 4. Data Mart: views analiticas para consumo no Power BI.
CREATE VIEW datamart.vw_dashboard_oportunidades AS
SELECT
    fact.oportunidade_id,
    time_gen.data_referencia AS data_geracao,
    supplier.fornecedor_nome,
    supplier.pedido_minimo_estimado,
    product.raw_title AS produto_titulo_original,
    product.normalized_title AS produto_titulo_normalizado,
    recommendation.recomendacao_codigo,
    recommendation.recomendacao_rotulo,
    confidence.confianca_codigo,
    confidence.confianca_rotulo,
    run.scoring_version,
    fact.supplier_price,
    fact.estimated_market_price,
    fact.estimated_net_profit,
    fact.net_margin_pct,
    fact.total_fee_pct,
    fact.market_offer_count,
    fact.market_source_count,
    fact.price_history_count,
    fact.mercado_livre_count,
    fact.demand_score,
    fact.match_confidence,
    fact.decision_score,
    fact.risk_flag_count
FROM dw.fato_oportunidade_compra AS fact
LEFT JOIN dw.dim_tempo AS time_gen
    ON time_gen.tempo_sk = fact.geracao_tempo_sk
LEFT JOIN dw.dim_fornecedor AS supplier
    ON supplier.fornecedor_sk = fact.fornecedor_sk
LEFT JOIN dw.dim_produto_fornecedor AS product
    ON product.produto_sk = fact.produto_sk
LEFT JOIN dw.dim_recomendacao AS recommendation
    ON recommendation.recomendacao_sk = fact.recomendacao_sk
LEFT JOIN dw.dim_confianca AS confidence
    ON confidence.confianca_sk = fact.confianca_sk
LEFT JOIN dw.dim_execucao_motor AS run
    ON run.execucao_motor_sk = fact.execucao_motor_sk;

CREATE VIEW datamart.vw_resumo_executivo AS
SELECT
    COUNT(*) AS produtos_avaliados,
    COUNT(*) FILTER (WHERE recommendation.recomendacao_codigo = 'comprar_teste') AS oportunidades_comprar_teste,
    COUNT(*) FILTER (WHERE recommendation.recomendacao_codigo = 'revisar') AS oportunidades_revisar,
    COUNT(*) FILTER (WHERE recommendation.recomendacao_codigo = 'ignorar') AS oportunidades_ignorar,
    ROUND(AVG(fact.net_margin_pct), 4) AS margem_liquida_media,
    ROUND(SUM(fact.estimated_net_profit) FILTER (WHERE recommendation.recomendacao_codigo = 'comprar_teste'), 2) AS lucro_estimado_comprar_teste,
    ROUND(AVG(fact.decision_score), 2) AS score_decisao_medio,
    ROUND(AVG(fact.match_confidence), 2) AS confianca_match_media,
    SUM(fact.market_offer_count) AS evidencias_marketplace,
    SUM(fact.price_history_count) AS evidencias_historico_preco,
    MAX(time_gen.data_referencia) AS ultima_rodada_motor
FROM dw.fato_oportunidade_compra AS fact
LEFT JOIN dw.dim_recomendacao AS recommendation
    ON recommendation.recomendacao_sk = fact.recomendacao_sk
LEFT JOIN dw.dim_tempo AS time_gen
    ON time_gen.tempo_sk = fact.geracao_tempo_sk;

CREATE VIEW datamart.vw_evolucao_motor AS
SELECT
    time_gen.data_referencia AS data_rodada,
    run.scoring_version,
    recommendation.recomendacao_codigo,
    recommendation.recomendacao_rotulo,
    COUNT(*) AS quantidade_produtos,
    ROUND(AVG(fact.net_margin_pct), 4) AS margem_liquida_media,
    ROUND(AVG(fact.decision_score), 2) AS score_decisao_medio,
    ROUND(AVG(fact.match_confidence), 2) AS confianca_match_media
FROM dw.fato_oportunidade_compra AS fact
LEFT JOIN dw.dim_tempo AS time_gen
    ON time_gen.tempo_sk = fact.geracao_tempo_sk
LEFT JOIN dw.dim_execucao_motor AS run
    ON run.execucao_motor_sk = fact.execucao_motor_sk
LEFT JOIN dw.dim_recomendacao AS recommendation
    ON recommendation.recomendacao_sk = fact.recomendacao_sk
GROUP BY
    time_gen.data_referencia,
    run.scoring_version,
    recommendation.recomendacao_codigo,
    recommendation.recomendacao_rotulo;

CREATE VIEW datamart.vw_saude_coleta AS
SELECT
    time_start.data_referencia AS data_inicio,
    source.source_name,
    source.source_type,
    source.raw_table_name,
    fact.pipeline_name,
    fact.status,
    COUNT(*) AS execucoes,
    SUM(fact.records_extracted) AS registros_extraidos,
    SUM(fact.records_loaded) AS registros_carregados,
    SUM(fact.records_skipped) AS registros_descartados,
    ROUND(AVG(fact.duracao_segundos), 2) AS duracao_media_segundos,
    MAX(fact.duracao_segundos) AS duracao_maxima_segundos
FROM dw.fato_execucao_fonte AS fact
LEFT JOIN dw.dim_tempo AS time_start
    ON time_start.tempo_sk = fact.inicio_tempo_sk
LEFT JOIN dw.dim_fonte_coleta AS source
    ON source.fonte_coleta_sk = fact.fonte_coleta_sk
GROUP BY
    time_start.data_referencia,
    source.source_name,
    source.source_type,
    source.raw_table_name,
    fact.pipeline_name,
    fact.status;

CREATE VIEW datamart.vw_heuristica_ml AS
SELECT
    time_score.data_referencia AS data_score,
    supplier.fornecedor_nome,
    product.raw_title AS produto_titulo_original,
    recommendation.recomendacao_codigo AS recomendacao_final,
    confidence.confianca_codigo,
    model.model_version,
    model.model_type,
    fact.heuristic_score,
    fact.ml_score,
    fact.final_score,
    fact.score_difference,
    fact.ml_prediction,
    fact.decision_source
FROM dw.fato_score_ml AS fact
LEFT JOIN dw.dim_tempo AS time_score
    ON time_score.tempo_sk = fact.score_tempo_sk
LEFT JOIN dw.dim_fornecedor AS supplier
    ON supplier.fornecedor_sk = fact.fornecedor_sk
LEFT JOIN dw.dim_produto_fornecedor AS product
    ON product.produto_sk = fact.produto_sk
LEFT JOIN dw.dim_recomendacao AS recommendation
    ON recommendation.recomendacao_sk = fact.recomendacao_final_sk
LEFT JOIN dw.dim_confianca AS confidence
    ON confidence.confianca_sk = fact.confianca_sk
LEFT JOIN dw.dim_modelo_ml AS model
    ON model.modelo_ml_sk = fact.modelo_ml_sk;

COMMIT;
