# Motor de Decisao de Compra

Backend local para coletar dados de mercado, historico de precos e sinais de demanda de produtos de baixo valor, com foco inicial em perifericos de tecnologia: mouse, teclado, headset, webcam, hub USB, cabos e itens similares.

O objetivo do projeto e construir uma base de dados para decidir o que vale comprar e revender, combinando preco praticado no varejo, historico de preco, concorrencia, sinais de demanda e, nas proximas etapas, margem estimada.

## Estado Atual

Ja esta pronto:

- Infra local com Docker Compose.
- Postgres 15 com schemas `control`, `bronze`, `silver` e `gold`.
- Redis.
- API FastAPI com healthcheck.
- Alembic para migracoes.
- Tabelas de controle de execucao.
- Coleta via API do Mercado Livre para catalogo e enriquecimento.
- Scraper Playwright para marketplaces fora Mercado Livre.
- Scraper Playwright para Zoom e Buscape com captura de preco atual e resumo de historico visivel.
- Conexao validada no DBeaver via Postgres local.
- Primeiras coletas reais gravadas no banco.

Decisao importante: o Mercado Livre fica como fonte de catalogo/enriquecimento. Para benchmarking de preco e demanda, a coleta principal sera feita por web scraping em outras fontes.

## Arquitetura Local

```text
scripts/
  collect_market_web.py          -> coleta Shopee, Amazon, Kabum, Terabyte, Pichau, Magalu, AliExpress
  collect_price_history_web.py   -> coleta Zoom e Buscape por paginas de produto
  collect_mercado_livre.py       -> coleta catalogo/itens Mercado Livre

pipelines/
  market_web/                    -> fontes de varejo e marketplace
  price_history/                 -> comparadores e historico de preco
  mercado_livre/                 -> API oficial Mercado Livre
  common/                        -> browser, db, config e utilitarios

bronze/
  market_web_listings_raw        -> ofertas brutas dos marketplaces
  price_history_raw              -> preco/historico bruto de Zoom e Buscape
  mercado_livre_products_raw     -> produtos do catalogo Mercado Livre
  mercado_livre_items_raw        -> anuncios/itens vinculados quando disponiveis
```

## Fontes Implementadas

| Fonte | Status | Uso | Observacoes |
| --- | --- | --- | --- |
| Mercado Livre API | Parcial | Catalogo e enriquecimento | A busca aberta de anuncios pode retornar `403`; por isso nao sera a fonte principal de benchmarking. |
| Shopee Brasil | Funcional | Preco e sinais de demanda | SPA pesada; requer Playwright. Volume vendido nem sempre aparece no HTML inicial. |
| Amazon Brasil | Funcional | Preco, reviews e proxy de demanda | Anti-bot forte; BSR detalhado ainda precisa de etapa de pagina de produto. |
| Zoom | Funcional | Historico/preco | Captura preco atual e resumo visivel, mas nao serie historica completa ponto a ponto. |
| Buscape | Funcional | Historico/preco | Mesmo limite do Zoom; alguns produtos podem bloquear ou esconder historico. |
| Kabum | Funcional | Preco varejo nacional | A busca pode retornar itens fora do termo exato; precisa filtro Silver por relevancia. |
| Terabyte | Funcional | Preco varejo nacional | Boa coleta de ofertas; tambem precisa filtro Silver por relevancia. |
| Pichau | Mapeado, bloqueado | Preco varejo nacional | O HTML foi mapeado, mas o coletor encontrou bloqueio anti-bot na execucao atual. |
| Magalu | Bloqueado | Preco varejo nacional | Ambiente atual retornou bloqueio/acesso negado. |
| AliExpress | Parcial | Benchmark de custo importado | Funciona em alguns cenarios, mas pode cair em captcha/bloqueio. B2B nacional esta pausado. |

## Resultado da Coleta Local Atual

Ultimo estado observado no banco local:

| Tabela | Fonte | Linhas | Com preco | Bloqueios |
| --- | --- | ---: | ---: | ---: |
| `bronze.market_web_listings_raw` | Shopee | 53 | 53 | 0 |
| `bronze.market_web_listings_raw` | Amazon | 53 | 53 | 0 |
| `bronze.market_web_listings_raw` | Kabum | 48 | 48 | 0 |
| `bronze.market_web_listings_raw` | Terabyte | 25 | 25 | 0 |
| `bronze.market_web_listings_raw` | AliExpress | 11 | 10 | 1 |
| `bronze.market_web_listings_raw` | Magalu | 2 | 0 | 2 |
| `bronze.market_web_listings_raw` | Pichau | 1 | 0 | 1 |
| `bronze.price_history_raw` | Buscape | 99 | 96 | 3 |
| `bronze.price_history_raw` | Zoom | 98 | 95 | 3 |

Esses numeros sao da execucao local de desenvolvimento e nao devem ser tratados como dataset final. A proxima etapa e transformar o Bronze em Silver com deduplicacao, filtros de relevancia e normalizacao de preco.

## Como Rodar em Outra Maquina

Requisitos:

- Docker Desktop.
- Git.
- PowerShell.
- Opcional: DBeaver para visualizar tabelas.

Passo a passo:

```powershell
git clone https://github.com/luis7gustavo/motor_decisao.git
cd motor_decisao
Copy-Item .env.example .env
docker compose up -d postgres redis
docker compose run --rm api alembic upgrade head
docker compose run --rm api python scripts/validate_setup.py
docker compose up -d api
```

Verificar API:

```powershell
Invoke-RestMethod http://127.0.0.1:8010/health
```

Resposta esperada:

```json
{
  "status": "ok",
  "environment": "development",
  "database": true
}
```

## Conexao no DBeaver

Use uma conexao PostgreSQL:

```text
Host: localhost
Port: 55432
Database: motor_decisao
User: motor
Password: motor
```

Tabelas principais:

- `bronze.market_web_listings_raw`
- `bronze.price_history_raw`
- `bronze.mercado_livre_products_raw`
- `bronze.mercado_livre_items_raw`
- `silver.mercado_livre_product_prices`
- `control.pipeline_runs`
- `control.source_runs`

Consulta util:

```sql
SELECT
  source_name,
  query,
  title,
  price,
  currency_id,
  fetched_at
FROM bronze.market_web_listings_raw
WHERE price IS NOT NULL
ORDER BY fetched_at DESC
LIMIT 100;
```

Para Zoom e Buscape:

```sql
SELECT
  source_name,
  query,
  title,
  current_price,
  avg_price,
  min_price,
  max_price,
  fetched_at
FROM bronze.price_history_raw
ORDER BY fetched_at DESC
LIMIT 100;
```

## Comandos de Coleta

Coleta de marketplaces:

```powershell
docker compose run --rm api python scripts/collect_market_web.py --source shopee --source amazon --source kabum --source terabyte --query "mouse gamer" --query "teclado mecanico" --query "headset gamer" --max-results 5
```

Coleta focada em Terabyte:

```powershell
docker compose run --rm api python scripts/collect_market_web.py --source terabyte --query "mouse sem fio" --query "hub usb" --query "webcam" --max-results 4
```

Coleta Zoom e Buscape:

```powershell
docker compose run --rm api python scripts/collect_price_history_web.py --source all --query "mouse gamer" --query "headset gamer" --max-results 2 --product-detail-limit 2
```

Coleta Mercado Livre:

```powershell
docker compose run --rm api python scripts/collect_mercado_livre.py --query "mouse gamer" --max-items 50
```

OAuth Mercado Livre com ngrok:

```powershell
ngrok http 8010
docker compose run --rm api python scripts/mercado_livre_oauth.py auth-url
```

Guia: `docs/mercado_livre_ngrok.md`.

## Limitacoes Encontradas nos Scrapers

- Sites com SPA pesada exigem Playwright e esperas por renderizacao; requests simples nao sao suficientes.
- Shopee e Amazon mudam estrutura e exibem demanda de forma incompleta na busca.
- Amazon tem protecao anti-bot forte; para escala real sera necessario reduzir taxa, persistir sessao e possivelmente usar proxies residenciais.
- Pichau foi mapeada, mas retornou bloqueio anti-bot no coletor atual.
- Magalu retornou bloqueio/acesso negado neste ambiente.
- Kabum e Terabyte funcionaram, mas a busca retorna produtos correlatos e patrocinados; o Bronze ainda contem ruido.
- Zoom e Buscape nao expuseram a serie historica completa em formato simples; por enquanto extraimos preco atual e agregados/indicadores visiveis na pagina.
- As datas registradas sao `fetched_at`/`fetched_date`, ou seja, data da nossa coleta. Quando o site nao mostra datas historicas internas, nao inventamos essa informacao.
- Os dados Bronze ainda nao fazem deduplicacao entre lojas nem normalizacao de SKU.

## Proximos Passos

1. Criar Silver de ofertas com limpeza de titulo, marca, categoria, preco normalizado e filtro de relevancia por query.
2. Criar Silver de historico Zoom/Buscape separando preco atual, media, minimo, maximo e janela do historico.
3. Deduplicar produtos equivalentes entre Kabum, Terabyte, Shopee, Amazon, Zoom e Buscape.
4. Implementar score inicial de oportunidade: preco minimo, preco medio, dispersao de preco, demanda proxy e confianca da fonte.
5. Melhorar Pichau com sessao persistente, headers mais conservadores e fallback via Selenium Grid.
6. Melhorar Amazon com coleta de pagina de produto para BSR e Buy Box quando a busca permitir.
7. Criar dashboard simples para visualizar runs, fontes bloqueadas e top oportunidades.
8. Preparar rotina agendada de coleta local com baixo volume por fonte para evitar bloqueios.

## Seguranca

Arquivos sensiveis e dados locais nao devem ser versionados:

- `.env`
- `.env.*`
- `data/`
- bancos locais
- logs

Use `.env.example` como modelo e coloque credenciais reais apenas no `.env` local.
