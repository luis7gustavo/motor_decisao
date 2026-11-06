# Radar Licita — MVP independente do SILLO

Este diretório reúne o MVP executável de inteligência para licitações. Ele roda separadamente do motor de revenda na raiz deste repositório. Código, testes, captura oficial e documentação de demonstração estão versionados aqui; o banco SQLite da instância é gerado localmente a partir da captura.

Plataforma local de inteligência comercial para coletar, analisar e priorizar oportunidades reais de compras públicas. O sistema combina dados oficiais do PNCP, documentos do edital, cotações cadastradas pelo usuário, simulação financeira e regras explicáveis de logística/ranking.

> O Radar Licita é apoio à decisão. Ele não envia propostas, não substitui análise jurídica/contábil e nunca trata preço estimado como preço final garantido.

## Demonstração local de 16/10/2026

Para iniciar o recorte validado com banco isolado, agendador desligado e sem sincronização externa, siga [README_DEMO.md](README_DEMO.md). O procedimento cria uma cópia nova do snapshot, confere a base e inicia API e interface nas portas 8000 e 5173. O [roteiro de sete minutos](docs/demo/ROTEIRO_7_MIN.md), a [ficha de apoio](docs/demo/FICHA_APOIO.md) e o [índice de capturas](docs/demo/INDICE_EVIDENCIAS.md) acompanham a apresentação.

O percurso principal na aba **Tecnologia** usa **7 pregões eletrônicos históricos reais** extraídos do PNCP, com 19 itens, 19 resultados publicados e 11 empresas distintas. A base ativa contém somente esses registros oficiais; não há fornecedores, cotações ou cenários financeiros de exemplo. A captura é exibida sem rede, com links e hashes da fonte; há reconsulta opcional ao vivo. O histórico não comprova capacidade de fornecimento, contrato executado ou preço de aquisição atual. Estado e limites verificados estão em [STATUS.md](plans/mvp-apresentacao-2026-10-16/STATUS.md).

## O que está operacional

- Coleta nacional de contratações com propostas abertas, publicações e atualizações pela API oficial do PNCP.
- Paginação, retry exponencial, limitação de requisições, sincronização incremental, idempotência e histórico de versões.
- Persistência do JSON oficial bruto; deduplicação pelo número de controle PNCP e pela chave CNPJ/ano/sequencial.
- Itens e documentos oficiais da contratação; preços zero, ausentes ou sigilosos são mantidos como indisponíveis.
- Download seguro apenas do host oficial do PNCP, limite de tamanho e armazenamento por hash.
- Extração de PDFs textuais com PyMuPDF, evidências com documento/página/trecho/confiança e fila de revisão manual.
- Classificação logística A/B/C/D e `remote_delivery_score`, sem promover dados indeterminados a entrega remota confirmada.
- Indicador conservador de envio direto, sempre com pendências fiscais, de prazo e capacidade do fornecedor.
- Cadastro de fornecedores/produtos/cotações, importação CSV e matching técnico explicável.
- Simulador decimal com cenários, custos adicionais, capital, equilíbrio, margem-alvo e sensibilidades.
- Ranking explicável com bloqueios de prazo, margem, capital, incompatibilidade, operação local e julgamento por lote/grupo.
- Dashboard React responsivo, filtros, detalhes, itens, documentos/evidências, simulador, fornecedores, pipeline e configurações.
- Coleta automática com APScheduler e execução manual pela API ou CLI.
- Aba Tecnologia: consulta limitada de pregões eletrônicos com propostas abertas, separa itens de TI, compara a estimativa pública com até oito orçamentos e cruza descrições com empresas de resultados homologados históricos. Cada empresa vem com CNPJ e link da evidência pública, com compatibilidade técnica pendente.
- 42 testes automatizados passaram no pacote publicado; os 19 testes citados na entrega inicial são a linha de base histórica.

## Arquitetura

```text
radar_licita/
├── backend/
│   ├── alembic/                 # migrações
│   ├── app/
│   │   ├── api/                 # endpoints FastAPI
│   │   ├── ingestion/           # PNCP, normalização e idempotência
│   │   ├── documents/           # download e extração de PDFs
│   │   ├── logistics/           # classificação A/B/C/D
│   │   ├── suppliers/           # matching técnico
│   │   ├── finance/             # cálculos monetários
│   │   ├── ranking/             # elegibilidade e score
│   │   ├── tasks/               # agendamento
│   │   ├── models.py            # modelo relacional SQLAlchemy
│   │   └── main.py
│   └── tests/
├── frontend/
│   └── src/                     # React, TypeScript e Tailwind
├── examples/                    # captura oficial e scripts de carga
├── docker-compose.yml
└── .env.example
```

O modelo distingue contratação, lote e item e inclui: `procurements`, `procurement_items`, `procurement_lots`, `procurement_documents`, `procurement_versions`, `document_extractions`, `delivery_requirements`, `suppliers`, `supplier_products`, `supplier_quotes`, `product_matches`, `historical_prices`, `opportunity_analyses`, `financial_scenarios`, `opportunity_scores`, `opportunity_status_history`, `ingestion_jobs` e `system_settings`.

## Fontes oficiais e contratos verificados

Foram usados os contratos publicados em 9 de outubro de 2026:

- [Swagger da API de consulta do PNCP](https://pncp.gov.br/api/consulta/swagger-ui/index.html)
- [OpenAPI JSON da consulta](https://pncp.gov.br/api/consulta/v3/api-docs)
- [OpenAPI JSON dos recursos detalhados](https://pncp.gov.br/api/pncp/v3/api-docs)
- [Manual do PNCP](https://pncp.gov.br/manual/pt-br/latest/)
- [Portal de Dados Abertos do Compras.gov.br](https://www.gov.br/compras/pt-br/cidadao/portal-de-dados-abertos)

Endpoints implementados:

- `GET /api/consulta/v1/contratacoes/proposta`
- `GET /api/consulta/v1/contratacoes/publicacao`
- `GET /api/consulta/v1/contratacoes/atualizacao`
- `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}`
- `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens`
- `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/arquivos`
- `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/historico`
- `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numeroItem}/resultados`

### Busca de tecnologia

No topo da aba **Tecnologia**, o painel histórico local mostra a cadeia real contratação → item → resultado por item para sete pregões de 2026. `GET /api/v1/technology/historical-exhibit` lê a captura oficial validada por hash sem acesso externo; `POST /api/v1/technology/historical-exhibit/refresh` reconsulta os mesmos controles no PNCP, sem alterar o snapshot. [README_DEMO.md](README_DEMO.md) traz o percurso de apresentação.

Na interface, abra **Tecnologia**, informe orçamentos separados por `;` e clique em **Consultar PNCP**. Também é possível filtrar termo, UF, prazo das propostas e janela histórica. A API equivalente é `POST /api/v1/technology/search`, por exemplo:

```json
{"budgets":[5000,10000,50000],"keyword":"notebook","uf":"DF","open_horizon_days":14,"history_start":"2026-07-01","history_end":"2026-07-31"}
```

A resposta informa quantas propostas, compras e itens históricos foram consultados. O recorte tem limites de páginas e requisições; resultado vazio não prova inexistência de oportunidades ou empresas. “Estimativa cabe” compara somente o valor público do item com o orçamento informado; não calcula custo, lucro ou capital de execução. Um resultado homologado identifica uma empresa que já participou daquele item histórico, mas não comprova que ela forneça o produto atual. É preciso validar especificações, habilitação, estoque, preço e interesse com a empresa.

Datas de consulta usam `yyyyMMdd`, páginas começam em 1 e a API de consulta limita `tamanhoPagina` a 50.

## Execução recomendada com Docker

Requisitos: Docker Desktop com Compose.

```powershell
Copy-Item .env.example .env
docker compose up --build
```

Acesse:

- Interface: http://localhost:5173
- API: http://localhost:8000
- Swagger local: http://localhost:8000/docs
- Saúde: http://localhost:8000/api/v1/health

Os volumes `postgres_data` e `document_data` preservam o banco e os editais. Uma indisponibilidade temporária do PNCP nunca remove registros locais.

## Execução sem Docker

Backend (Python 3.12+ e PostgreSQL recomendado; SQLite funciona para desenvolvimento):

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
alembic upgrade head
uvicorn app.main:app --reload --port 8000
```

Frontend, em outro terminal:

```powershell
cd frontend
pnpm install
pnpm dev
```

Sem `DATABASE_URL`, o backend usa `backend/radar_licita.db`. Em produção local com Compose, usa PostgreSQL.

## Sincronização

Pela interface, use **Sincronizar**. Pela CLI:

```powershell
cd backend
python -m app.cli sync --mode open --pages 2
python -m app.cli sync --mode updates --start 2026-10-06 --end 2026-10-09 --pages 10
python -m app.cli documents --limit 20
```

Ou pela API:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/v1/sync/pncp `
  -ContentType application/json `
  -Body '{"mode":"open","max_pages":2,"hydrate_details":true}'
```

A coleta completa nacional pode ser demorada; comece com poucas páginas. Ajuste `PNCP_REQUESTS_PER_SECOND` de forma conservadora e nunca acima dos limites aceitos pelo portal.

## Importação de cotações

Para importar uma cotação obtida diretamente de um fornecedor, use as colunas obrigatórias `fornecedor`, `produto` e `preco_unitario`. Não há cotações pré-carregadas na demonstração.

O CSV cadastra fornecedor, produto e cotação. Para associar uma cotação a um item e executar o matching técnico, use os endpoints documentados no Swagger local ou o formulário de detalhes.

## Testes e build

```powershell
cd backend
pytest -q

cd ..\frontend
pnpm build
```

Validação da implementação inicial, registrada antes das sessões do MVP:

- `19 passed` no backend.
- Build TypeScript/Vite concluído.
- Coleta real: 50 oportunidades, zero erros.
- Contratação real detalhada: 39 itens e 2 documentos.
- PDF oficial processado: 361.780 bytes e 14 evidências.
- Dashboard e telas de detalhes/evidências/simulador verificados no navegador sem erros de console.

Atualização da demonstração em 16/10/2026: a instância local foi iniciada sobre uma cópia validada do snapshot oficial: sete pregões, 19 itens, 19 resultados e 11 empresas. Os antigos casos sintéticos não fazem parte deste pacote. O histórico de planejamento registra etapas anteriores, sem fornecer dados à instância atual. Esses resultados não implicam validação comercial nem cobertura completa do PNCP.

## Configuração

Principais variáveis em `.env.example`:

- `DATABASE_URL`: PostgreSQL/SQLite.
- `PNCP_*_BASE_URL`: hosts oficiais.
- `PNCP_REQUESTS_PER_SECOND`: controle de acesso.
- `SYNC_INTERVAL_MINUTES`: frequência do job.
- `DOCUMENT_MAX_BYTES`: limite por documento.
- `DOCUMENT_STORAGE_PATH`: diretório de PDFs.
- `AUTO_START_SCHEDULER`: ativa/desativa APScheduler.

As preferências comerciais (capital, margem mínima, UF, prazo e tributos) são editáveis na página de configurações e persistidas no banco.

## Limitações honestas da V1

- A integração operacional de preços históricos do Compras.gov.br ainda não foi ativada. A tabela e as interfaces estão prontas, mas o portal oficial atual direciona para a API de produção e seus módulos específicos; isso deve ser integrado por CATMAT/CATSER e unidade, com validação de comparabilidade — nunca por nome apenas. O ambiente de homologação não é usado.
- PDFs escaneados sem camada de texto ficam para revisão manual; OCR/LLM é extensão futura.
- A extração determinística cobre cláusulas recorrentes, mas não substitui leitura integral do edital.
- O matching técnico só confirma compatibilidade quando há especificações estruturadas suficientes; caso contrário retorna “possivelmente compatível” ou “informação insuficiente”.
- A classificação de envio direto não confirma autorização fiscal/contratual do distribuidor.
- Não há autenticação complexa porque a V1 é local e individual.
- Docker não estava instalado no ambiente desta implementação; os arquivos foram preparados, mas a execução Compose deve ser validada em uma máquina com Docker Desktop.

## Segurança e conformidade

- URLs de documentos são aceitas somente via HTTPS no host oficial `pncp.gov.br`.
- Downloads têm limite de tamanho; caminhos são resolvidos e verificados antes da gravação.
- Conteúdo de editais é tratado apenas como dado, nunca como instrução executável.
- Credenciais não ficam no código.
- O frontend não injeta HTML vindo dos documentos.
- Nenhum lance ou proposta é enviado automaticamente.
- Toda conclusão relevante conserva evidência e pendências para confirmação humana.

