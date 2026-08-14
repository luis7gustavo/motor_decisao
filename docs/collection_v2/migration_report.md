# Relatório de migração incremental

## Fases 0 a 4

- Inventário criado antes da alteração arquitetural.
- Contratos Pydantic v2 e registry YAML adicionados.
- Teto de compra centralizado em `settings.max_product_price_brl = 3000`; evidência de preço de mercado acima desse valor é preservada.
- Factory Crawlee criada para HTTP, Parsel, AdaptivePlaywright e Playwright, usando concorrência, retries, timeouts, rate limit, RequestQueue e bloqueio configurável de recursos.
- Mercado Livre permanece API; scrapers web atuais ganharam collectors Crawlee e parsers puros. O legado não foi apagado antes da validação comparativa.
- Os canaries ao vivo respeitaram `robots.txt`: Terabyte, Buscape e Zoom proibiram suas URLs de busca e foram registrados como `COLLECTION_BLOCKED`, desabilitados na V2 e mantidos sem tentativa de contorno.

## Fases 5 a 8

- Flows `collect_source`, `collect_profile` e `collect_all` adicionados.
- Compose PC1 e Compose worker PC2 usam um pool e três filas.
- Schedules individuais têm stagger e começam pausados.
- Métricas, worker/hostname, logs JSON, histórico, alertas, detecção de atraso e canary foram adicionados.
- Migration `20260812_0008` evolui as tabelas existentes, sem duplicar `pipeline_runs`, `source_runs` ou `data_quality_checks`.
- A migration historica `20260601_0008` foi mantida como ponte porque o banco existente ja estava carimbado nela; nenhum fluxo ou modelo de ML foi reativado por esta fase.

## Fases 9 e 10

- Reconnaissance registrado em `source_reconnaissance.md`.
- Grupo TEK e Cia da Informática receberam collectors declarativos e foram ativados com agenda inicialmente pausada.
- FCInfo e Netshop possuem adapters preparados, mas seguem desativados até o canary local validar seletor e estabilidade.
- Fontes autenticadas/comerciais permanecem desativadas; nenhuma credencial, CAPTCHA ou controle de acesso foi contornado.

## Fase 11

- Métricas e consulta de benchmark estão prontas.
- O Compose central do PC1 foi construído e validado com PostgreSQL, Redis, API, Prefect Server, inicialização de deployments e três workers por fila.
- O deployment Prefect foi validado ponta a ponta pelo run `f2aacf65-dbb9-46e8-b5d1-169c24d0a661`; a coleta integral da Kabum persistiu 420 itens no run `9df553e0-e815-4e49-92a3-bb59577715bd`.
- Os perfis `market` e `full` produziram runs rastreáveis. O canary `full` `8adc90b6-58b7-4eae-bbc6-d9b93cca4c30` isolou um `AUTH_ERROR` do Mercado Livre e continuou nas demais fontes.
- O token autorizado do Mercado Livre precisa ser renovado antes de ativar agendas; o valor existente não foi exibido nem alterado.
- O teste físico comparativo PC1 versus PC2 permanece pendente porque o segundo computador não está acessível neste ambiente. O Compose do PC2 e o protocolo exato estão em `operations.md` e `benchmark.md`.

## Validação operacional de 13/08/2026

- O daemon legado, que reiniciava uma rota concorrente e acumulava runs órfãos, foi desativado. A operação contínua ficou exclusivamente no Prefect.
- As agendas estão ativas e escalonadas para Amazon, KaBuM, Mirão, Grupo TEK e Cia da Informática. Seus canaries persistiram, respectivamente, 1, 1, 20, 28 e 1 registros sem erro de request.
- Mercado Livre respondeu `401` com o token existente e `403` sem token. A fonte ficou desativada até que `ML_CLIENT_ID`, `ML_CLIENT_SECRET`, `ML_ACCESS_TOKEN` e `ML_REFRESH_TOKEN` sejam configurados; o cliente passou a renovar automaticamente um token expirado quando essas credenciais estiverem disponíveis.
- Alertas anteriores foram reconciliados. Novos runs resolvem alertas antigos da fonte, e canaries não são comparados com o volume de uma coleta integral.
- A suíte local terminou com 24 testes aprovados, incluindo renovação de token e ciclo de vida de alertas.

## Compatibilidade e rollback

FastAPI, Redis, PostgreSQL, Alembic, Bronze/Silver/Gold, heurística, ML e Power BI foram preservados. Selenium continua no Compose para o legado durante o período de comparação; sua remoção só deve ocorrer após canaries e paridade de todas as fontes. O rollback de schema pode ser feito por Alembic; o rollback de código deve usar Git, sem apagar dados Bronze.
