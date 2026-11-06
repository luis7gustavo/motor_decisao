# S06 — execução offline do plano de desbloqueio em 29/10/2026

**Atualização posterior:** a campanha e a ampliação de orçamento descritas como próximas ações abaixo foram executadas e registradas em `S06_CALIBRACAO_20261029.md`. Esta seção preserva o estado anterior às sondas.

## Resultado executado

- A política da missão registra `max_window_days_effective=1` como teto **local**. As sondas 2 e 3 de 28/10 retornaram HTTP 200 para publicação e atualização global na janela de um dia 13/10/2026. Os dois recibos e corpos protegidos existem, e tamanho/SHA-256 foram reconferidos nesta sessão. Isso não comprova inclusão das bordas nem data inicial da fonte.
- A política registra intervalo mínimo local de 1000 ms e concorrência 1, os mesmos limites usados pela sonda isolada. Não são anunciados como limites oficiais do PNCP. A missão continua `enabled=false` e com orçamento externo zero.
- `scripts/radar_pncp_history.py` agora rejeita no plano seco tipos/valores de intervalo, concorrência e teto de tentativas que o gate de execução real recusaria. Seis testes focados em `tests/test_history_policy_plan.py` passaram no runner Docker, exit 0.
- `scripts.radar_pncp_history plan` no runner Docker: exit 3, **9 gates**, `network_calls=0`, `database_writes=0`. Saíram apenas `window_limit_unresolved`, `effective_min_interval_ms_unresolved` e `effective_max_concurrency_unresolved`.
- `scripts.radar_pncp_s06_probe status`, somente leitura: orçamento `pncp:public-read:shared-docker-egress`, restrição `unauthenticated-public-read-shared-docker-egress`, limite 12, consumidas 11, `enabled=false`. Não houve renovação do teto nem nova tentativa PNCP.

## Bloqueios ainda ativos

1. `coverage_start_unverified`: a data inicial recuperável por rota não foi comprovada. Janelas antigas com HTTP 204 não demonstram ausência anterior.
2. `modality_catalog_unverified`: o catálogo atual retornou códigos 1–19, todos ativos; faltam evidências de modalidades históricas inativas ou retiradas.
3. `date_filter_bounds_unverified`: faltam inclusividade de `dataInicial`/`dataFinal`, fuso e semântica do marco final.
4. `publication_resource_blocked` e `global_update_resource_blocked`: falta prova controlada de paginação com mais de uma página, identidade e reconciliação de fonte mutável.
5. `effective_restriction_scope_unresolved` e `stable_budget_scope_key_unresolved`: a sonda já usa uma chave durável no C2, mas a missão histórica ainda não pode herdar seu orçamento. `SourceControl.register_budget` exige limite idêntico e rejeita ampliação, corretamente. O backend da raiz tem cliente PNCP com limite em memória por processo e não participa desse ledger; seus serviços estavam parados na conferência. Antes da ativação, é preciso coordenar todos os consumidores efetivos ou impedir de forma verificável sua concorrência.
6. `attempt_budget_zero`: não há teto novo dimensionado e registrado para calibração/carga histórica. O saldo 1/12 da sonda anterior é contingência daquela missão, não verba nova.
7. `mission_disabled`: deve ser o último gate a liberar, após os demais e a prova S06.

## Trabalho necessário antes de chamadas de dados

1. Solicitar ou localizar evidência oficial para: primeira data recuperável de publicação e atualização, possibilidade de dados retroativos, catálogo histórico de modalidades, inclusão das bordas e fuso, amplitude máxima de janela e restrições de taxa/quota por IP ou credencial. Se a fonte não puder confirmar o início, manter a cobertura anterior como `não identificada` e não declarar o histórico integral.
   Minuta de consulta e canal oficial em `S06_PEDIDO_EVIDENCIA_PNCP.md`; nenhuma mensagem foi enviada.
2. Projetar uma ampliação auditável do orçamento compartilhado existente: operação transacional com `SELECT ... FOR UPDATE`, orçamento desabilitado, comparação explícita de chave, restrição, limite 12 e consumo 11, novo teto cumulativo aprovado, sem alterar `attempts_consumed`, `not_before` ou `next_allowed_at`. Registrar um evento imutável da ampliação. Uma missão nova terá também seu próprio teto de tentativas, sem reabrir a sonda pausada.
3. Integrar ou isolar de forma verificável o cliente PNCP do backend da raiz antes de afirmar que o limitador cobre a restrição efetiva. O cliente atual usa espera local e retries próprios; isso não substitui o ledger persistente da missão.
4. Só com um orçamento finito separado, executar sondas de borda/paginação dentro do limitador, armazenar corpos/recibos e reconciliar identidades, totais e versões. Calcular orçamento total do backfill a partir de partições e páginas observadas, enriquecimento, reconciliação e margem de tentativas; não extrapolar uma página para o país.

## Decisão de etapa

S06 continua bloqueada pelos 9 gates acima. S07 e S08 não foram iniciadas. Nenhuma alteração foi feita no C2, banco principal, deployment ou agenda nesta execução; o job C2 `partial/SourceChanged`, Bronze e ledger foram preservados. A redução de gates é apenas documental e de validação offline, sem prova de cobertura histórica.
