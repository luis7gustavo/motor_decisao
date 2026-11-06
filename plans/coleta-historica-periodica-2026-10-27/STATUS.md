# Estado — prompt da coleta histórica e periódica

Atualizado em 04/11/2026, America/Sao_Paulo. A seção seguinte é o estado vigente; os registros datados abaixo permanecem como histórico e alguns deles descrevem bloqueios que já foram superados. Evidência detalhada anterior em `S06_S07_EXECUCAO_20261029.md`; repasses anteriores em `repasses/S08_AGENDAMENTO_20261103.md` e `repasses/S08_VERIFICACAO_20261103.md`.

## Estado vigente — limites de API, decisão de ritmo e teste de 1 dia, 03/11/2026

**Decisão do usuário em 03/11, execução em 04/11:** teste contínuo de 24 horas no Prefect dedicado, com alvo de **até 60 tentativas/minuto**, teto cumulativo **100.000 da missão** e teto de segurança **90.000 por dia civil de São Paulo**. A fonte compartilhada tem teto **100.099**, que inclui as 99 tentativas anteriores de outras missões. Os limites são decisões locais, não cotas oficiais do PNCP. Nenhum contador foi zerado. A seção anterior de 200/dia e 20.000/20.099 abaixo é registro histórico, vigente até a alteração auditada descrita aqui.

### Evidência sobre limites e escolha do ritmo

Não foi identificado limite numérico oficial nos trechos locais consultados dos manuais de integração e de APIs de consulta. O texto do PDF do Manual de Consultas não pôde ser lido naquela revisão. Isso **não** demonstra inexistência de limite oficial. Referências de terceiros, todas **não verificadas por nós**:

| Fonte | Relato de terceiro, não verificado |
| --- | --- |
| [TabNews, autor de servidor MCP](https://www.tabnews.com.br/dmanoel/a-api-do-pncp-ignora-filtros-em-silencio-o-que-aprendi-montando-um-servidor-mcp-de-licitacoes) | HTML/429 após cerca de 15 chamadas em rajada; adotou 12/min por instância; relata filtros ignorados silenciosamente. |
| [Apify, Brazil Government Transparency](https://apify.com/viralanalyzer/brazil-government-transparency) | Afirma 400/min, 700/min à noite e uso de cerca de 300/min. A página repete valores para o Portal da Transparência; confiabilidade baixa para PNCP. |
| [pncp-cli](https://pypi.org/project/pncp-cli/) | Relata 429 ou HTML em HTTP 200, WAF/headers, truncamento por paginação padrão, limites de tamanho de página por endpoint e HTTP 422 acima de 365 dias. Os comportamentos são hipóteses operacionais, não fatos comprovados nesta missão. |

**Inferência, não limite oficial:** os relatos sugerem sensibilidade a ritmo e rajadas. O alvo de 60/min excede o único ritmo medido publicamente nesse conjunto (12/min), por isso o teste tem freios automáticos. O intervalo mínimo é **1.000 ms entre reservas** e a concorrência é **1** em fonte, pool, fila e deployment. Com latência e trabalho local, a taxa efetiva deve ser menor que 60/min. Não será usado scraping do portal nem User-Agent de navegador para contornar WAF; os GETs permanecem nas rotas oficiais com o cliente identificável e `tamanhoPagina=50` nas listagens validadas.

### Proteções, janela e implantação

A janela persistida após a retomada é **04/11/2026 09h42 a 05/11/2026 09h42, São Paulo** (`2026-11-04T12:42:00Z` a `2026-11-05T12:42:00Z`). O ledger rejeita reservas antes do início e após o fim. O primeiro 429 ou HTTP 200 com HTML/JSON inválido mantém Bronze e outcome, não promove página, dobra o intervalo (metade da taxa) e respeita `Retry-After`; páginas válidas recuperam a taxa gradualmente até o piso de 1 segundo. HTTP 403 leva a `blocked_waf` e pausa o deployment dedicado antes da fonte. O flow também pausa ambos quando detectar fim da janela ou esgotamento de teto. Em falha de fonte/controle, aplicar a mesma ordem de pausa, preservando C2, Bronze, ledger e `exclusive.lock`.

Antes da ampliação, deployment e fonte foram pausados em **2.013/20.000** tentativas da missão, **2.112/20.099** da fonte e zero outcomes faltantes. O backup protegido `history-20261103-pre-24h-current-0012.dump` tem **924.828.941 bytes** e SHA-256 `765d508323a5718060f9c7427ea8ce8b27da229bb7d2877f78665f928f1e7160`. O relatório `history-20261103-pre-24h-restore-report.json` comprovou `pg_restore` integral exit 0, fingerprints iguais, hashes Bronze válidos e limpeza do ensaio. A suíte Docker descartável passou com **218 testes e 1 aviso** (`backups/20261103T192559Z_6f3163c0.json`). O recibo `backups/pncp-history-budget/20261103-24h-test-budget.json` registrou a ampliação atômica, sem GET e sem reset: missão **2.013/100.000** e fonte **2.112/100.099**.

O deployment dedicado preservou o ID `9183361f-fe0e-4a10-8469-d348f765fdb7`; a agenda horária foi substituída por `backfill-continuous` **a cada minuto**, até 100 passos por run (50 de publicação e 50 de atualização), sem sobreposição. Sync a cada 6 horas e reconciliação semanal permanecem; três agendas ativas, worker `ONLINE` e concorrência 1 foram conferidos em 04/11 antes do início da janela. O orçamento diário de 90.000 é uma trava adicional; o limite temporal e o teto cumulativo também são persistentes. A média e os resultados reais serão medidos no primeiro run e por hora; agendamento ativo não prova avanço de dados.

**Interrupção observada em 04/11:** o primeiro run agendado pelo worker dedicado (`01a1a4b5-441c-7751-954d-2dcf911432f6`) iniciou às 01h05 locais e consumiu **100 tentativas** com 100 outcomes, zero 429/403/5xx/HTML em 200 e **3.093 identidades distintas novas**. Por volta de 01h11, runs seguintes falharam ao carregar `/app/pipelines/__init__.py` (`OSError: [Errno 5] Input/output error`); o worker também perdeu a API Prefect. Os serviços dedicados voltaram a iniciar às 09h17 locais, indicando interrupção do runtime; a causa raiz do erro de montagem ainda não foi demonstrada. Às 09h19, o teste estava sem novas tentativas havia horas. Deployment e fonte foram pausados nessa ordem: missão **2.113/100.000**, fonte **2.212/100.099**, zero outcomes faltantes, 62.953 identidades históricas distintas. A janela inicial não é teste contínuo concluído.

**Retomada auditada em 04/11:** o backup protegido `history-20261104-pre-rewindow-current-0012.dump` tem **928.674.797 bytes**, SHA-256 `9125a06065c218b8b2621237a37d847f62dcb9d090946e6441e5e028d437cb04`; `history-20261104-pre-rewindow-restore-report.json` confirma `pg_restore` exit 0, fingerprints iguais, zero hashes Bronze inválidos, ensaio removido e principal íntegro. A suíte Docker repetida passou com **218 testes e 1 aviso** (`backups/20261104T122611Z_d59f6a46.json`). O recibo `backups/pncp-history-budget/20261104-24h-rewindow.json` moveu apenas o intervalo; preservou 2.113 tentativas da missão, 2.212 da fonte e as 100 da janela interrompida, sem GET e sem reset. O relatório somente leitura reiniciou a medição em zero tentativas e baseline de 62.953 identidades distintas. Fonte e deployment foram retomados antes do novo início; três agendas ativas e worker `ONLINE` foram conferidos. Um run órfão em `Submitting` e 71 runs `Scheduled` vencidos durante a parada foram cancelados apenas no Prefect, com recibo `backups/pncp-history-budget/20261104-prefect-stale-runs.json`, zero GET e zero escrita no C2; os runs concluídos e o ledger foram preservados. O worker voltou a executar runs antes da janela e obteve `partial_or_deferred` sem GET.

**Prova após retomada:** o run agendado `01a1a689-db45-7f24-b3e6-e879dff8614d` estava `RUNNING` no Prefect dedicado após 09h42 locais, com as três agendas ativas. Às 09h45 locais, o relatório somente leitura mostrava **38 tentativas na nova janela**, missão **2.151/100.000**, fonte **2.250/100.099**, **1.709 novas identidades distintas** desde a segunda baseline, zero outcomes faltantes no instante da leitura e zero 429, 403, 5xx e HTML/JSON inválido em 200. A mediana de latência dos outcomes concluídos era ~4,4 s e o p95 ~8,8 s; a taxa real é inferior ao teto teórico de 60/min. O run ainda estava em execução e a S08 permanece parcial.

### Estimativa e avaliação ao fim de 24 horas

Estimativa inferior grosseira, **não dimensionamento comprovado**: cerca de 632 partições de listagem ainda pendentes/em reconciliação na primeira fotografia × cerca de 18,6 tentativas observadas por partição concluída ≈ 11.750 tentativas; quatro recursos de enriquecimento × **62.953** identidades históricas distintas no início da janela retomada, com apenas uma chamada por identidade/recurso, ≈ 251.812; total aproximado ≈ **264 mil**. Não inclui paginação adicional, retries nem identidades futuras. O gargalo anterior era o teto local de 200/dia. Valores seguintes são tempos mínimos aritméticos, supondo a taxa constante e sem latência; 300/min é apenas comparação, não ritmo autorizado.

| Ritmo hipotético | Orçamento restante no início (97.887) | Listagens (~11.750) | Detalhe de 62.953 identidades (1 GET) | Total (~264 mil) |
| --- | ---: | ---: | ---: | ---: |
| 200/dia | ~489 dias | ~59 dias | ~315 dias | ~3,6 anos |
| 12/min | ~5,7 dias | ~16 h | ~3,6 dias | ~15,3 dias |
| 60/min | ~27 h | ~3,3 h | ~17,5 h | ~3,1 dias |
| 300/min | ~5,4 h | ~39 min | ~3,5 h | ~14,6 h |

A 60/min teóricos, 24 horas permitiriam até **86.400 tentativas**, cerca de 33% dessa estimativa; a latência e a concorrência 1 reduzem o volume real. O teto cumulativo de 100.000 é apenas para este teste e será revisto a partir de evidência. `scripts/radar_pncp_s08_24h_report.py` mede tentativas por hora e média efetiva, 429, HTML/JSON inválido em 200, 403, 5xx, latências mediana/p95 quando disponíveis, partições históricas e recorrentes separadas, identidades novas, orçamento e outcomes faltantes (meta **zero**). Após as 24 horas: manter/subir somente com bloqueios raros (proposta: <1%), sem 403 e ledger íntegro; reduzir com bloqueios recorrentes ou 5xx acima de limiar proposto de 10%; pausar e revisar diante de 403 ou inconsistência. Esses limiares são propostas para avaliação, não aceite automático da S08.

Antes de gastar orçamento no enriquecimento, verificar a página oficial de Dados Abertos para eventual carga em lote, comparar campos já trazidos pelas listagens e validar um piloto por recurso. A minuta `S06_PEDIDO_EVIDENCIA_PNCP.md` não foi enviada em nome do usuário. **S08 segue parcial e sem aceite**: listagens, retificações e os quatro recursos de enriquecimento ainda exigem verificação de cobertura; 200/204, partição `complete` e Prefect `Completed` não a demonstram.

## Estado vigente — coleta automática persistente e limite diário, 03/11/2026

O usuário escolheu até **200 tentativas por dia civil de São Paulo** e teto **cumulativo de 20.000 tentativas da missão**, incluindo as 2.000 já consumidas. A fonte compartilhada tem teto 20.099, incluindo as 99 tentativas anteriores de sonda/calibração/descoberta. Não houve reset de contadores. O limite diário é aplicado no banco isolado durante a reserva, sob o lock do orçamento compartilhado, contando tentativas de todas as missões da fonte; quando atingido, retorna adiamento até a meia-noite local sem chamada PNCP. O orçamento total permanece finito e exige nova revisão auditada quando acabar.

Antes da ampliação, deployment e fonte foram pausados em missão **2.000/2.000**, fonte **2.099/2.100**, com zero reservas sem outcome. O backup protegido `history-20261103-prebudget-current-0012.dump` tem **924.589.639 bytes** e SHA-256 `da06c0d12b585681a1c91831151aea4dc8c14b6818d19a2d78df06124f7571a5`. O relatório `history-20261103-prebudget-restore-report.json` confirmou `pg_restore` integral exit 0, fingerprints iguais, zero hashes Bronze inválidos, limpeza do ensaio, principal sem schema `procurement` e C2 na revisão `20261027_0012`. A suíte Docker em banco descartável passou com **212 testes e 1 aviso**, manifesto `backups/20261103T171640Z_9b70824e.json`.

A ampliação atômica em `control.source_budgets` e `control.source_missions` deixou ambos pausados em fonte **2.099/20.099** e missão **2.000/20.000**, com limite diário 200, zero outcomes faltantes e nenhum GET. Recibo `backups/pncp-history-budget/20261103-continuous-budget.json`. O deployment Prefect dedicado preservou o ID `9183361f-fe0e-4a10-8469-d348f765fdb7`, foi reaplicado pausado e migrou a agenda de backfill de 5 minutos para **1 hora**, até **6 páginas por run**; a agenda antiga foi removida  conferir que estava inativa. Sync a cada 6 horas e reconciliação semanal continuam. Há exatamente três agendas ativas com parâmetros `backfill/sync/reconcile`, pool/fila/deployment em concorrência 1 e worker `ONLINE`; PostgreSQL/Redis dedicados usam volumes persistentes e os serviços reiniciam com Docker.

Depois da retomada, um passo real de publicação gravou a página 72 (`page_committed`) e avançou a missão para 2.001. O run **manual de verificação pelo deployment** `61033e41-7e06-4087-9f68-e07ccfcf972d`, executado pelo worker dedicado, persistiu três páginas de publicação e encontrou HTTP 500 na atualização; Prefect terminou `Completed`, com resultado de negócio **`partial_or_deferred`**. O ledger avançou para **2.005/20.000** na missão e **2.104/20.099** na fonte, cinco reservas no dia local e zero outcomes faltantes. O próximo run automático de backfill estava agendado para **15h de 03/11**, horário de São Paulo; sua execução ainda não tinha sido observada ao fechar este registro.

**S08 permanece sem aceite:** auditoria `partial`, 108 partições completas, 610 pendentes e 22 em reconciliação, mais lacunas de detalhe, itens, resultados e documentos. Recorrência ativa e respostas HTTP válidas não provam cobertura nacional integral. Em falha de fonte/controle, pausar primeiro o deployment dedicado e depois a fonte, preservando C2, Bronze, ledger e `exclusive.lock`; não ativar o worker compartilhado.

### Continuação S08 — separação do marco e diagnóstico dos recursos, 03/11/2026

A coleta automática dedicada foi mantida ativa, sem alteração de agenda ou chamada manual PNCP nesta continuação. O run horário das 15h locais estava `Scheduled` na última inspeção às 14h38; por orientação do usuário, não se aguardou sua conclusão para avançar no restante da S08. O ledger, lido em transação somente leitura no C2 validado, tinha missão **2.005/20.000**, fonte **2.104/20.099**, cinco tentativas no dia civil local e zero outcomes faltantes. `plan` retornou `ready=true`, zero gates para as duas listagens; o verificador C2 confirmou integridade válida e manteve o job legado `partial/SourceChanged` na página 2, sem alegar cobertura.

  `audit --summary-only` e `status` distinguem o marco histórico (`pass_no < 100000`) da recorrência. Às 14h45, **histórico: 97 completas, 317 pendentes, 22 em reconciliação (436)**; **recorrência: 11 completas, 293 pendentes (304)**. O agregado anterior de 108/610/22 segue correto para aquela fotografia, mas não deve ser usado sozinho para o aceite S08. O flow agora registra auditoria resumida e apresenta marco e agregado separadamente no artefato. A suíte Docker passou com **214 testes, 1 aviso** (`backups/20261103T174428Z_dde5a847.json`).

O novo diagnóstico somente leitura `scripts/radar_pncp_s08_resource_plan.py` validou os hashes/recibos dos OpenAPI locais e as rotas GET documentadas de detalhe, itens, resultados e documentos. No histórico já observado há **57.289 identidades oficiais distintas**, todas com projeção e componentes locais candidatos à rota de detalhe; a fila de enriquecimento está vazia. Uma consulta de detalhe por identidade já vista exigiria mais que as **17.995 tentativas cumulativas restantes**, mesmo antes de novas identidades, paginação, resultados, anexos e retries. É um limite inferior condicional, não dimensionamento final. Os quatro gates de enriquecimento permanecem pendentes; nenhuma rota foi habilitada por essa checagem documental. A suíte Docker final passou com **215 testes, 1 aviso** (`backups/20261103T175400Z_d5a910a8.json`), e o comando real teve `pncp_network_calls=0`, `database_writes=0`.

**Próxima dependência S08:** observar em leitura posterior o run horário e seus outcomes sem interromper a coleta; preparar semeadura paginada de filas e validação limitada por recurso, com orçamento auditado que preserve backfill/sync. Não fechar a carga ou o aceite histórico por agenda ativa, partição `complete`, hash de contrato ou respostas 200/204.

## Estado vigente e mutação do plano — 30/10/2026

**S06:** prova real controlada no C2 concluída, com backup/restauração e integridade verificadas; a cobertura nacional segue parcial. **S07:** recorrência automática comprovada no Prefect dedicado pelos runs `01a18deb-2eab-7d7f-831a-2c5a6a298e7a` e `01a18deb-2eab-7a55-a9c1-4434aa06409b`, ambos com parâmetro/log `mode=backfill`. O segundo avançou publicação e tentou atualização. `Completed` no Prefect não foi usado como sinônimo de carga completa: o resultado de negócio foi `partial_or_deferred`. **S08:** em execução, ainda sem aceite. Em 30/10 às 15h22 locais, a missão registrava 228/2000 tentativas e 51 partições completas, 417 pendentes e 12 em reconciliação; fonte compartilhada 327/2100. Esses valores são uma fotografia; a agenda dedicada segue ativa e os contadores mudam.

**Motivo da mutação:** a ativação no Prefect compartilhado apagava os parâmetros das agendas e fazia o backfill rodar como `sync`; além disso, o Redis compartilhado voltou a reiniciar. A operação foi contida com worker antigo parado e pausa do ledger. A rotina `pause/resume` agora reenvia e confere os parâmetros. A missão migrou para uma pilha Prefect própria em `compose.radar-history-dedicated.yml`, sem migração do banco principal/C2 nem reparo do Redis compartilhado. O flow bloqueia explicitamente o URL do servidor compartilhado antes de qualquer transporte. A agenda dedicada está ativa com `backfill` a cada 5 minutos, `sync` a cada 6 horas e reconciliação semanal, concorrência 1.

**Dependência nova para S08:** doze janelas anuais da atualização esgotaram três tentativas, principalmente HTTP 500/504. Backup anterior à divisão `history-20261030-presplit-current-0012.dump` e restauração integral passaram; a suíte Docker registrou 206 testes aprovados e 1 aviso. A divisão idempotente produziu 24 filhas menores sem consumo de rede/quota e preservou os pais/recibos para duas passagens de reconciliação. Uma filha semestral ainda retornou HTTP 500; novas divisões podem ser necessárias. O lote de backfill foi elevado de 4 para 12 passos por execução, com intervalo mínimo de 1 segundo, concorrência 1, teto cumulativo 2000/2100 e tratamento de deferrals curtos. Os quatro recursos de enriquecimento continuam como lacunas explícitas, sem afirmação de que sejam inacessíveis.

**Adaptação posterior na mesma data:** sondas protegidas para 09/05/2023 e 01–30/05/2023, atualização modalidade 1, retornaram HTTP 200 em ambas as janelas e foram persistidas no C2. Janelas largas da mesma rota retornavam 500/504. A divisão passou a produzir subintervalos de até 30 dias após 5xx em janela maior, com reconciliação de N filhas em duas passagens; abaixo de 31 dias mantém o teto de três tentativas antes de dividir. Um teste sintético de três subintervalos e duas passagens integrou a suíte Docker: **207 aprovados, 1 aviso**, manifesto `backups/20261030T184417Z_08cf85ba.json`. A agenda dedicada foi retomada após as sondas, com três parâmetros corretos; os números do parágrafo anterior são fotografia anterior e não saldo atual.

**Critério de saída e reversão:** S07 exige run agendado correlacionado, worker ativo, parâmetros corretos e ledger íntegro; está comprovada no runtime dedicado. S08 exige tratar as partições e os recursos acessíveis, reconciliar mutações e registrar lacunas do que não puder ser obtido; ainda não foi atingida. Em erro de fonte/controle, pausar agenda dedicada e missão da fonte, preservar C2, ledger, Bronze e trava exclusiva do cliente PNCP da raiz. Não religar o worker compartilhado; sua recuperação é independente. Não enviar a consulta técnica ao PNCP em nome do usuário.

### Verificação de falha e retomada — 31/10/2026

A interrupção da sessão anterior ocorreu após o backup posterior e antes da retomada: agenda dedicada pausada, três schedules inativos, worker `ONLINE`, missão/fonte pausadas em 259/2000 e 358/2100. **Não houve falha no backup/restauração.** `history-20261030-postmonthly-current-0012.dump` tem 861.698.170 bytes e SHA-256 `f86db3969478a26c6748258257ac1e8d690359bca8511a76b83d4aec00be2867`; relatório adjacente confirmou `restored_equal=true`, `full_restore_exit_code=0`, zero hashes Bronze inválidos, ensaio limpo e principal sem schema `procurement`. A correção da classificação de tentativas históricas de atualização como `backfill` passou na suíte Docker: **207 testes, 1 aviso**, manifesto `backups/20261031T130944Z_b922999b.json`. A fonte foi retomada preservando 358/2100 e 259/2000, e o deployment dedicado reativado com três agendas ativas e parâmetros `backfill/sync/reconcile`; próximo run de backfill criado para 13:15 UTC. S08 continua parcial.

O run automático `01a19215-ce1d-7490-afc1-103f5dd9aa7a` das 13:15 UTC executou seis passos de publicação e seis de atualização, ambos exit 0, com resultado de negócio `batch_completed`. A missão avançou para **271/2000**, a fonte para **370/2100**, 62 partições completas, 421 pendentes e 15 em reconciliação; 8348 identidades observadas nas partições. A agenda permanece ativa. Esse lote concluído não encerra a carga histórica S08.

## Entrega do planejamento anterior

- [x] Prompt mestre contextualizado no checkout `motor_decisao`.
- [x] Skills `blueprint`, `docker-patterns` e `supabase-postgres-best-practices` consultadas para o planejamento.
- [x] Estado inicial conferido por arquivos locais: política, ledger, job, código, flow e verificador.
- [x] Revisão crítica independente do prompt e incorporação de três achados; ver `REVISAO.md`.
- [x] S00 executada em 27/10/2026; inventário, política e repasse em `S00_RELATORIO.md`, `config/contracts/pncp-history-mission-policy.json` e `repasses/S00.md`.

**Natureza da entrega anterior:** especificação em etapas. A execução S00 posterior está registrada abaixo; não confundir esta linha histórica com o estado atual.

## Execução S00 — 27/10/2026

**Estado:** concluída documentalmente; rede da missão histórica bloqueada. Nenhum GET externo, migração, job novo, schedule, reset de quota ou alteração no principal. A política geral permanece `enabled=false`; a nova missão tem orçamento externo zero até resolução dos gates. Nenhuma identidade histórica adicional foi coletada.

- Contrato arquivado com hash confirmado; página >=1 e tamanho 10–50; data `yyyyMMdd`. Vigência, bordas/fuso, início da cobertura, catálogo de modalidades, quota/taxa oficial, lote oficial e contratos de itens/documentos/resultados seguem pendentes. Manual local ausente; não houve busca de manual pela rede.
- Docker 29.8.2, Compose 5.5.1, Prefect 3.8.8 e PostgreSQL 15.19 observados. Alvo C2 reconfirmado por servidor/OID/role/revisão; principal `20260817_0009`, sem schema `procurement`.
- C2 intacto: 11 reservas, DOCUMENTAÇÃO 7/15, DADOS 4/50, calibração 2/3; 9 corpos verificados por hash e tamanho; 4 respostas Bronze reconciliadas aos recibos de dados. Job `e47c6fe2-2cc9-4b58-803a-49a4aa44a443` permanece `partial`, `next_page=2`, `error_code=ValueError`, 10 identidades/versões/observações.
- Política C2: segunda chave JSON `operator_authorization` redundante removida sem ampliar o grant; parser estrito integrado. Quatro testes focados passaram em Docker. Espaço livre físico C: observado: 30.907.371.520 bytes; dimensionamento nacional não identificado.
- Artefatos: `S00_RELATORIO.md`, `config/contracts/pncp-history-mission-policy.json`, `app/procurement/source_policy.py`, `scripts/radar_pncp_s00_inventory.py`, `tests/test_pncp_source_policy.py` e `repasses/S00.md`.
- Revisão crítica S00: escopo C2 não foi promovido a autorização geral; quota não foi renovada; não houve checkpoint avançado, agenda duplicada nem afirmação de cobertura integral. O problema de paginação mutável e o verificador desatualizado seguem para S01.

**Dependência seguinte na data da S00:** S01 offline. A execução posterior está registrada abaixo.

## Execução S01 — 27/10/2026

**Estado:** concluída offline. `SourceChanged` tipado, verificador dinâmico e classificação do job legado pela página Bronze preservada. Nenhum GET PNCP, job novo, migração, schedule, reset de quota ou alteração no principal. Relatório completo em `S01_RELATORIO.md`; repasse em `repasses/S01.md`.

- Dois testes novos reproduziram a falha antiga (`partial/ValueError`) para totais 9577/958 → 9562/957 e ID repetido entre páginas com totais estáveis; a suíte inicial teve 2 falhas e 145 aprovações. Após a correção, o runner descartável concluiu **153 testes aprovados, 1 aviso**, manifesto `backups/20261027T173116Z_53390ed2.json`, exit 0 e limpeza sem erros.
- O job C2 `e47c6fe2-2cc9-4b58-803a-49a4aa44a443` agora está `partial/SourceChanged`, `next_page=2`, `request_count=2`, `records_seen=10`. Checkpoint registra totais esperados/observados, ID Bronze e política local de reconciliação. A mesma página 2 não foi promovida. O coletor recusa retomada automática desse job.
- `verify`: `evidence_integrity_valid=true`, `collection_business_state=partial`, `coverage_verified=false`, `issues=[]`, `warnings=[]`, exit 0. Aceita crescimento reconciliado e rejeita hash adulterado, observação ausente ou cursor sem página Bronze. Não usa contagens fixas 3/10.
- Principal reconfirmado em `20260817_0009`, sem schema `procurement`; alvo isolado OID 2508600, revisão `20261013_0010`, 4 Bronze e 10 identidades/versões/observações. Ledger antes/depois: DOCUMENTAÇÃO 7/15, DADOS 4/50, calibração 2/3, 11 reservas.
- O flow expõe `business_outcome=reconciliation_required` para este tipo de parcial. Sucesso de paginação sem snapshot recebe `coverage_status=pagination_consistent_unverified`; totais estáveis não provam ausência absoluta de omissões.
- Arquivos da S01: `app/procurement/pncp.py`, `pipelines/procurement/pncp_flow.py`, `scripts/radar_pncp_live_validation.py`, `tests/test_pncp_ingestion.py`, `tests/test_pncp_verifier.py`, `docs/licitacoes/operacao-ciclo-2.md`, `S01_RELATORIO.md`, `repasses/S01.md` e este `STATUS.md`. Alterações anteriores foram preservadas.

**Dependência seguinte na data da S01:** S02 — persistência/controle/histórico. A execução posterior está registrada abaixo. S06 real e S07 permanecem bloqueadas pelos gates da S00.

## Execução S02 — 27/10/2026

**Estado:** concluída a fundação offline de controle e histórico persistentes no destino segregado; integração do worker histórico e do transporte ao novo limitador é S03. Relatório completo em `S02_RELATORIO.md`; repasse em `repasses/S02.md`. Sem GET PNCP, agenda, job novo, reset do ledger ou escrita no principal.

- O dump C2 anterior à migração tem 854.439.511 bytes e SHA-256 `b943710883b92e9bf65e6d3067b0d47376db075f97f0192a67bf566241e8ffa0`. `pg_restore --list` e restauração integral em banco descartável passaram, com revisão e contagens históricas iguais; banco de ensaio removido por OID. Evidência em `backups/c2/c2-20261013/s02-backup-report.json`.
- Migração aditiva `20261027_0011` aplicada somente a `test_20261020_ba894ce71405` (OID 2508600). O principal continua `20260817_0009`, sem schema `procurement`; role C2 tem zero privilégios nas tabelas do principal. Job e contagens C2 preservados: 3 jobs, 4 Bronze, 10 identidades, 10 versões e 10 observações. Oito tabelas de controle vazias, dez índices válidos, FKs validadas e grants verificados; evidência em `s02-migration-report.json`.
- `source_control.py` persiste manifesto, partição, watermark, fila e orçamento compartilhado com limite zero por padrão. Reserva de tentativa antes da rede, contador atômico, intervalo/concurrency, lease e fencing foram testados. Novo cursor de partição não avança sem promoção atômica, a ser implementada em S03. Observações C1 registram ordem por timestamp oficial e mantêm replay tardio sem regressão do ponteiro.
- Runner Docker descartável: **162 aprovados, 1 aviso**, exit 0, manifesto `backups/20261027T184031Z_4816b716.json`, cleanup sem erros. `verify` offline após a migração: integridade válida, estado de negócio `partial`, cobertura não verificada, sem issues/warnings. Ledger C2: 11 reservas, DOCUMENTAÇÃO 7/15, DADOS 4/50, calibração 2/3; não foram incorporados ao orçamento novo.
- S02 não habilitou a missão histórica: `enabled=false`, limite externo zero e gates da S00 pendentes. O novo limitador ainda não está conectado ao transporte C2 ou ao worker de backfill. Cobertura integral e funcionamento em chamadas reais não foram demonstrados.

**Próxima dependência:** S03 — backfill retomável, promoção atômica por página e cobertura auditável em fonte sintética, sem rede externa até autorização por recurso. Preserve o C2 `partial/SourceChanged` na página 2 e o principal.

## Execução S03 — 27/10/2026

**Estado:** concluída offline para o backfill sintético; rede da missão histórica
continua bloqueada. Relatório completo em `S03_RELATORIO.md` e repasse em
`repasses/S03.md`. Nenhum GET PNCP, migração, job histórico no C2, schedule,
reset de quota ou escrita no principal.

- `app/procurement/history.py` promove Bronze, identidades, versões,
  observações e cursor por página com transação e fencing. Tenta reservar no
  limitador persistente antes de qualquer transporte; conserva `not_before`,
  `Retry-After`, teto de 3 tentativas/URL e circuit breaker local.
  Checkpoint não carrega lista de IDs. Paginação mutável mantém o pai em
  `reconcile` e cria duas filhas quando a janela permite, sem afirmar
  integralidade. Totais estáveis com ID repetido também bloqueiam avanço.
- CLI `scripts/radar_pncp_history.py`: `plan/backfill/status/audit`, dry-run
  sem rede/DB e gate estrito. `plan` identificou 12 pendências, `backfill`
  retornou bloqueio sem requisição, `status` retornou `not_started`.
  A política permanece `enabled=false`, orçamento externo zero.
- Runner Docker em banco/role descartáveis: **172 testes aprovados, 1 aviso**,
  `pytest_exit_code=0`, manifesto `backups/20261027T233544Z_363a13f0.json`,
  cleanup sem erros. Provas incluem 2 modalidades/2 janelas/vazio, reinício
  real de processo na página 2, 2 processos concorrendo por quota e espaçamento,
  crash após Bronze, 429 durável e subdivisão de janela mutável.
- Inventário pós-teste: principal `20260817_0009`, sem schema `procurement`;
  C2 OID 2508600, revisão `20261027_0011`, 3 jobs, 4 Bronze e 10
  identidades/versões/observações. Job C2 `partial/SourceChanged`, página 2.
  Ledger 11 reservas, DOCUMENTAÇÃO 7/15, DADOS 4/50, calibração 2/3.
- Cobertura real: nenhuma identidade histórica adicional; `integral_verificado`
  não demonstrado. Filhos sintéticos completos recebem no máximo
  `pagination_consistent_unverified`.

**Próxima dependência:** S04 — sincronização, enriquecimento, replay e
reconciliação offline, com watermarks contíguos e o mesmo orçamento. S06 real
e S07 permanecem bloqueadas pelos gates de fonte/contrato/limites/orçamento.

## Execução S04 — 27/10/2026

**Estado:** concluída offline com fonte sintética; missão histórica real e
agenda permanecem bloqueadas. Relatório `S04_RELATORIO.md` e repasse
`repasses/S04.md`. Nenhum GET PNCP, job histórico real, schedule, reset de
quota ou escrita no principal.

- Publicação e atualização global usam partições e watermarks independentes,
  sobreposição diária e recuperação limitada. Watermark não atravessa data
  parcial. Retificações/cancelamentos antigos entram sem regressão por replay
  tardio; empate e timestamp sem fuso permanecem ambíguos.
- Reconciliação exige duas passadas filhas coincidentes e preserva estado
  `reconciliado_sem_snapshot`, sem alegar completude absoluta. Fila de
  detalhe/itens/resultados/documentos tem contrato validado como gate;
  anexos A→B sintéticos preservaram ambos os corpos em Bronze. Replay Bronze
  usou zero rede e zero reservas.
- Runner Docker: **181 aprovados, 1 aviso**, manifesto
  `backups/20261027T235824Z_3fd05348.json`, limpeza sem erros.
  `plan` final reportou 13 gates, `network_calls=0`, `database_writes=0`,
  em `backups/s04-cli-plan-final.json`.
- Backup C2 de 854.469.360 bytes, SHA-256
  `0e409ee255b7be85d45db4fdf756f9322f78ba5ea76d27ce70601a2f408e39e9`,
  teve `pg_restore --list` e restauração integral validada em banco descartável.
  Migração aditiva `20261027_0012` aplicada só ao C2 OID 2508600; contagens
  3 jobs, 4 Bronze, 10 identidades/versões/observações e oito tabelas novas
  vazias. Principal `20260817_0009`, sem schema `procurement`. Evidência
  `backups/c2/c2-20261013/s04-migration-report.json`.
- Job C2 `partial/SourceChanged`, página 2; ledger 11 reservas,
  DOCUMENTAÇÃO 7/15, DADOS 4/50, calibração 2/3. Missão nova sem manifesto
  iniciado, tentativas externas ou cobertura histórica adicional.

**Próxima dependência:** S05 — agenda Prefect/Docker idempotente e testada
offline, sem ativação real antes de resolver os gates da política. S06/S07
reais continuam bloqueadas; S08 permanece o aceite separado da cobertura.

## Execução S05 — 27/10/2026

**Estado:** implementação e ensaio isolado concluídos; aplicação no Prefect
compartilhado pendente por falha no Redis/AOF. Relatório `S05_RELATORIO.md`,
repasse `repasses/S05.md` e runbook
`docs/licitacoes/operacao-coleta-historica-prefect.md`. Nenhum GET PNCP,
agenda real, job histórico, reset de quota ou escrita no principal/C2.

- `plan/apply/pause/resume/status` implementados em Prefect 3.8.8, pool/fila
  exclusivos e limites 1. Um deployment com agendas distintas de backfill
  (30 min), sync (6 h) e reconciliação (semanal) inicia pausado. Flow com
  `retries=0`, lotes finitos, gate de idade e artefato de negócio.
- `prefect-init` foi inspecionado e não executado: seu entrypoint chama
  `alembic upgrade head`. Compose validado com `config --quiet`, exit 0.
- Pilha Prefect de teste com PostgreSQL/Redis próprios: `apply` duas vezes
  preservou um deployment/uma agenda do probe e um deployment/três agendas
  inativas da missão. `resume`, execução agendada pelo worker, reinício do
  worker e `pause` comprovados com checkpoint de quatro IDs únicos. A missão
  teve zero runs; `resume` foi bloqueado por 13 gates.
- Prefect 3.8.8/SQLAlchemy 2.1.4 reproduziu falha do scheduler na pilha de
  teste. Imagem derivada com SQLAlchemy 2.0.49 corrigiu o ensaio; dependência
  fixada em `requirements.txt`, sem reconstruir a imagem compartilhada.
- Redis compartilhado reinicia por AOF inválido e `prefect-services` também.
  `apply` no servidor compartilhado falhou antes de criar pool/fila/deployment;
  `backups/s05-live-prefect-status.json` confirma ausência. Volume/AOF não
  foram alterados. Serviços de ensaio estão parados, volumes conservados.
- Inventário/verificador finais: C2 OID 2508600/revisão `20261027_0012`,
  3 jobs, 4 Bronze, 10 identidades/versões/observações; job
  `partial/SourceChanged`, página 2; ledger 11 reservas (DOCUMENTAÇÃO 7/15,
  DADOS 4/50, calibração 2/3). Principal `20260817_0009`, sem schema
  `procurement`. Evidência `backups/s05-c2-inventory.json` e
  `backups/s05-c2-verify.json`: integridade válida, cobertura não verificada.

**Próxima dependência:** S06 — testes integrados e prova controlada. Trabalho
sintético pode avançar; prova real segue bloqueada por 13 gates, pelo Redis
compartilhado e pela necessidade de validar o scheduler/worker no runtime
real. S07 ativação e S08 aceite histórico permanecem separados.

## Execução S06 — 28/10/2026
**Estado:** validação offline concluída; prova real e critério de saída S06 bloqueados. Relatório `S06_RELATORIO.md`, repasse `repasses/S06.md`. O pedido desta sessão abrangeu todas as fases restantes, preservando as dependências e gates do plano. Nenhum GET PNCP, job histórico real, schedule, reset de quota, migração ou escrita no principal/C2.
- Runner Docker final: **186 testes aprovados, 1 aviso**, manifesto `backups/20261028T162438Z_378918e6.json`, pytest exit 0, banco/role descartáveis limpos. Novos testes cobrem `Retry-After` segundos/data HTTP durável, teto total de 3 tentativas entre reinícios e auditoria após lote parcial.
- Backup atual C2 de 854.469.951 bytes, SHA-256 `b4509f88b8859b0d7a132f585cca3d2c0bbb0413311f3a42c3d2e3ac0a25f517`; `pg_restore --list` e restauração integral em banco descartável passaram. Revisão, contagens e fingerprints Bronze/identidade/versão/observação iguais, zero hash Bronze inválido. `backups/c2/c2-20261013/s06-restore-report.json`, ensaio removido.
- Prefect isolado executou flow `sync` como `Completed`/negócio `blocked_contract`, 13 gates, `commands=[]`, zero rede PNCP; ensaio parado com volumes preservados. Prefect compartilhado segue sem pool/fila/deployment da missão; Redis reinicia por AOF inválido. `backups/s06-prefect-flow-blocked.txt`, `s06-prefect-status.txt`.
- Plano seco: 13 gates, política `enabled=false`, orçamento externo zero, rede e escrita zero. `status/audit` da missão `not_started`; auditoria local conservadora agora integra o flow para futuros lotes habilitados. Manual local ausente, sem busca pela rede.
- Inventário/verificador finais: principal `20260817_0009` sem `procurement`; C2 OID 2508600/revisão `20261027_0012`, 3 jobs, 4 Bronze, 10 identidades/versões/observações; job `partial/SourceChanged` página 2, ledger 11 reservas (DOCUMENTAÇÃO 7/15, DADOS 4/50, calibração 2/3). Integridade válida, cobertura não verificada, zero issues/warnings.
**Dependência seguinte:** repetir a prova real S06 só após resolver contrato, limites, cobertura, modalidades, semântica de datas, orçamento e Prefect compartilhado. S07 ativação e S08 auditoria do universo não executadas; nenhum critério de saída delas foi atingido.

## Complemento documental S06 — 28/10/2026

Após autorização explícita para buscar documentação na internet, foram arquivados os OpenAPI oficiais atuais de **Consulta** (12 caminhos, SHA-256 `b2c1fb8874861f63ebf7c065756f82c04dc4a84c8def63e5b4aa641d5edb4430`) e **Integração** (124 caminhos, SHA-256 `d721ad1c59a82f5dea3ced7ce4e92dc51da91096c2041382a567aa5773a57aa2`) em `config/contracts/`. Recibos de duas consultas documentais HTTP 200 e tentativa malsucedida de baixar o manual PDF estão em `backups/pncp-docs/20261028/`. O portal oficial lista o manual de consultas v1.0, mas o PDF não foi salvo localmente; nenhum conteúdo dele foi tratado como leitura local confirmada.

Comparação semântica com o OpenAPI de 13/10: os 12 caminhos e suas operações/parâmetros permanecem iguais; 35 mudanças são somente valores de exemplo de datas. A política da missão aponta ao snapshot atual e `contract_currently_revalidated=true` **apenas para a estrutura de consulta**. As rotas GET de detalhe, itens, resultados, documentos, histórico e modalidades foram identificadas no contrato de integração; validação operacional por recurso ainda pendente. Fonte e limites detalhados em `docs/licitacoes/fontes/pncp-consulta-2026-10-28.md`.

O `plan` após a atualização retornou exit 3 e **12 gates**, `network_calls=0`, `database_writes=0`, em `backups/s06-docs-policy-plan.txt`. Permanecem desconhecidos a primeira data recuperável, o catálogo completo de modalidades, as bordas/fuso e amplitude de janelas, quota/taxa oficial numérica, dimensionamento/orçamento, além do Redis/Prefect compartilhado. **Nenhum GET de dados PNCP, missão, partição ou agenda real foi iniciado.** S06 real, S07 e S08 permanecem bloqueadas; esta atualização não altera as dependências nem os critérios de saída do plano.

Ensaio adicional do Redis: backup somente leitura do volume compartilhado com SHA-256 `c8bda756389aa1f9ac4882c116310ae50cc96c37f9e1a07401bc32d16a1adc66`; cópia isolada idêntica, AOF inválido nos últimos 3.983 bytes, reparo **somente no clone**, validação do AOF e Redis isolado `PONG`/85 chaves. Volume real não foi modificado. Relatório `S06_REDIS_ENSAIO.md`; reparo compartilhado depende de decisão explícita porque descarta bytes e pode afetar outros fluxos.

Pesquisa oficial complementar: a [página de perguntas e respostas do PNCP](https://www.gov.br/pncp/pt-br/pncp/perguntas-e-respostas) confirma que a visualização de dados publicados é pública e gratuita. O [manual de integração](https://pncp.gov.br/manual/pt-br/latest/singlehtml/) documenta consulta de modalidades com filtro `statusAtivo` opcional. Isso não comprova quota numérica, catálogo observado nem primeira data recuperável. A autorização do usuário para pesquisa documental substitui a antiga restrição de busca online; o estado da política foi atualizado para `documented_data_collection_blocked`. Orçamento de dados continua zero, sem GET de produção e sem mudança no C2.

Foi preparada a proposta reviewável `S06_SONDAS_PROPOSTAS.md`: até 12 tentativas reais totais com reserva compartilhada, sem ativar backfill/agenda. Naquele momento ainda não executada; seu teto não substitui orçamento dimensionado da missão completa. A busca oficial também não identificou exportação integral de contratações: o CSV documentado no manual de integração refere-se a itens de PCA por órgão/ano, escopo diferente.

**Execução da sonda após autorização, 28/10/2026:** `S06_SONDAS_RESULTADO.md`. Quatro de 12 tentativas reais reservadas e reconciliadas; três HTTP 200 (catálogo 19 modalidades, publicação e atualização global da página 1) e um HTTP 301 no detalhe pela base de integração. O corpo do 301 apontou porta não autorizada; nenhuma conexão de seguimento foi feita. Quatro corpos/recibos tiveram SHA-256 conferidos. O budget compartilhado da sonda e sua missão estão **pausados**, 4/12 consumidas, 8 restantes sem uso. A política da missão histórica principal continua `enabled=false` e orçamento zero; modalidades 1–19 constam apenas como catálogo atual observado, sem verificação histórica. Inventário posterior confirmou principal e C2 intactos. S06 real não atingiu o critério de saída; S07/S08 permanecem bloqueadas. Redis compartilhado não foi reparado.

**Retomada da sonda autorizada, 28/10/2026:** `S06_SONDAS_RETOMADA_PROPOSTA.md` e resultado final no mesmo `S06_SONDAS_RESULTADO.md`. O detalhe pela base da API de Consulta retornou 200 com identidade conferida; itens, lista de arquivos e histórico retornaram 200. Resultado por item foi omitido porque o detalhe indicou `existeResultado=false`; anexos não foram baixados. Três janelas antigas da modalidade 6 deram 204, sem provar início absoluto. Estado final: **11/12** reservas/outcomes, 7 HTTP 200, 3 HTTP 204, 1 HTTP 301; 11 corpos/recibos com hash/tamanho conferidos. Budget e missão de sonda pausados; uma tentativa de contingência não usada. Principal e C2 novamente conferidos intactos. A missão histórica principal permanece desabilitada e não iniciou Bronze/partições; S06 real, S07 e S08 continuam sem aceite. Redis compartilhado permanece pendente.
## Retomada S06 — 29/10/2026

Conferência somente leitura em `S06_RETOMADA_20261029.md`: Redis compartilhado respondeu `PONG`, carregou AOF e estava saudável; servidor Prefect e PostgreSQL Prefect estavam saudáveis. O deployment `radar-pncp-history/mission` já existe pausado com três agendas inativas, sem runs recentes, e o worker da missão está offline. Não foi identificado nesta sessão quem aplicou o deployment ou recuperou o Redis. A falha operacional registrada em 28/10 não foi reproduzida agora; a prova de scheduler/worker real continua pendente.

O `plan` atual da missão saiu com código 3 e **12 gates**, zero chamadas PNCP e zero escritas de domínio. `status` e `audit` retornaram `not_started`. Política `enabled=false`, orçamento externo zero e sonda anterior pausada em 11/12. **S06 permanece sem aceite; S07 e S08 não iniciadas.**

### Continuação offline do desbloqueio — 29/10/2026

Em `S06_DESBLOQUEIO_20261029.md`, duas respostas HTTP 200 previamente arquivadas e seus hashes sustentaram um teto local de janela de um dia. O intervalo mínimo local de 1000 ms e a concorrência 1 foram registrados sem atribuí-los ao PNCP como limites oficiais. O plano seco foi alinhado às validações de tipo/valor da execução; seis testes focados passaram no runner Docker. Naquele momento, o `plan` retornava **9 gates**, zero rede PNCP e zero escritas de domínio. A missão permanecia desabilitada, orçamento externo zero e sem início de carga. A sonda anterior mantinha 11/12 tentativas e orçamento pausado.

### Segunda campanha de calibração — 29/10/2026

Em `S06_CALIBRACAO_20261029.md`, após backup íntegro e restauração em banco descartável, o orçamento persistente da fonte foi ampliado de 12 para 36 sem reiniciar as 11 tentativas anteriores. A nova missão de calibração consumiu 22/24, com 20 HTTP 200, um HTTP 204 e um timeout; o orçamento compartilhado terminou pausado em 33/36. Recibos/corpos protegidos: 21 pares com hash e tamanho válidos, mais um erro, 22 outcomes para 22 reservas. Publicação e atualização tiveram pequenas partições de duas/três páginas integralmente reconciliadas na amostra; janelas combinadas de 13–14/10 corresponderam à união de dias individuais. O catálogo inativo retornou array vazio, sem provar inexistência de códigos históricos retirados. Uma contagem de publicação e outra de atualização mudaram desde 28/10, reforçando o tratamento de fonte mutável. Backup posterior/restauração passaram; a suíte Docker passou com 196 testes, 1 aviso. Principal e C2 conferidos; missão histórica principal segue `enabled=false`, orçamento zero, sem Bronze/partições e sem aceite S06/S07/S08.

### Mutação de escopo e dependências — 29/10/2026

**Motivo:** o usuário esclareceu que “todo o histórico” significa o universo acessível pela API e que o indisponível pode ser ignorado. A resposta à consulta técnica ao PNCP, que ele próprio poderá enviar, deixa de ser pré-condição da S06. Decisão e critérios em `S06_DECISAO_ESCOPO_ACESSIVEL_20261029.md`. A etapa S06 foi dividida operacionalmente em descoberta limitada do início acessível, coordenação do limitador, dimensionamento/orçamento e prova real; S07 e S08 mantêm sua ordem.

**Alteração e impacto:** o catálogo atualmente exposto (1–19), as bordas diárias observadas na amostra e as duas listagens foram aceitos para planejar o universo acessível. A política não afirma que códigos retirados, datas anteriores ou recursos inacessíveis inexistem. Resultado vazio por HTTP 204 é partição consultada; timeout/429/5xx continuam pendentes até resolução ou esgotamento documentado, e rejeição estável vira lacuna de fonte. O plano seco passou de 9 para **5 gates**: `mission_disabled`, `coverage_start_unverified`, `effective_restriction_scope_unresolved`, `stable_budget_scope_key_unresolved`, `attempt_budget_zero`; `ready=false`, zero rede/escrita. Dez testes focados passaram no Docker. A missão principal segue `enabled=false` e orçamento externo zero; calibração 33/36 na fonte desabilitada e 22/24 na missão pausada, sem renovar tentativas.

**Critério de saída e reversão:** S06 exige primeiro intervalo acessível estabelecido por sondas auditadas, fonte coordenada ou isolada durante a carga, orçamento cumulativo finito e prova real de persistência/reconciliação. S08 exige todas as partições/recursos acessíveis do manifesto tratados e risco de paginação mutável explicitado, sem alegação de completude absoluta sem controle independente. Se novas sondas ou resposta oficial invalidarem o catálogo/semântica usados, pausar, reabrir as partições afetadas e reverter os gates; preservar dados e ledger. Não houve nova chamada PNCP nem ativação nesta mutação.

## Estado anterior à S00, mantido como histórico

- Sessão C2: `c2-20261013`.
- Ledger: DOCUMENTAÇÃO 7/15; DADOS 4/50; calibração 2/3; 11 tentativas totais. São limites locais do ensaio, não quotas oficiais da API.
- Job preservado: `e47c6fe2-2cc9-4b58-803a-49a4aa44a443`; arquivo `backups/c2/c2-20261013/collection-job.json`.
- Política geral: `enabled=false`, `limits_reviewed=false`; autorização restrita a C2; a chave `operator_authorization` estava duplicada e foi corrigida na S00.
- Fonte de mutabilidade atual: `app/procurement/pncp.py` ainda levanta `ValueError('Source changed pagination totals')`; tipagem `SourceChanged` e reconciliação precisam ser implementadas/testadas.
- Verificador atual compara constantes antigas de 3 respostas/chamadas de dados; deve ser corrigido antes de novas coletas.
- `docs/licitacoes/fontes/` ausente; restrição de não buscar manual pela rede preservada.
- Destino anteriormente comprovado: `test_20261020_ba894ce71405`, OID 2508600. Identidade atual não validada ao vivo nesta sessão.
- Git do checkout: branch observada `codex/radar-licita-mvp-sacy-2026-10-16`; referência padrão remota `origin/main`; `gh` indisponível no PATH. Mudanças locais preexistentes preservadas.

## Estado das etapas — atualizado em 04/11/2026

| Etapa | Estado | Evidência de conclusão |
|---|---|---|
| S00 — inventário/contrato/política | concluída documentalmente; rede bloqueada | `S00_RELATORIO.md`, política JSON, inventário ao vivo somente leitura, 4 testes focados |
| S01 — paginação/verificador | concluída offline | `S01_RELATORIO.md`, 153 testes, `verify` e inventário C2 após classificação |
| S02 — persistência/controle | concluída no destino segregado; worker pendente S03 | `S02_RELATORIO.md`, dump/restauração, migração `20261027_0011`, 162 testes |
| S03 — backfill histórico | concluída offline; rede bloqueada | `S03_RELATORIO.md`, 172 testes, CLI e inventário final |
| S04 — atualização/reconciliação | concluída offline; rede bloqueada | `S04_RELATORIO.md`, 181 testes, backup/restauração e migração `20261027_0012` |
| S05 — agenda Prefect/Docker | ensaio isolado concluído; runtime dedicado em uso | `S05_RELATORIO.md`, `S06_S07_EXECUCAO_20261029.md`; worker dedicado `ONLINE` |
| S06 — validação/prova controlada | prova integrada real no C2 concluída; cobertura nacional parcial | backup/restauração, Bronze, ledger, cursor e integridade conferidos em `S06_S07_EXECUCAO_20261029.md` |
| S07 — ativação/prova de recorrência | comprovada no Prefect dedicado | runs agendados com `mode=backfill`, worker dedicado, agenda e ledger íntegros; `Completed` sem alegar cobertura |
| S08 — acompanhamento/aceite histórico | segunda janela de 24 horas agendada e fonte retomada; sem aceite | missão 100.000, fonte 100.099, 90.000/dia; 100 tentativas anteriores preservadas e lacunas explícitas |

Estados de resultado independentes: **S00–S05 concluídas em seus escopos**, **S06 comprovada em execução real controlada**, **S07 comprovada no Prefect dedicado**, **S08 em carga e sem aceite**, **cobertura do universo acessível ainda não demonstrada**. A agenda ativa e a conclusão técnica de runs não substituem a auditoria de partições, versões, reconciliação e lacunas por recurso. Sem snapshot/controle suficiente, a cobertura mantém risco residual de omissão.

S00 foi concluída documentalmente com gates de rede bloqueados, permitindo S01–S05 offline; esse é o registro histórico do início do plano. Os gates validados de publicação e atualização foram abertos depois, sob orçamento isolado. Detalhe, itens, resultados e documentos continuam pendentes de piloto e dimensionamento.

Próxima ação: acompanhar o primeiro run e o relatório por hora da segunda janela de 24 horas, conferir ledger/outcomes e consumo, e avaliar ritmo e teto ao fim da janela em 05/11/2026 às 09h42 locais. Depois, validar piloto e orçamento por recurso de enriquecimento, preservar C2 e principal e não declarar S08 aceita antes de auditar cobertura e lacunas. Uma eventual resposta oficial à minuta `S06_PEDIDO_EVIDENCIA_PNCP.md` poderá corrigir o manifesto; a minuta permanece sem envio.

## Como iniciar em outra sessão

```text
Retome a etapa S08 na pasta plans/coleta-historica-periodica-2026-10-27.
Leia primeiro o topo de STATUS.md e PROMPT_MESTRE.md. Confira o estado do deployment
Prefect dedicado, o primeiro run, o ledger e scripts/radar_pncp_s08_24h_report.py.
Preserve o banco principal, o C2, Bronze, ledger e exclusive.lock. Separe marco
histórico da recorrência; não declare aceite por agenda ativa, Prefect Completed
ou respostas HTTP 200/204. No fim da janela, registre métricas e decisão de ritmo.
```

Para continuação posterior, use sempre o estado vigente no topo e o relatório de teste mais recente.
