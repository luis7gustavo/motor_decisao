# Radar Licita — controle de implementação

Estado atual: **Ciclo 1 concluído no escopo técnico definido**. Persistência pública implementada e validada exclusivamente em ambiente isolado; aplicação no principal pendente. Ciclo 0 concluído no commit 50eeaeb. Histórico integral de seu encerramento em [_implementacao-historico-c0-concluido.md](_implementacao-historico-c0-concluido.md), interrupções em [_implementacao-historico-c0.md](_implementacao-historico-c0.md). Atualização: 2026-10-13, America/Sao_Paulo.

## 1. Linha de base e parâmetros

- Sessão C1 iniciada no commit 50eeaebaecd0125b4bc472f35add0fc6bcdc5833, branch feat/radar-licita-implementacao. Repositório luis7gustavo/motor_decisao; checkout pai não é alvo.
- Git inicial: nenhum diff rastreado; documentação de especificação não rastreada preexistente, preservada e fora dos commits. Adendos novos mantêm rastreabilidade conforme ADR-021.
- CICLO_ALVO automático → 1, pois 0 concluído. Categoria vazia. Janela configurada últimos 7 dias: 2026-10-07 a 2026-10-13 America/Sao_Paulo; nenhuma coleta neste ciclo, sem campo/limites de fonte presumidos. Orçamento 50; intervalo 1 segundo. Aplicar migração principal não; agendamentos Prefect novos não; push não. Valores válidos, permissões normalizadas para não. A mensagem continue retoma a mesma sessão, sem novo ciclo nem renovação de orçamento.
- Docker Engine 29.8.2, Compose 5.5.1, daemon respondeu. Configuração base e aditiva passaram antes da implementação. Runner efêmero existente, imagem reutilizada; nenhum novo build ou runtime no host.
- Serviços já ativos: api/postgres/redis/selenium/prefect-postgres/prefect-server com healthchecks saudáveis; prefect-services e três workers ativos sem healthcheck. prefect-init encerrado 0, inspecionado e não reiniciado: executa upgrade head. Nenhum serviço legado iniciado/parado/reconfigurado.
- Principal motor_decisao, servidor 7696140971977625639, origem esperada 20260817_0009. Runner reconfirma origem no principal antes de restaurar. Nova revisão 20261013_0010 é descendente linear; aplicação principal desabilitada e recusada pela própria revisão, conforme ADR-022.
- Baseline da sessão: comando tests --origin 20260817_0009, manifesto 20261013T165613Z_186cc7c0.json: 70 aprovados em 9,40 s, 0 falhas/erros/ignorados, 1 warning legado FastAPI/Starlette/httpx. Papel sem privilégios nas tabelas principais; banco/papel removidos.

## 2. Comandos canônicos atuais

Executar na raiz do checkout. Detalhes e backup exato em [operacao-ciclo-1.md](operacao-ciclo-1.md). Comandos C0 pertencem ao histórico e à suíte daquele commit.

```powershell
docker compose config --quiet
docker compose -f docker-compose.yml -f compose.radar-ops.yml config --quiet
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps radar-ops tests --origin 20261013_0010
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps radar-ops rehearse --origin 20260817_0009 --target 20261013_0010 --dump /evidence/radar-c1-20261013.dump
docker compose ps -a
docker compose logs --tail 50 postgres
```

No modo tests, origin é a revisão instalada no banco sintético. No ensaio, origin é a revisão restaurada e target é o destino. Runner recusa ensaio real sem verificador estrutural específico. Nenhum comando de aplicação principal é autorizado. Logs operacionais não precisaram ser lidos; relatórios sanitizados ficam em backups/.

## 3. Backup

- Arquivo backups/radar-c1-20261013.dump, 854363876 bytes, SHA256 ad9ad83e61b8db747dfaed022ffa2f077a3340e3abe24c7084825f3ae1763fef.
- pg_dump custom e pg_restore --list via docker compose exec retornaram DUMP_AND_LIST_OK, código 0. Transferência binária docker compose cp concluída. Última escrita 2026-10-13 17:04:46 UTC (14:04:46 America/Sao_Paulo).
- Credenciais somente do ambiente do container; dump/manifestos ignorados pelo Git. Dumps desta sessão e anteriores, inclusive /tmp do container, preservados; nenhuma limpeza automática de backup.
- **Backup com restauração verificada**: manifesto 20261013T223425Z_34e76a5d.json, pg_restore --exit-on-error --single-transaction --no-owner código 0, operation_passed e restoration_verified verdadeiros. Ensaio iniciado 22:34:25 UTC e concluído 22:38:49 UTC (19:38:49 America/Sao_Paulo), aproximadamente 4 min 24 s incluindo restauração e verificações. Origem restaurada 20260817_0009; 36 tabelas conferidas contra o índice do dump.
- Referência consistente: cópia restaurada do mesmo dump; nenhuma comparação entre consultas independentes ao principal ativo e o snapshot. Escritas legadas não suspensas, aplicação principal não autorizada.


### Contagens do snapshot restaurado C1

Origem exclusiva: cópia restaurada do dump C1; somente identificadores e totais.

| Tabela | Linhas |
|---|---:|
| bronze.market_web_listings_raw | 127858 |
| bronze.mercado_livre_items_raw | 16040 |
| bronze.mercado_livre_products_raw | 59360 |
| bronze.price_history_raw | 15198 |
| bronze.supplier_products_raw | 229784 |
| control.collection_alerts | 83 |
| control.data_quality_checks | 5936 |
| control.pipeline_runs | 6211 |
| control.source_runs | 6444 |
| dw.dim_confianca | 4 |
| dw.dim_execucao_motor | 24 |
| dw.dim_fonte_coleta | 16 |
| dw.dim_fornecedor | 3 |
| dw.dim_modelo_ml | 8 |
| dw.dim_produto_fornecedor | 77675 |
| dw.dim_recomendacao | 4 |
| dw.dim_tempo | 27 |
| dw.fato_execucao_fonte | 3839 |
| dw.fato_oportunidade_compra | 5957 |
| dw.fato_score_ml | 22159 |
| feedback.opportunity_reviews | 2 |
| gold.decision_engine_runs | 41 |
| gold.decision_opportunities | 6878 |
| gold.decision_opportunity_snapshots | 232590 |
| gold.ml_model_runs | 8 |
| gold.ml_opportunity_scores | 123266 |
| public.alembic_version | 1 |
| repositorio.market_web_listings_raw | 33385 |
| repositorio.mercado_livre_items_raw | 16040 |
| repositorio.mercado_livre_products_raw | 59360 |
| repositorio.pipeline_runs_raw | 3649 |
| repositorio.price_history_raw | 9313 |
| repositorio.source_runs_raw | 3830 |
| repositorio.supplier_products_raw | 77675 |
| silver.mercado_livre_product_prices | 4272 |
| silver.supplier_products_normalized | 226643 |

## 4. Ciclos e escopo

| Ciclo | Escopo | Estado | Início | Fim | Commits |
|---|---|---|---|---|---|
| 0 | Fundação Docker, backup/restauração, runner e ensaio sintético | concluído | 2026-10-13 | 2026-10-13 | 88c0186, bd12387, 9f49a33, 50eeaeb |
| 1 | TEC-02, persistência do EP-01: esquema, modelos, repositório/auditoria e ensaio real | concluído | 2026-10-13 | 2026-10-13 | 2a7153e; commit de encerramento informado no relatório |

Escopo técnico fixado antes de codificar, reproduzido ao final. HU-001/003/004 não são declaradas funcionalmente completas. Contratos PNCP, DV fiscal, documentos/evidências, invalidação de análises e operação são incrementos dependentes posteriores. Não criar procurement_lots redundante; judgment_units representa item/lote/grupo/global. Nenhuma coleta real prevista neste ciclo.

## 5. Checklist e evidências

- [x] C1-01, TEC-02 (RT-001/014/015; RF-003/004 parciais): nove tabelas novas, modelos de seis tabelas procurement, FKs RESTRICT, ponteiro/composição com pertencimento, gatilhos de imutabilidade e valores NUMERIC. Commit 2a7153e.
- [x] C1-02 (RN-026/028; RT-001): repositório transacional com savepoint, lock do pai, hash de conteúdo/manifesto, replay, observação append-only, retorno v1→v2→v1, NULL/Decimal, origem Bronze validada e guarda de falha atômica. Mesmo commit coerente 2a7153e.
- [x] C1-03 (RT-014; RNF-014/017): ensaio 20261013T223425Z_34e76a5d aprovado; contagens e estruturas das 36 tabelas legadas iguais em todas as etapas; nove tabelas novas, 19 FKs RESTRICT, oito triggers e quatro funções conferidos; downgrade removeu objetos novos, segundo upgrade reproduziu estrutura; cópia removida e cleanup_errors vazio.
- Testes primeiro: 20261013T165828Z_1d2b40fe.json falhou com ModuleNotFoundError do módulo novo, esperado antes da implementação; banco/papel removidos. Não é falha preexistente.
- Rodada intermediária 20261013T170147Z_5621e347: 89 aprovados, 1 warning. Rodada ampliada 20261013T170515Z_0ef2d0bc: 95 aprovados, 10,69 s.
- Suíte completa final 20261013T223714Z_a4938bf7: **95 aprovados em 19,40 s**, 0 falhas, 0 erros, 0 ignorados, 1 warning preexistente. Todos os 70 casos anteriores e 25 novos executados; nenhum teste legado editado. Papel de teste sem privilégios nas tabelas principais, cleanup_errors vazio.
- Configurações Compose base/aditiva e git diff --check passaram. docker compose ps -a final confirmou todos os healthchecks existentes saudáveis; ausência de healthcheck nos workers/prefect-services registrada. Consulta final via docker compose exec -T postgres / psql: principal ainda 20260817_0009; zero schemas procurement/commercial no principal; zero bancos rehearsal_/test_ e zero papéis radar_test_ presentes. Apenas leitura de inventário, sem exclusão por prefixo.

Rastreabilidade detalhada em [17-rastreabilidade-ciclo-1.md](17-rastreabilidade-ciclo-1.md). Testes e ensaio executados somente em Docker/Compose. A suíte usa banco sintético separado da cópia real; zero chamadas a fontes externas.

## 6. Migrações

Revisão nova 20261013_0010 sobre 20260817_0009. Revisão manual: upgrade cria schemas/tabelas/índices/checks/FKs/triggers/funções novos; ALTER somente para FK em tabela criada pela revisão. Não altera linhas ou objetos legados. lock_timeout de 5 segundos limita espera DDL. FK para pipeline_runs pode adquirir lock no pai; nenhum corte principal autorizado. Downgrade remove somente objetos desta revisão em cópia registrada pelo runner, sem CASCADE e sem recuar a revisão legada.

Ensaio executado e aprovado: 20260817_0009→20261013_0010→20260817_0009→20261013_0010. Verificador específico confere nove tabelas, oito gatilhos, quatro funções, FKs RESTRICT, fingerprint estrutural novo idêntico nos upgrades e ausência após downgrade. Compara contagens e hashes de colunas/constraints/índices legados em todas as etapas. Cópia não recebe fixtures.

Aplicação principal: **não; pendente**. Não há janela autorizada de suspensão de escritas. Mesmo após aprovação do ensaio, esta implementação permanece disponível somente em ambiente isolado. Limpeza valida nome exato, registro próprio de criação, OID e servidor; prefixo sozinho não autoriza remoção.

## 7. Coletas reais

Sessão C1: **0 consumidas / 50 restantes**. Sem validação HTTP de contrato, download, cursor, coleta ou agendamento novo. Janela configurada não é execução; campo de data e semântica dos limites serão fixados antes de coleta futura. A continuação mantém o mesmo orçamento.

## 8. Desvios e falhas

[ADR-022](19-adr-022-persistencia-publica.md): tabela de observações exigida conceitualmente por 08; version_id adicional na associação para FKs compostas; implantação incremental com evidence_id junto ao futuro módulo de evidências; guarda de migração principal por causa do bootstrap legado; ensaio real com verificador específico. Adendos preservam documentação alheia conforme ADR-021.

Falhas preexistentes: nenhuma na suíte; somente warning de depreciação já conhecido. Erro inicial de importação foi a etapa vermelha TDD. Nenhuma autorização ampliada, nenhum push, nenhuma ativação operacional.

## 9. Limitações e bloqueios

Nenhum bloqueio técnico aberto no escopo do Ciclo 1. CNPJ é texto original com validação pendente: não há DV nem identidade fiscal confirmada. Não há adapter/endpoint/flow PNCP, UI, políticas comerciais ativadas, validação de manifestos de API, lease/fencing operacional ou análise/invalidação comercial. Campos preparatórios não demonstram esses comportamentos. JSON e manifesto são contrato interno sintético.

Modelos não são autoridade para create_all; o DDL pertence à revisão explícita. Locks de pai implementados, concorrência sob carga/desempenho não medidos. Contagens e hashes estruturais não provam igualdade de cada valor legado; RPO operacional, recuperação de documentos/Prefect e segundo host não validados. Limitações do audit hook e CONNECT herdado de PUBLIC do C0 permanecem.

## 10. Notas para retomada/próximo ciclo

1. Ciclo 1 concluído no escopo técnico; não repetir fundação ou ensaio sem nova necessidade.
2. Próxima sessão deve registrar o hash de encerramento informado no relatório, sem tentar autorreferência no commit.
3. Não iniciar outro ciclo nesta sessão, inclusive após a mensagem continue.
4. Próxima execução explícita: contrato/adapter PNCP conforme roadmap; categoria continua pendente quando necessária.
5. Usar revisão explícita 20261013_0010 nos testes atuais; histórico C0 conserva comandos antigos.
6. Novas migrações exigem backup e verificador próprios; não presumir liberação operacional.
7. Principal, schedules e push continuam desabilitados.

## Plano original da sessão

## Sessão C1 — 2026-10-13: plano registrado antes de codificar

Base 50eeaebaecd0125b4bc472f35add0fc6bcdc5833 (encerramento C0), branch feat/radar-licita-implementacao. Somente documentos preexistentes não rastreados; nenhum diff rastreado inicial. Originais permanecem fora dos commits conforme ADR-021.

Parâmetros efetivos: CICLO_ALVO automático seleciona 1; categoria vazia; janela últimos 7 dias (2026-10-07 a 2026-10-13, America/Sao_Paulo), sem coleta neste escopo e sem contrato de campo/limites presumido. Orçamento 50, intervalo mínimo 1 segundo, principal não, agendamentos não, push não. Valores válidos, permissões normalizadas para não.

Escopo fixado: TEC-02, incremento de persistência do EP-01 (RT-001/014/015, RF-003/004 parciais, RN-026/028). Três unidades coerentes: C1-01 migração aditiva e modelos de identidade/versões/itens/unidades e seus pré-requisitos mínimos de Bronze/jobs/política; C1-02 repositório transacional e observações auditáveis com replay, retorno ao conteúdo antigo e vínculos de mesma versão; C1-03 ensaio da revisão real, restauração, preservação e encerramento. Não selecionar HU-001 completa antes do contrato; nenhuma HU funcional integral será declarada concluída neste ciclo técnico. Esquema de documentos, fornecedores e operação comercial ficam em incrementos próprios; não criar lotes redundantes.

Aceites: uma identidade por source/source_id; desconhecidos NULL; versões publicadas imutáveis e conteúdo deduplicado; observação preservada no retorno a conteúdo anterior; pointer somente para versão do mesmo pai; composição sem versões cruzadas; Decimal exato; FK RESTRICT; migração aditiva com upgrade/downgrade/upgrade só em cópia; inventário estrutural e contagens legadas iguais; testes sintéticos sem rede externa e sem principal; limpeza confirmada. CNPJ original textual sem validação fiscal presumida, identidade fiscal fica pendente até contrato/algoritmo oficial. Nenhum resultado PNCP nem análise comercial será fabricado.

Leituras: _implementacao integral; roadmap/ADRs; 04/07/08 (incluindo R1 e integridade)/14/16/17/21 e RT em 00-trd; Compose, init Prefect, Alembic, runner e testes legados. C0 já confirma 70 testes; reconferência iniciada nesta sessão. Docker 29.8.2 / Compose 5.5.1 / daemon respondem; config base e aditiva código 0. Serviços previamente ativos com healthchecks existentes saudáveis; workers/prefect-services sem healthcheck. prefect-init encerrado 0; não iniciar (executa upgrade head). Nenhum serviço iniciado/parado.

Migração prevista: descendente explícita de 20260817_0009; confirmar revisão principal e ausência dos objetos antes de ensaiar. Banco sintético descartável para testes; cópia restaurada separada somente para verificações de leitura e migrações. Backup fresco obrigatório; aplicação principal pendente por permissão não. Estado C1 em andamento até todos os aceites e commits.


## Evidências finais C1

Rehearsal rehearsal_20261013_d23d08eedca5 removido pelo registro próprio; código 0. Manifesto registra todos os passos e hash de cada revisão. SHA256 da revisão nova reconfirmado após ensaio: 5712c06b99a571ed0e0d0f137d6d0aff118f902a581f2a907b3ea9fe441ddefc. Código no commit 2a7153e, sem alteração posterior de runtime. Dump preservado com hash inalterado. Suíte final 95/95 e limpeza de test_20261013_64b847b5aa93/radar_test_09a895ee497c confirmadas. Nenhuma operação de escrita no principal além de catálogo para criar/remover bancos/papéis temporários próprios no servidor; nenhuma linha operacional alterada pela execução.

Documentação/rastreabilidade encerradas antes do commit final, com histórico C0 preservado integralmente. O hash de encerramento pertence ao relatório e poderá ser registrado na próxima sessão. Encerrar aqui: próximo ciclo somente sob nova execução explícita.
