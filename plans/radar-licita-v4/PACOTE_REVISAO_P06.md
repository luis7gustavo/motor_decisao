---
id: P06-revisao
tipo: pacote_revisao
estado: pendente
atualizado_em: 2026-11-19
---

# P06 — pacote para revisão independente

## Escopo e base

- Branch local: `codex/radar-p06-catalog-20261119`, worktree exclusivo `motor_decisao_p06_20261119`, base integrada P31 `e0eaa0b3b8c6034cf08dd4d18f8354efcbdc186f`.
- Commit da guarda examinado pela sessão autora: `e113c3f` (revisão independente ainda pendente).
- Entrega: auditoria offline dos catálogos, ensaio isolado de importação, exigência de data de origem para MegaMix e guarda por hash para impedir atribuir outra data aos mesmos bytes.
- Fontes rastreadas: `data/megamix_catalog_raw.json` (SHA-256 `b6dd728fa89027f85828f075a0d7f19aaed81ed957479d548ee9be6d200e3a40`) e `data/coletek_catalog_raw.json` (SHA-256 `83b0f0aa9634ec61654bf88064a1a4c427c6fe6af5eeabe12398bfd809077b65`). A data de origem MegaMix permanece desconhecida.

## Prova observada

- O auditor reconciliou 2.360 MegaMix e 735 Coletek, 3.095 IDs únicos; 111 Coletek sem preço. O ensaio anterior em banco descartável carregou 3.095 linhas e zero na repetição no mesmo dia, antes da guarda nova.
- Para a guarda nova, migração até `20261013_0010` em banco `test_20261119_cdef12345678` descartável passou. Com fixture E2 de dois registros: primeira importação 2, repetição 0, tentativa dos mesmos bytes com outra data rejeitada. Consulta Bronze: 2 linhas com data 01/06/2026. Dois testes unitários passaram. Nenhuma carga do JSON MegaMix real foi feita com a guarda nova.
- Os três testes do auditor e os dois da guarda passaram; o lint documental `--no-git` e `git diff --cached --check` passaram. O CLI do motor recusou `--import-megamix` sem data antes de consultar qualquer banco.
- Após o checkpoint inicial, foi acrescentado bloqueio transacional PostgreSQL por hash antes de consultar execuções, incluindo execuções em andamento. Nova migração descartável e teste E2 confirmaram carga 2/2 e rejeição de outra data. Concorrência simultânea de dois processos não foi ensaiada e segue para revisão.
- Banco, contêiner e rede de teste foram descartados após a consulta. O checkout do produto principal, worker, coleta, banco operacional e remoto não foram modificados.

## Revisão solicitada

Verificar o diff contra `e0eaa0b`, a identidade de snapshot e seus casos negativos, o comportamento de chamadas existentes que importam MegaMix, e os limites de duplicatas legadas ou concorrência. Conferir `git diff --check`, testes focados e lint documental. Registrar achados em sessão independente; esta sessão autora não aprova a própria entrega.

## Limites e decisão

P06 permanece **parcial**. A data real do catálogo MegaMix precisa de evidência humana; não inferir a data do arquivo, da carga ou do ensaio. A guarda nova não retroage às execuções sem SHA-256 em metadados. G2, revisão independente, integração e qualquer uso comercial permanecem pendentes. O plano 1.0 vincula P04 a P02, enquanto o controle V4 o vincula a P31; P04 não foi iniciado nesta execução e a divergência deve ser resolvida antes de sua autoria.

## Correção após parecer independente de 19/11/2026

O [parecer](REVISAO_INDEPENDENTE_P06_20261119.md) encontrou duas entradas que ainda importavam MegaMix sem data. A rota `POST /decision-engine/run` agora deixa essa importação desligada por padrão e responde 422 antes do banco quando `import_megamix=true` sem `megamix_snapshot_date`. Com data informada, ela a passa ao importador. `collect_all.py` mantém a importação Coletek e só acrescenta MegaMix quando recebe `--megamix-snapshot-date`. README e guia local foram corrigidos para não instruir importação sem data; essa data ainda exige comprovação humana da origem.

Na revisão local da autoria, 8 testes passaram: cinco testes anteriores de catálogo/importação e três novos de entrada, incluindo HTTP 422, chamada datada e comandos do estágio. O teste HTTP usou `TestClient` com importador e motor simulados; não houve banco, coleta, PNCP nem importação real. O lint documental e o diff ainda devem ser conferidos no checkpoint. O parecer anterior segue preservado; nova sessão independente deve revisar o diff corrigido. G2 e integração continuam pendentes.

## Ensaio de concorrência E2 posterior ao reexame

Em 19/11, `tests/test_p06_concurrency_integration.py` passou (1/1) em `pgvector/pgvector:pg15`, banco descartável `test_20261119_eed039d0c27f`, migração `20261013_0010`. Duas importações em processos distintos iniciaram juntas sobre a mesma fixture de dois itens, uma com data `2026-06-01` e outra `2026-06-02`. Uma carregou 2 itens; a outra recebeu `ValueError` por tentar atribuir outra data aos mesmos bytes. A consulta final confirmou 2 linhas `bronze.supplier_products_raw` e 1 `control.source_runs` MegaMix. A transação do primeiro processo foi mantida aberta por 1,5 s no teste para garantir sobreposição. O contêiner e a rede exclusivos foram removidos. O preparo inicial usou um nome de banco recusado pela guarda da migração; a repetição com nome permitido passou.

O teste novo exige `RADAR_P06_INTEGRATION=1` e confere que a URL aponta para banco `test_...` em host `radar-p06-pg-...`; é ignorado na suíte comum. Solicita-se revisão independente do teste e deste resultado. O ensaio não comprova data real MegaMix nem cobre execuções legadas sem hash. G2, publicação e integração seguem pendentes.
