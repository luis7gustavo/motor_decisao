# Repasse S08 — auditoria do marco histórico e disparo horário, 03/11/2026

## Objetivo e escopo

Continuar a S08 após a ampliação auditada do orçamento e a agenda horária. Separar a carga até o marco fixo de 26/10/2026 dos ciclos posteriores, conferir o ledger e avançar no diagnóstico dos quatro recursos de enriquecimento. Por orientação do usuário, a coleta agendada segue sem intervenção e esta sessão não espera seu disparo das 15h. Nenhuma ampliação de orçamento, migração, reset ou chamada manual PNCP foi feita nesta sessão.

## Verificação antes do disparo

- `git status --short` confirmou mudanças preexistentes extensas e arquivos ainda não rastreados; nenhuma delas foi limpa. `backups/pncp-control/exclusive.lock` permanece com dono `pncp-history-2026-10-27`.
- `docker info`: servidor 29.8.2. `docker compose ... config --quiet`: exit 0. PostgreSQL, Redis, servidor e serviços Prefect dedicados estavam em execução; o worker `pncp-history-dedicated-pc1` estava `ONLINE`.
- `radar_pncp_history plan`: `ready=true`, zero gates, zero rede/escrita, contrato do marco 2019-01-01 a 2026-10-26. `target_store()` e `radar_pncp_live_validation verify` confirmaram C2 `test_20261020_ba894ce71405`, OID 2508600, revisão `20261027_0012`, principal protegido na revisão `20260817_0009` sem schema `procurement`; C2 preservado `partial/SourceChanged`, página 2, `evidence_integrity_valid=true`, `coverage_verified=false`.
- `radar_pncp_schedule status` às 14h38: deployment `9183361f-fe0e-4a10-8469-d348f765fdb7` não pausado, três agendas ativas com `parameters.mode=backfill/sync/reconcile`, concorrência 1; run horário `01a1a26b-39df-7567-85d4-9263b684ea04` agendado para 15h locais.
- Consulta SQL somente leitura por `target_store()`, sob identidade validada, às 14h42: missão 2.005/20.000, fonte 2.104/20.099, cinco reservas no dia civil local de São Paulo, zero tentativas sem outcome. Outcomes da missão: HTTP 200 = 1.905, HTTP 204 = 39, HTTP 500 = 53, HTTP 504 = 6, erro de transporte = 2 (total 2.005). Todas as cinco tentativas novas tinham outcome e Bronze associados.

## Ajuste de observabilidade da S08

- `scripts/radar_pncp_history.py`: `audit_summary` agora apresenta separadamente partições históricas (`pass_no < 100000`) e recorrentes; `audit --summary-only` omite as linhas individuais sem perder contadores/lacunas.
- `app/procurement/history.py`: `status` expõe `historical_partitions` e `recurring_partitions`, preservando `partitions` agregado. `pipelines/procurement/history_flow.py` usa auditoria resumida no run e rotula o marco histórico e o agregado separadamente no artefato de negócio.
- `tests/test_pncp_history_audit.py` cobre a separação e a chamada resumida. Runbook em `docs/licitacoes/operacao-coleta-historica-prefect.md` inclui o comando operacional.
- Suíte Docker `radar-ops tests --origin 20261027_0012`: exit 0, **214 passed, 1 warning**, manifesto `backups/20261103T174428Z_dde5a847.json`, `operation_passed=true`, `pytest_exit_code=0`, `cleanup_errors=[]`. `audit --summary-only` e `status` reais saíram com exit 0.

## Cobertura antes do disparo

Auditoria somente leitura às 14h45: estado `partial` para o marco e para o agregado. **Marco histórico: 436 partições, 97 completas, 317 pendentes, 22 em reconciliação.** Recorrência: 304 partições, 11 completas, 293 pendentes. Agregado: 740, sendo 108 completas, 610 pendentes, 22 em reconciliação. As quatro lacunas de detalhe da contratação, itens, resultados e documentos continuam pendentes de validação e dimensionamento.

As partições históricas observadas somam 90.678 ocorrências de identidade e 57.289 identidades oficiais distintas entre publicação e atualização; isso é apenas o conjunto já observado, pois há partições pendentes e a enumeração da API é mutável. A fila `control.source_queue` está vazia. Se cada identidade já observada exigir ao menos uma chamada de detalhe, essa etapa sozinha excede as 17.995 tentativas restantes da missão; validar o contrato, a estratégia de lote e o orçamento antes de habilitar enriquecimento. Não tratar essa estimativa condicional como quantidade final de requisições.

## Etapa seguinte: diagnóstico dos recursos

`scripts/radar_pncp_s08_resource_plan.py` é um diagnóstico repetível, somente leitura, que usa `target_store()` para validar o destino segregado, confere bytes/SHA-256 dos OpenAPI de Consulta e Integração contra os recibos arquivados e verifica GET, variáveis de caminho e resposta 200 para detalhe, itens, resultados por item, lista de documentos e download. Sua transação SQL é `REPEATABLE READ READ ONLY`; não cria filas nem chama a PNCP. Teste novo em `tests/test_pncp_s08_resource_plan.py`. O runbook contém o comando Compose.

Execução real às 14h55, exit 0: 57.289 identidades históricas distintas já observadas, todas com projeção e versão atual, e todas candidatas a construir a rota de detalhe pelo CNPJ, ano e sequencial do snapshot. Há zero itens em fila de enriquecimento, cinco tentativas compartilhadas no dia e 17.995 tentativas cumulativas restantes como **teto superior**. Uma chamada de detalhe por identidade já observada excederia o saldo; o universo final ainda não foi enumerado. Os quatro gates continuam pendentes: fila/dimensionamento de detalhe, paginação de itens, validação/semeadura de resultados e política de armazenamento/download de documentos. A rota em OpenAPI e o exemplo pontual da S06 não validam a coleta integral desses recursos.

A primeira suíte após o comando novo falhou em uma fixture de teste que montava incorretamente o nome de parâmetro com `}`; a fixture foi corrigida sem alterar a lógica de produção. Nova suíte Docker `radar-ops tests --origin 20261027_0012`: exit 0, **215 passed, 1 warning**, manifesto `backups/20261103T175400Z_d5a910a8.json`, `operation_passed=true`, `pytest_exit_code=0`, `cleanup_errors=[]`. O comando `--help` e o diagnóstico real saíram com exit 0. A falha intermediária está preservada no manifesto `backups/20261103T175303Z_ba05deed.json`.

## Agenda em curso e próximos passos

O run automático `01a1a26b-39df-7567-85d4-9263b684ea04` permanecia agendado para 15h locais na última leitura do Prefect às 14h38; ele não foi apresentado como observado nesta sessão. A próxima retomada deve consultar estado/logs de negócio, ledger, Bronze e partições, sem interromper o agendamento. Para enriquecimento, projetar fila paginada a partir das identidades observadas e validar cada recurso em amostra limitada antes de habilitar o gate; registrar orçamento suficiente e reserva para backfill/sync. Manter S08 sem aceite enquanto houver partições/recursos recuperáveis pendentes. O teto diário e cumulativo é finito e não deve ser renovado por restart. Em falha de controle/fonte, pausar primeiro o deployment dedicado e depois a fonte, preservando C2, Bronze, ledger e a trava exclusiva; não tocar no worker/Redis compartilhados.
