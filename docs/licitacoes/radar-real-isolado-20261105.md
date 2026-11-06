# Radar Licita — descoberta real em cópia isolada (05/11/2026)

## Resultado e limites

`http://127.0.0.1:8011/radar-demo` mostra busca por texto do objeto e filtros exatos de UF, modalidade e situação bruta; até 20 identidades por página, em ordem determinística, limitadas a 500 páginas. O detalhe mostra identidade PNCP, objeto, UF, modalidade, situação e prazo **brutos**, versão, ID da versão, ID Bronze e SHA-256 do dump. A abertura é **não verificada**, o frescor é **desconhecido** e a cobertura é **parcial**. Há telas de resultado vazio, registro ausente e cópia indisponível. `/radar-demo/sintetico` preserva a demonstração inventada, inclusive comparação/viabilidade/jornada comercial, sem confundi-la com os dados reais.

O arquivo do dump terminou de ser escrito em **05/11/2026 16:01:34 America/Sao_Paulo** (`2026-11-05T19:01:34.2701742Z`, mtime). O instante transacional exato do `pg_dump` não consta do manifesto; a tela explicita essa limitação. A origem é `test_20261020_ba894ce71405`, nunca o banco principal. Dump no checkout operacional: `backups/c2/c2-20261013/history-20261105-pre-resume-current-0012.dump`, 1.302.033.006 bytes, SHA-256 `1c0d1fdd7970eec7deb1640ca7df54cf95f3983db86894f1b7c7eefe7499f424`. O relatório `backups/c2/c2-20261013/history-20261105-pre-resume-restore-report.json`, também no checkout operacional, compara contagens e fingerprints com a origem; o SHA-256 local foi recalculado e coincidiu.

## Gates na cópia

| Gate | Evidência nesta execução |
| --- | --- |
| G0 — inventário | Contêiner novo `radar-preview-pg-20261105`, imagem `pgvector/pgvector:pg15`, volume próprio `radar_preview_pg_20261105`, porta `127.0.0.1:55434`, DB `radar_preview_20261105`. Revisão `20261027_0012`, extensões `pgcrypto`, `plpgsql`, `postgres_fdw`, `vector`; schemas `bronze`, `control`, `procurement`; 25 PK, 42 FK, 20 unique e 66 check. Snapshot tem 426 jobs (365 `success`, 61 `partial`), sem worker/schedule ligado à cópia. |
| G1 — recuperação | `pg_restore --exit-on-error` terminou com código 0 na instância nova. Contagens iguais ao manifesto: 340.985 identidades, 340.992 versões, 647.170 observações, 13.370 respostas Bronze. Identidades duplicadas, versões órfãs, vínculos Bronze ausentes e ponteiros atuais sem identidade correspondente: zero. O backup já tinha ensaio integral verificado. RPO/RTO operacional não foi medido nesta prévia e requer ensaio próprio antes de corte. |
| G2 — expansão | `scripts/radar_preview_g2.sql` criou somente na cópia o schema aditivo `radar_preview`, PK/constraints, índice GIN `pg_trgm` para objeto e índices B-tree para filtros. Lock transacional, `lock_timeout` e destino explícito; nenhum índice no C2 operacional. Leitura das tabelas legadas continua disponível. |
| G3 — backfill | `scripts/radar_preview_g3.sql` reconciliou 340.978 identidades projetadas. **Sete** têm versão 2 armazenada mas ponteiro atual ainda na versão 1; foram excluídas, inclusive uma versão 2 `Revogada` e uma `Suspensa`. A consulta repete a proteção contra versão posterior, mesmo se a projeção ficar defasada. Após restauração do ensaio de retificação, nova execução produziu `DELETE 0`, `INSERT 0`; 340.978 projetadas e sete em quarentena. Nenhuma identidade foi silenciosamente substituída. |
| G4 — revisão da prévia | Testes locais passaram, capturas e plano de consulta registrados abaixo. A aprovação de release operacional, rollback de imagem e medição de RPO/RTO continuam pendentes; esta prévia não autoriza G5–G6. |

O diagnóstico agregado anterior não detectava o atraso do ponteiro em relação à **maior versão persistida**: conferia o vínculo e a identidade do ponteiro. Essa nova condição é uma pendência de reconciliação da coleta, não foi corrigida no C2 operacional.

## Desempenho e testes

Na cópia, a busca real `papel` + `SP`, com `LIMIT 21` e validação de ponteiro, teve **47,948 ms** de execução e 4,655 ms de planejamento em `EXPLAIN (ANALYZE, BUFFERS)`, cache aquecido. O plano usou `Bitmap Index Scan` em `discovery_object_trgm_idx` e `Bitmap Heap Scan`; custo estimado do `Limit` 232,79–232,80, 7.939 blocos compartilhados em cache no plano. A busca textual diagnóstica no C2 tinha ultrapassado 5 s; a interface não executa aquela consulta operacional. O banco impõe 2,5 s por consulta, conexão em 3 s, pool limitado e resultados máximos; esta medição não equivale a teste de carga.

Correção após teste no navegador: a página inicial sem filtros retornava 503 porque o planejador juntava as 340.978 linhas antes de aplicar o `LIMIT`; `EXPLAIN ANALYZE` mediu **5.424 ms**. A consulta agora materializa primeiro os 21 IDs paginados e só então valida ponteiro/versão e carrega os snapshots. O novo plano leu 21 entradas da PK e executou em **2,831 ms** (planejamento 4,466 ms), com 258 blocos compartilhados em cache. A rota sem filtros respondeu HTTP 200 em três chamadas repetidas e o navegador exibiu resultados e detalhe. Um teste de regressão exige HTTP 200 na página inicial.

No contêiner de teste, a suíte focada terminou com código 0: **14 testes passaram e um ensaio opcional ficou pulado** na execução normal. Cobrem busca real, UF/modalidade/situação, paginação sem duplicações, detalhe, vazio, 404, versão e identidade, estados desconhecidos, bloqueio de destinos operacionais, conta sem escrita, erro de conexão e indisponibilidade; incluem os testes anteriores do contrato e do modo sintético. O ensaio opcional de retificação passou separadamente: somente nesta cópia, o ponteiro de uma identidade foi temporariamente movido para a versão 2 `Revogada`, o detalhe exibiu versão 2 sem marcar abertura, e o ponteiro/projeção foram restaurados em seguida. A conta `radar_reader` tem `default_transaction_read_only=on`, `INSERT=false`, e só recebeu `SELECT` das tabelas necessárias. A aplicação dedicada inclui apenas os dois routers de prévia. Esta execução fez **zero chamadas PNCP e zero comandos de escrita no C2 operacional ou no banco principal**; escritas ocorreram apenas no volume novo da cópia.

Capturas verificáveis, sem credenciais ou dados pessoais aparentes: [página inicial](evidence/radar-real-inicio.png), [busca](evidence/radar-real-busca.png) e [detalhe](evidence/radar-real-detalhe.png). `scripts/radar_preview_capture.py` reproduz a navegação e as capturas.

## Início reproduzível

Executar no checkout **`motor_decisao_produto`**, com Docker disponível e o dump acima verificado. Os nomes/portas abaixo são exclusivos desta prévia. Guardar senhas em arquivo local ignorado pelo Git; nunca usar credenciais do C2 ou do banco principal.

```powershell
$dump = 'C:\Users\luisg\OneDrive\Documentos\ChatGPT\motor publico\motor_decisao\backups\c2\c2-20261013\history-20261105-pre-resume-current-0012.dump'
(Get-FileHash -LiteralPath $dump -Algorithm SHA256).Hash # Deve ser 1C0D1FDD7970EEC7DEB1640CA7DF54CF95F3983DB86894F1B7C7EEFE7499F424
$adminPassword = [guid]::NewGuid().ToString('N')
docker run -d --name radar-preview-pg-20261105 -e "POSTGRES_PASSWORD=$adminPassword" -p 127.0.0.1:55434:5432 -v radar_preview_pg_20261105:/var/lib/postgresql/data pgvector/pgvector:pg15
docker exec radar-preview-pg-20261105 createdb -U postgres radar_preview_20261105
docker cp $dump radar-preview-pg-20261105:/tmp/c2.dump
docker exec radar-preview-pg-20261105 pg_restore -U postgres -d radar_preview_20261105 --no-owner --no-privileges --exit-on-error /tmp/c2.dump
docker exec radar-preview-pg-20261105 psql -U postgres -d radar_preview_20261105 -Atc "SELECT version_num FROM alembic_version; SELECT count(*) FROM procurement.procurements; SELECT count(*) FROM procurement.procurement_versions; SELECT count(*) FROM procurement.version_observations; SELECT count(*) FROM bronze.pncp_responses;"
# Esperado: 20261027_0012, 340985, 340992, 647170, 13370. Conferir vínculos como em G0/G1 acima antes das linhas seguintes.
docker cp scripts/radar_preview_g2.sql radar-preview-pg-20261105:/tmp/g2.sql
docker exec radar-preview-pg-20261105 psql -U postgres -d radar_preview_20261105 -v ON_ERROR_STOP=1 -f /tmp/g2.sql
docker cp scripts/radar_preview_g3.sql radar-preview-pg-20261105:/tmp/g3.sql
docker exec radar-preview-pg-20261105 psql -U postgres -d radar_preview_20261105 -v ON_ERROR_STOP=1 -f /tmp/g3.sql
docker network create radar-preview-net-20261105
docker network create --internal radar-preview-internal-20261105
docker network connect radar-preview-internal-20261105 radar-preview-pg-20261105
$readerPassword = [guid]::NewGuid().ToString('N')
$sql = "CREATE ROLE radar_reader LOGIN PASSWORD '$readerPassword' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION; ALTER ROLE radar_reader SET default_transaction_read_only = on; GRANT CONNECT ON DATABASE radar_preview_20261105 TO radar_reader; GRANT USAGE ON SCHEMA radar_preview, procurement TO radar_reader; GRANT SELECT ON radar_preview.discovery_current, procurement.procurements, procurement.procurement_versions TO radar_reader;"
docker exec radar-preview-pg-20261105 psql -U postgres -d radar_preview_20261105 -v ON_ERROR_STOP=1 -c $sql
Set-Content -LiteralPath .radar-preview.env -NoNewline -Value "RADAR_PREVIEW_DATABASE_URL=postgresql+psycopg://radar_reader:$readerPassword@radar-preview-pg-20261105:5432/radar_preview_20261105"
docker run -d --name radar-preview-app-20261105 --network radar-preview-net-20261105 --env-file .radar-preview.env -v "${PWD}:/app:ro" -w /app -p 127.0.0.1:8011:8000 motor_decisao-api:latest uvicorn app.radar_preview_app:app --host 0.0.0.0 --port 8000
docker network connect radar-preview-internal-20261105 radar-preview-app-20261105
# Abra http://127.0.0.1:8011/radar-demo
# Testes: docker run --rm --network radar-preview-internal-20261105 --env-file .radar-preview.env -v "${PWD}:/app" -w /app motor_decisao-api:latest pytest -q tests/test_radar_demo.py tests/test_radar_c2_contract.py tests/test_radar_c2_preview.py
```

## Próximas liberações

1. **Ligação operacional:** reconciliar os sete ponteiros e testar revogação/anulação no C2; definir captura/atualização consistente, monitoração por recorte, credenciais e rede de leitura, revisão de release/rollback e G5–G6 em janela própria. S08 segue parcial.
2. **Abertura, prazo e alerta:** validar semântica/fuso de `dataEncerramentoProposta`, `source_updated_at`, sync e situação oficial por recurso. Hoje `deadline_at` e `source_updated_at` confiáveis são zero; não há filtro temporal nem alerta real.
3. **Comparação histórica:** persistir e validar itens, itens de julgamento, resultados, unidade, etapa, equivalência e proveniência por recurso. As tabelas de itens/julgamento têm zero registros nesta cópia; preço histórico não é cotação atual.
4. **Viabilidade e jornada comercial:** validar exigências, fornecimento e cotação atuais, custos, logística, capital, revisão humana e atos externos. Essas telas continuam declaradamente sintéticas; nenhuma recomendação positiva ou ato comercial deriva desta descoberta.

Esta entrega é uma **prévia local de descoberta**, não MVP, aceite S08 nem recomendação de participar de licitação.
