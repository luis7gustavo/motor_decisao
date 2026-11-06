# S06 — resultado das sondas PNCP de 28/10/2026

**Estado final:** ensaio limitado encerrado; controle persistente pausado. O usuário autorizou até 12 tentativas reais seguindo `S06_SONDAS_PROPOSTAS.md` e, após o 301, autorizou retomar com o mesmo saldo pela rota de Consulta documentada. Foram consumidas **11/12**; a última tentativa ficou sem uso. A carga histórica, a missão principal e a agenda continuam desabilitadas.

## Execução

Executor: `docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_s06_probe ...`. `target_store()` confirmou antes de cada execução servidor, banco e role segregados, OID 2508600, revisão `20261027_0012` e ausência de schema `procurement` no principal. Cada requisição teve reserva atômica no `control.source_attempts` antes da conexão. Não houve retry nem redirecionamento automático.

| Nº | Operação | HTTP | Resultado observado | Recibo em `backups/pncp-probe/20261028/` |
|---:|---|---:|---|---|
| 1 | Catálogo `/api/pncp/v1/modalidades`, sem filtro | 200 | 19 códigos, 1–19, todos `statusAtivo=true` na resposta; 7.199 bytes | `c74619b5-6884-414a-81c7-e4d1e08052f7.json`, SHA-256 do corpo `f5fbd2c3abcb6b5a940bcfa6997052f0a6721b91a60dd8d634c8aeec240ccfdc` |
| 2 | Publicação, modalidade 6, 13/10/2026, página 1 de 10 | 200 | 10 registros nesta página; resposta declarou 1.495 registros/150 páginas naquele instante; 21.963 bytes | `235fc310-0dd6-4689-a289-e3d3c05b6d0c.json`, SHA-256 `e22119cb6d2214c7404261e67de71d2a85d7a09284ecb2f06969d8485094b58b` |
| 3 | Atualização global, mesmos filtros | 200 | 10 registros nesta página; resposta declarou 3.160 registros/316 páginas naquele instante; 20.952 bytes | `02c15839-f03a-43d8-bf47-a300c9d33bdf.json`, SHA-256 `cc873b452b8b058beaa2f127f9c713aa79f36741c5a193097d5df529874bd5a6` |
| 4 | Detalhe de uma identidade retornada pela tentativa 2, na base `/api/pncp` documentada pela integração | 301 | Corpo JSON informou mudança para `/api/consulta/...`, mas apontou `pncp.gov.br:55969`, porta fora da allowlist. Não houve cabeçalho `Location` nem segunda conexão. 281 bytes | `10e8109d-b171-4eef-8fdf-a4e5848e6b6f.json`, SHA-256 `2f46f0c763bb8a5124587f084bef4e006a4970fbba0fe6e0f19a72b50b0bd13c` |

Os quatro corpos foram persistidos como `.body` antes dos respectivos recibos; SHA-256 de todos os quatro arquivos foi recalculado e conferiu com o recibo. `control.source_attempt_outcomes` contém uma saída por reserva. Após a tentativa 4, `source_budgets.enabled=false`, missão de sonda `paused`, `attempts_consumed=4`, `attempt_limit=12`. O comando `pause` não devolveu tentativas. O estado final após retomada aparece abaixo.

## Interpretação e continuidade

- O catálogo observado serve para planejar sondas, mas não comprova que nenhum código histórico inativo foi retirado da fonte. A política principal registra os códigos 1–19 como `observed_current_unverified_historical`, sem liberar a carga.
- Os totais de uma página são voláteis; não demonstram cobertura, estabilidade de paginação, semântica de bordas nem data inicial recuperável.
- A [API oficial de Consulta](https://pncp.gov.br/api/consulta/swagger-ui/index.html) também anuncia GET de detalhe sob `/api/consulta`, mas ele **não foi chamado** neste ensaio. A [documentação de Integração](https://pncp.gov.br/manual/pt-br/latest/singlehtml/) ainda anuncia a rota sob `/api/pncp`; a divergência deve ser tratada como mudança de fonte e revalidada antes de prosseguir. Não seguir a porta 55969 por inferência.
- Na interrupção inicial, sondas de itens, resultados, documentos, histórico e períodos antigos ainda não tinham sido executadas. A retomada autorizada foi registrada em `S06_SONDAS_RETOMADA_PROPOSTA.md` e executada conforme o complemento abaixo.

Verificação posterior somente leitura por `scripts/radar_pncp_s00_inventory.py`: principal `20260817_0009` sem `procurement`; C2 OID 2508600/revisão `20261027_0012`, 3 jobs, 4 Bronze, 10 identidades/versões/observações. Job C2 `partial/SourceChanged`, página 2; ledger C2 segue 11 reservas, DOCUMENTAÇÃO 7/15, DADOS 4/50. As quatro sondas foram registradas em missão e orçamento próprios no mesmo destino segregado; nenhuma foi atribuída ao C2.

## Retomada autorizada e estado final

Após nova autorização do usuário, a rota de detalhe foi construída sob `/api/consulta` e sua existência foi conferida no OpenAPI arquivado **sem rede**. O mesmo orçamento foi reativado com 4/12 tentativas preservadas. Nenhum pedido foi enviado à porta 55969.

| Nº | Operação | HTTP | Resultado observado | Recibo em `backups/pncp-probe/20261028/` |
|---:|---|---:|---|---|
| 5 | Detalhe na API de Consulta, mesma identidade da tentativa 2 | 200 | Identidade, ano e sequencial conferidos; 2.066 bytes | `f79e2143-fb8d-4fa7-a7ce-950b135cbde2.json` |
| 6 | Itens da contratação, página 1 | 200 | Array de itens recebido; 8.462 bytes | `7d13dea5-3b88-4307-a134-39218aab11f1.json` |
| 7 | Lista de arquivos da contratação, página 1 | 200 | Array com 10 referências nesta página; bytes dos anexos não baixados; 5.200 bytes | `6c27ae77-d93d-491d-8875-05aad7f44a95.json` |
| 8 | Histórico da contratação, página 1 | 200 | Array de eventos recebido; 4.892 bytes | `d0f5fedf-c530-4888-9475-9b8bf66809bc.json` |
| 9 | Publicação, modalidade 6, 01–07/08/2021 | 204 | Sem registros nessa janela específica | `8ac70e25-bfe3-4284-9dd5-344858d3651b.json` |
| 10 | Publicação, modalidade 6, 01–07/10/2021 | 204 | Sem registros nessa janela específica | `ab595793-56e1-4763-8df6-667413195ea1.json` |
| 11 | Publicação, modalidade 6, 01–07/01/2022 | 204 | Sem registros nessa janela específica | `3eb23047-6ecc-47e4-ae1d-029b3b3f7cee.json` |

O detalhe informou `existeResultado=false`; portanto, a consulta de resultados por item foi omitida. O 204 da tentativa 9 pausou inicialmente o script por tratamento excessivamente conservador. Ele foi classificado como resposta vazia válida, sem devolver a tentativa, e o mesmo orçamento foi retomado em 9/12. Após a tentativa 11, o orçamento e a missão foram pausados deliberadamente: **11 reservas, 11 outcomes, 7 HTTP 200, 3 HTTP 204, 1 HTTP 301, limite 12, 1 tentativa não usada**. Os 11 corpos e recibos existem; SHA-256 e tamanho de todos conferem. Nenhum retorno 204 foi interpretado como primeira data da fonte ou cobertura integral.

Novo inventário somente leitura após todas as sondas confirmou principal `20260817_0009` sem `procurement`; C2 OID 2508600/revisão `20261027_0012`, 3 jobs, 4 Bronze e 10 identidades/versões/observações, com ledger C2 ainda em 11 reservas. A missão principal segue `enabled=false`, sem manifesto/partição de carga. As sondas guardam corpos e recibos em arquivos protegidos e outcomes no controle, **não** são Bronze nem registros históricos promovidos. S06 real, S07 e S08 não alcançaram os critérios de saída.
