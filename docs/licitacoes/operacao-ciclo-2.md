# Operação e retomada — Ciclo 2

Estado em 2026-10-20: sonda e coleta mínima executadas em banco isolado. A coleta
segue parcial no checkpoint da página 2. Principal permaneceu em 20260817_0009;
schedules e push continuam não autorizados. Executar aplicação, testes e scripts
somente por Docker. Não iniciar `prefect-init`: seu bootstrap legado executa
`upgrade head`.

## Ledger e testes

```powershell
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps --entrypoint python radar-ops -m scripts.radar_pncp_contract --session c2-20261013
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps radar-ops tests --origin 20261013_0010
```

Ledger: DOCUMENTAÇÃO 7/15, DADOS 3/50, calibração 2/3; permanecem 8 chamadas
documentais e 47 de dados. O runner sintético executa em banco temporário próprio
e remove somente recursos registrados/OID/servidor confirmados. Última suíte:
140 aprovados, um warning conhecido, manifesto
`backups/20261020T152553Z_78c9da2b.json`. O alvo da coleta real é permanente e
separado: `test_20261020_ba894ce71405`, OID 2508600. Não apontar ao principal nem
apagar o alvo persistente por prefixo.

## Evidência real e retomada

O snapshot C1 `backups/radar-c1-20261013.dump` foi restaurado e verificado antes da
migração C1 no destino isolado. SHA256 do dump:
`ad9ad83e61b8db747dfaed022ffa2f077a3340e3abe24c7084825f3ae1763fef`.
Manifesto `backups/20261020T152131Z_e6215d7c.json`: baseline 20260817_0009,
destino 20261013_0010, nove tabelas C1, 19 FKs RESTRICT, zero privilégio da role
fora da lista permitida. O arquivo com credenciais fica ignorado em
`backups/c2/c2-20261013/target-credentials.json`; não o incluir em logs ou commits.

Calibração executada por
`docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps --entrypoint python radar-ops -m scripts.radar_pncp_live_validation calibrate`.
Usou dois GETs da modalidade código 6, rotulada pela própria resposta como
“Pregão - Eletrônico”: janela 13/10–13/10 e 13/10–14/10. Cada resposta tinha dez
registros, todos com data de publicação 13/10, sem data ausente. Os hashes e IDs
Bronze constam em `backups/c2/c2-20261013/calibration.json`. Essa amostra não
determina a inclusão da borda superior: não houve registro publicado em 14/10.
Não solicitar manual pela rede; `docs/licitacoes/fontes/` não existe.

Coleta mínima executada com uma requisição, uma página de dez registros, modalidade
6, janela sobreposta 06/10–14/10. Job `e47c6fe2-2cc9-4b58-803a-49a4aa44a443`
permanece `partial/sample_page_limit`, `next_page=2`, com resposta Bronze e dez
versões/observações persistidas. Nenhum item ou lote foi inferido do cabeçalho.
O intervalo inclui a janela fixa 07/10–13/10 e um dia adicional em cada borda;
deduplicação preserva identidade/conteúdo.

Comando de verificação, sem rede:

```powershell
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps --entrypoint python radar-ops -m scripts.radar_pncp_live_validation verify
```

Para retomar a mesma coleta, rode `collect` de novo: reutiliza o job guardado em
`backups/c2/c2-20261013/collection-job.json`, continua da página 2 e respeita o
ledger comum e o teto de três tentativas por página. O destino possui a role
`radar_test_f7b391ab6f2c`; o relatório registra seus OIDs e os privilégios
limitados. Não recrie o ledger nem remova credenciais/target para reiniciar cotas.

```powershell
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps --entrypoint python radar-ops -m scripts.radar_pncp_live_validation collect
```

A amostra cobre somente a primeira página e uma modalidade. O limite superior
temporal, termos oficiais, cobertura nacional e motor Prefect continuam sem
validação abrangente. O ciclo fica em andamento; nenhum agendamento, push ou
migração principal ocorreu.