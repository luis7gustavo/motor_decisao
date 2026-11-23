# Recibo do corte da coleta para Carlos

**Data:** 23/11/2026, America/Sao_Paulo. **Origem:** PC de Luis. **Estado da transferência:** coleta dedicada parada e pacote exportado; restauração e ativação no PC de Carlos ainda pendentes.

## Corte observado

- O deployment dedicado `mission` foi pausado; as cinco agendas retornaram `active: false`.
- O último backfill visto no Prefect terminou em `COMPLETED` antes da parada. Esse estado do run não comprova cobertura completa.
- O status final da missão foi salvo com **50.857/100.000 tentativas** e `coverage: partial_or_unverified`. O ledger da fonte ainda indicava `enabled`.
- O comando legado de pausa da fonte recusou a mudança com `History source state or ledger drift`. A política usa datas com `-03:00` e o escopo salvo no C2 usa os mesmos instantes em `+00:00`; não foi feita atualização direta no banco para contornar a guarda.
- Os contêineres `radar-history-dedicated-worker`, `prefect-history-dedicated-services`, `prefect-history-dedicated-server` e `prefect-history-dedicated-redis` terminaram às 12h44 (São Paulo). Os dois PostgreSQL ficaram ativos para os dumps. Nenhum volume foi removido.
- Os demais serviços da pilha principal não foram parados. Não inicie a coleta em dois hosts ao mesmo tempo.

## Pacote

A pasta `RadarLicita-Carlos-20261123-1244` contém dumps lógicos de `motor_decisao`, C2 e Prefect dedicado; estado do Redis dedicado; imagem Docker; código montado e evidências; inventário, estado final da missão, manifestos e instruções. O pacote contém dados sensíveis, inclusive credenciais C2 dentro de `evidencias`. O `.env` operacional não foi incluído e precisa de repasse privado separado.

Carlos deve verificar o manifesto, restaurar em destino novo, comparar ledger/contagens e só ligar um worker após confirmar que esta origem continua parada. A origem deve ser preservada até o aceite do destino.

## Verificações da exportação

- Pacote: 1.087 arquivos no manifesto, 35,85 GB ao todo (incluindo o manifesto).
- Todos os 1.087 SHA-256 foram verificados pelo importador em modo -Verificar.
- Os três dumps passaram em pg_restore --list; o arquivo imagens-docker.tar pôde ser listado.
- O ledger do C2 permaneceu enabled com 50.857 tentativas após a parada; o deployment está pausado e o worker da origem permanece parado. Carlos precisa reconciliar a divergência de datas entre política e escopo antes de usar os comandos de pausa/retomada da fonte.


