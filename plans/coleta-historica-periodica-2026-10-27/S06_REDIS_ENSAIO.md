# Ensaio isolado do AOF Redis compartilhado — 28/10/2026

Objetivo: investigar o bloqueio de Prefect sem modificar o volume `motor_redis_data`. Este ensaio **não autoriza nem executa o reparo do volume compartilhado**.

## Identidade e cópia

- `docker inspect motor_redis` confirmou o volume nomeado `motor_redis_data` montado em `/data`. Tamanho observado: 28,9 MB.
- Backup somente leitura via Docker: `backups/redis/20261028/motor_redis_data-before-repair.tar.gz`, 1.780.008 bytes, SHA-256 `c8bda756389aa1f9ac4882c116310ae50cc96c37f9e1a07401bc32d16a1adc66`. O arquivo `tar -tzf` lista `dump.rdb`, manifesto e os arquivos AOF base/incremental; leitura passou.
- Volume de ensaio `pncp-redis-rehearsal-20261028` criado do tar. `cmp` confirmou igualdade byte a byte dos quatro arquivos com o volume original antes do reparo. O original permaneceu montado somente leitura em todos os diagnósticos.

## Diagnóstico e reparo apenas da cópia

| Etapa | Resultado |
|---|---|
| `redis-check-aof /data/appendonlydir/appendonly.aof.manifest` no clone, sem `--fix` | Exit 1. Base RDB válida; incremental de 30.244.864 bytes válido até o offset 30.240.881; **3.983 bytes finais inválidos**. `backups/redis/20261028/check-before.txt`. |
| `redis-check-aof --fix ...` no clone | Exit 0 após confirmação; truncou somente o AOF incremental do volume de ensaio para 30.240.881 bytes. `fix-clone.txt`. |
| Novo `redis-check-aof` no clone | Exit 0; base, incremental e manifesto válidos. `check-after.txt`. |
| Redis 7-alpine no clone, `--network none` | Iniciou e respondeu `PONG`; `INFO keyspace` reportou `db1:keys=85`, consistente com 88 chaves da base e 3 expiradas. Container de ensaio parado, volume conservado. |

O conteúdo dos 3.983 bytes descartados não foi interpretado nem preservado como operações lógicas no ensaio. Aplicar o mesmo truncamento ao volume compartilhado pode perder operações recentes de Prefect/Redis e afetar outros fluxos. O backup tar guarda os bytes originais para recuperação, mas não garante que mensagens descartadas possam ser reconstruídas. Não comparar somente a contagem de chaves como prova de equivalência de todas as operações.

## Procedimento proposto, dependente de aprovação específica

1. Confirmar que a cópia/relatório acima permanecem íntegros e fazer novo backup do volume original imediatamente antes da mudança, sem sobrescrever o existente; registrar hash e igualdade de arquivos.
2. Parar apenas os serviços que usam o Redis compartilhado, preservando os bancos e volumes. Identificar dependências antes de parar e evitar `prefect-init` e qualquer migração.
3. Executar `redis-check-aof --fix` no volume compartilhado uma única vez; conferir o número de bytes efetivamente descartados e verificar com `redis-check-aof` novamente.
4. Reiniciar o Redis e os serviços afetados conforme Compose, validar `PONG`, logs, Prefect API/scheduler e fluxos existentes. Se houver falha, parar novamente e usar o backup somente após identificar o volume exato; não restaurar às cegas.
5. Registrar resultados e manter a missão PNCP pausada/desabilitada até os 12 gates de fonte/orçamento também serem resolvidos.

Estado atual: **aguardando decisão do usuário para tocar o volume compartilhado**. Nenhuma etapa acima foi aplicada no volume real.
