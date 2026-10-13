# Operação da fundação do Radar Licita

Executar da raiz do repositório, com Docker/Compose e imagem local da API.
O runner usa a imagem de dependências `motor_decisao-api`; construir essa imagem
se ainda não existir com `docker compose build api` (não inicia serviços).
O código atual é montado por diretórios explícitos somente leitura, sem .env.
Não usar `up` geral: pressupõe PostgreSQL operacional previamente verificado.

## Preparação e testes

```powershell
docker compose config --quiet
docker compose -f docker-compose.yml -f compose.radar-ops.yml config --quiet
docker compose -f docker-compose.yml -f compose.radar-ops.yml build radar-ops
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps radar-ops tests --origin 20260817_0009
```

O destino é criado automaticamente com nome aleatório, recusando colisões.
O runner aplica a cadeia legada somente nesse banco vazio, cria credencial
restrita para fixtures e limpa apenas seus próprios recursos. Não aceita nome
de destino arbitrário do operador. `--baseline` exclui apenas o novo arquivo
test_radar_ops.py para reproduzir a linha de base de 54 testes.

## Backup e ensaio

O comando abaixo foi executado nesta sessão; para outro backup, escolher outro
nome em ambos os comandos. O `test ! -e` impede sobrescrever um dump existente.
Não usar redirecionamento textual do PowerShell para transportar binário.

```powershell
docker compose exec -T postgres sh -c 'set -eu; umask 077; test ! -e /tmp/radar-c0-20261013-r2.dump; pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom --file=/tmp/radar-c0-20261013-r2.dump; test -s /tmp/radar-c0-20261013-r2.dump; pg_restore --list /tmp/radar-c0-20261013-r2.dump >/dev/null; echo DUMP_AND_LIST_OK'
docker compose cp postgres:/tmp/radar-c0-20261013-r2.dump backups/radar-c0-20261013-r2.dump
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps radar-ops rehearse --origin 20260817_0009 --dump /evidence/radar-c0-20261013-r2.dump
```

O último comando restaura integralmente, ensaia revisão sintética e elimina sua
cópia. Relatórios JSON identificam criação/OID/servidor, backup/hash, contagens,
etapas e limpeza. Códigos não zero exigem inspeção antes de repetir. Não apagar
bancos por prefixo; interrupção abrupta exige conferir manifesto e identidade.
Os backups são preservados, inclusive cópia no /tmp do container. Não há
política automática de retenção ou limpeza de dumps nesta fundação.

## Migrações futuras e logs

O modo `rehearse` aceita `--target REVISAO_EXATA_NOVA` para ensaiar descendentes
lineares da origem em cópia. Isso não valida automaticamente aditividade ou
estrutura de uma migração real: a próxima história deve revisar todas as novas
revisões, adicionar asserts de estrutura/reversibilidade e gerar backup fresco.
Símbolos `head`, `base`, `-1`, merges e destinos anteriores não são aceitos como
alvo de ensaio. Nunca fornecer downgrade legado como teste.

Não há comando de aplicação no principal neste runner. A permissão vigente é
não. Aplicação futura exige os gates do prompt/16, janela de escrita autorizada,
ensaio específico e reconferência de origem. Testes e ensaio nunca ativam Radar
ou schedules Prefect no principal.

```powershell
docker compose ps -a
docker compose logs --tail 50 postgres
```

Os logs do runner efêmero são a saída do comando e os relatórios em backups/.
Logs operacionais podem conter dados e devem ser revisados antes de compartilhar.
