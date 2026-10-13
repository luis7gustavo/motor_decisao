# Operação isolada — Ciclo 1

Executar na raiz do checkout. Docker/Compose são os únicos runtimes usados.
O PostgreSQL existente deve estar operacional; não iniciar prefect-init nem usar
up geral, pois o bootstrap legado executa upgrade head. A revisão pública recusa
o banco operacional enquanto a liberação explícita não for implementada/autorizada.

```powershell
docker compose config --quiet
docker compose -f docker-compose.yml -f compose.radar-ops.yml config --quiet
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps radar-ops tests --origin 20261013_0010
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps radar-ops rehearse --origin 20260817_0009 --target 20261013_0010 --dump /evidence/radar-c1-20261013.dump
docker compose ps -a
docker compose logs --tail 50 postgres
```

Neste runner o argumento origin de tests é a revisão instalada no banco
sintético vazio. No modo rehearse, origin é a revisão inicial da cópia restaurada
e target é a revisão nova. Nunca usar head/base/-1. A suíte atual exige a revisão
20261013_0010; os comandos com 20260817_0009 em operacao-ciclo-0 são históricos.

Backup da sessão foi gerado com pg_dump custom dentro de postgres, verificado
com pg_restore --list e copiado binariamente com docker compose cp. Comandos:

```powershell
docker compose exec -T postgres sh -c 'set -eu; umask 077; test ! -e /tmp/radar-c1-20261013.dump; pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom --file=/tmp/radar-c1-20261013.dump; test -s /tmp/radar-c1-20261013.dump; pg_restore --list /tmp/radar-c1-20261013.dump >/dev/null; echo DUMP_AND_LIST_OK'
docker compose cp postgres:/tmp/radar-c1-20261013.dump backups/radar-c1-20261013.dump
```

Não repetir esse nome para outro backup: a guarda recusa sobrescrita. Em nova
sessão gerar arquivo novo e registrar origem/janela; o exemplo é evidência da
sessão C1, não autorização para reutilizar indefinidamente a mesma cópia.
Backups/manifestos ficam ignorados pelo Git; não remover dumps automaticamente.

Cada teste cria banco/papel descartáveis com dados sintéticos. Ensaio cria outra
cópia, restaura sem ignorar erros, registra contagens consistentes e executa
upgrade→downgrade até origem→upgrade. Somente DDL e leituras nessa cópia; testes
de fixture nunca rodam nela. O runner verifica registro próprio, nome, OID e
identidade do servidor antes de limpar; falha de limpeza mantém código não zero.

Não há comando de migração no principal autorizado. Funcionalidade disponível
somente no ambiente isolado. Não existe endpoint/flow que acione este repositório
na aplicação operacional; importá-lo não abre conexão nem ativa agendamento.
