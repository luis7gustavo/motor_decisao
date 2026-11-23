# Repasse da coleta ao Carlos: RTK e contêineres

**Preparado em:** 23/11/2026 (America/Sao_Paulo). **Responsável pela coleta a partir de agora:** Carlos, por decisão de Luis nesta conversa. O corte e a exportação final foram executados em 23/11/2026; a importação e a ativação no PC de Carlos continuam pendentes. Veja RECIBO_CORTE_CARLOS_20261123.md antes de iniciar qualquer worker.

## 1. Instalar o hook RTK no Codex de Carlos

No Windows, instale PowerShell 7, Git, Docker Desktop com WSL 2 e o RTK. A instalação desta máquina usa o pacote WinGet `rtk-ai.rtk` e a versão observada é **1.1.0**. No PowerShell de Carlos:

```powershell
winget install --id rtk-ai.rtk --exact
rtk --version
rtk init --codex --global --dry-run
rtk init --codex --global
rtk init --codex --global --show
rtk hook check --agent codex git status
```

O resultado de `--show` deve indicar `Global RTK.md`, `Global hook` e `Global AGENTS.md` com `[ok]`. O hook desta máquina está em `%USERPROFILE%\.codex\hooks.json`, na fase `PreToolUse`, para `Bash`, com comando `rtk hook codex`. O `AGENTS.md` global referencia `RTK.md`. Não copie os caminhos `C:\Users\luisg\...` literalmente para o PC de Carlos: o `rtk init --codex --global` gera os arquivos no perfil dele. Reinicie o Codex depois da instalação e execute `rtk init --codex --global --show` novamente. Se o WinGet não encontrar o pacote, obtenha a versão Windows da distribuição oficial do RTK, coloque `rtk.exe` no `PATH` e só então rode `init`.

## 2. O que compõe a coleta atual

Inventário observado em 23/11/2026, não uma promessa de estado futuro:

| Parte | Origem atual | Transferência |
| --- | --- | --- |
| Banco C2 da coleta | `motor_postgres`, banco `test_20261020_ba894ce71405`, volume `motor_postgres_data` | Dump próprio do C2, onde estão ledger, Bronze e identidades. O alvo é indicado por `backups/c2/c2-20261013/target-credentials.json` e foi confirmado pelo identificador do servidor. |
| Banco principal compartilhado | `motor_postgres`, banco `motor_decisao`, no mesmo volume | Dump separado; contém dados de outros fluxos e não deve ser confundido com o C2. |
| Orquestração dedicada | `motor_decisao-prefect-history-dedicated-postgres-1`, banco `prefect`, volume `motor_decisao_radar_history_prefect_dedicated_pg` | Dump próprio do Prefect. |
| Mensageria dedicada | `motor_decisao-prefect-history-dedicated-redis-1`, volume `motor_decisao_radar_history_prefect_dedicated_redis` | Cópia de `/data` só após parar worker, serviços, servidor e Redis; a restauração do AOF exige validação antes de religar. |
| Worker e servidor | imagem `radar-history-prefect:3.8.8-sa2.0.49` | `docker save`/`docker load`; contêineres são recriados pelo Compose, não por `docker export/import`. |
| Código efetivamente montado | `app`, `config`, `pipelines`, `scripts` de `motor_decisao_coleta_20261111` | Cópia dos diretórios, inclusive alterações locais ainda não commitadas. Testes, docs, migrations e Dockerfiles acompanham o pacote para manutenção. |
| Evidências no host | `motor_decisao/backups` montado em `/evidence` | Cópia integral; contém backups e possivelmente credenciais. |
| Configuração | `docker-compose.yml` e `compose.radar-history-dedicated.yml` do checkout `motor_decisao`; overlay `compose.radar-history-enrichment-20261111.yml` do checkout da coleta | Copiar os arquivos; recriar os bind mounts com os caminhos de Carlos. `.env` deve ser transferido separadamente por canal privado. |

Outros serviços do Compose principal (`api`, workers genéricos, Selenium, Prefect compartilhado e Redis compartilhado) não são a pilha dedicada da coleta. O Redis compartilhado desta máquina estava reiniciando em 23/11; não o misture com o Redis dedicado. O volume `radar_vscode_pg_20261110` é um banco auxiliar, não o banco ativo `motor_postgres`.

Na leitura de 23/11, `python scripts/radar_pncp_history.py status` no worker dedicado informou missão `enabled`, 50.750/100.000 tentativas e cobertura `partial_or_unverified`; a janela registrada termina em **24/11/2026 01h49** em São Paulo. O C2 tinha cerca de **11 GB**, o banco principal **3,3 GB** e o Prefect dedicado **317 MB**. Releia esses estados no corte. A exportação feita enquanto a missão continua escrevendo é apenas uma fotografia preliminar; a final precisa de parada coordenada para que C2, Prefect, Redis e evidências representem o mesmo corte.

## 3. Preparar o corte e exportar

O script [Exportar-Coleta.ps1](./Exportar-Coleta.ps1) faz pré conferência sem mudar a operação quando chamado sem `-Execute`. Execute no PowerShell 7, em um disco local com espaço suficiente. O diretório `backups` sozinho tinha **30,3 GB** em 23/11; os três bancos somavam cerca de **14,6 GB** antes da compressão, além das imagens. Reserve espaço para o pacote, cópia temporária e verificação. Não coloque o pacote em Git ou pasta pública.

```powershell
.\Exportar-Coleta.ps1 -Destino 'D:\transferencia-radar-carlos'
```

Antes do corte final, Carlos deve confirmar que receberá o pacote em mídia ou canal privado criptografado e que o host dele está pronto. No host de origem, pause os deployments, aguarde as execuções em curso terminarem conforme o runbook vigente e salve o status imediatamente antes da parada.

Pare **somente** `radar-history-dedicated-worker`, `prefect-history-dedicated-services`, `prefect-history-dedicated-server` e `prefect-history-dedicated-redis`, usando os arquivos Compose corretos e sem `down -v`. Mantenha os dois PostgreSQL ativos para os dumps. Confirme no Docker que os quatro contêineres estão parados e que não há execução em trânsito. Preserve a máquina de origem até a validação no destino. Não ative duas agendas da mesma fonte em paralelo.

No checkout operacional `motor_decisao`, os comandos de parada correspondentes à pilha observada são:

```powershell
$overlay = '..\motor_decisao_coleta_20261111\compose.radar-history-enrichment-20261111.yml'
docker compose -p motor_decisao -f docker-compose.yml -f compose.radar-history-dedicated.yml -f $overlay --profile control --profile radar-history-dedicated run --rm -T --entrypoint python radar-history-dedicated-ops -m scripts.radar_pncp_schedule pause
docker compose -p motor_decisao -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T --entrypoint python radar-ops -m scripts.radar_pncp_s06_activate_history pause
# Aguarde o fim do run em curso e confira o status antes de parar os serviços.
docker exec motor_decisao-radar-history-dedicated-worker-1 python scripts/radar_pncp_history.py status | Set-Content -Encoding utf8 'D:\status-final-coleta.json'
docker compose -p motor_decisao -f docker-compose.yml -f compose.radar-history-dedicated.yml -f $overlay --profile radar-history-dedicated stop radar-history-dedicated-worker prefect-history-dedicated-services prefect-history-dedicated-server prefect-history-dedicated-redis
```

Esses comandos foram derivados do runbook e do Compose atuais; confirme o estado dos deployments e o efeito de `pause` antes de continuar, pois os arquivos operacionais podem mudar.

```powershell
.\Exportar-Coleta.ps1 -Destino 'D:\transferencia-radar-carlos' -Execute -Final -StatusArquivo 'D:\status-final-coleta.json'
```

O script recusa sobrescrever o destino, faz dumps lógicos, copia a árvore montada e `/evidence`, salva as imagens e gera `MANIFESTO-SHA256.csv`. Se algum passo falhar, o pacote é incompleto; descarte a tentativa somente depois de inspecionar o erro e refaça em **outro** diretório. O `.env` e outros segredos devem ir em um cofre privado separado. O pacote em si também é **sensível**: dumps e evidências podem conter dados e credenciais. Proteja a mídia com criptografia e entregue o hash do manifesto por canal separado.

## 4. Importar no PC de Carlos

1. Instale os requisitos da seção 1, copie o pacote para disco local e confira os hashes com `Importar-Coleta.ps1 -Pacote ... -Verificar`.
2. Guarde um clone do repositório `motor_decisao` no commit compatível para manutenção futura. O importador monta a operação em uma raiz **nova** a partir dos arquivos exportados, incluindo as mudanças locais do código de coleta que um clone não contém.
3. Copie `CARLOS_ENV_MODELO.txt` para um `.env` privado, substitua a senha marcada e guarde esse arquivo fora do pacote; envie-o por canal privado separado. Deixe agendas e outras coletas desabilitadas durante a restauração. O script de importação cria uma raiz de operação nova e põe os quatro diretórios montados dentro dela, de modo que o Compose dedicado use binds relativos locais. O overlay de enriquecimento com caminhos de Luis é mantido no pacote apenas como evidência e **não deve ser usado** no PC de Carlos.
4. Em Docker novo, sem volumes de mesmo nome, rode o importador abaixo. Ele faz `docker load`, cria/sube apenas os dois contêineres PostgreSQL, restaura os três bancos e recria o papel do C2 com a senha no arquivo de credenciais exportado; também cria o contêiner Redis parado com `/data` copiado. Não rode `prefect-init` nem `alembic upgrade head` nos bancos restaurados.
5. Confira `backups` montado em `/evidence`, permissões e AOF do Redis. Ligue primeiro Redis e servidor do Prefect; depois serviços. Ligue o worker **somente após** a auditoria abaixo e após confirmar que a origem continua parada.

```powershell
.\Importar-Coleta.ps1 -Pacote 'D:\transferencia-radar-carlos' -Verificar
.\Importar-Coleta.ps1 -Pacote 'D:\transferencia-radar-carlos' -Destino 'C:\radar-coleta-carlos' -EnvArquivo 'D:\cofre-privado\radar.env' -Restaurar
```

Comandos de restauração dos dumps em contêineres novos, após confirmar nomes, bancos e volumes de destino:

```powershell
docker cp .\motor_decisao.dump motor_postgres:/tmp/motor_decisao.dump
docker exec motor_postgres pg_restore -U motor -d motor_decisao --no-owner --no-privileges --exit-on-error /tmp/motor_decisao.dump
# O C2 exige recriar seu papel a partir do arquivo de credenciais privado antes do restore --create.
docker cp .\c2-coleta.dump motor_postgres:/tmp/c2-coleta.dump
docker exec motor_postgres pg_restore -U motor -d postgres --create --exit-on-error /tmp/c2-coleta.dump
docker cp .\prefect-dedicado.dump motor_decisao-prefect-history-dedicated-postgres-1:/tmp/prefect.dump
docker exec motor_decisao-prefect-history-dedicated-postgres-1 pg_restore -U prefect -d prefect --no-owner --no-privileges --exit-on-error /tmp/prefect.dump
```

Os comandos acima são equivalentes para diagnóstico manual; **não os repita** após `Importar-Coleta.ps1 -Restaurar`. Use somente em **bancos vazios**. O nome do contêiner dedicado depende do nome do projeto Compose; o importador fixa `motor_decisao`. O importador copia o Redis com o contêiner parado, mas sua validação de permissões e AOF continua manual. Se o AOF falhar, não trunque nem apague o volume; investigue antes de iniciar agendas. A restauração do banco `motor_decisao` usa `--no-privileges`; se Carlos for executar também outros fluxos desse banco, as permissões deles precisam de validação separada. Se o importador falhar no meio, preserve o destino para diagnóstico; ele não apaga nem sobrescreve volumes numa segunda tentativa.

## 5. Aceite antes de ativar

- Todos os arquivos do manifesto têm SHA-256 válido no destino; dumps passam em `pg_restore --list`.
- `pg_isready` confirma os dois servidores PostgreSQL; comparar contagens do ledger, respostas Bronze, identidades e esquema do C2 entre origem e destino. Comparar também os arquivos de `/evidence` e suas hashes.
- `docker compose config --quiet` passa; os binds apontam para as pastas de Carlos, sem caminhos de Luis. Prefect dedicado responde `/api/health`; o worker ainda está parado.
- O status da missão, tentativas consumidas, políticas, janela, watermarks e `exclusive.lock` são coerentes. Não zerar contador, ampliar teto ou duplicar o agendamento por causa da mudança de host.
- Carlos assina o recebimento e a validação. Só então iniciar um worker único e observar uma execução controlada. Se falhar, manter a origem preservada e decidir a retomada em **um** host.

## Fontes consultadas

- Cofre: `../motor publico/RADAR LICITA/00 - Início.md`, `AGENTS.md`, `Referências/Mapa das fontes.md` e cópia `Documentos/motor publico/motor_decisao_coleta_20261111/plans/pncp-enrichment-20261111/REPASSE_CARLOS_COLETA_COMPLETA.md`.
- Originais atuais: `../motor publico/motor_decisao/docker-compose.yml`, `compose.radar-history-dedicated.yml`, `docs/licitacoes/operacao-coleta-historica-prefect.md`; `../motor publico/motor_decisao_coleta_20261111/compose.radar-history-enrichment-20261111.yml`; Docker e `rtk --help`/`rtk init --codex --global --show` lidos em 23/11/2026.

