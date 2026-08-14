# Runbook operacional

## PC1 - control node e worker

1. Instale Docker Desktop e habilite containers Linux.
2. Copie `.env.example` para `.env` e preserve os tokens ja existentes.
3. Defina `SILLO_PREFECT_HOST` com o IPv4 LAN do PC1 se a UI for acessada pelo PC2.
4. Mantenha `SILLO_ENABLE_SCHEDULES=false` na primeira subida.
5. Suba a plataforma:

```powershell
docker compose --profile control up -d --build
docker compose --profile control ps
```

O job `prefect-init` aplica `alembic upgrade head`, cria o pool `sillo-collection`, sincroniza as filas e registra os deployments. A UI fica em `http://<IP_PC1>:4200`.

Não exponha as portas à internet. Na rede privada, permita no Firewall do Windows apenas o PC2 nas portas 4200 (Prefect) e 55432 (PostgreSQL SILLO). Redis não é necessário para o worker remoto.

## PC2 - compute node

Use o mesmo commit do repositório e um `.env` local. Defina:

```dotenv
SILLO_PREFECT_HOST=<IP_LAN_PC1>
SILLO_DB_HOST=<IP_LAN_PC1>
PREFECT_SERVER_PORT=4200
MOTOR_POSTGRES_PORT=55432
```

Depois:

```powershell
docker compose -f docker-compose.worker.yml up -d --build
docker compose -f docker-compose.worker.yml ps
```

Os workers PC2 não hospedam banco, Redis ou Prefect Server. Se o PC2 desligar, os workers PC1 continuam consultando as mesmas filas. Ao religar, os workers PC2 se registram novamente.

## CLI

Dentro de um worker PC1:

```powershell
docker compose --profile control exec worker-manual-pc1 sillo collect list
docker compose --profile control exec worker-manual-pc1 sillo collect list --all
docker compose --profile control exec worker-manual-pc1 sillo collect run --source kabum
docker compose --profile control exec worker-manual-pc1 sillo collect run --profile market
docker compose --profile control exec worker-manual-pc1 sillo collect run --profile suppliers
docker compose --profile control exec worker-manual-pc1 sillo collect run --profile local
docker compose --profile control exec worker-manual-pc1 sillo collect run --profile full
docker compose --profile control exec worker-manual-pc1 sillo collect status
```

Por padrão a CLI executa no processo atual. Acrescente `--prefect` para submeter à fila `manual`. Toda coleta integral faz um canary rastreável antes de avançar. Para executar somente o canary, use `--canary --max-results 1`.

## Agendas

Os deployments são criados pausados e com minutos diferentes por fonte. Depois dos canaries individuais:

```powershell
# Altere SILLO_ENABLE_SCHEDULES=true no .env e ressincronize.
docker compose --profile control run --rm prefect-init
```

Para pausar novamente, volte a variável para `false` e repita o comando. Intervalos e offsets ficam em `config/sources/current.yaml`; limites PC1/PC2 ficam em `config/collection.yaml` e podem ser sobrepostos pelas ENV documentadas.

Antes de ativar o Mercado Livre, configure `ML_CLIENT_ID` e `ML_CLIENT_SECRET`, renove `ML_ACCESS_TOKEN`/`ML_REFRESH_TOKEN` pelo fluxo oficial e execute um canary. A fonte permanece desativada enquanto essas credenciais não estiverem completas. Terabyte, Buscape e Zoom permanecem desativados com `COLLECTION_BLOCKED`, pois os canaries de 12/08/2026 confirmaram que o `robots.txt` proíbe as URLs de busca usadas; não os reative sem uma rota oficial permitida.

## Alertas e diagnóstico

`sillo collect status` apresenta runs recentes, alertas abertos e fontes atrasadas. A tabela `control.collection_alerts` registra zero itens, queda de volume, erros de parser, taxa de erro, 403/429, timeouts e falhas consecutivas. Um novo run resolve os alertas anteriores da fonte e reabre apenas os problemas ainda presentes; canaries não geram falso alerta de queda de volume. Logs JSON incluem run, fonte, worker, hostname e tipo de erro, com dados sensíveis redigidos.

## Teste físico obrigatório

Com os dois PCs ligados, submeta pelo menos dois deployments e confirme na UI que um foi executado por `pc1-*` e outro por `pc2-*`. Confirme no banco central os dois `worker_id`. Desligue PC2, submeta outro job e confirme execução no PC1; religue PC2 e confirme seu retorno ao pool.

PC1 é inicialmente um ponto único de falha para Prefect e ambos os bancos. Alta disponibilidade e backup automatizado são melhorias futuras, fora desta fase.
