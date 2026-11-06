# Ferramentas auxiliares da revisão R1

Versão 0.2-R1. 2026-10-12 (America/Sao_Paulo). Revisado em R1-E1.

## Estado e fronteira

**Não executado:** Docker Engine inacessível (`dockerDesktopLinuxEngine` não encontrado). CLI 29.8.2 e Compose v5.5.1 consultados sem iniciar serviços. Nenhuma imagem foi obtida, nenhum digest/arquitetura foi verificado, nenhum script de processamento foi executado. Não há linguagem/dependência instalada no host para esta missão. Não confundir este roteiro com validação aprovada.

Esta pasta contém documentação executável futura da investigação, não adapter do Radar. Não importar módulos da aplicação. Amostras/saídas ficam em [amostras/r1](../amostras/r1/manifesto.md); documentos-fonte nunca são montados com escrita. Contrato insuficiente e Docker indisponível são motivos independentes para não coletar.

## Ambiente reproduzível proposto — gates antes de qualquer execução

1. Em missão autorizada, conferir Engine/arquitetura com consulta segura; não iniciar Compose operacional. Escolher imagem pública oficial Python com biblioteca padrão, tag versionada e digest verificado no registry oficial. Registrar URL de procedência, tag informativa, digest de manifesto/plataforma, arquitetura e versão exata de Python. Não preencher digest por memória nem usar `latest`. Pull explícito é obtenção de ferramenta, separado de amostragem.
2. Escrever e revisar um runner mínimo nesta pasta, com LF, sem dependências externas e sem import da aplicação. Antes de usar rede, provar em fixtures sintéticas offline os limites, ledger pré-envio, contagem de tentativa incerta e remoção de dados pessoais. Fixar hashes do runner, configuração e fixtures dentro de container offline. Só então autorizar o modo de amostragem. Runner ainda não criado porque os contratos/gates permanecem pendentes.
3. Rede local de validação = none. Para amostra, conexão externa apenas a hosts/rotas HTTPS oficiais aprovados no manifesto, nunca host network/rede operacional. Runner deve validar destino/IP público no ponto de conexão, TLS e host; rejeitar privados, loopback, link-local/metadados. Uma bridge comum não é firewall de egress: se não for possível demonstrar restrição efetiva, não executar amostra.
4. UID/GID 65534, cap-drop ALL, no-new-privileges, rootfs read-only, 1 CPU, 256 MiB RAM, sem swap adicional, 64 PIDs, tmpfs 32 MiB noexec/nosuid. Deadline do runner: 900 s amostra/120 s validação, com encerramento e relatório parcial. Nenhuma porta, socket Docker, .env, banco, credencial ou repositório inteiro montado.
5. Montar só scripts em `/tool:ro`, entradas necessárias em `/input:ro`, saída exclusiva em `/output:rw`. Para documento, preparar somente os textos requeridos em staging de entrada dentro desta pasta, sem copiar ambiente/dados. Para amostra, entrada contém contrato/configuração/ledger anterior sanitizados. Persistir ledger novo somente no destino de saída, transação atômica/flush antes do GET; impedir duas execuções concorrentes.
6. De dentro da raiz do checkout, resolver caminhos relativos e colocá-los entre aspas; validar que cada caminho resolvido continua em `docs/licitacoes/`. Testar leitura RO, escrita UID não root e rejeição de escrita fora de `/output` em caminho com espaços e acentos. Não corrigir permissão executando como root. LF e permissões no Docker Desktop **não foram ensaiados** nesta R1.

## Comando modelo: proposto; não executado nesta missão

O bloco é um modelo PowerShell, não está pronto para executar: `R1Image` precisa ser atribuído a referência oficial verificada por digest e o runner/entradas ainda precisam ser criados e revisados. A guarda rejeita referência não fixada. O entrypoint é explicitamente substituído; a duração deve ser imposta pelo runner, não presumida pelo Docker.

```powershell
# Proposto; não executado nesta missão — somente validação offline auxiliar.
# Executar a partir da raiz do checkout, após todos os gates acima.
if ($R1Image -notmatch '^python@sha256:[a-f0-9]{64}$') {
    throw 'Preencher imagem oficial com digest verificado; nenhuma execução autorizada sem isso.'
}
$r1ToolPath = (Resolve-Path 'docs/licitacoes/ferramentas-r1').Path
$r1InputPath = (Resolve-Path 'docs/licitacoes/ferramentas-r1/entradas').Path
$r1OutputPath = (Resolve-Path 'docs/licitacoes/amostras/r1/validacao').Path
docker run --rm --network none --user 65534:65534 --cap-drop ALL `
  --security-opt no-new-privileges --read-only --cpus 1 --memory 256m `
  --memory-swap 256m --pids-limit 64 --tmpfs /tmp:rw,noexec,nosuid,size=32m `
  --mount "type=bind,source=$r1ToolPath,target=/tool,readonly" `
  --mount "type=bind,source=$r1InputPath,target=/input,readonly" `
  --mount "type=bind,source=$r1OutputPath,target=/output" `
  --entrypoint python $R1Image /tool/runner.py validate --deadline 120
```

Não trocar `none` por rede aberta para improvisar amostra: o modo rede exige implementação/revisão própria dos controles do item 3. Imagem de renderer Mermaid/PlantUML precisará procedência, digest e versões próprias; não usar serviço público de renderização nem instalar dependências no host. Aquisição de dependências fixadas com hashes, se necessária, acontece antes, sem dados de amostra e em container descartável separado.

## Contrato proposto do runner de amostragem

- Ler manifesto e ledger cumulativos; abortar em inconsistência, contador >=30 ou tentativa aberta sem contabilização. Registrar número/horário/recurso antes do envio, fsync e checkpoint. Reinício considera tentativa aberta consumida.
- Validar allowlist de GET públicos contra contrato fechado; não enviar auth/cookies. TLS validado. Sem redirects/retries automáticos. Redirect só após validar destino e reservar outra tentativa; erro conta. Intervalo monotônico >=1 s entre inícios, concorrência 1. Retry-After superior ao restante do prazo encerra/reagenda, sem burlar bloqueio.
- Limitar bytes em streaming (2 MiB/resposta; 20 MiB agregados) e até 1.000 registros processados, sem anexos. Excesso interrompe recurso, registra parcial e não confirma página completa. Parse monetário preserva Decimal e tipo original; resposta bruta só em memória, descartada após perfil.
- Saídas: manifesto efetivo, ledger com status/bytes/contagens, perfil n/N por recurso/campo, limitações e exemplos sintéticos. Logs por allowlist, sem corpos, CPF, nomes/identificadores que revelem pessoa ou URLs sensíveis. Discriminar documento de contrato, dados, erro de transporte e fixture.
- Registrar imagem/digest/plataforma, versões, hashes SHA-256 dos scripts/configurações não sensíveis, início/fim/duração, comando sanitizado, código de saída e motivo. Exit 0 não aprova utilidade comercial. Resultado determinístico só se promete para fixture/configuração versionadas; fonte ao vivo pode mudar.

## Validação documental futura

Em container offline: checar links relativos/âncoras; unicidade de definições e referências de IDs; todos os campos de RF/RNF/RT; decomposição RF-028; fases/dependências sem ciclo; revisão/rótulos; render local dos diagramas quando houver ferramenta fixada. Não usar número antigo de RF/links como expected imutável. Distinguir teste automatizado, inspeção humana e não executado. Saída nova com data/escopo/hash de entradas não sobrescreve silenciosamente os relatórios da missão original.

## Histórico de revisões

2026-10-12 — 0.2-R1: roteiro de reprodução condicionado; nenhum script ou digest inventado; limitações de execução explícitas.

## Execução efetiva R1-E2 — revisado em R1

Versão 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). O estado de Engine inacessível acima refere-se à E1 e foi preservado. Na E2, CLI/Engine 29.8.2 e Compose v5.5.1 responderam. Somente ferramentas auxiliares novas, efêmeras/offline; sem container ativo reutilizado, sem stack operacional, banco/volumes/credenciais/socket. Nenhuma amostragem: orçamento E1 permanece 0/30.

### Procedência e ambiente usado

Obtenção de ferramenta separada de dados: páginas oficiais [Python Docker Official Image](https://hub.docker.com/_/python) e [Mermaid CLI upstream](https://github.com/mermaid-js/mermaid-cli) consultadas em 13/10. Digests de plataforma obtidos por `docker buildx imagetools inspect` no registry correspondente; pull explícito pelo digest, nenhum build. Consultas de versão/manifesto não são endpoints de compras nem amostragem.

| Ferramenta | Tag informativa / índice | Digest de plataforma efetivamente usado / arquitetura | Versões observadas |
|---|---|---|---|
| Python oficial, biblioteca padrão | `python:3.13.7-alpine3.22`; índice `sha256:9ba6d8cbebf0fb6546ae71f2a1c14f6ffd2fdab83af7fa5669734ef30ad48844` | `python@sha256:527c28b29498575b851ad88e7522ac7201bbd9e920d2c11b00ff2b39b315f5f8`, linux/amd64 | Python 3.13.7, x86_64, UID 65534 |
| Mermaid CLI upstream | `ghcr.io/mermaid-js/mermaid-cli/mermaid-cli:11.12.0`; índice `sha256:bad64c9d9ad917c8dfbe9d9e9c162b96f6615ff019b37058638d16eb27ce7783` | `ghcr.io/mermaid-js/mermaid-cli/mermaid-cli@sha256:e8edd23a12b7939cabdb7fd9d2c6cbe7d7b8c37f68d62c782fd8e405b61529d6`, linux/amd64 | CLI/Mermaid 11.12.0, Node v18.20.5, Puppeteer 23.11.1, Chromium 124.0.6367.78 Alpine Linux |

Imagem auxiliar antiga fixada para reprodução offline não é recomendação de versão de produção. Nenhuma dependência adicional instalada no host ou container. Renderer usa recursos já incluídos na imagem; `--no-sandbox` do Chromium acompanha isolamento externo não root/sem rede/cap-drop/RO, não deve ser copiado como política irrestrita de navegador operacional.

Scripts autorados aqui: [validar-documentos.py](validar-documentos.py), [render-mermaid.cjs](render-mermaid.cjs). Não importam código de aplicação nem consultam rede. Python verifica links/âncoras limitadas, cercas, campos de RF/RNF/RT, Must em CP/RT/matriz, definições e preservação do corpo de 09/12/D10 contra baseline; extrai fontes Mermaid para saída. Renderer chama mmdc por caminho absoluto da imagem, gera SVG local e registra resultado por bloco. Hashes de scripts/entradas e durações nos JSON de [E2](../amostras/r1/e2/relatorio-validacao.md).

Limites antes da execução: Python 1 CPU/256 MiB/64 PIDs, tmpfs 32 MiB, deadline interno signal.alarm(120). Renderer 1 CPU/1 GiB/256 PIDs, tmpfs 256 MiB, timeout 45 s por bloco e 900 s total via entrypoint timeout; maior memória/tempo que Python justificados por Chromium e 15 blocos sequenciais. Sem swap adicional em ambos, UID/GID 65534, raiz RO, cap-drop ALL, no-new-privileges, rede none e nenhuma porta. Somente docs/licitacoes como entradas necessárias RO, scripts RO e diretório específico e2 RW. Não montar checkout inteiro. Renderer recebe somente as fontes .mmd extraídas e manifest.

### Comandos sanitizados de reprodução

**Executado nesta revisão — ferramenta auxiliar isolada (obtenção de ferramentas, sem amostra).** Inspeção e pull tiveram código 0; registro do digest confirmado no pull. Aquisição não é validação de negócio. Duração individual de rede da aquisição não foi instrumentada; não estimar número fictício. Fonte/digest efetivamente usados acima.

```powershell
docker buildx imagetools inspect python:3.13.7-alpine3.22
docker buildx imagetools inspect ghcr.io/mermaid-js/mermaid-cli/mermaid-cli:11.12.0
docker pull python@sha256:527c28b29498575b851ad88e7522ac7201bbd9e920d2c11b00ff2b39b315f5f8
docker pull ghcr.io/mermaid-js/mermaid-cli/mermaid-cli@sha256:e8edd23a12b7939cabdb7fd9d2c6cbe7d7b8c37f68d62c782fd8e405b61529d6
```

**Executado nesta revisão — ferramenta auxiliar isolada.** A partir da raiz do checkout, preparar somente diretórios de saída desta revisão. Comando baseline executado antes das edições 08–23; **não repetir baseline sobre evidência existente**. Para nova execução, usar destino novo e manter baseline já registrada como entrada. Montagens resolvidas de caminhos relativos e entre aspas funcionaram no checkout com espaços. Scripts finais LF/UTF-8; ensaio de acentos/permissões relatado separadamente. Não é ensaio em host limpo/segundo ambiente.

```powershell
$r1Input=(Resolve-Path docs/licitacoes).Path
$r1Output=(Resolve-Path docs/licitacoes/amostras/r1/e2).Path
$r1Tool=(Resolve-Path docs/licitacoes/ferramentas-r1).Path
docker run --rm --platform linux/amd64 --network none --user 65534:65534 --cap-drop ALL --security-opt no-new-privileges --read-only --cpus 1 --memory 256m --memory-swap 256m --pids-limit 64 --tmpfs /tmp:rw,noexec,nosuid,size=32m --mount "type=bind,source=$r1Tool,target=/tool,readonly" --mount "type=bind,source=$r1Input,target=/input,readonly" --mount "type=bind,source=$r1Output,target=/output" --entrypoint python python@sha256:527c28b29498575b851ad88e7522ac7201bbd9e920d2c11b00ff2b39b315f5f8 /tool/validar-documentos.py validate
```

Na execução inicial somente o argumento final foi `baseline`; saída baseline.json preserva hash/trecho dos corpos para comparação, sem dados de aplicação. Extração .mmd no modo validate não valida sintaxe Mermaid, essa verificação pertence ao renderer seguinte.

**Executado nesta revisão — ferramenta auxiliar isolada.** Entradas em RO e saída própria; entrypoint original da imagem substituído explicitamente, sem bootstrap.

```powershell
$r1Input=(Resolve-Path docs/licitacoes/amostras/r1/e2/mermaid).Path
$r1Output=(Resolve-Path docs/licitacoes/amostras/r1/e2/renderizados).Path
$r1Tool=(Resolve-Path docs/licitacoes/ferramentas-r1).Path
docker run --rm --platform linux/amd64 --network none --user 65534:65534 --cap-drop ALL --security-opt no-new-privileges --read-only --cpus 1 --memory 1g --memory-swap 1g --pids-limit 256 --tmpfs /tmp:rw,nosuid,size=256m --mount "type=bind,source=$r1Tool,target=/tool,readonly" --mount "type=bind,source=$r1Input,target=/input,readonly" --mount "type=bind,source=$r1Output,target=/output" --entrypoint /usr/bin/timeout ghcr.io/mermaid-js/mermaid-cli/mermaid-cli@sha256:e8edd23a12b7939cabdb7fd9d2c6cbe7d7b8c37f68d62c782fd8e405b61529d6 900 node /tool/render-mermaid.cjs
```

Resultado: 15 blocos, zero falhas, 55,529 s internos; código 0. Warnings Chromium de fonte local `file:///...woff2` com fallback ocorreram; container sem rede, não indicam consulta externa. Não alegar inspeção visual no GitHub. D04/D05 PlantUML somente revisão textual; ferramenta fixada não preparada. **Proposto; não executado nesta missão:** selecionar imagem PlantUML upstream por digest/plataforma, substituir entrypoint pela ferramenta, montar somente D04/D05 RO/saída dedicada RW, aplicar os mesmos controles offline e timeout, registrar versão/hash/render; nenhum Java no host.

### Falhas e limites efetivos

Inspeção inicial de renderer em container isolado: `mmdc` não estava no PATH, exit 1 (~4,9 s incluindo consulta de metadados), versões Node/Chromium localizadas. Corrigido para `/home/mermaidcli/node_modules/.bin/mmdc`, conforme entrypoint da imagem, sem instalar dependência. Não é falha de diagrama. Primeira checagem documental exit 1 (0,509 s): duas âncoras com underscore reportadas incorretamente pelo slug auxiliar; corrigido o verificador, links originais preservados. Resultado inicial em validate-01.json; rerun registra resultado novo. Baseline inicial: exit 0, 0,322 s internos, SHA do script naquela versão no próprio JSON.

Relatórios da missão original conservam datas e resultados; adendo de escopo impede atribuí-los à R1. Os ensaios auxiliares não validam contratos públicos/DV, processamento real, banco/restore, segurança completa do produto, qualidade comercial ou testes TST. QA-R1-01 resolvido somente quanto ao runtime de **validação documental**; ambiente/runner de amostragem e produto permanecem pendentes.

### Histórico de revisões

2026-10-13 — 0.3-R1-E2: runtime offline efetivamente usado, digests/versões/scripts/comandos/limites e falhas reais; E1 e orçamento de dados preservados.

### Comando do ensaio de portabilidade — revisado em R1

**Executado nesta revisão — ferramenta auxiliar isolada.** Mesmos controles Python; deadline interno de 30 segundos, saída exclusiva. Evidência e hashes em e2/saída com espaços e acentos/portabilidade.json. Esse ensaio não é validação da aplicação.

```powershell
$r1Input=(Resolve-Path 'docs/licitacoes/ferramentas-r1/entrada com espaços e acentos').Path
$r1Output=(Resolve-Path 'docs/licitacoes/amostras/r1/e2/saída com espaços e acentos').Path
$r1Tool=(Resolve-Path docs/licitacoes/ferramentas-r1).Path
docker run --rm --platform linux/amd64 --network none --user 65534:65534 --cap-drop ALL --security-opt no-new-privileges --read-only --cpus 1 --memory 256m --memory-swap 256m --pids-limit 64 --tmpfs /tmp:rw,noexec,nosuid,size=32m --mount "type=bind,source=$r1Tool,target=/tool,readonly" --mount "type=bind,source=$r1Input,target=/input,readonly" --mount "type=bind,source=$r1Output,target=/output" --entrypoint python python@sha256:527c28b29498575b851ad88e7522ac7201bbd9e920d2c11b00ff2b39b315f5f8 /tool/portabilidade.py
```
