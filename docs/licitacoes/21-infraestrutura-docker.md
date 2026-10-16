# 21 — Infraestrutura Docker-first

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: [PROPOSTO], com inventário [CONFIRMADO] no código; operação atual [NÃO-CONFIRMADO].

## 1. Arquitetura dos containers

Evoluir o Compose existente por mudanças incrementais, sem trocar nomes dos volumes operacionais. [07](07-arquitetura-to-be.md) e [D12](diagramas/D12-implantacao.md) fixam a topologia. O arquivo real continua inalterado. ADR-003 preserva o PostgreSQL de domínio e o PostgreSQL exclusivo do Prefect; não introduzir terceiro banco operacional. Docker Desktop/Engine e Compose são os únicos runtimes requeridos no host. Versões/imagens deverão ser fixadas e testadas na implementação, sem pressupor que intervalos abertos atuais sejam reprodutíveis.

## 2. Responsabilidade e evolução de cada serviço

| Serviço atual [CONFIRMADO] | Responsabilidade | Evolução [PROPOSTO] |
|---|---|---|
| postgres | Dados SILLO, imagem pgvector PostgreSQL 15 | Mesmo banco/volume; novos schemas por Alembic; conservar extensões e IDs |
| redis | Persistência AOF e mensageria Prefect | Manter por necessidade do controle; sem cache comercial novo |
| selenium | Navegador remoto para coleta legada | Opcional no perfil de coleta comercial; não depender dele para ler carteira ou PNCP |
| api | FastAPI/Jinja2; reload e bind mount | Dev mantém reload; produção local usa imagem imutável, sem reload/bind do repositório |
| daemon | Processo legado opcional | Não agendar PNCP nele; evitar duplicidade com Prefect |
| prefect-postgres | Metadados Prefect | Preservar banco/volume e versão compatível |
| prefect-server / prefect-services | API/UI e serviços do orquestrador | Reusar; acesso administrativo local; saúde e logs separados |
| prefect-init | Bootstrap com efeitos/migrations | Separar gate de migration do bootstrap; impedir migração concorrente/autônoma em reinício |
| worker-http-pc1 | Execução HTTP via Prefect | Adapter PNCP e coordenação do spool documental; cotas por fonte |
| worker-browser-pc1 / worker-manual-pc1 | Coleta especializada/manual | Preservar como opcionais, sem condicionar fluxo público |
| worker-http-pc2 / worker-browser-pc2 | Workers em segundo host | Fora da V1 individual; manter arquivo histórico sem exigir rede remota |
| document-parser — novo | Não existe hoje | Container isolado sem rede, sem credenciais; processa spool com limites |
| migration / tests — tarefas novas | Não existem como serviços dedicados | Perfis/tarefas efêmeras, sem portas; permissões próprias e destino explícito |

Não há container React na solução escolhida. A imagem existente inclui browsers e dependências de controle; extrair futuramente um alvo mínimo para parser, sem obrigar reescrita da imagem dos workers. Usuário não root, filesystem raiz somente leitura, capacidades removidas, `no-new-privileges`, tmpfs com limite e sem socket Docker são requisitos do parser. Não prometer sandbox completo apenas por usar container.

## 3. Comunicação

API e worker HTTP usam PostgreSQL por nome de serviço, porta interna 5432. Workers consultam Prefect pela API interna; Prefect usa seu banco e Redis na porta interna 6379. API não usa Redis para transações comerciais. Origem PNCP e documentos passam pelo downloader HTTP com egress controlado; browser fica restrito a coletores que o exigem.

Parser usa `network_mode: none`, lê um volume de entrada somente leitura e escreve um volume de saída; não monta banco, documentos completos, repositório ou ambiente. Coordenador prepara entrada imutável com job UUID, hash e versão do parser; parser publica resultado por rename atômico de arquivo temporário. Coordenador verifica schema/hash/tamanho, grava resultado e conclusão no banco na mesma transação. Claim por arquivo atômico para uma instância inicialmente; lease no banco pertence ao coordenador, não ao parser. Crash abandona temporário; lease expirado permite reenvio; unicidade documento+hash+parser+config elimina duplicação. Saída hostil não é executada nem desserializada com mecanismo inseguro.

## 4. Redes e portas

| Superfície | AS-IS [CONFIRMADO] | TO-BE [PROPOSTO] |
|---|---|---|
| Aplicação | Host 8010 → 8000 por padrão | Publicar somente em 127.0.0.1; manter porta configurável |
| Bancos | Host 55432/55433 → 5432 | Sem publicação no modo normal; diagnóstico opcional em loopback |
| Redis | Host 6380 → 6379 | Somente rede de controle |
| Prefect | Host 4200 | Administração em loopback, sem exposição pública |
| Selenium | Host 4444/7900 | Perfil legado, diagnóstico em loopback quando necessário |
| Parser | Inexistente | Sem rede/portas |

Propor rede de dados interna para API/worker/banco, rede de controle para Prefect/Redis/workers e saída externa apenas aos componentes que consultam fontes. Associação a múltiplas redes deve ser explicitada no Compose futuro; o desenho lógico não afirma que existe firewall de egress hoje. Dependências: banco saudável + migration aprovada → API/worker; Prefect saudável + bootstrap concluído → workers. Parser não depende do servidor PNCP. Healthcheck de DB, heartbeat/lease de worker e prontidão de API são distintos. Corrigir `/health` degradado com HTTP 200 antes de utilizá-lo como readiness (RNF-006).

## 5. Volumes e persistência

| Volume | Status | Conteúdo / mounts |
|---|---|---|
| motor_postgres_data | [CONFIRMADO] nome global fixo | Banco da aplicação; preservar identidade e caminho interno atual |
| prefect_postgres_data | [CONFIRMADO] nome global fixo | Metadados; preservar independente do domínio |
| motor_redis_data | [CONFIRMADO] nome global fixo | AOF; recuperação coordenada com jobs Prefect |
| radar_documents | [PROPOSTO] | Originais e hashes; downloader escreve, API lê somente documentos liberados |
| radar_parser_input / radar_parser_output | [PROPOSTO] | Spool limitado, sem originais de outros jobs quando não necessários |

Quarentena é área segregada com acesso do coordenador, não servida como estático. Documentos liberados são obtidos por rota validada e identificador, nunca caminho enviado pelo usuário. Metadados ficam no banco; conteúdo no volume com chave gerada por hash/UUID. Usar gravação temporária, hash e rename antes de vincular registro pronto. Órfão não promove evidência; reconciliador identifica e trata sem apagar histórico automaticamente.

Volumes de teste e restauração devem ter nomes distintos e não herdar os `name:` globais atuais: mudar apenas o project name não isola esse Compose. Proibir acesso ao volume operacional em testes destrutivos; gate inspeciona configuração/destino e credenciais segregadas. Backups não ficam somente nesses volumes.

## 6. Configuração por variáveis

Apenas nomes, sem valores. Reutilizar `DATABASE_URL`, `MOTOR_POSTGRES_USER`, `MOTOR_POSTGRES_PASSWORD`, `MOTOR_POSTGRES_DB`, `MOTOR_API_PORT`, `ENVIRONMENT`, `LOG_LEVEL`, `SILLO_REVIEW_CSRF_SECRET`; controle usa `PREFECT_API_URL`, `PREFECT_API_DATABASE_CONNECTION_URL`, `PREFECT_POSTGRES_USER`, `PREFECT_POSTGRES_PASSWORD`, `PREFECT_POSTGRES_DB`, `PREFECT_MESSAGING_BROKER`, `PREFECT_MESSAGING_CACHE`, `PREFECT_REDIS_MESSAGING_URL`, `PREFECT_SERVER_DOCKET_URL`, `SILLO_ENABLE_SCHEDULES`, `SILLO_WORKER_ID`. Inventário completo atual está em 02, seção 9.1.

Nomes novos [PROPOSTO]: `RADAR_PNCP_BASE_URL`, `RADAR_HTTP_CONNECT_TIMEOUT`, `RADAR_HTTP_READ_TIMEOUT`, `RADAR_HTTP_MAX_ATTEMPTS`, `RADAR_SOURCE_RATE_LIMIT`, `RADAR_DOCUMENT_MAX_BYTES`, `RADAR_DOCUMENT_ALLOWED_HOSTS`, `RADAR_DOCUMENT_ROOT`, `RADAR_PARSER_INPUT_ROOT`, `RADAR_PARSER_OUTPUT_ROOT`, `RADAR_PARSER_TIMEOUT`, `RADAR_SETTINGS_VERSION`. Configuração operacional por ambiente; parâmetros comerciais/financeiros versionados em `commercial.system_settings`, sem segredos. Validar tipos/domínios ao iniciar; erro não escolhe valor favorável. `.env.example` só descreve contratos de nomes na futura implementação; `.env` real não entra em imagem/log/backup documental. Parser não recebe credenciais da aplicação.

## 7. Atualização e migrations

Procedimento futuro: identificar release/imagens e revisão Alembic atual; suspender novas tarefas/mutações; concluir ou registrar jobs parciais; obter backup validado; executar uma única tarefa de migration com lock e destino explícito; aplicar mudanças aditivas; validar integridade; atualizar API/workers; reabrir gradualmente. Não confiar em `IF NOT EXISTS` como prova de schema correto. Expansão → backfill idempotente → validação → mudança de leitura, sem drop nesta evolução inicial. `prefect-init` não deve disputar migrations com API/worker.

Rollback preferido é retornar a imagem anterior mantendo schema aditivo compatível. Se dados novos impedirem isso, interromper escrita e executar recuperação planejada em instância isolada, reconciliando eventos posteriores ao backup. Não prescrever downgrade destrutivo automático. Procedimento detalhado e gates em 16, Execução 3.

## 8. Backup e restauração

Meta RNF-014: RPO de até 24 horas e RTO de até 4 horas, sujeitos a ensaio. Copiar banco de domínio em formato lógico restaurável, metadados Prefect, manifesto de documentos/hashes, originais e versões de regras/imagens. Congelar mutações e processamento enquanto estabelece ponto consistente; finalizar arquivos antes do snapshot e registrar IDs/checkpoints. Não copiar diretório PostgreSQL ativo como se fosse backup consistente.

Guardar cópia independente, protegida e criptografada fora do host controlado. Manifesto inclui instante UTC, versões, hashes, revisões e resultado da verificação. Segredos têm canal de recuperação separado, não entram na documentação. Retenção definitiva será fixada em 13/16; nenhuma limpeza automática aqui.

Ensaio: subir destino isolado com nomes de volumes e credenciais próprios; restaurar ambos os bancos e documentos; manter schedules desligados; validar contagens/PK/FK, hashes e reconstrução de amostra de análises/revisões; medir tempo; reconciliar jobs em voo antes de habilitar Prefect. Redis não é fonte autoritativa da análise; não reativar AOF antigo sem reconciliar execução para evitar disparos duplicados. Nunca apontar teste de restauração aos volumes existentes.

## 9. Testes, logs e diagnósticos nos containers

Todos os comandos desta seção são **propostos; não executados nesta missão**. O serviço `tests`, o arquivo `compose.test.yml` e testes do Radar ainda não existem. Antes de torná-los executáveis, criar ambiente explicitamente isolado que não estenda volumes operacionais globais. Não executar estes exemplos no Compose atual supondo isolamento pelo nome do projeto.

```text
docker compose -f compose.test.yml run --rm tests pytest tests/ -q
docker compose logs --tail 200 api worker-http-pc1
docker compose exec api python -m pip check
docker compose config --quiet
```

Fixtures determinísticas substituem fonte oficial; relógio controlado cobre prazo/cotação. Testar crash após persistência e antes do checkpoint; JSON inválido; PDF excedente; falha DB; concorrência; cálculos Decimal e recuperação. Logs precisam de sanitização antes de compartilhamento. Testes antigos fazem TRUNCATE e não devem acessar o banco operacional; inspeção apenas do nome contendo `test` é insuficiente. Nenhum resultado de testes é alegado nesta etapa.

## 10. Implantação incremental

Fase 1: schema público e ingestão com schedules inicialmente suspensos, recorte explícito e consulta local. Fase 2: volumes/spool/parser e revisão de evidências. Fase 3: fornecedores/matching. Fase 4: finanças e ranking em modo assistido, sem ações externas. Fase 5: carteira e acompanhamento comercial. Só habilitar cada módulo após evidências dos RNF relevantes; manter rollback por versão e resultados anteriores legíveis. Limites iniciais do parser: 1 vCPU, 1 GiB, 120 s, 1.000 páginas; download: 50 MiB, três redirects, 10 s conexão/120 s total, conforme RNF-003/004. Capacidade é hipótese a testar, não garantida.

## 11. Legado e Windows

Manter scripts/rotas históricos sem reinterpretação dos dados. `daemon`, Selenium, Crawlee e coletores comerciais ficam opcionais no fluxo público, preservados. Mudança de profiles/dependências só será implementada em missão futura. Segundo host e caminho absoluto Windows não são pré-requisitos da V1.

Scripts Linux com LF via política futura de `.gitattributes`; binds relativos ao projeto, caminhos com espaços/acentos e permissões testados; volumes nomeados para dados. Em produção local não montar toda a pasta OneDrive como código mutável. Validar UID/GID e escrita de spool sob Docker Desktop sem exigir privilégio administrativo no parser. Imagens incluem Python e bibliotecas; host não exige Python/Node/PostgreSQL. Saúde, backup e portabilidade permanecem [NÃO-CONFIRMADO] por execução.

## 12. Docker do incremento e fronteira da ferramenta — revisado em R1

Revisão 0.3-R1-E2, 2026-10-13. [PROPOSTO] para produto. A ferramenta auxiliar offline executada nesta E2 possui manifesto próprio em ferramentas-r1 e amostras/r1/e2. Ela não testa API, worker, PostgreSQL, volumes, readiness, migrations ou backup. Engine acessível não comprova saúde da stack.

Produção do marco 2-H usa worker-http-pc1 e Prefect existentes, banco de domínio e Bronze PostgreSQL; sem novo serviço/volume de JSON/cache. Expansão dos schemas exige 16/G0–G6, com schedules inicialmente desligados. Pipeline futuro: validar contrato/política → obter lease → GET limitado → persistir bruto + normalização + checkpoint → revisão de identidade/qualidade → publicar projeções. Backoff não multiplica orçamento entre Prefect/HTTP; uma quota compartilhada por fonte/recurso inclui cada tentativa/redirect. Schedules propostos de seis horas/sobreposição de 10 dependem do contrato e quotas, não entram ativos por default.

| Tema | Contrato de implementação [PROPOSTO] e gate |
|---|---|
| Imagens/dependências | Fixar tag informativa, digest de manifesto/plataforma, versão do runtime, locks com hashes de dependências e revisão do código; conferir procedência do registry. Manifesto de release inclui hash dos arquivos de build/configuração sem segredos. Digest do produto ainda não obtido: recurso não está pronto para operar |
| Recursos | Piloto R1-P1 em Docker 4 vCPU/8 GiB (hipótese); worker inicial 1 CPU/512 MiB/128 PIDs, deadline 15 min/job, páginas limitadas pelo contrato; UI/API com orçamento restante documentado na release. Saturação pausa, não trunca. Metas calibradas por TST-043/055; não confundir com limites da ferramenta de 256 MiB |
| Egress e rede | HTTPS com TLS validado, hosts/rotas allowlist e proteção contra DNS/IP interno/metadados em cada conexão/redirect. Worker com saída restrita; bancos na rede interna, API/admin em loopback, sem porta adicional. `bridge` não comprova firewall; teste deve demonstrar destinos proibidos recusados |
| Identidade e filesystem | Não root, cap-drop ALL, no-new-privileges, raiz RO quando compatível; tmpfs limitado para temporários. Código/imagens imutáveis em produção; bind relativo somente dev/teste; nunca socket Docker ou credenciais de host. Entrada documental RO, saída estritamente própria |
| Saúde | API: conexão/readiness e revisão de schema compatível; worker: heartbeat, lease e checkpoint; Prefect: saúde de controle separada. Falha externa gera alerta e último sucesso, não derruba leitura local ou análise suficientemente fundamentada |
| Configuração | Nomes adicionais: `RADAR_HISTORY_ENABLED`, `RADAR_HISTORY_ALLOWED_HOSTS`, `RADAR_HISTORY_POLICY_VERSION`, `RADAR_HISTORY_MAX_REQUESTS_PER_JOB`, `RADAR_HISTORY_MAX_BYTES`, `RADAR_HISTORY_MAX_RECORDS`, `RADAR_HISTORY_JOB_TIMEOUT`. Tipos/domínios validados; valores comerciais em system_settings. Nenhum valor de segredo publicado |
| Logs/métricas | Fonte/recurso/job/versão, tentativas/bytes/duração, registros por qualidade, frescor/retificações/exclusões. Sem corpos/CPF/contatos pessoais/credenciais/URLs sensíveis; logs sanitizados. Painel exibe cobertura desconhecida e N=0 não calculável |
| Teste e restauração | Arquivo Compose independente futuro, sem extends do operacional nem volumes globais; usuário/credencial sem acesso operacional, allowlist da identidade real de destino e volumes antes de escrita. Relógio/fixtures fixos, sem rede externa. TST-070 injeta destino errado antes de começar; não acessar destino para descobrir por escrita |
| Atualização/retorno | Backup/restore com manifesto de autoridades, retenção e cotações; migration aditiva única com lock, nunca bootstrap automático. Retornar imagem anterior compatível e desligar incremento mantém novos registros; restore só isolado com reconciliação |
| Windows | Caminhos relativos ao checkout resolvidos/entre aspas; scripts LF. Ensaiar checkout com espaços/acentos e UID sem root. Host só Git e Docker/Compose; runtime/banco dependências dentro de imagens. Resultado auxiliar não aprova automaticamente portabilidade do produto |

Comandos de produto das seções anteriores continuam **Proposto; não executado nesta missão**. Não executar Compose operacional para esta revisão. Procedimentos auxiliares efetivamente executados ficam rotulados separadamente em ferramentas-r1; nenhuma coleta é autorizada na E2. Ver RT-011/012/013/014/015/017–019, ADR-020 e 23.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: requisitos de release, recursos, egress, readiness, teste/restore e configuração para 2-H, separados da validação auxiliar offline.

