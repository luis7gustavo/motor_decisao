# 05 — Requisitos não funcionais

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-10 (America/Sao_Paulo).  
Revisão: 0.2-R1, 2026-10-12 (America/Sao_Paulo). Revisado em R1-E1; extensões normativas na seção 5.
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: [PROPOSTO] — especificação para revisão do operador; nenhum benchmark, teste de aplicação ou procedimento operacional executado nesta missão.

## 1. Escopo, evidências e protocolo de validação

Os requisitos qualificam os comportamentos de [04 — requisitos funcionais](04-requisitos-funcionais.md), preservando a operação individual local e a decisão humana. Não fixam containers, tabelas ou contratos PNCP: essas escolhas pertencem à Execução 2. Os requisitos técnicos RT e a rastreabilidade até testes serão consolidados na Execução 3.

Todos os RNF abaixo são [PROPOSTO]. O [diagnóstico AS-IS](02-diagnostico-as-is.md) confirma recursos úteis, mas não atendimento integral destes requisitos:

| Evidência AS-IS | Status | Lacuna relevante |
|---|---|---|
| FastAPI, PostgreSQL, SQLAlchemy, Compose e Prefect no código/configuração | [CONFIRMADO] | Operação Docker e desempenho atuais [NÃO-CONFIRMADO] por execução |
| Histórico de revisão, snapshots e observabilidade da coleta | [CONFIRMADO] | Proveniência documental, regras públicas e reconstrução integral [PROPOSTO] |
| `/health` consulta o banco, mas responde HTTP 200 também quando degradado (`app/api/health.py`) | [PARCIAL] | Sucesso HTTP não comprova prontidão |
| Uso de tipos NUMERIC e conversões financeiras para `float` no motor legado | [PARCIAL] | Precisão decimal ponta a ponta não demonstrada |
| Testes de revisão limpam tabelas com TRUNCATE CASCADE e verificam apenas substring `test` no nome do banco (`tests/test_review_integration.py`) | [PARCIAL] | Guarda nominal não comprova banco descartável isolado |
| Imagem sem declaração USER e uso amplo de bind mount | [CONFIRMADO] | Contenção de documentos externos [PROPOSTO] |

**Metas de capacidade, latência, disponibilidade, recuperação e limites de recursos são iniciais e calibráveis; sua linha de base é desconhecida.** Não são resultados medidos nem SLA de fontes oficiais. Calibrar após piloto, registrar medição, versão, justificativa e efeito sobre os critérios. Invariantes de integridade, isolamento, preservação de histórico e não aprovação de pendências críticas são condições de aceite, não percentuais que possam ser relaxados para melhorar métricas.

### 1.1 Perfil de referência hipotético

| Aspecto | Hipótese para tornar as medições reproduzíveis |
|---|---|
| Host | Windows com Docker/Compose compatíveis; 16 GiB RAM, SSD e 100 GiB livres; não é inventário da máquina atual |
| Recursos do ambiente Docker | 4 vCPU e 8 GiB RAM disponíveis à stack medida; registrar limites realmente aplicados |
| Serviços ativos | Aplicação, PostgreSQL e componentes necessários à tarefa; declarar perfil, imagens, limites e eventual custo de Prefect/Redis/browser no relatório, sem ocultá-los |
| Base sintética | 10 mil contratações, 100 mil itens, 50 mil produtos de fornecedor, 10 mil análises versionadas e até 10 GiB de documentos |
| Uso concorrente | Um operador, até cinco requisições locais simultâneas e uma execução incremental; processamento documental sujeito a fila e limites próprios |
| Medição HTTP | Pelo menos 500 requisições por operação, após 20 de aquecimento; registrar p50/p95/p99, erros, cache, CPU/RAM e duração total |
| Repetição | Três rodadas com mesma fixture, parâmetros e versões; relatar todas, sem selecionar somente a melhor |

Se a implementação não couber nesse perfil, medir e propor revisão justificada da capacidade ou da estratégia. O perfil não autoriza remover serviços necessários para obter números favoráveis. Tempos de rede e da fonte externa são medidos separadamente; não entram como desempenho controlável da aplicação.

### 1.2 Convenções

Cada RNF informa prioridade MoSCoW, fase inicial, RF relacionados, meta e verificação futura. **Must** é condição de conclusão da fase indicada; o requisito continua aplicável nas seguintes. Fases seguem o documento 04. Nenhum procedimento descrito aqui deve ser executado durante esta missão documental. Não são incluídos comandos Docker operacionais; quando detalhados em 14/21, deverão receber o rótulo **“proposto; não executado nesta missão”**.

## 2. Requisitos

### RNF-001 — Controle da superfície de acesso

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1.  
**Rastreabilidade:** RF-001, RF-013 a RF-016, RF-019, RF-020, RF-029 a RF-034.

- **Meta:** operação individual restrita ao host local por padrão; 100% das operações que alteram dados devem passar pelos controles definidos para a interface/API. Requisições de navegador que alteram estado devem verificar origem e proteção contra CSRF; consulta GET não deve produzir efeito de negócio. Exposição em rede exige decisão explícita e autenticação/autorização antes da liberação.
- **Verificação:** inventariar rotas e portas; tentar alteração com origem não permitida, token ausente/inválido, identificador inválido e repetição. Todas as tentativas não autorizadas devem ser rejeitadas sem efeito persistido. Validar acesso de outro host no perfil local. Mecanismo de sessão/autenticação será decidido na Execução 2, sem pressupor multiusuário complexo.

### RNF-002 — Proteção de dados e segredos

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1.  
**Rastreabilidade:** RF-001, RF-005 a RF-007, RF-013 a RF-016, RF-027, RF-033.

- **Meta:** zero credenciais em repositório, imagens distribuídas, respostas de erro, logs e exportações; configurações sensíveis ficam fora dos artefatos versionados. Registrar apenas dados necessários à análise, com origem, finalidade e política de acesso/retenção. Dados públicos não tornam contatos pessoais ou documentos comerciais irrestritos. Backups com dados/segredos recebem proteção de acesso e criptografia quando armazenados fora do host controlado.
- **Verificação:** usar valores sentinela sintéticos em cabeçalhos, campos, URLs e mensagens de exceção; provocar falhas e examinar logs/respostas/exportações. Nenhuma sentinela sensível pode aparecer. Inspecionar contexto de build, bind mounts e permissões; confirmar que exportações omitem campos restritos e que o operador identifica os dados armazenados. Prazos de retenção e eventual obrigação legal serão definidos em 13/16 com fonte aplicável; não se presume prazo universal nesta etapa.

### RNF-003 — Download seguro de documentos externos

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 2.  
**Rastreabilidade:** RF-006, RF-007, RF-009.

- **Meta:** todo download passa por validação de destino, protocolo, tamanho e conteúdo. Permitir somente HTTP(S) segundo política explícita; validar TLS no HTTPS, host/IP resolvido e cada redirecionamento; rejeitar loopback, redes privadas, link-local e endpoints de metadados. Política de origem deve admitir apenas domínios oficiais aprovados ou exceções justificadas e registradas. Documento não pode definir caminho local, acessar credenciais nem sobrescrever arquivo anterior.
- **Limites iniciais calibráveis:** 50 MiB por arquivo, três redirecionamentos, 10 segundos para conexão e 120 segundos de duração total, medindo bytes reais inclusive com resposta sem Content-Length. Arquivo excedente/URL fora da política gera pendência e alternativa de revisão controlada; não é ignorado nem aprovado automaticamente.
- **Verificação:** fixtures com redirecionamento para endereço interno, mudança de resolução DNS, nome com travessia de diretório, extensão/MIME incompatíveis, arquivo truncado e fluxo acima do limite. Revalidar a conexão efetiva e aplicar restrição de saída compatível com a política para impedir contorno entre resolução e conexão. Aceite: nenhuma leitura de rede/local proibida; evidência e motivo de recusa disponíveis. Contratos e lista de origens serão fechados em 10, sem inventar endpoints.

### RNF-004 — Contenção do processamento documental

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 2.  
**Rastreabilidade:** RF-006 a RF-009.

- **Meta:** 100% dos arquivos externos são tratados como não confiáveis. Parser opera sem privilégios, sem credenciais e sem acesso de rede, com acesso somente aos arquivos necessários e saída controlada. Conteúdo ativo, anexos executáveis, ações JavaScript e links não são executados. Original, hash e estado de processamento permanecem identificáveis; suspeitos/inválidos seguem para quarentena e revisão.
- **Limites iniciais calibráveis por tarefa:** 1 vCPU, 1 GiB RAM, 120 segundos e 1.000 páginas; fila deve impedir concorrência acima dos recursos reservados. Exceder limite produz falha controlada ou solicitação de tratamento especializado, sem concluir que não há exigências. OCR fica fora do desempenho textual e depende de decisão posterior.
- **Verificação:** corpus controlado de PDFs malformados, cifrados, com imagens, objetos expansivos e conteúdo ativo. Demonstrar ausência de rede/escrita indevida, término ao exceder recursos, continuidade da API e status de erro/pendência. Detalhar mecanismo de isolamento em 07/21 e testes em 14; a imagem atual não comprova esse atendimento.

### RNF-005 — Proveniência e auditoria preservadas

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1; ampliado nas fases 2–5.  
**Rastreabilidade:** RF-003, RF-004, RF-006 a RF-009, RF-015, RF-016, RF-019, RF-027, RF-030 a RF-034.

- **Meta:** 100% das análises e decisões guardam identidade, versão dos insumos/regras, instante, autoria lógica e motivo. Campos extraídos apontam para documento/hash, página e trecho quando disponíveis; dado estruturado aponta para fonte e campo. Instantes técnicos preservam fuso/offset ou UTC e são exibidos conforme contexto; datas sem horário não ganham horário fictício. Correções adicionam evento e não apagam o anterior.
- **Verificação:** selecionar todos os positivos do piloto e amostra de negativos/pendentes; reconstruir suas fontes. Alterar edital/cotação/parâmetro e comprovar preservação da versão anterior. Tentar alteração sem motivo/versão quando exigidos e confirmar rejeição. Comparar eventos perto da meia-noite e mudanças de fuso; nenhum prazo deve ser recalculado silenciosamente por interpretação ambígua. Retenção e arquivamento serão compatibilizados em 13/16 sem eliminação automática de histórico nesta missão.

### RNF-006 — Disponibilidade local e prontidão verificável

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1.  
**Rastreabilidade:** RF-002, RF-005, RF-029, RF-033.

- **Meta inicial calibrável:** disponibilidade local >=99% durante janelas de operação declaradas no piloto, medida a cada minuto; excluir apenas host desligado e manutenção previamente registrada. Falhas inesperadas contam como indisponibilidade. Readiness deixa de indicar pronto em até 60 segundos após perda de dependência essencial; a consulta local deve continuar quando somente PNCP estiver indisponível.
- **Verificação:** registrar início/fim das janelas e razão das exclusões; razão de sondagens prontas/sondagens previstas produz a métrica. Injetar falha de banco e de fonte externa separadamente. Falha do banco deve impedir indicação enganosa de prontidão; falha externa deve sinalizar desatualização sem derrubar a consulta local. A separação de liveness/readiness e dependências essenciais será definida em 21.

### RNF-007 — Resiliência da integração oficial

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1.  
**Rastreabilidade:** RF-001, RF-004, RF-005, RF-006, RF-028.

- **Meta:** nenhuma falha de rede pode apagar a última versão válida ou ser interpretada como conjunto vazio completo. Retry é limitado, com backoff, jitter, timeout, respeito ao Retry-After válido e controle de taxa por fonte. Erros permanentes não entram em repetição infinita. Limites publicados pela fonte prevalecem sobre hipóteses locais.
- **Parâmetros iniciais calibráveis:** no máximo três tentativas totais por requisição; timeout de conexão de 10 segundos e leitura de 60 segundos para JSON; intervalo base de 2 segundos, exponencial, com jitter e teto configurado. Espera indicada pela fonte além da janela da execução deve reagendar o trabalho, sem manter chamada aberta indefinidamente. Downloads obedecem também RNF-003.
- **Verificação:** simular timeout, conexão interrompida, 429 com Retry-After, 5xx, 4xx permanente e resposta estruturalmente inválida. Confirmar contagem limitada, checkpoint íntegro, falha visível, nenhuma perda de dado anterior e retomada posterior. Valores finais dependem da verificação oficial em 10; não representam rate limit PNCP conhecido.

### RNF-008 — Idempotência e integridade transacional

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1; ampliado nas fases 2–5.  
**Rastreabilidade:** RF-003, RF-004, RF-006, RF-014, RF-015, RF-029 a RF-034.

- **Meta:** repetir a mesma entrada/evento dez vezes, inclusive após interrupção, produz uma única versão lógica por identidade e conteúdo, sem oportunidades/decisões ativas duplicadas. Cada alteração de estado e respectivo histórico são atômicos. Concorrência rejeita versão obsoleta ou reconcilia explicitamente; não aceita perda silenciosa de atualização.
- **Verificação:** reprocessar fixture completa, falhar antes/depois da persistência de página e executar duas submissões concorrentes. Contagens lógicas e hashes permanecem iguais quando conteúdo não mudou; retificação distinta cria versão vinculada. Confirmar integridade de referências e que transação revertida não deixa estado sem histórico. Algoritmo/chaves serão especificados em 08/10, preservando identidade oficial validada.

### RNF-009 — Desempenho de consulta e análise

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1; análise ampliada na fase 4.  
**Rastreabilidade:** RF-002, RF-005, RF-020 a RF-024, RF-026, RF-029, RF-033.

- **Metas iniciais no perfil 1.1:** p95 <=2 segundos para busca paginada/detalhe local; p95 <=3 segundos para painel de carteira; p95 <=5 segundos para simulação de uma unidade de julgamento com até 100 itens e três cenários, sem rede/OCR. Tarefas longas devem confirmar aceite/enfileiramento em até 2 segundos p95 e permitir consultar progresso.
- **Verificação:** executar protocolo 1.1 com filtros seletivos e amplos, base completa e ingestão concorrente. Registrar tamanho da página (hipótese: 50 registros), número de cenários, estado de cache e serviço externo separado. Não considerar timeout/erro como resposta rápida válida; relatar taxa de erro, com meta de zero erro funcional na fixture válida. Se extrapolar, medir gargalo antes de propor cache, banco adicional ou serviço novo.

### RNF-010 — Integridade financeira decimal

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 4.  
**Rastreabilidade:** RF-020 a RF-024, RF-027, RF-032.

- **Meta:** usar Decimal ponta a ponta para entradas, cálculo, persistência e serialização monetária, sem conversão intermediária para float. Conforme documento 04, aceitar custo/preço unitário e quantidade com até seis casas, conservar pelo menos oito casas decimais nos intermediários e apresentar totais BRL em duas casas com ROUND_HALF_UP. Precisão do contexto deve suportar os valores máximos aceitos e ser documentada; entrada acima de limites não é truncada silenciosamente.
- **Integridade:** política versionada define pontos de quantização, rateio e distribuição determinística de resíduos. Soma das linhas apresentadas deve reconciliar com total contratual na precisão da política; todo ajuste fica demonstrável. Nulos não viram zero; divisores nulos e equações inviáveis produzem “não calculável” com motivo.
- **Verificação:** fixtures independentes com decimais fracionários, taxas sobre receita, rateio de centavos, quantidades altas e pequenas, receita zero, custos ausentes e valores de limite. Comparar cálculo esperado manual independente, representação persistida, exportação e UI; nenhuma diferença de centavo inexplicada é aceita. Fórmulas financeiras e casos detalhados serão desenvolvidos em 12, sem importar diretamente a heurística de revenda.

### RNF-011 — Manutenibilidade e contratos entre módulos

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1.  
**Rastreabilidade:** RF-001 a RF-009, RF-017 a RF-028.

- **Meta:** integração oficial, extração, regras e cálculos devem ter contratos documentados e dependências explícitas. Alterar o parser de uma fonte não exige alterar o núcleo financeiro; alterar peso não exige editar código de UI ou payload bruto. Dependências e versões necessárias à reprodução da imagem devem ser identificáveis, sem depender de instalações Python/Node/PostgreSQL do host.
- **Verificação:** inspeção de dependências e exercício com fixture de fonte alterada; executar contratos afetados e confirmar preservação dos demais. Revisão de mudança verifica 100% dos contratos públicos novos/alterados com entradas, saídas e falhas documentadas. Mecanismo de fixação de dependências e fronteiras de módulos serão decididos em 07/21; não se exige microsserviço para cumprir isolamento lógico.

### RNF-012 — Testabilidade e isolamento do ambiente de validação

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1.  
**Rastreabilidade:** RF-001 a RF-034, especialmente RF-003, RF-004, RF-021, RF-025 e RF-030.

- **Meta:** testes executáveis em container, com fixtures sintéticas e fonte HTTP substituível, relógio controlável e parâmetros explícitos. Suites determinísticas não dependem de PNCP disponível nem enviam mensagens/lances. Teste destrutivo só usa instância/volume e credenciais descartáveis separados dos dados operacionais; substring `test` não basta. Credencial de teste não acessa banco operacional e o projeto de teste não reutiliza volumes de nome global fixo.
- **Verificação:** antes da primeira escrita, validar identidade do servidor/banco/volume e permissões contra lista explícita de destinos de teste. Configuração apontada ao operacional deve falhar antes de TRUNCATE/migration. Checar que cada bloqueio crítico, transição proibida, cálculo-limite, falha de integração e regra de segurança tenha caso positivo/negativo apropriado; toda falha impede aceite. Cobertura de linhas pode auxiliar diagnóstico, mas não substitui esses casos. Plano e comandos propostos ficam em 14/21; não executados nesta missão.

### RNF-013 — Observabilidade operacional

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1.  
**Rastreabilidade:** RF-001, RF-004 a RF-007, RF-014, RF-026, RF-027, RF-033.

- **Meta:** 100% das execuções têm ID de correlação, início/fim/estado, fonte, contagens lidas/aceitas/rejeitadas, tentativas, checkpoint e motivo de falha. Métricas incluem atraso de sincronização, duração, backlog, extrações pendentes e recursos; logs estruturados ficam acessíveis pelos containers sem revelar segredo. Falha ou dado obsoleto deve aparecer no painel em até 60 segundos após sua detecção local.
- **Verificação:** injetar falha conhecida e rastrear execução → registros → erro → sinal operacional usando o mesmo ID. Conferir reconciliação das contagens e redaction de RNF-002. Relógio e timestamps devem permitir ordenar eventos. Notificação externa não é obrigatória nem autorizada por este requisito; canal padrão é a operação local.

### RNF-014 — Backup e recuperação demonstráveis

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1; documentos incluídos na fase 2.  
**Rastreabilidade:** RF-003 a RF-007, RF-015, RF-016, RF-027, RF-030 a RF-034.

- **Metas iniciais calibráveis:** RPO <=24 horas para dados persistidos e RTO <=4 horas após ambiente Docker de destino disponível e acesso às cópias. Cobrir banco de aplicação, documentos originais/evidências e configurações/versionamentos necessários à reconstrução; preservar metadados Prefect existentes conforme dependência identificada. Manter pelo menos uma cópia independente do volume operacional, com acesso protegido e verificação de integridade.
- **Verificação:** restauração em destino isolado, nunca sobre o volume ativo; reconciliar contagens, hashes, vínculos de documento e amostra de análises/decisões. Registrar instante da última cópia consistente, ponto recuperado, tempo total e falhas. Backup que termina sem erro não é prova de restauração. Periodicidade inicial de ensaio: mensal e após mudança relevante no procedimento. Estratégia de consistência entre banco/arquivos e retenção será definida em 21/16.

### RNF-015 — Usabilidade da revisão individual

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 2; consolidado na fase 5.  
**Rastreabilidade:** RF-002, RF-007 a RF-012, RF-018 a RF-024, RF-029 a RF-034.

- **Metas iniciais calibráveis:** em cinco tarefas representativas, operador deve concluir pelo menos quatro sem SQL, logs ou intervenção técnica; 100% das ações críticas apresentam consequência, exigem motivo quando previsto e mostram sucesso/erro. A partir de uma conclusão, abrir sua evidência em até duas ações de navegação. Campos de formulário têm rótulos, foco visível e erros textuais; severidade não depende apenas de cor.
- **Verificação:** roteiro inclui localizar oportunidade, abrir trecho, corrigir matching, simular lote e registrar decisão/pendência. Registrar tempo, erros e necessidade de ajuda; comparar com MN-01 do DAN. Navegação principal e submissão devem funcionar por teclado, com bloqueio/pendência claramente diferenciados. Um piloto com operador único é evidência limitada: registrar isso, sem generalizar para população de usuários.

### RNF-016 — Explicabilidade e comunicação de incerteza

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 2; ampliado na fase 4.  
**Rastreabilidade:** RF-010, RF-011, RF-017 a RF-019, RF-021 a RF-028, RF-029.

- **Meta:** 100% das classificações, bloqueios, pendências e pontuações exibem justificativa, fonte/premissa e versão. Score apresenta componentes, pesos, cobertura e penalizações; não transforma bloqueio em penalização compensável. Classe A/B/C/D, resultado analítico e estado comercial são campos distintos. Dado desconhecido continua visível e não gera afirmação positiva.
- **Verificação:** auditoria de todos os positivos do piloto conforme MN-07/MN-08 e casos adversos com classe D, cotação vencida, incompatibilidade e lote incompleto. Reproduzir o resultado com snapshot e versão; saída determinística deve coincidir, preservada a política numérica. Exibir “não calculável” onde necessário e diferenciar cenário de resultado realizado. Fórmula global pertence a 09; este RNF não inventa pesos nem limiar de participação.

### RNF-017 — Evolução arquitetural e preservação do legado

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1.  
**Rastreabilidade:** RF-003, RF-004, RF-013 a RF-016, RF-027, RF-029 a RF-034.

- **Meta:** novas entidades de domínio usam o PostgreSQL existente da aplicação, sem duplicar banco/cache/orquestrador sem necessidade e ADR. Preservar 100% dos IDs, históricos e registros legados incluídos na linha de base de migração. Estados de revenda não são reinterpretados como estados licitatórios. Mudança de schema/configuração deve ter procedimento de evolução, validação e recuperação documentado antes da aplicação futura.
- **Verificação:** em cópia isolada, comparar contagens, chaves, vínculos e amostra de histórico antes/depois da migração proposta; executar leitura legada relevante. Registrar exceções justificadas e mapeamentos explícitos. Prefect é ponto de partida; troca de orquestração, novo serviço ou frontend React exige análise/ADR na Execução 2. Migração e ensaio de rollback serão especificados em 16; nada é aplicado nesta missão.

### RNF-018 — Processamento incremental e atualidade

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1.  
**Rastreabilidade:** RF-001, RF-003 a RF-005, RF-006, RF-027, RF-033.

- **Meta:** persistir checkpoint somente após confirmação durável da unidade de trabalho; retomada deve processar alterações com janela de sobreposição/reconciliação definida pelo contrato oficial. Execução repetida sem mudança não produz versões de negócio adicionais. Filtros/limites e backlog ficam visíveis; não se declara cobertura nacional completa se a execução cobriu apenas um recorte.
- **Metas iniciais calibráveis:** consultar mudanças do recorte ativo a cada seis horas e, com fonte disponível, tornar 95% das mudanças retornadas visíveis em até 30 minutos após a obtenção. Processar lote local sintético de 1.000 contratações/10 mil itens em até 10 minutos no perfil 1.1, excluindo download/extração e tempo externo. Exibir aviso de atualidade vencida quando a última sincronização válida exceder a janela configurada.
- **Verificação:** páginas repetidas, item atrasado, retificação, exclusão/situação alterada conforme contrato e interrupção entre páginas. Contagens reconciliadas, nenhuma omissão silenciosa e revalidação das análises afetadas. Intervalos e estratégia serão confirmados em 10 frente a limites oficiais; sincronização periódica não dispensa conferência imediatamente anterior à decisão humana.

### RNF-019 — Portabilidade Windows/Docker

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1.  
**Rastreabilidade:** RF-001 a RF-034; restrição Docker-first.

- **Meta:** desenvolvimento, execução, testes e diagnóstico devem funcionar via Docker/Compose sem instalar Python, Node.js ou PostgreSQL no host. Nenhum componente necessário depende de caminho absoluto pessoal Windows. Scripts Linux usam LF; caminhos, mounts, permissões e nomes de arquivos toleram checkout com espaços/acentos. Volumes de dados sobrevivem à substituição de containers.
- **Verificação:** instalação futura em host Windows limpo com Docker/Compose compatíveis, checkout em caminho diferente e variáveis configuradas a partir de nomes documentados. Validar leitura/escrita de documentos, permissões, fim de linha e reinício controlado preservando hashes/contagens. Repetir execução funcional em segundo ambiente Docker compatível; registrar versões, recursos e limitações. Esse ensaio será detalhado em 21/14; inspeção estática atual não equivale a aprovação de portabilidade.

### RNF-020 — Configuração e reprodução de decisões

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 2; ampliado na fase 4.  
**Rastreabilidade:** RF-011, RF-018 a RF-028, RF-034.

- **Meta:** 100% dos parâmetros que alteram logística, matching, elegibilidade, finanças ou ranking possuem versão, unidade, domínio permitido, responsável e data de vigência. Configuração inválida é rejeitada antes de processar; configuração nova cria análise nova e não modifica resultado histórico. Uma execução deve identificar versões de software, regras e parâmetros suficientes à reprodução.
- **Verificação:** repetir fixture/snapshot com mesma versão e obter os mesmos valores/classificações; alterar um parâmetro válido e confirmar nova versão; enviar peso negativo, soma inconsistente, taxa inválida e unidade incompatível e confirmar rejeição explicada. Distinguir alteração de parâmetro comercial de segredo operacional. Calibração de metas destes RNF também deve manter registro e não alterar retroativamente o resultado medido.

### RNF-021 — Capacidade, armazenamento e falha controlada

**Status:** [PROPOSTO]. **Prioridade:** Must. **Fase inicial:** 1; documentos na fase 2.  
**Rastreabilidade:** RF-004 a RF-007, RF-014, RF-027.

- **Meta inicial calibrável:** sinalizar armazenamento acima de 80% e suspender novas tarefas que precisem gravar quando a projeção exceder 90% da capacidade reservada, preservando consulta e diagnóstico sempre que tecnicamente possível. Fila tem limite configurado e recusa controlada; arquivo temporário não é promovido a original antes de download completo e hash verificado. Memória de ingestão deve depender de página/lote configurado, sem carregar todo o histórico.
- **Verificação:** simular disco cheio, arquivo parcial, limite de fila e base com dez vezes o tamanho do lote; registrar memória máxima e ausência de perda/corrupção. Nenhum histórico/documento é apagado automaticamente para liberar espaço sem política aprovada; temporários órfãos são distinguíveis e sua limpeza futura é restrita a área própria. Metas de crescimento/retenção serão refinadas em 08/21 após medição real.

## 3. Cobertura dos temas obrigatórios

| Tema da seção 8 do prompt | RNF |
|---|---|
| Segurança | RNF-001, RNF-003, RNF-004 |
| Rastreabilidade | RNF-005, RNF-020 |
| Disponibilidade local | RNF-006 |
| Resiliência da integração PNCP | RNF-007, RNF-018 |
| Idempotência | RNF-008 |
| Desempenho | RNF-009, RNF-021 |
| Integridade financeira | RNF-010 |
| Manutenibilidade | RNF-011, RNF-020 |
| Testabilidade | RNF-012 |
| Observabilidade | RNF-013 |
| Proteção de dados e segredos | RNF-002 |
| Backup e recuperação | RNF-014 |
| Usabilidade | RNF-015 |
| Explicabilidade | RNF-016 |
| Evolução arquitetural | RNF-017 |
| Capacidade de processamento incremental | RNF-018, RNF-021 |
| Compatibilidade com Windows/Docker | RNF-019 |
| Segurança no download/processamento de documentos externos | RNF-003, RNF-004 |

## 4. Pendências de calibração e continuidade

1. Confirmar hardware, espaço e volume real; executar benchmark somente em futura fase de implementação autorizada. Metas atuais não foram medidas.
2. Validar contratos, paginação, limites oficiais e tratamento de retificações em 10; recalibrar RNF-007/RNF-018 sem comprometer preservação e atualidade visível.
3. Definir em 07/21 mecanismos de contenção documental, acesso local, readiness, dependências reproduzíveis, perfis e backups. Este documento expressa resultados exigidos, não arquitetura consolidada.
4. Fixar limites numéricos, fórmulas e reconciliação em 12, coerentes com a política Decimal do documento 04/RNF-010.
5. Definir retenção, proteção e recuperação em 13/16/21, preservando histórico e diferenciando dados oficiais, comerciais e pessoais.
6. Na Execução 3, cada RNF deve ser ligado a RT, componente e caso de teste em 00-trd/17. O aceite de qualidade exige evidência produzida na implementação; a presente entrega comprova apenas especificação mensurável.

## 5. Extensões para descoberta de fornecedores — revisado em R1

Todos os 21 IDs preservados, nenhum RNF novo. As obrigações já cobrem integração, privacidade, qualidade e reprodução; extensões abaixo acrescentam perfil e verificações de RF-028/035–038. Status [PROPOSTO], **baseline desconhecida e nenhuma meta medida** em todas as linhas. Prioridade Must do controle quando a funcionalidade correspondente é habilitada, sem promover o incremento histórico a Must universal. Fase de aplicação: 2-H; contato/cotação na fase 3. Rastreabilidade adicional às RN-005/006/026/028/033–037; RT/TST propagados na E2 em 00-trd/14/17.

Perfil **R1-P1**, proposto e calibrável: 1 categoria, 1 operador, até 10 mil observações históricas, 1.000 estabelecimentos, 100 itens pesquisados e 500 eventos de contato/cotação sintéticos; infraestrutura do perfil 1.1. Consulta local com página de 50 registros, cinco requisições simultâneas, uma ingestão por vez. Não confundir esse perfil de teste do produto com o limite da amostra E1 (30 GET/1.000 registros processados). Não executar suíte de aplicação nesta revisão.

| RNF estendido | Meta/aceite específico e perfil | Método futuro de medição |
|---|---|---|
| RNF-002 — proteção/retencão | Zero CPF/sócio/contato pessoal desnecessário ou segredo em logs/perfis de amostra. Produção: 100% dos conjuntos pessoais com finalidade, fundamento, acesso, revisão de retenção e responsável (ADR-016), antes do piloto | Fixture R1-P1 com sentinelas em campos livres/erros/URLs, inspeção de logs/exportação e inventário; dados públicos não dispensam proteção |
| RNF-005 — proveniência | 100% dos atributos usados em comparável/candidato com fonte/recurso/chave/campo/consulta/versão ou autoria manual; divergências e exclusões auditadas. Perfil R1-P1 | Reconstruir todos os comparáveis aceitos da fixture e todos os positivos do piloto; razão completos/avaliados, N=0 não aprova |
| RNF-007 — contrato/limites | Zero operação fora de contrato público validado; limites futuros por fonte/recurso configurados com versão, retries/redirects contados no orçamento total do job. Quota oficial mais restritiva prevalece; sem quota publicada, limite local conservador não implica SLA | HTTP sintético 429/Retry-After, redirect, 403, timeout e erro permanente; conferir tentativas/checkpoint. Parâmetros do RNF original são hipóteses do produto, não limites oficiais e não sobrepõem a quota E1 |
| RNF-008 — identidade/deduplicação | Dez replays do mesmo conjunto R1-P1 não alteram contagem lógica/IDs; duplicatas comprovadas entre fontes contam uma vez por etapa; raiz nunca funde filiais. Zero soma resultado+contrato como execução | Fixture com replicação entre fontes, múltiplos resultados, contrato vinculado, retificação e possível duplicidade; comparar identidades e proveniências após concorrência/interrupção |
| RNF-009 — desempenho | Busca/detalhe de comparáveis/candidatos p95<=2 s no R1-P1, sem tempo externo; manter erro funcional zero no fixture válido. Meta calibrável, não desempenho observado | 500 consultas após 20 de aquecimento, três rodadas, p50/p95/p99/erros/CPU/RAM registrados; não considerar timeout resposta rápida |
| RNF-010 — precisão | Valores e quantidades históricas seguem Decimal, sem corte de frações por divergência Integer/Decimal do manual; nenhum total global convertido arbitrariamente em unitário. R1-P1 | Fixture com frações, zero, null, inválido e tipos discordantes; conflito contratual falha explicitamente antes da publicação do comparável |
| RNF-011 — contratos | 100% dos recursos habilitados com base/método/rota/parâmetros/security/paginação/tipos/semântica/hash verificados, dicionário e fixtures; contrato em dúvida mantém recurso desabilitado | Revisão do contrato por recurso e fixture de remoção/adição/troca de tipo; mudança no adapter não altera cálculo financeiro |
| RNF-012 — testes | Toda validação programática em container fixado e offline para fixtures, com entrada RO/saída exclusiva. Sem import do produto nas ferramentas R1; testes futuros de produto com banco/credenciais/volumes próprios | Destino operacional deve ser recusado antes de escrita; reprodução de fixture sintética em duas execuções/ambientes compatíveis, incluindo CNPJ válido/inválido/ausente nos dois formatos |
| RNF-013 — qualidade observável | 100% dos jobs mostram N por recurso, ausentes/null/inválidos/não aplicáveis/válidos, utilizáveis/rejeitados/pendentes e duplicidades como subconjunto; categorias não são somadas indevidamente. Alertas no prazo original <=60 s | Fixture com cada categoria e resposta parcial; reconciliação de contagens/bytes/tentativas, registro das exclusões e aviso de cobertura. Nenhuma métrica pessoal em log |
| RNF-016 — explicabilidade | 100% dos candidatos/agregados têm razões, etapa/natureza, janela, qualidade e limitações; zero alegação automática de capacidade/execução ou score de oportunidade alterado por histórico | Revisar saídas R1-P1 e casos sem cotação/sem histórico; distinguir documentação/amostra/suporte. Agregado insuficiente mostra motivo, não valor confiável |
| RNF-018 — atualidade | Checkpoint durável e janela conforme recurso; data de resultado, assinatura, publicação, atualização e consulta distintas. Retificação invalida 100% dos dependentes identificados, sem apagar histórico/cotação independente | Fixture de cancelamento/retificação/timeout/retorno a conteúdo antigo; frescor com relógio fixo. Cadência de seis horas é hipótese do produto, depende de quota/contrato; não medir atraso por publicação apenas |
| RNF-019 — Docker | Imagem por digest e plataforma, runtime/dependências identificáveis; zero dependência de linguagem instalada no host. Caminhos relativos com espaços/acentos, LF e escrita não root devem passar | Executar em Docker Desktop e outro ambiente compatível; registrar hashes, comandos sanitizados, exit code/duração e limites. Ausência de Engine aqui significa não executado, não aprovação |
| RNF-020 — políticas | 100% de regras de comparabilidade, outliers, suficiência, precedência e ordenação com versão/autor/escopo; mesma fixture/configuração gera mesmo resultado. Não alterar fórmula global v0.1 | Replay R1-P1; alterar parâmetro válido produz versão nova; entrada inválida é rejeitada. Preservar estado e fontes do resultado anterior |
| RNF-021 — recursos | Processar por página com memória limitada; saturação não vira sucesso nem perde último dado válido. Para ferramenta E1: 1 CPU, 256 MiB, 64 PIDs, raiz RO, temporário 32 MiB, 900 s no máximo; limites de dados do manifesto | Fixture de bytes acima de 2 MiB/resposta ou 20 MiB total, registros acima de 1.000 e deadline: interromper/relatar parcial. Produto calibrará seus próprios limites no R1-P1, sem herdar os 30 GET |

RNF-001/003/004/006/014/015/017 permanecem aplicáveis sem nova obrigação: superfície, download/parser, consulta local, backup, usabilidade e legado já tratados. Particularização: disponibilidade da fonte histórica não pode ser dependência da consulta local ou da análise com insumos suficientes. Retenção não é um prazo inventado para todos os dados; definir por finalidade antes de piloto.

### Exceção documental Docker

A frase histórica da seção 1.2 que proíbe execução durante a missão continua válida para produto/stack/suíte. ADR-020 concilia o prompt R1 v2.0: autoriza somente ferramentas auxiliares isoladas. Na E1 não foram executadas, porque o Engine estava inacessível. A E2 teve Engine acessível e validação documental auxiliar offline, com evidência própria em ferramentas-r1; não executou TST da aplicação. [Roteiro](ferramentas-r1/README.md) e [manifesto](amostras/r1/manifesto.md) discriminam procedimentos propostos, sem afirmar imagem/digest efetivamente usado.

## Histórico de revisões

2026-10-12 — 0.2-R1: estendidos RNF existentes para contratos, identidade/qualidade, proteção, métricas e Docker, sem novos IDs. Metas, perfil, baseline, medição e fase explícitos; validações automatizadas não executadas.

2026-10-13 — 0.3-R1-E2: rastreabilidade técnica e distinção temporal E1/E2 atualizadas; metas de produto não medidas.

