# 19 — Decisões arquiteturais — consolidação final

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Revisão: 0.2-R1, 2026-10-12 (America/Sao_Paulo). Revisado em R1-E1/E2; propagação documentada ao final.

Versão: 0.1. Produção: 2026-10-11 (America/Sao_Paulo). Inspeção base: 2026-10-10, reconferida em 2026-10-11.
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: rascunho para revisão do operador. Decisões [PROPOSTO] para a especificação, sem implementação nesta missão.

## 1. Como interpretar este registro

“Aceita para especificação” fixa uma convenção dos documentos; não significa código alterado, infraestrutura operante, migração autorizada ou homologação do produto. Evidências [CONFIRMADO] no código são estáticas e correspondem ao commit base. Arquitetura, proposta física e contratos acessíveis estão detalhados em 07–12/21, com limitações de acesso em 10. Novas decisões devem receber IDs sequenciais, sem reutilizar IDs nem apagar alternativas e consequências anteriores.

| ID | Decisão | Estado | Continuidade |
|---|---|---|---|
| ADR-001 | Execução documental, retomável e restrita à etapa pendente | Aceita para especificação | Todas as execuções |
| ADR-002 | Evolução incremental de FastAPI e das camadas existentes | Aceita para especificação | 07 e 08, Execução 2 |
| ADR-003 | Banco de domínio existente e preservação do banco de metadados Prefect | Aceita para especificação | 08/21 na 2; 16 na 3 |
| ADR-004 | Prefect, Redis e Crawlee com responsabilidades justificadas | Aceita para especificação | 07/10/21, Execução 2 |
| ADR-005 | Comparação da interface sem pressupor migração para React | Fechada: Jinja2 no MVP | 07/21; ADR-013 |
| ADR-006 | Bloqueios, incerteza, logística, ranking e revisão independentes | Aceita para especificação | 09/11, Execução 2 |
| ADR-007 | Simulação decimal por unidade de julgamento e cronograma de caixa | Aceita para especificação | 12, Execução 2 |
| ADR-008 | Histórico imutável e revalidação sem apagar fatos externos | Aceita para especificação | 08 na 2; 14/16 na 3 |
| ADR-009 | Progresso como índice provisório até o README da Execução 3 | Aceita para especificação | Leitura de abertura da Execução 2 |
| ADR-010 | Contratos oficiais verificados e lacunas de acesso explícitas | Aceita para especificação | 10, Execução 2 |
| ADR-011 | MVP sem dependência de ML, BI externo ou histórico oficial futuro | Aceita para especificação | 07/09 na 2; 15 na 3 |
| ADR-012 | Documentos externos tratados como entrada não confiável | Aceita para especificação | 07/21 na 2; 13/14 na 3 |
| ADR-013 | Jinja2 no monólito modular e contenção documental | Aceita para especificação | 07/21; validação em 14 |
| ADR-014 | Identidades, versões e autoridade transacional | Aceita para especificação | 08/10; migração em 16 |
| ADR-015 | Políticas financeiras e ranking condicionado | Aceita para especificação | 09/11/12; testes em 14 |
| ADR-016 | Retenção por finalidade e preservação histórica | Aceita para especificação | 13/16 |
| ADR-017 | Precisão interna do score persistido | Aceita para especificação | 08/09/12, RT-008 |
| ADR-018 | Antecipação limitada de comparáveis e descoberta | Aceita condicionada | Marco 2-H/23 |
| ADR-019 | Estabelecimento, evento e relação comercial independentes | Aceita para especificação | 08/16/22/23 |
| ADR-020 | Ferramenta auxiliar efêmera e reprodução condicionada | Aceita para especificação | ferramentas-r1; E1 sem execução e E2 offline |

## ADR-001 — Execução documental, retomável e restrita à etapa pendente

**Contexto e conflito:** o requisito Docker-first descreve o sistema futuro, enquanto as seções 0.3/0.5 e 20 limitam a missão a documentação. O texto também permite consultar versões/configuração Docker e exige rotular todo comando Docker documentado como proposto. O checkout já contém trabalho da Execução 1; uma sessão nova não implica automaticamente iniciar a 2.

**Alternativas:** executar infraestrutura para comprovar funcionamento; refazer a documentação; ou retomar o primeiro entregável pendente mediante inspeção.

**Decisão [PROPOSTO]:** retomar a Execução 1 em `docs/licitacoes/`, atualizar o progresso após cada entregável e encerrar a sessão ao concluí-la. Preservar a branch `docs/radar-licita-especificacao` solicitada, o commit base e os arquivos anteriores. Os resultados históricos de consultas Docker permanecem registrados como evidência; comandos Docker que aparecerem como procedimentos receberão o rótulo “proposto; não executado nesta missão”.

**Justificativa:** obedece à precedência explícita e evita confundir planejamento com operação. A infraestrutura declarada não prova runtime saudável.

**Consequências:** aplicação, coletas, testes de integração, volumes e migrations não são executados. Não há commit obrigatório quando o mesmo checkout preserva os arquivos; não há push. A última conferência Git deve distinguir os arquivos preexistentes do repositório pai do escopo no repositório alvo. Referências: [02](02-diagnostico-as-is.md), [_progresso](_progresso.md).

## ADR-002 — Evolução incremental de FastAPI e das camadas existentes

**Contexto:** [CONFIRMADO] `app/main.py`, os serviços/repositórios, templates e migrations já oferecem API, revisão, persistência e trilha histórica. O diagnóstico identifica Bronze/Silver/Gold e código de coleta reais, mas não encontra o domínio público. O ORM não cobre o banco inteiro: `alembic/env.py` tem metadados não associados ao schema completo e predominam consultas SQL textuais.

**Alternativas:** reconstruir a aplicação; decompor imediatamente em microsserviços; ou manter a base e introduzir fronteiras lógicas próprias para o novo domínio.

**Decisão [PROPOSTO]:** manter FastAPI e evoluir a base existente com módulos de responsabilidade delimitada. Preservar o conceito Bronze/Silver/Gold, distinguindo registros oficiais, dados comerciais, fatos transacionais e projeções analíticas. Não assumir que as tabelas de revenda podem ser renomeadas para representar contratação/lote/item.

**Justificativa:** a matriz de reaproveitamento comprova valor em infraestrutura e padrões de revisão, sem comprovar compatibilidade de todas as regras com licitações.

**Consequências:** 07 deve comparar alternativas e seus custos; 08 deve justificar entidades/schemas e relações sem duplicar responsabilidade. Separação lógica não exige um container por módulo. Reutilização de código só será decidida após avaliar acoplamentos de SQL, domínio e side effects. Referências: [02, seções 5–9 e 12](02-diagnostico-as-is.md), RNF-011/017.

## ADR-003 — Banco de domínio existente e metadados Prefect preservados

**Contexto e divergência:** o requisito proíbe múltiplos bancos desnecessários, porém [CONFIRMADO] o Compose já contém `postgres`, com dados de domínio, e `prefect-postgres`, com metadados do orquestrador. Os volumes declarados têm identidades próprias. Isso diverge de uma leitura literal de “um único serviço PostgreSQL”.

**Alternativas:** fundir bancos e mover metadados; criar um novo banco Radar; ou preservar a separação existente e acrescentar o domínio público ao PostgreSQL da aplicação.

**Decisão [PROPOSTO]:** usar o banco existente da aplicação para os novos módulos; preservar o banco auxiliar do Prefect e ambos os conjuntos de dados. Não criar um terceiro banco de domínio. Novos schemas/tabelas e estratégia de migração serão propostos em 08; não são criados por este ADR.

**Justificativa:** manter a separação operacional já implementada atende à preservação de investimentos e evita uma consolidação destrutiva sem necessidade demonstrada.

**Consequências:** 21/16 devem identificar volumes, destinos de backup/restauração, evolução e rollback sem perda de IDs/históricos. Nome global fixo de volume exige atenção ao ensaio em outra stack. Existência em YAML não confirma volume instalado nem backup restaurável. Referências: `docker-compose.yml`, [02, seção 9](02-diagnostico-as-is.md), RNF-014/017.

## ADR-004 — Prefect, Redis e Crawlee com responsabilidades justificadas

**Contexto:** [CONFIRMADO] `pipelines/collection/prefect_flows.py` define flows; scripts fazem bootstrap/deploy; o Compose configura Redis como apoio à mensageria do Prefect. Crawlee implementa filas e políticas de requisição. Não se encontrou cache de domínio Redis implementado, além de diagnóstico de conectividade. Retries não estão declarados nos decorators dos flows inspecionados; clientes/Crawlee contêm mecanismos próprios.

**Alternativas:** trocar o orquestrador; adicionar APScheduler; reutilizar o plano de controle com adaptador de consulta oficial. Para Crawlee: exigir browser em toda fonte ou limitá-lo aos coletores que precisam dele.

**Decisão [PROPOSTO]:** reutilizar Prefect como ponto de partida. Manter Redis enquanto a configuração do orquestrador o requerer; cache de negócio depende de necessidade medida e nova decisão. Preservar Crawlee para coletas comerciais que o utilizem. Especificar o PNCP a partir de HTTP e do contrato público, sem impor browser ou fila distribuída inexistente.

**Justificativa:** evita duplicação de agendamento/cache e atribuição de funções não implementadas. “Orquestração equivalente” não autoriza troca silenciosa.

**Consequências:** 07/10/21 devem explicitar onde ficam retry, checkpoint, concorrência e limites para evitar multiplicação de retentativas. O `prefect-init` chama migrations no código existente; não é diagnóstico somente leitura e não foi executado. Redis/Crawlee podem ser reavaliados com evidência e novo ADR, preservando dados. Referências: [02, seções 5 e 9](02-diagnostico-as-is.md), RNF-007/018.

## ADR-005 — Comparar a interface sem pressupor migração para React

**Contexto:** [CONFIRMADO] a aplicação serve UI Jinja2 (`app/api/review.py`, `templates/review/`). Não há frontend React no repositório alvo; arquivos de outro projeto no diretório pai não comprovam capacidade do SILLO. A V1 prevista é local e inicialmente individual.

**Alternativas:** adaptar Jinja2; substituir por React/TypeScript; ou ampliar progressivamente a interação com o backend existente.

**Decisão [PROPOSTO], fechada na Execução 2 em 2026-10-11:** manter Jinja2 com melhoria progressiva no MVP, conforme comparação de 07 e ADR-013. Paginação e serviços de aplicação no backend; cálculo financeiro somente Decimal no servidor. React não é entrega existente nem necessária nesta arquitetura. Reavaliar mediante protótipo e evidência de complexidade de estado/usabilidade não resolvida por melhoria localizada, sem atribuir problemas de SQL ao frontend.

**Justificativa:** a interface atual já tem formulários, filtros e auditoria, mas ainda não demonstra usabilidade para o fluxo público.

**Consequências:** se React for escolhido, 21/D12 devem prever container próprio para desenvolvimento e produção local. Se Jinja2 permanecer, documentar gatilhos de reavaliação: metas de usabilidade não atendidas por solução simples, complexidade de estado interativo comprovada ou novos perfis de uso. A avaliação deve ser baseada em protótipo/medição futura, sem alegar benchmark nesta etapa. Referências: RF-029/033, RNF-009/015/019.

## ADR-006 — Separar bloqueios, incerteza, logística, ranking e revisão

**Contexto:** [CONFIRMADO] o motor de revenda usa margem, sinais comerciais e confiança; essas variáveis não demonstram habilitação, capacidade técnica, prazos ou capital de uma licitação. A similaridade textual também não comprova equivalência técnica.

**Alternativas:** adaptar pesos da heurística antiga e compensar falhas com score; ou aplicar regras impeditivas antes da priorização, mantendo evidências e decisão humana.

**Decisão [PROPOSTO]:** aplicar elegibilidade, viabilidade, priorização e revisão. Resultado de elegibilidade tem três valores: bloqueada, pendente e viável para priorização. Revisão, situação oficial, classe logística e estado comercial são dimensões independentes. Desconhecido não constitui evidência favorável. Presença de bloqueio ou pendência obrigatória impede ranking positivo.

**Justificativa:** um score alto não compensa incompatibilidade, prazo vencido, lote incompleto ou ausência de capital. Não apresentar probabilidade de vitória sem modelo calibrado.

**Consequências:** a fórmula logística inicial `v0.1-logistica` e suas fontes normativas são [04, seção 9](04-requisitos-funcionais.md#9-proposta-inicial-de-remote_delivery_score) e RN-012. Não renormalizar pesos sobre evidências presentes; exibir cobertura e intervalo. D prevalece quando há lacuna/conflito crítico, sem apagar presença local conhecida. A fórmula global, rubricas e três casos numéricos pertencem a 09 na Execução 2. Classe A e E07 continuam sujeitos à decisão humana; não enviam proposta. Referências: RF-010/011/017/018/025–027, RN-027/029–032.

## ADR-007 — Simulação decimal por unidade de julgamento e caixa

**Contexto:** o motor atual converte preços para `float` [CONFIRMADO] em `pipelines/decision_engine/build.py`; os custos de revenda não cobrem o ciclo contratual completo. Um item isolado pode ser lucrativo enquanto o lote obrigatório é inviável.

**Alternativas:** reutilizar aritmética e margem atuais; calcular item a item sem consolidação; ou especificar cálculo decimal e cenário completo por unidade de julgamento.

**Decisão [PROPOSTO]:** cumprir RN-007–009 e RN-016–021: Decimal de entrada a saída, até seis casas nos preços/custos unitários e quantidades, pelo menos oito casas nos intermediários e contexto adequado à magnitude. Totais BRL em centavos com ROUND_HALF_UP e rateio reconciliado. Separar referência oficial, proposta hipotética e receita recebida. Avaliar pico de capital a partir de desembolsos/recebimentos datados, sem pressupor adiantamento.

**Justificativa:** evita perdas por arredondamento, dupla contagem, omissão de item obrigatório e recomendação economicamente inviável.

**Consequências:** 12 deve definir limites máximos, pontos exatos de quantização, distribuição de resíduos, taxas/bases, margens, ROI, giro e cenários; esta diretriz não substitui essa matemática. Entradas ausentes não viram zero; denominador inválido produz “não calculável”. Parâmetros fiscais exigem validação aplicável ao caso. Referências: RF-020–024, RNF-010.

## ADR-008 — Histórico preservado e revalidação sem apagar fatos externos

**Contexto:** [CONFIRMADO] runs, snapshots e revisão com invalidação já existem, mas os estados atuais são de compra para revenda. Retificações, cotações vencidas e novos documentos precisam invalidar atualidade da análise pública sem reescrever o passado.

**Alternativas:** sobrescrever análise; mapear automaticamente aprovação de compra para participação; ou manter identidade, versões, análises congeladas e eventos auditados.

**Decisão [PROPOSTO]:** preservar padrões de auditoria e históricos existentes; criar semântica pública própria. Toda análise referencia versões de fontes, documentos, cotações, regras e parâmetros. Correções invalidam/substituem com motivo, sem exclusão de evidência. RN-032 é a fonte normativa dos 16 estados E01–E16; D10 a representa.

**Justificativa:** uma nova versão do edital deve bloquear recomendação desatualizada, mas não pode apagar uma participação ou entrega que efetivamente ocorreu.

**Consequências:** após participação, reabrir revisão da análise em paralelo ao estágio factual. Importação/reconciliação excepcional de fato externo exige autor, evidência, datas e sinalização de desvio, sem criar aprovação retroativa. 08/16 devem comparar as entidades atuais com as novas e proteger IDs; o detalhamento físico não é antecipado aqui. Referências: RF-004/027/030/034, RN-026/028/030–032, RNF-005/008/020.

## ADR-009 — Progresso como índice provisório até o README final

**Contexto e conflito:** a seção 0.1 manda ler `README.md` nas Execuções 2/3, enquanto 0.6 reserva `docs/licitacoes/README.md` à Execução 3. O README principal do SILLO já existe e não deve ser alterado para sugerir migração implementada.

**Alternativas:** antecipar um README de consolidação; bloquear a Execução 2 pela ausência esperada; ou distinguir o README atual do índice final e usar o controle provisório.

**Decisão [PROPOSTO]:** na abertura da Execução 2, ler o README principal do repositório como contexto histórico, `_progresso.md`, 18, 19 e os documentos de dependência. Enquanto o README específico não existir, o checklist do progresso é o índice documental provisório. Na Execução 3, produzir o README de licitações na ordem especificada.

**Justificativa:** resolve explicitamente a ambiguidade sem antecipar consolidação, sobrescrever histórico ou interromper uma etapa que possui seus insumos necessários.

**Consequências:** não criar PRD/TRD/README final nesta execução. Se um README de licitações for criado legitimamente antes de uma retomada, lê-lo também, sem ignorar o controle. QA-003 fica resolvida por este ADR.

## ADR-010 — Contratos oficiais verificados e acessibilidade explícita

**Contexto:** a inspeção histórica de 2026-10-10 registrou respostas das cinco URLs oficiais. A reconsulta de 2026-10-11 obteve Swagger, OpenAPI de consulta e portal Compras.gov.br, mas o OpenAPI principal excedeu timeout local e o manual falhou no envio. Isso não comprova indisponibilidade global. O contrato principal pode conter operações de manutenção que não são consulta pública.

**Alternativas:** presumir parâmetros por conhecimento anterior; tratar qualquer rota encontrada como autorizada; ou documentar fonte, data, acessibilidade, finalidade e pendências antes de fechar o contrato.

**Decisão [PROPOSTO]:** adotar a terceira alternativa. Esta execução registra conectividade, sem inventar endpoints/campos nem realizar coleta. Na Execução 2, verificar os contratos oficiais atuais, distinguir consulta/manutenção e fundamentar paginação, identidade, atualização, erros e limites nas fontes acessíveis. Ausências permanecem “pendente de validação”.

**Justificativa:** resultado HTTP e identificação OpenAPI não validam o comportamento operacional ou a atualização dos dados.

**Consequências:** snapshots de contratos/fixtures e estratégia incremental serão especificados em 10; respeitar restrições documentais da missão. Os links oficiais e os resultados por fonte estão em [02, seção 11](02-diagnostico-as-is.md#11-fontes-oficiais-e-conectividade). Conteúdo de edital/documento não é instrução operacional para ferramentas ou agente. Referências: RF-001–009/028, RNF-007/008/018.

## ADR-011 — MVP independente de ML, BI externo e histórico oficial futuro

**Contexto:** [PARCIAL] ML tem tabelas/view e consumo de score, sem treino/inferência completos localizados. Power BI possui exportador e especificação, sem arquivo de dashboard encontrado. RF-028 é Should da fase 6; sua inclusão acidental em um intervalo de dependências de RF-033 tornaria a fase 6 pré-requisito da 5.

**Alternativas:** exigir todos os componentes sugeridos no prompt desde o MVP; ou separar capacidades essenciais das extensões futuras e preservar ativos existentes.

**Decisão [PROPOSTO]:** manter decisão determinística e explicável como base do MVP. Preservar dados/exportações sem exigir ML ou BI externo. Preços históricos oficiais entram quando comparabilidade/contrato forem validados; não impedem a carteira local. RF-024 acrescenta sensibilidade ao ranking, sem impedir RF-026 básico. As dependências corrigidas estão em 04.

**Justificativa:** componentes declarados ou parciais não demonstram ganho preditivo nem justificam bloquear o fluxo essencial.

**Consequências:** 15 deve explicitar incrementos opcionais; a futura avaliação ML precisa de objetivo, rótulos suficientes, separação temporal e comparação com baseline. Metas do DAN são hipóteses calibráveis, sem evidência atual de tamanho de mercado ou retorno financeiro. A regra de consistência Must/Should deve ser aplicada novamente na consolidação CP/RT da Execução 3.

## ADR-012 — Documentos externos como entrada não confiável

**Contexto:** o extrator localizado atende catálogo comercial específico, sem pipeline completo de editais. A imagem atual inclui browsers e não declara usuário não privilegiado; filtros de link da UI não comprovam proteção contra SSRF ou contenção do parser.

**Alternativas:** baixar/extrair qualquer URL no processo da API; ou controlar origem, limites, armazenamento e processamento de documentos antes de promover seu conteúdo a evidência.

**Decisão [PROPOSTO]:** separar obtenção, quarentena/validação, extração textual e evidência publicada, conforme RNF-003/004. Conteúdo extraído não pode executar comandos, alterar regras ou decidir autorização. OCR/LLM são extensões futuras; texto ausente ou ambíguo gera pendência e revisão.

**Justificativa:** documento oficial em origem presumida continua sendo entrada externa sujeita a redirecionamento, arquivo corrompido, conteúdo adverso e falha de extração.

**Consequências:** 07/21 devem comparar isolamento adequado e persistência de documentos; 13/14 detalharão ameaças e casos adversos. Não há decisão antecipada de criar microsserviço por documento. Limites e metas permanecem propostas verificáveis; nenhum PDF foi processado para validar a segurança nesta missão. Referências: RF-006–009, RNF-002–004, RN-026/027.

## 2. Registro histórico de decisões previstas para a Execução 2

- Escolha fundamentada Jinja2 versus React, fronteiras de módulos e topologia de containers.
- Nomes finais de schemas/entidades, chaves oficiais, versionamento e relações com o legado.
- Contratos PNCP, acesso ao manual/OpenAPI principal, limites e estratégia de reconciliação.
- Rubricas logísticas, fórmula global, parâmetros e três estudos de caso.
- Política financeira completa, exemplos numéricos e garantias de reconciliação.
- Procedimentos Docker-first de persistência, atualização, backup/restauração e testes, sempre apenas propostos nesta missão.

Situação em 2026-10-11: arquitetura/interface, domínio e políticas numéricas detalhados nos documentos 07–12/21 e ADR-013–015 abaixo. Contratos PNCP de itens/documentos e validação operacional continuam pendentes, conforme 10 e progresso. A lista acima preserva o planejamento original, não indica que os documentos da Execução 2 estejam ausentes.

## ADR-013 — Jinja2 no monólito modular e contenção documental por processo

**Data:** 2026-10-11. **Status:** aceita para especificação [PROPOSTO].

**Contexto:** templates/revisão FastAPI já existem; execução inicialmente individual não justifica por si só outra stack. Parser trata dados externos e RNF-004 exige ausência de rede/credenciais.

**Alternativas:** React em container próprio; Jinja2 com melhoria progressiva; parser dentro da API; parser isolado. Microsserviços por módulo também foram avaliados em 07.

**Decisão:** monólito modular FastAPI/Jinja2 e trabalhadores Prefect. Parser em container sem rede/DB/segredos, com spool de entrada/saída e coordenador HTTP responsável por validação/persistência. Não montar socket Docker nem lançar container pelo parser. Preservar banco e plano de controle existentes; browser opcional no fluxo público.

**Justificativa:** reusa UI e transações e estabelece contenção verificável sem duplicar bancos/agendadores. Jinja2 exige adaptação real; não se afirma que a interface existente atenda ao Radar.

**Consequências:** produção local sem reload/bind do repositório; dev/teste isolados. Spool precisa de protocolo atômico, limites, retry e deduplicação. React será reavaliado apenas com evidência conforme 07; se escolhido depois, novo ADR/container. RNF-003/004/009/015/019; documentos 07/21 e D02/D12.

## ADR-014 — Identidades estáveis, versões imutáveis e autoridade transacional

**Data:** 2026-10-11. **Status:** aceita para especificação [PROPOSTO].

**Contexto:** tabelas atuais Gold/feedback têm semântica de revenda; retificação pode mudar itens/lotes enquanto fatos externos já ocorreram. Contrato detalhado de itens/documentos ainda não confirmado.

**Alternativas:** renomear tabelas legadas; copiar tudo em Silver/Gold duplicados; novos schemas de domínio no mesmo PostgreSQL, com bruto e projeções explícitas.

**Decisão:** procurement, sourcing e commercial como domínio; bronze.pncp_responses preserva original, control.ingestion_jobs especializa run/checkpoint. judgment_units cobre item/lote/grupo/global; não criar procurement_lots redundante. Commercial mantém cenário/análise/revisão/evento; Gold é projeção recuperável. Itens e produtos são versões; oportunidade conserva identidade comercial, com reconciliação de unidade quando necessária. FKs restritivas, controle otimista, leases/fencing e invalidações append-only.

**Justificativa:** preserva IDs/históricos e impede tratar projeção mutável como única prova de decisão. Novo schema não significa novo banco. Campos internos não são alegação de contrato PNCP.

**Consequências:** migrations aditivas, backfill validado e mapeamento sem conversão automática de decisões SILLO. Manifestos JSON precisam schema e validação de referências; UNIQUE hash não dispensa observação temporal quando conteúdo anterior retorna. Documentos físicos e banco requerem backup coerente. Modelo completo em 08; integração em 10; D05/D06/D11.

## ADR-015 — Políticas v0.1 de finanças e priorização separadas de bloqueios

**Data:** 2026-10-11. **Status:** aceita para especificação [PROPOSTO], pesos sujeitos a calibração.

**Contexto:** valores altos/margem aparente não demonstram caixa, lote completo ou execução viável. Arredondamento pode deixar preço teoricamente suficiente abaixo da meta.

**Alternativas:** reutilizar float/pesos de revenda; score que compensa impedimentos; ou Decimal, cenários completos, guardas e ranking condicionado.

**Decisão:** v0.1-financeiro com contexto 50 dígitos, quantização por linha/obrigação, rateio reconciliado, caixa datado e distinção garantia/reserva. v0.1-ranking usa F/L/T/C/E com pesos 0,30/0,20/0,20/0,15/0,15 e penalidades residuais limitadas a 6 na configuração inicial. Bloqueadas/pendentes têm global NULL. Mantida v0.1-logistica sem renormalizar ausências. Critérios e exemplos em 09/11/12.

**Justificativa:** torna comparações reproduzíveis e evita inferir autorização/viabilidade de um número. Margem mínima, capital, folga e parâmetros fiscais continuam decisões do caso, não defaults pessoais.

**Consequências:** versão dos parâmetros em toda análise; recalcular após mudanças, preservar histórico; verificar meta após arredondamento e não apenas na fórmula contínua. Casos de hardware, lote sem capital e presença não resolvida fazem parte da especificação de testes futura. Nenhuma probabilidade de vitória ou desempenho do software é afirmada. RN-012/015–021/026–030; RNF-010/016/020.

## ADR-016 — Preservação histórica com retenção por finalidade

**Data:** 2026-10-12. **Status:** aceita para especificação [PROPOSTO].

**Contexto:** preservar IDs/evidências não autoriza guardar dados pessoais indefinidamente; excluir tudo no encerramento quebraria auditoria e obrigações. 13 e 16 precisam distinguir retenção operacional de obrigação legal.

**Alternativas:** retenção ilimitada universal; expiração automática por prazo único; política por conjunto/finalidade com revisão e impedimento de exclusão de referências necessárias.

**Decisão:** terceira alternativa. Nenhuma limpeza nesta missão; futura retenção terá responsável, fundamento, prazo/revisão e dependências. Proposta operacional de backups 7 diárias/4 semanais/12 mensais e logs 30 dias não define prazo legal dos registros. Antes de eliminar/anonimizar, avaliar obrigações, solicitações e integridade histórica; manter evento auditado e evitar remoção automática de única cópia recuperável.

**Justificativa:** permite recuperação e rastreabilidade sem pressupor autorização irrestrita para conservar dado pessoal.

**Consequências:** inventário de tratamento e validação jurídica do caso antes da política operacional definitiva; backup expirado não elimina referência da base ativa. Gates G0–G7, testes TST-036/048/051 e RNF-002/005/014/017; 13/16 são fontes de procedimento.

## ADR-017 — Preservar score interno ao persistir

**Data:** 2026-10-12. **Status:** aceita para especificação [PROPOSTO]; correção retroativa em 08.

**Contexto:** 09/12 ordenam pelo score interno e arredondam somente exibição, mas 08 previa global_score NUMERIC(8,4). Persistir quatro casas criaria empates artificiais e mudaria ranking após recarga.

**Alternativas:** arredondar antes de ordenar e mudar regra; recalcular todo score a cada consulta; persistir Decimal sem escala fixa.

**Decisão:** NUMERIC sem escala fixa para global_score, valor finito 0–100 e contexto de cálculo 50 dígitos; componentes decimais em strings para replay. UI formata quatro casas, consulta usa valor exato. Mantidos pesos, fórmulas e exemplos.

**Justificativa:** preserva a política já fixada com menor alteração e evita depender de cache para ordem correta.

**Consequências:** validar serialização Decimal e entrada finita, TST-054 compara scores distintos após a quarta casa. Corrigir somente especificação física; nenhuma mudança de código/migration. RT-008, RNF-010/020 e 08/09/12.

## Extensões de decisões existentes — revisado em R1

- **ADR-010:** evidência externa passa a separar documentação, amostra e suporte por recurso, conforme matriz R1. Manual acessível não comprova resposta, cobertura nem implementação. Divergências factuais ficam em QA-R1-02/03, sem criar uma decisão por campo desconhecido.
- **ADR-011:** sua postergação integral do histórico é parcialmente sucedida por ADR-018. Mantida independência do MVP quanto a histórico/ML/BI. Histórico básico pode ser antecipado como Should condicionado, sem se tornar obrigação universal do ranking.
- **ADR-014:** UUID, autoridade transacional, versões e observações preservados. ADR-019 detalha identidade fiscal e correspondência de eventos; corrigir proposta de CNPJ apenas numérico em 08 na E2, sem migration automática.
- **ADR-016:** minimização alcança perfil de candidatos. CNAE/papel inferido são opcionais, rotulados e não determinam capacidade. Nesta amostra não há persistência de CPF, sócios, contatos pessoais ou bruto; produção exigirá política por finalidade, sem apagar auditoria indiscriminadamente.

## ADR-018 — Antecipação limitada de comparáveis e descoberta

**Data:** 2026-10-12. **Status:** aceita para especificação [PROPOSTO], habilitação condicionada. Revisado em R1.

**Contexto:** RF-028 inteiro na fase 6 adiava pesquisa de candidatos que pode ajudar a obter cotações. Documentação Compras descreve recursos úteis, mas não há amostra nem conversão demonstradas.

**Alternativas:** manter tudo na fase 6; promover todo histórico a Must do MVP; antecipar recorte opcional e verificável. **Decisão:** terceira alternativa: marco 2-H depois da identidade/ingestão, com RF-035/036/037 e parcela básica de RF-028; RF-038 na fase 3. Should no MVP, controles obrigatórios quando habilitado. Mercado amplo/lances completos/CNAE em escala/ML continuam fase 6.

**Justificativa:** permite investigar utilidade comercial sem pressupor fornecedor que venda ao operador. RF-028 mantém obrigação de apresentação contextualizada e parcela ampliada; ingestão/comparáveis/candidatos recebem donos distintos, sem duplicação. **Consequências:** remover dependências circulares com matching financeiro/cadastro; RF-013/015 continuam independentes da pesquisa. Histórico não muda score/guardas e sua ausência não bloqueia oportunidade suficiente. Gates QA-R1-01–05 antes de operação; alternativa manual com evidência. E2 propaga roadmap, CP/RT/testes e piloto.

## ADR-019 — Estabelecimento, evento e relação comercial independentes

**Data:** 2026-10-12. **Status:** aceita para especificação [PROPOSTO]. Revisado em R1.

**Contexto:** raiz de CNPJ, vencedor e preço público podem induzir fusão indevida de filiais, dupla contagem e confirmação comercial inexistente. Receita prevê coexistência numérica/alfanumérica; suporte das APIs ainda desconhecido.

**Alternativas:** fornecedor por raiz/nome e estado único; copiar cada fonte sem correspondência; identidade técnica + estabelecimento completo + eventos/proveniências e dimensões separadas. **Decisão:** terceira alternativa. Preservar original/canônico textual, validar DV pela Receita, raiz só analítica; chave por fonte/evento/item e correspondência oficial auditada; resultado e contrato vinculados permanecem etapas diferentes. Preferência por recurso/atributo; conflitos não são apagados.

**Justificativa:** preserva evidência sem inferir equivalência econômica/comercial. Fuzzy apenas sugere e duplicidade incerta exclui agregado padrão com razão. **Consequências:** modelo/migração aditivos em E2; contato, disposição de venda, cotação e disponibilidade com autores/escopos/validade próprios. Reutilizar supplier_id confirmado, sem segunda autoridade de fornecedor. Histórico não comprova entrega, estoque ou capacidade; preço público não vira custo. RN-033–037 e extensões RN-005/006/026/028; 22 descreve guardas/transições, sem alterar RN-032/D10.

## ADR-020 — Ferramenta auxiliar efêmera e reprodução condicionada

**Data:** 2026-10-12. **Status:** aceita para especificação [PROPOSTO]; execução não realizada. Revisado em R1.

**Contexto:** prompt R1 v2.0 autoriza amostragem/validação limitada fora da stack, somente em Docker. Engine local inacessível nesta sessão.

**Alternativas:** usar runtime do host; reutilizar worker/banco atual; ferramenta auxiliar isolada com imagem por digest ou registrar não execução. **Decisão:** última alternativa. Nesta E1 registrar não execução; roteiro em ferramentas-r1. Imagem/arquitetura/digest ainda não obtidos, sem valor fictício. Máximo cumulativo de 30 chamadas de dados, limites de bytes/registro/tempo, ledger pré-envio, corpo descartado. Validações offline sem rede.

**Justificativa:** mantém reprodutibilidade e limites sem expor dados/volumes da operação. **Consequências:** nenhuma coleta ou validação programática no host; sem conclusão de fonte por exit code. Documento pode concluir com gate explícito, piloto não. E2 não faz nova amostragem. Futura produção continua Docker/Compose e políticas próprias, sem adotar orçamento investigativo como quota permanente. Refinamento restrito do ADR-001 para a exceção auxiliar do prompt v2.0; proibições operacionais permanecem.

## Histórico de revisões

| Data | Versão | Alteração |
|---|---|---|
| 2026-10-12 | 0.2-R1 | ADR-018–020; extensões explícitas de 001/010/011/014/016, preservados IDs, ranking e histórico original |

## Propagação das decisões na R1-E2 — revisado em R1

2026-10-13, versão 0.3-R1-E2. Nenhum ADR novo: as decisões necessárias já pertencem a ADR-018/019/020 e extensões 010/011/014/016.

- **ADR-018:** contexto/evidência continuam insuficientes para operação; alternativa de pesquisa/cadastro manual preservada. Decisão propagada em CP-09/04, marco 2-H, HU-028/035–038 e 23. Consequência: testar pesquisa/cotação pode preceder o MVP integral, mas não autoriza margem/ranking/participação sem Must correspondentes. CNAE/mercado amplo/lances/ML não são gates do piloto.
- **ADR-019/014:** comparada extensão de historical_prices com tabela histórica duplicada: escolhida extensão mais public_events/event_links somente para responsabilidades de evento e correspondência que preço não cobre. suppliers é identidade única até para candidato; cadastro comercial reutiliza UUID. historical_comparisons não substitui product_matches; candidato/interação não altera E01–E16. Consequências em 08/16: original/canônico fiscal textual, DV oficial nos dois formatos, raiz analítica, nenhuma fusão de filial, retorno aditivo e projeções reconstruíveis.
- **ADR-020:** indisponibilidade do Engine refere-se à E1. Na E2 Engine 29.8.2 acessível permite somente ferramentas documentais efêmeras offline, com imagens de procedência/digest verificados e logs em ferramentas-r1/e2. Alternativas host/runtime ou container ativo continuam rejeitadas. Consequência: validação documental não aprova fonte, ambiente de produção, TST da aplicação ou quota; amostra continua não executada, 0/30.
- **ADR-010/016:** evidências de APIs/Receita/Lei/ANPD permanecem datadas na matriz da E1, sem revalidação amostral. Termos/quota/DV operacional/base de tratamento requerem gates próprios antes de ativação; preservação histórica e minimização coexistem. Bronze multirrecurso no PostgreSQL não exige novo volume de JSON.

Histórico de revisões — 2026-10-13, 0.3-R1-E2: propagação e consequências concretas, estado Docker atualizado sem apagar limitação histórica da E1; nenhuma decisão de ranking alterada.


