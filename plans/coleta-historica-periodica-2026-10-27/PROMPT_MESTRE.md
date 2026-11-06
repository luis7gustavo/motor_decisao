# Prompt mestre — coleta histórica integral e sincronização periódica de licitações

Versão 1.1 · 27/10/2026 · America/Sao_Paulo · revisão crítica incorporada

## Instrução para o executor

Implemente e valide os scripts de coleta de licitações do Radar Licita/SILLO, reaproveitando a infraestrutura existente. O requisito central é **armazenar todas as licitações históricas recuperáveis nas fontes oficiais habilitadas, preservar os dados e suas versões e manter a base atualizada por coleta periódica que respeite os limites de cada API**.

Não entregue somente um desenho ou exemplos de código. Entregue scripts integrados, persistência real, retomada após falhas, agendamento na infraestrutura do projeto, testes e evidências reproduzíveis. Execute uma etapa por sessão, na ordem das dependências abaixo. Ao receber apenas “continue”, leia `STATUS.md` e execute a próxima etapa pendente. Não declare uma etapa concluída sem seu critério de saída.

Este arquivo é uma especificação para execução posterior. Sua criação não significa que scripts foram implementados, dados foram coletados ou agendamentos foram ativados.

## 1. Workspace, instruções e skills

- Raiz de execução: `C:\Users\luisg\OneDrive\Documentos\ChatGPT\motor publico\motor_decisao`.
- O diretório pai e `radar_licita/` contêm outros checkouts/coletores. Não misture bancos, scripts ou entregas entre eles.
- Leia as instruções `AGENTS.md` aplicáveis e `C:\Users\luisg\.codex\RTK.md`.
- Use as skills instaladas `blueprint`, `docker-patterns` e `supabase-postgres-best-practices`. Antes de escrever testes, carregue `python-testing-patterns`; para instrumentação, `python-observability`. Use `diagnosing-bugs` ao corrigir o erro atual de paginação. Leia os respectivos `SKILL.md` e referências pertinentes; não invoque skills apenas para enumerá-las.
- Aplicação, coletores, testes, PostgreSQL e restaurações executam por Docker. Windows/PowerShell servem para edição, Git e invocação do Compose. Não substitua a operação por Python/PostgreSQL no host.
- Preserve as mudanças preexistentes. Não execute reset/clean, limpeza de volumes, push ou migração do banco principal. O pedido autoriza preparar e ativar a coleta recorrente desta missão no destino segregado validado, depois dos critérios técnicos; não autoriza alterar outras automações.
- Git está disponível; `gh` não foi encontrado na inspeção de 27/10. Planeje execução local sem depender de PR ou instalação de CLI. Se surgir necessidade de branch, use `codex/`; não mude de branch sobre alterações existentes sem protegê-las.

## 2. Estado inicial: evidência local, não validação de runtime

Revalide estes fatos no começo: o conteúdo pode mudar depois de 27/10/2026. Não leia nem publique credenciais ao conferir arquivos.

| Elemento | Evidência encontrada no checkout |
|---|---|
| Adapter/transporte/repositório | `app/procurement/pncp.py`, `pncp_transport.py`, `repository.py`, `models.py` |
| Flow PNCP | `pipelines/procurement/pncp_flow.py`: manual, `retries=0`, recebe job existente |
| Runner isolado | `compose.radar-ops.yml`, `scripts/radar_ops.py`, `scripts/radar_pncp_live_validation.py` |
| Política atual | `config/contracts/pncp-publicacao-policy.json`: `enabled=false`, `limits_reviewed=false`, autorização restrita a C2 |
| Contrato arquivado | `config/contracts/pncp-consulta-20261013.json`; hash esperado `a5d87b60a54acfd681e84e0a77d541b0db7065401171508c53c911d12bdd121d` |
| Sessão preservada | `backups/c2/c2-20261013/`; ledger DOCUMENTAÇÃO 7/15, DADOS 4/50, calibração 2/3, 11 tentativas no total |
| Job preservado | `e47c6fe2-2cc9-4b58-803a-49a4aa44a443`, em `collection-job.json`; janela 06/10–14/10/2026, modalidade 6, página de 10 registros |
| Destino anteriormente validado | `test_20261020_ba894ce71405`, OID 2508600; a identidade atual do servidor/banco/role precisa de verificação ao vivo antes de escrita |
| Problema atual | `Store.apply_page` levanta `ValueError('Source changed pagination totals')`; o job permanece parcial na página 2 |
| Verificador desatualizado | `verify()` em `radar_pncp_live_validation.py` ainda exige exatamente 3 respostas Bronze e 3 chamadas DADOS; o ledger já registra 4 chamadas DADOS |
| Política JSON | O arquivo observado contém duas propriedades `operator_authorization`; eliminar a ambiguidade sem ampliar a autorização original |
| Manuais | `docs/licitacoes/fontes/` não foi encontrado. A instrução existente é não buscar manual pela rede |

Não recrie o ledger, o job, a role ou o destino C2 para renovar a quota. **50 chamadas é o teto do ensaio C2, não a quota oficial da API nem um orçamento suficiente para a carga nacional.** Sua autorização restrita não habilita coleta geral. A nova missão histórica precisa de escopo, orçamento operacional e política próprios, registrados e auditáveis; a limitação C2 permanece intacta. Separar missões não pode contornar limites compartilhados da fonte ou autorizações anteriores.

Consulte também `docs/licitacoes/10-integracoes-pncp.md`, `08-modelo-de-dados.md`, `19-adr-024-calibracao-janela-e-cotas.md`, `operacao-ciclo-2.md` e `17-rastreabilidade-ciclo-2.md`. Documentos antigos podem descrever estados anteriores; dê precedência à evidência atual reconciliada, sem apagar o histórico documental.

## 3. Significado de “todas as licitações históricas”

1. A carga começa na data mais antiga comprovadamente disponibilizada por cada fonte, até um marco final fixo registrado no manifesto. Não escolha uma data inicial arbitrária, “últimos 30 dias”, limite de resultados ou amostra como substituto do histórico.
2. Abranja todas as modalidades e demais partições necessárias comprovadas pelo contrato, com cobertura nacional quando a fonte a permitir. Não aplique filtros de categoria, orçamento, UF, situação ou palavras-chave na aquisição se isso excluir registros do universo definido. Esses filtros pertencem à consulta/análise local.
3. Inclua processos abertos, encerrados, homologados, cancelados, anulados, revogados e outras situações disponibilizadas. Estado final ou fim do prazo não é motivo para excluir dados.
4. Preserve cada identidade oficial, seu conteúdo recebido, observações, versões, retificações e situação. Itens, resultados e documentos/anexos oficiais precisam de descoberta e fila de coleta própria, quando os contratos de leitura forem validados. Cabeçalho coletado não significa licitação documentalmente completa.
5. Identifique explicitamente lacunas por período, modalidade, partição e recurso. Não declare “todas as licitações brasileiras”: cobertura da PNCP não demonstra cobertura de outras fontes nem existência de dados anteriores à cobertura do portal.
6. “Histórico de licitações” é o conjunto de processos antigos recuperáveis. “Histórico de versões” inclui as versões expostas pela fonte ou observadas desde a coleta. Se uma revisão antiga nunca foi disponibilizada/capturada, registre essa limitação; não a reconstrua por inferência.
7. Se a fonte não oferecer algum período/recurso, registre `não identificado`, `indisponível na fonte` ou `pendente de validação`, conforme a evidência. Fontes oficiais complementares ou arquivos oficiais em lote podem preencher lacunas após validação própria; não invente endpoints ou equivalências.

## 4. Contratos, limites e orçamento de requisições

- Reutilize o OpenAPI arquivado e confira hash, parâmetros, paginação, domínio de modalidades, datas e recursos. O snapshot de 13/10 não confirma as regras vigentes. Não buscar manuais pela rede; se o manual local continuar ausente, registre pendência e avance no trabalho independente. Consultas de documentação não abrangidas por essa restrição só ocorrem com orçamento documental registrado.
- Registre a fonte oficial e a data de cada limite: taxa, concorrência, quota por período, tamanho de página, amplitude de janela, retenção/cobertura e eventual restrição de horário. Diferencie limite oficial, campo contratual, observação e política conservadora local. **Não invente “limite oficial de 1 req/s”**: esse número aparece como política local proposta.
- Só habilite coleta geral quando houver contrato de leitura confirmado e política de uso resolvida. Se a fonte não publicar quota numérica, documente essa ausência com evidência, configure teto conservador de missão e resposta a throttling; não transforme ausência de informação em permissão ilimitada. Se termos/limites ainda impedirem operação, mantenha o adapter bloqueado e informe a pendência concreta.
- Implemente um limitador persistente compartilhado por fonte e pelo escopo efetivo da restrição (credencial/IP, quando aplicável), abrangendo backfill, sincronização, itens, documentos, calibração e retries. Uma trava apenas por job ou apenas em memória é insuficiente. Em múltiplos workers/processos, a reserva deve ser atômica e a espera durável.
- Use o menor teto aplicável entre fonte, missão e configuração. Reserve a tentativa antes de DNS/conexão/envio. Timeout, conexão interrompida, 429 e falha de validação consomem tentativa; não devolva quota por reiniciar container, job ou scheduler. Quotas de períodos oficiais só renovam na fronteira verificada, preservando os registros anteriores; reinício não é renovação.
- Mantenha `not_before` persistente e compartilhado. Respeite `Retry-After` válido, tanto segundos quanto data HTTP; nunca reduza uma espera oficial para caber no teto local de backoff. Se a espera exceder o tempo do job, grave o checkpoint e reagende. Use backoff exponencial com jitter e tentativas máximas de ponta a ponta, sem multiplicar retries HTTP × Prefect × scheduler.
- 400/422, contrato inválido, 401/403 e divergência estrutural não recebem retry cego. 429/transporte/5xx usam a política limitada. Circuit breaker impede tempestade de requisições. Redirecionamentos não escapam da allowlist, da quota ou da validação de destino.
- Dimensione o volume antes de iniciar: estimativa de páginas + enriquecimentos + reconciliação + margem de retries, tempo mínimo pelo teto aplicável e armazenamento. Não prometa prazo sem medir e não aumente a taxa para compensar atraso. Se houver exportação oficial em lote, avalie-a para o histórico, preservando proveniência e incremental pela API.

## 5. Persistência e integridade obrigatórias

- Reaproveite os schemas `bronze`, `control` e `procurement`, seus modelos e o repositório. Evoluções aditivas ocorrem somente no destino segregado aprovado. Faça inventário e ensaio de restauração antes de migrações; não aplique `upgrade head` no principal, nem inicie `prefect-init`, cujo bootstrap legado pode fazê-lo.
- Preserve bytes originais, hash SHA-256, fonte, rota, parâmetros sanitizados, contrato/parser, status, horários de observação e referência ao job/tentativa. Separe conteúdo bruto de projeção normalizada; a reconstrução deve funcionar sem chamar a API.
- Retenha histórico sem expiração automática. Planeje crescimento, disco livre, compressão e arquivamento recuperável. Qualquer remoção futura de evidência depende de política explícita; esta missão não autoriza limpeza destrutiva.
- Identidade por fonte + identificador oficial (`numeroControlePNCP` na PNCP); chaves alternativas apenas com validação. CNPJ permanece texto, inclusive zeros e formatos alfanuméricos quando aplicáveis. Não una registros por título ou similaridade.
- Deduplicação idempotente por identidade e hash canônico versionado: conteúdo igual não cria nova versão de negócio; nova observação continua rastreável. Alteração e reversão A→B→A preservam a sequência observada. UPSERT de identidade não sobrescreve versões imutáveis.
- Ao adicionar sincronização de atualizações, diferencie `source_updated_at` de `observed_at`: uma resposta histórica coletada hoje não pode regredir a versão atual só por ter sido observada depois. Datas sem fuso ou divergências entre recursos exigem regra documentada, origem textual preservada e pendência explícita. Não invente ordem temporal.
- Commit de página e avanço do checkpoint são atômicos. Registre tentativas e Bronze duravelmente antes da promoção. Crash após reserva, após resposta, após Bronze ou entre persistência/checkpoint deve permitir retomada sem perda nem efeitos lógicos duplicados. Reutilize lease com fencing e as restrições de unicidade/FK existentes.
- Totais de paginação mutáveis, páginas repetidas, ordenação instável e registros movidos entre páginas devem produzir erro tipado e estado parcial. Retenha Bronze, não avance falsamente o watermark e registre replay/subdivisão da janela. Não remova a validação de totais para “fazer passar”.
- `SourceChanged` precisa guardar metadados sanitizados da divergência, política de reconciliação e limite de reexecuções. Preserve o job C2 e seus recibos; futuras tentativas de reconciliação podem ser jobs filhos vinculados, sem reset da quota e sem perder a genealogia. Uma partição só é concluída após reconciliação demonstrada.
- Para comprovação integral, prefira snapshot/exportação oficial ou controle independente capaz de demonstrar a enumeração do universo. Em API mutável sem snapshot/ordenação estável, várias varreduras convergentes e totais iguais não provam ausência absoluta de omissões. Registre método, passes, instantes, partições, hipóteses e risco residual; use `reconciliado_sem_snapshot` quando aplicável. Esse estado não equivale a `integral_verificado` e não autoriza anunciar cobertura garantida.
- Um 204, uma página vazia ou ausência na lista não apaga nem cancela registros locais. Quarentena retém dados inválidos e permite reprocessamento; pendência não pode desaparecer do relatório de completude.
- Dinheiro/quantidade usam Decimal/NUMERIC coerentes com o contrato. Dados históricos não comprovam capacidade atual, disponibilidade, cotação ou custo de aquisição. Não altere score, regras financeiras ou jornadas comerciais nesta missão.

## 6. Carga histórica, atualização e agendamento

Crie um manifesto durável com fonte, escopo, cobertura inicial comprovada, marco final fixo, contrato, modalidades, partições, orçamento, dependências e status. Divida o período em janelas compatíveis com a fonte e com a mutabilidade observada; implemente subdivisão sem lacunas e deduplicação nas bordas. Janela, modalidade, recurso e página têm progresso independente. Colete em streaming/lotes; não mantenha todo o histórico nem a lista de todos os IDs em um JSON de checkpoint que cresça sem limite.

Separe três fluxos coordenados pelo mesmo limitador: **backfill histórico**, **novas publicações/atualizações** e **reconciliação**. A sincronização não espera a carga histórica inteira terminar. Reserve orçamento para manter a atualidade sem deixar o backfill permanentemente sem progresso. O marco final da carga é fixo; a sincronização trata o que vier depois, com sobreposição e deduplicação.

A sincronização consulta publicação e atualização global nas operações oficialmente validadas, com watermarks independentes por fonte/recurso/partição. Não use somente a data de publicação, nem só a rota de propostas abertas. Reavalie retificações antigas, cancelamentos e documentos/resultados com fila de enriquecimento e varredura periódica mais ampla. Na ausência de feed de mudança para um recurso, documente e implemente reconciliação por detalhe/partição dentro do orçamento.

O watermark avança apenas até o último intervalo contíguo confirmado. Uma falha antiga não pode ser escondida pelo sucesso de intervalos posteriores. Preserve timestamps originais; scheduler usa `America/Sao_Paulo`, instantes técnicos usam UTC com fuso, e a semântica dos filtros de data deve estar validada.

Use **Prefect na versão instalada** como único dono do agendamento do pipeline. Não crie também cron, Agendador do Windows ou automação de chat para disparar a mesma coleta. Proponha sincronização a cada 6 horas e reconciliação semanal de 30 dias como valores locais configuráveis, já sugeridos nos documentos do projeto; ajuste-os ao contrato e ao dimensionamento. Nenhum deles é limite ou garantia da PNCP. Backfill roda em lotes contínuos/reagendáveis com tempo e orçamento finitos, sem precisar terminar dentro das 6 horas.

Implemente deployment/schedule idempotentes com chave estável, pool/queue validados, prevenção de execução sobreposta, pausa/retomada e inspeção. Não herde `max_pages=1` do ensaio C2 como limite da carga histórica. Retentativas e reagendamentos não criam autorização nova nem renovam quota C2.

Valide worker e scheduler saudáveis, execução agendada real e retomada após reinício. Registro de schedule sem worker executando não é automação comprovada. Em caso de PC/Docker desligado, recupere intervalos faltantes com checkpoints e limite a recuperação acumulada para evitar rajada. A disponibilidade local depende de PC, Docker, banco, servidor Prefect e worker ligados; não prometa operação 24/7 sem infraestrutura correspondente.

O resultado de negócio deve distinguir sucesso, parcial, adiado, quota esgotada, contrato bloqueado e falha. Não aceite apenas `Prefect Completed` como coleta concluída; mapeie resultados parciais/falhas para estado ou artefato operacional visível, sem ativar retries externos descontrolados. Registre alertas no mecanismo local existente; mensagens por e-mail/Slack/terceiros não estão autorizadas.

## 7. Scripts e entregáveis esperados

Os nomes abaixo são **propostas a implementar**, não comandos existentes ou já validados. Reaproveite/estenda comandos equivalentes se a inspeção demonstrar melhor integração; documente o nome final, parâmetros, exit codes e exemplos PowerShell/Compose executados.

| Entregável | Operações e contrato mínimo |
|---|---|
| `scripts/radar_pncp_history.py` | `plan`, `backfill`, `sync`, `reconcile`, `enrich`, `replay`, `status`, `audit`; manifesto explícito, retomada, dry-run de rede zero, limites e saída JSON sanitizada |
| `scripts/radar_pncp_schedule.py` | `plan`, `apply`, `pause`, `resume`, `status`; diff do agendamento, aplicação idempotente e destino segregado validado |
| Flow(s) em `pipelines/procurement/` | Integração com coletor existente, estado de negócio, prioridade e orçamento compartilhados |
| Política/configuração específica | Fonte, contrato/hash, cobertura, modalidades, janelas, rate limit, concorrência, quotas, retries, horários, circuit breaker e critério de habilitação; parser rejeita chaves JSON duplicadas |
| Persistência de controle | Manifestos, partições, fila, watermarks, erros, tentativas e genealogia; aditiva e indexada, com migração apenas se necessária |
| Testes e fixtures | Casos sintéticos isolados, replay do Bronze permitido e prova controlada com fonte real |
| Runbook operacional | Instalação/configuração, comandos reais, inicialização, parada, pausa/retomada, recuperação, alertas, auditoria e backup/restauração |
| Relatórios | Cobertura histórica por fonte/período/modalidade/recurso; lacunas; requisições/cotas; último sucesso; atraso; disco; baseline antes/depois do principal protegido |

Estados e exit codes devem permitir automação e inspeção humana. `dry-run` não chama a API nem altera jobs/watermarks/schedules/banco de domínio; diferencie-o de uma simulação com escrita em alvo de teste. `replay` não acessa a rede e não consome quota externa.

## 8. Etapas de execução e dependências

| Etapa | Resultado | Depende de | Ordem |
|---|---|---|---|
| S00 | Inventário, escopo, contrato e política de habilitação | — | Primeira |
| S01 | Correção da paginação mutável e verificador | S00 | Sequencial |
| S02 | Persistência de controle e integridade histórica | S01 | Sequencial |
| S03 | Carga histórica retomável e cobertura | S02 | Sequencial |
| S04 | Sincronização, enriquecimento e reconciliação | S03 | Sequencial |
| S05 | Prefect, controle de agenda e operação em Docker | S04 | Sequencial |
| S06 | Validação integrada, carga real controlada e restauração | S05 | Sequencial |
| S07 | Ativação recorrente e acompanhamento comprovado | S06 | Após a validação real |
| S08 | Auditoria e encerramento da carga histórica | S07 | Última, após processamento do universo |

Use o modelo configurado no chat; este plano não exige troca de modelo. Não existe paralelismo seguro entre as etapas centrais: elas compartilham contrato, orçamento, controle e destino. Revisões de documentos/fixtures podem ser delegadas como leitura independente se útil; nunca paralelize chamadas à fonte para contornar o limitador.

### S00 — inventário e contrato operacional

**Contexto de partida:** C2 é ensaio parcial, política geral desabilitada e principal protegido; não há autorização herdada para expandir C2. A missão nova pede histórico e recorrência. Leia seções 1–4 e o `STATUS.md` antes de qualquer rede.

**Execute:** registre Git/status sem alterar mudanças existentes; versões de Docker/Compose/Prefect/PostgreSQL; arquivos, roles e destinos pertinentes sem segredos. Valide identidade do destino segregado e proteção do principal. Reconcile ledger/recibos/checkpoints offline. Levante cobertura, catálogo de modalidades, contratos e disponibilidade de lote oficial; respeite a restrição de manual pela rede. Corrija a duplicidade da política com teste de parser. Defina a política e o orçamento próprios da missão histórica, limites compartilhados e estimativa de volume/disco/tempo, sem modificar contadores C2.

**Validação/saída:** inventário e manifesto legíveis por máquina; tabela de limites com fonte/data ou pendência; diff de escopo C2 × histórico; decisão de habilitação por recurso; zero chamadas externas não registradas. S00 pode ser marcada `concluída documentalmente; rede bloqueada` com gates por recurso registrados. Isso permite S01–S05 offline com contratos arquivados, fixtures e rede externa bloqueada; migrações/testes dependentes de infraestrutura continuam sujeitos às próprias pré-condições. S06 real e S07 permanecem bloqueadas até resolver os gates aplicáveis. Não marque integração real como pronta por concluir o inventário.

**Rollback:** conservar artefatos e deixar política geral bloqueada; nenhum dado histórico é removido.

### S01 — paginação mutável e evidência reconciliável

**Contexto de partida:** `pncp.py` rejeita mudança de totais com `ValueError`; Bronze da página 2 e job C2 devem continuar intactos. O verificador depende de contagens fixas já antigas. Dependência: S00 concluída para o trabalho necessário.

**Execute:** introduza `SourceChanged` tipado e tratamento parcial/reconciliação, sem transformar divergência em sucesso. Preserve cursor/recibos e limite tentativas. Faça o verificador comparar invariantes, IDs de sessão, hashes, recibos, transações e contadores reais, sem exigir exatamente 3 respostas ou 10 registros. Defina estado de negócio observável no flow.

**Validação/saída:** teste offline com mudança 9577/958 → 9562/957, Bronze retido, cursor não avançado e causa tipada; verificador aceita histórico reconciliado crescente e rejeita corrupção verdadeira. Teste também contagem estável com troca de IDs entre páginas; igualdade de totais sozinha não prova ausência de lacunas. Nenhum GET real é necessário para provar a correção.

**Rollback:** reverter só a mudança de código desta etapa, preservando os arquivos/linhas anteriores do usuário, jobs e evidências.

### S02 — controle e histórico persistentes

**Contexto de partida:** existe persistência C1 com identidades, versões e observações; não criar um segundo histórico concorrente. Dependência: S01. Leia `models.py`, `repository.py` e as regras PostgreSQL antes de projetar alteração.

**Execute:** implemente o mínimo faltante de manifesto/partições/watermarks/fila/limites compartilhados; mantenha compatibilidade C2, FKs e índices. Trate ordem das versões entre publicação/atualização e replay tardio. Valide migração aditiva só no alvo segregado, backup verificável e permissões da role. Checkpoint deve ter tamanho limitado, com progresso volumoso em tabelas indexadas.

**Validação/saída:** provas de idempotência, A→B→A, registros fora de ordem, concorrência, fencing, crash em cada fronteira de persistência e rejeição de destino principal. Integridade e restauração em banco descartável confirmadas; sem migração principal. Liste schema/índices alterados e o motivo.

**Rollback:** pausar consumidores e voltar código compatível; preservar tabelas/versões, sem downgrade destrutivo do histórico.

### S03 — backfill integral do universo definido

**Contexto de partida:** controle e persistência estão validados; amostra C2 não representa a base completa. Dependência: S02.

**Execute:** implemente `plan/backfill/status/audit` e particionamento a partir da cobertura comprovada, todas as modalidades pertinentes, marco final fixo, retomada e prioridade. Budget/limiter globais cobrem todas as tentativas. Produza auditoria de períodos/partições/recursos, incluindo vazios confirmados, desconhecidos e erros. Teste revarredura/subdivisão quando a fonte mudar; não guarde todos os IDs no checkpoint.

**Validação/saída:** fonte sintética com mais de uma janela/modalidade/página e partições vazias; interrupção na página 2 e reinício do processo; nenhuma perda/duplicação lógica; quotas e espaçamento compartilhados entre processos/container workers. Respeito aos limites em cada tentativa. Prova real limitada apenas se o recurso estiver habilitado e o orçamento permitir; informe como amostra, nunca como histórico concluído.

**Rollback:** pausar o backfill mantendo manifesto, checkpoints e registros; retomada usa o mesmo estado.

### S04 — sincronização, enriquecimento e reconciliação

**Contexto de partida:** backfill retomável, contratos por recurso e filas duráveis disponíveis. Dependência: S03. Não aguarde meses de carga histórica para começar a atualização.

**Execute:** implemente `sync/reconcile/enrich/replay`, publicação e atualização global, sobreposição calibrada, watermarks independentes/contíguos, varredura ampliada e filas de itens/resultados/documentos quando validadas. Registre lacunas por recurso desabilitado. Compartilhe orçamento com prioridade suficiente para atualidade e progresso do histórico.

**Validação/saída:** retificação hoje de licitação publicada em período antigo, cancelamento, resposta vazia, anexo alterado, recurso sem contrato, timestamp empatado/sem fuso, replay tardio, página que falha e intervalo seguinte que sucede. Nenhum watermark cruza lacuna; último dado válido permanece. Replay demonstrado com rede bloqueada.

**Rollback:** pausar sincronização/enriquecimento e voltar consumidores compatíveis; preservar filas, watermarks e dados recebidos.

### S05 — agenda Prefect e operação Docker

**Contexto de partida:** os três fluxos compartilham estado/limites; o flow C2 continua compatível e manual. Dependência: S04.

**Execute:** implemente comandos idempotentes de agenda e deployment específico da nova missão. Leia a versão instalada de Prefect e sua interface suportada antes de editar. Crie serviços adicionais se necessários sem executar bootstrap que migra o principal. Prepare diff de agenda, configuração de worker/pool/queue, volumes persistentes, limites de concorrência e pausas. Faça ensaio em destino de teste com transporte sintético/rede bloqueada.

**Validação/saída:** `apply` duas vezes produz uma agenda; `pause/resume/status` funcionam; disparo sintético executado pelo worker; reinício mantém checkpoints/limites e não dispara jobs duplicados nem rajada. `docker compose config --quiet` valida só configuração; prove saúde/executabilidade separadamente. Agenda externa real permanece desativada até S07.

**Rollback:** pausar exclusivamente o deployment criado nesta missão e parar seus serviços específicos, conservando volumes/bancos.

### S06 — validação integrada e prova real controlada

**Contexto de partida:** implementação e agenda de teste prontas. Dependência: S05. Nenhuma quantidade de testes sintéticos comprova sozinha cobertura nacional.

**Execute:** rode a suíte pertinente no runner isolado, com transporte falso e bloqueio de rede nos testes; prove teto total de retries e `Retry-After` segundos/data HTTP maior que a duração do job. Execute amostra real delimitada pelo manifesto, apenas com política habilitada, orçamento e identidade do alvo confirmados. Reabra o processo para conferir persistência; restaure backup em alvo descartável e verifique hashes/relações. Confira proteção do principal antes/depois.

**Validação/saída:** relatórios e logs sanitizados com comandos, horário, duração, saída e limitações; tentativas reconciliadas com Bronze/contador; prova de execução pelo motor Prefect e estado de negócio correto; restore íntegro. Se contrato/limites/bordas/modalidades impedirem operação, registre impedimento e mantenha a ativação bloqueada; não apresente teste parcial como aceite.

**Rollback:** parar novas tentativas/pausar agenda, registrar motivo e conservar amostra/ledger; não devolver quota nem excluir evidências.

### S07 — ativação e comprovação da recorrência

**Contexto de partida:** S06 aprovada, destino segregado identificado, política geral explícita e recursos habilitados. O pedido atual de automação autoriza a ativação do pipeline desta missão após esses critérios; não peça confirmação adicional apenas porque C2 antes era manual.

**Execute:** aplique somente a agenda da nova missão, inicie backfill retomável e deixe sincronização/reconciliação coexistirem pelo limitador comum. Prove ao menos um disparo automático real pelo scheduler/worker e persistência de seu resultado. Caso a periodicidade real exceda a sessão, mantenha “ativado, aguardando comprovação” no STATUS; uma chamada manual não substitui o evento agendado. Disponibilize comandos e estado para acompanhamento, com alertas locais sobre falha, atraso, quota e disco.

**Validação/saída:** agenda ativa identificável, execução agendada correlacionada a job/ledger/Bronze/normalização, pausa/retomada comprovadas e runbook operacional. Mostre separadamente implementação pronta, automação comprovada, carga histórica em andamento e cobertura verificada. Não espere encerrar o histórico para relatar progresso, mas não marque o objetivo de armazenamento integral concluído antes de cobrir o universo definido e auditar lacunas. Concluir S07 transfere obrigatoriamente o acompanhamento e o aceite histórico para S08.

**Rollback:** pausar apenas a agenda da missão e o avanço da fila; conservar tudo para retomada, inclusive orçamento consumido.

### S08 — acompanhamento e auditoria final do histórico

**Contexto de partida:** S07 comprovou recorrência; o backfill pode permanecer ativo por muitas sessões. A implementação e a automação têm estados separados da carga histórica. Dependência: S07; preserve o marco final fixo e o escopo obrigatório do manifesto.

**Execute:** implemente/ative a auditoria local do manifesto no próprio pipeline e execute `status/audit` nas retomadas. Acompanhe todas as partições e recursos obrigatórios até seu processamento, reconcile falhas/quarentenas/lacunas e confronte o conjunto de identidades com snapshot/exportação oficial ou controle independente quando disponível. Registre origem, período, modalidade, recurso, contagens, hashes e evidência de cobertura. Não retire uma partição/recurso obrigatório do escopo apenas para encerrar a missão. Indisponibilidade comprovada na fonte é limitação explícita, não dado fabricado nem coleta bem-sucedida.

**Validação/saída:** nenhuma partição ou recurso obrigatório recuperável permanece sem tratamento; filas do marco histórico estão drenadas e reconciliadas; auditoria e restauração preservam identidades/versões/observações. Classifique cobertura por recurso como `integral_verificado`, `reconciliado_sem_snapshot`, `parcial` ou `indisponível_na_fonte`, com justificativa e método. Sem snapshot/controle suficiente, registre passes, hipóteses e risco residual e mantenha o aceite de completude absoluta pendente; reporte apenas a cobertura efetivamente demonstrada. A automação continua depois do fechamento do marco histórico, sob o mesmo controle de limites. Atualize separadamente o estado da carga, o aceite de cobertura e a saúde da recorrência.

**Rollback:** pausar só as novas tentativas necessárias e conservar histórico, manifesto, relatórios e budget; restaurar somente em alvo segregado se a verificação demonstrar corrupção. Não confunda conclusão da auditoria com desligamento da sincronização.

## 9. Checklist de aceite do sistema

- [ ] Todas as identidades históricas recuperáveis do universo definido têm partição rastreável; omissões e recursos pendentes são visíveis.
- [ ] Bytes, identidade, versão e observação não são apagados por atualização, 204, cancelamento, reinício ou replay.
- [ ] Reexecução idempotente, retomada e crash não perdem dados nem renovam quota indevidamente.
- [ ] Publicação, atualização, reconciliação e enriquecimento compartilham limitador persistente e orçamento de tentativas.
- [ ] Limites oficiais e políticas locais estão separados, com evidência e data; o ensaio C2 continua reconciliado.
- [ ] Mudança de paginação ou ordem de registros mantém estado parcial até reconciliação demonstrada.
- [ ] Watermarks não atravessam lacunas; atualizações antigas chegam à base sem regredir versões mais recentes.
- [ ] Scripts reais possuem ajuda, saída sanitizada, exit codes, dry-run e comandos reproduzíveis por Compose.
- [ ] Scheduler/worker executam recorrência real, com pausa/retomada e recuperação após indisponibilidade.
- [ ] Backup restaurado e verificado; proteção do principal comprovada; nenhuma credencial nos artefatos.
- [ ] Relatório distingue completude por recurso/período, amostra, estimativa, pendência e resultado efetivamente observado.
- [ ] S08 auditou o marco histórico: automação ativa não encerrou a carga prematuramente, e convergência sem snapshot não foi descrita como prova absoluta.

## 10. Estado persistente, revisão e resposta ao usuário

Atualize `STATUS.md` e crie `repasses/SXX.md` no fim de cada sessão com: objetivo, dependências, arquivos alterados, evidências, comandos/exit codes, chamadas externas antes/depois, estado do banco, manifests/jobs/checkpoints, recursos agendados, pendências e próxima etapa. Nunca registre segredo; referência a caminho ignorado basta. Um novo executor deve retomar desses arquivos sem depender da conversa.

Antes de fechar cada etapa, revise criticamente: cobertura omitida, perda histórica, quotas renovadas por restart, retries multiplicados, checkpoint prematuro, total estável com IDs movidos, regressão temporal, agenda duplicada, principal atingido e evidência descrita além do que foi executado. Quando houver delegação, revisão é leitura e não faz chamadas à fonte nem altera runtime.

Mudança de plano exige registro datado em `STATUS.md`: motivo, etapa inserida/dividida/reordenada, impacto nas dependências, orçamento, critério de saída e rollback. Não suprima requisitos de retenção/cobertura para facilitar o aceite. Não altere a memória pessoal do usuário nesta missão.

Na resposta final de cada etapa, informe o resultado concreto, caminhos locais, validação efetiva, requisições/cotas, cobertura, bloqueios materiais e próximo passo. Na entrega do sistema, inclua os comandos reais para iniciar, consultar, pausar e retomar. Nunca declare “automação concluída” ou “todas as licitações armazenadas” por ter criado scripts/schedule ou por um flow aparecer como `Completed`.
