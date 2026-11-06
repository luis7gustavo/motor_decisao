# Plano de continuidade após a S08 da coleta PNCP

Data da proposta: 04/11/2026, `America/Sao_Paulo`. Estado: **proposto; nenhuma etapa pós-S08 iniciada**.

## Ponto de partida e alcance

Este plano se refere a `plans/coleta-historica-periodica-2026-10-27/STATUS.md`, e não à S08 já encerrada do MVP de apresentação. No estado registrado em 04/11, a S08 histórica continua parcial: a janela retomada de 24 horas ainda estava em execução, havia lacunas de recursos e a cobertura não tinha aceite. Números de tentativas, partições, identidades e saúde dos serviços são fotografias; reler `STATUS.md`, o ledger e a auditoria antes de executar qualquer etapa.

O objetivo seguinte é transformar um acervo histórico auditado em uma fonte durável de consulta para o Radar Licita, mantendo a sincronização e a trilha de origem. A S08 encerra o **marco histórico fixo até 26/10/2026**; não encerra a coleta recorrente. Nenhuma etapa abaixo autoriza aumento de orçamento, novas chamadas PNCP, migração do banco principal, contato com fornecedores ou implantação pública por si só.

### Gate de entrada comum

Antes de N01, verificar o repasse final da S08 e a auditoria por período, modalidade e recurso. Exigir: partições e filas recuperáveis do marco tratadas; falhas e itens indisponíveis classificados com evidência; ledger sem tentativas sem outcome; hashes e restauração íntegros; recorrência avaliada separadamente; cobertura classificada como `integral_verificado`, `reconciliado_sem_snapshot`, `parcial` ou `indisponível_na_fonte` por recurso. Sem controle independente suficiente, **não** chamar a cobertura de integral, mesmo se a S08 operacional for encerrada com limitações explícitas. Se o gate não passar, continuar a S08 pelo seu próprio plano.

Comandos iniciais de leitura no checkout `motor_decisao` (conferir a versão vigente do runbook antes do uso):

```powershell
docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm --no-deps -T --entrypoint python radar-ops -m scripts.radar_pncp_history audit --summary-only
docker compose -f docker-compose.yml -f compose.radar-history-dedicated.yml --profile control --profile radar-history-dedicated run --rm -T --entrypoint python radar-history-dedicated-ops -m scripts.radar_pncp_schedule status
git status --short --branch
```

## Sequência de entregas

Cada N é uma entrega revisável, que pode ocupar mais de uma sessão. Dependências: **S08 → N01 → N02 → N03 → N04 → N05**. Levantamento de UX e fixtures offline para N04 pode avançar em paralelo a N02/N03; nenhuma transferência de dados nem novo consumidor é ativado antes de N03. Não paralelizar consumidores PNCP para aumentar taxa. Reavaliar o plano se o resultado final da S08 mudar o universo ou revelar um controle oficial novo.

### N01 — Transição da coleta para operação contínua

**Contexto para retomada:** S08 aceitou o marco histórico com o nível de cobertura explicitado; C2 contém Bronze, identidades, versões, observações e ledger. A pilha Prefect dedicada coleta `backfill`, `sync` e `reconcile`. A decisão sobre orçamento suficiente para **concluir a S08** e a avaliação do teste de 24 horas pertencem à própria S08; esta entrega trata do regime recorrente depois do marco.

**Trabalho:** registrar fotografia final da S08 e distinguir três estados: acervo histórico, frescor da recorrência e saúde da infraestrutura. Definir, com base na medição real já feita na S08 e nos limites/termos oficiais que puderem ser verificados, orçamento e ritmo **do regime recorrente**, reserva para sincronização e condição de pausa. Atualizar o runbook vigente (a seção inicial ainda descreve a agenda e o teto de 03/11) com comandos, alertas de atraso/429/403/5xx, disco, fila, outcomes ausentes e procedimento de recuperação. Ensaiar backup e restore em destino descartável e preservar a trava da fonte compartilhada.

**Verificação/saída:** `status` e `audit --summary-only` distinguem marco e recorrência; agendamentos/worker e resultado de negócio são correlacionados com ledger; relatório de restore confirma hashes, contagens e limpeza do ensaio; política operacional tem limites e responsáveis explícitos. Nenhum aumento de teto ocorre apenas por passagem de etapa.

**Reversão:** pausar deployment dedicado, aguardar run em curso e pausar fonte, nessa ordem; conservar C2, Bronze, ledger, volumes e `exclusive.lock`. Não reiniciar contador nem worker compartilhado.

### N02 — Baseline reproduzível do coletor

**Contexto para retomada:** a S08 está auditada e N01 definiu operação. O checkout `motor_decisao` tinha mudanças extensas e arquivos não rastreados em 04/11; a branch local estava dois commits à frente da remota. Não pressupor que tudo pertença à mesma entrega.

**Trabalho:** inventariar o diff por coletor, infraestrutura, documentação e alterações alheias. Registrar revisão de código, contratos e imagens efetivamente em operação; preparar mudanças pequenas e revisáveis sem limpar nem sobrescrever trabalho preexistente. Fixar procedimento de build, migração em alvo segregado, testes offline, backup/restore e retorno a imagem compatível. Se o GitHub CLI continuar indisponível, preparar revisão local com Git; publicar PR somente quando houver ferramenta e destino apropriados.

**Verificação/saída:** `git diff --check`, suíte pertinente em banco descartável, `docker compose ... config --quiet`, replay offline e restore aprovado; o código reproduz o estado operacional sem usar o banco principal. Registrar versões exatas e evidências no repasse. Nenhuma validação sintética é apresentada como prova de cobertura.

**Reversão:** retornar apenas o código/serviços desta entrega à revisão compatível, sem downgrade destrutivo ou exclusão de dados e sem tocar em alterações preexistentes não relacionadas.

### N03 — Contrato de publicação do acervo para o produto

**Contexto para retomada:** N02 fornece uma baseline reproduzível. O C2 é alvo segregado de coleta, enquanto o banco principal ainda não recebeu o schema `procurement`; o protótipo `radar_licita` e a API SILLO têm destinos distintos. `docs/licitacoes/16-plano-de-migracao.md` propõe gates G0–G7, mas é especificação anterior, não prova de implantação.

**Trabalho:** escrever ADR curto que escolha o primeiro consumidor e a fronteira de dados. Opção inicial a validar: projeção **somente leitura**, versionada e idempotente, do C2 para uma cópia isolada do ambiente do produto; conservar C2 como autoridade da coleta. Definir campos mínimos (identidade oficial, versão, datas, modalidade, UF, status, itens/resultados/documentos quando comprovados), proveniência, classificação de cobertura, instante de atualização, lacunas e política de dados pessoais. Confrontar esse contrato com as fases 1 e 2 do roadmap e com a estrutura real do protótipo. Ensaiar G0–G4 em cópia, incluindo contagens/hashes, reexecução, retificação, cancelamento e recuperação; definir snapshot e ponto de retomada para não perder versões enquanto a recorrência segue ativa. Decidir explicitamente se e quando G5 pode levar dados ao principal; não fazer o corte nesta etapa.

**Verificação/saída:** ADR identifica consumidor e esquema de autoridade; exportação/projeção de uma amostra congelada é repetível sem duplicar versões; falhas preservam cursor e origem; consultas não exibem lacunas como dados ausentes confirmados. Relatório dos gates G0–G4 e de rollback em cópia fica disponível para revisão.

**Reversão:** desativar a publicação/projeção, preservar seu log de transferências e a autoridade no C2; descartar somente a cópia de ensaio identificada, nunca o banco principal.

### N04 — Consulta útil e explicável no Radar Licita

**Contexto para retomada:** N03 aprovou contrato, consumidor e cópia isolada. N04 e N05 usam somente essa cópia, sem G5 ou corte para o principal. O MVP de apresentação em `radar_licita` demonstra jornadas locais, mas não comprova consumo do acervo nacional nem aptidão comercial das empresas históricas.

**Trabalho:** entregar um recorte vertical de consulta: busca e filtros de contratação/itens, detalhe com link PNCP, versão/retificação, última sincronização, estado do prazo e indicador de cobertura por recurso. Mostrar resultado PJ histórico como **candidato à investigação**, sem transformá-lo em fornecedor atual, cotação, produto compatível ou oportunidade viável. Definir o comportamento quando documentos/itens/resultados estiverem incompletos e quando o PNCP estiver indisponível. Manter decisões humanas e premissas auditáveis.

**Verificação/saída:** casos com duas versões, prazo vencido, lacuna documental, fonte indisponível e dois IDs oficiais de mesmo título passam em API e UI; o usuário consegue rastrear cada fato ao payload/URL e ao instante de observação. Medir tempo e clareza da busca em um conjunto real delimitado, sem afirmar cobertura geral da interface.

**Reversão:** desligar o novo consumidor/feature flag e voltar à experiência anterior; manter dados publicados e trilha de versão para diagnóstico.

### N05 — Piloto interno delimitado e decisão de expansão

**Contexto para retomada:** N04 oferece consulta rastreável; recursos necessários ao caso de uso estão validados. `docs/licitacoes/23-especificacao-piloto-fornecedores.md` propõe uma categoria, até 20 itens e quatro semanas, mas categoria/UF, esforço e metas ainda dependem de decisão do operador.

**Trabalho:** registrar escopo versionado (categoria, UF, janela, regra de entrada consecutiva, horas de análise e critérios de sucesso) **antes da busca**. Incluir até 20 itens de oportunidades descritos de forma suficiente conforme essa regra, avaliando elegibilidade sem consultar resultados históricos, e manter todos no denominador, inclusive os sem resultado; se houver menos de 20, reportar o número real. Registrar lacunas/rejeições. Medir se a busca ajuda a identificar candidatos e preparar pesquisa/cotação **manual**, sem presumir estoque, preço ou fornecimento atual. Acompanhar precisão de vínculos, frescor, tempo humano, casos sem resultado e custo operacional. Decidir ampliar, corrigir ou encerrar o recorte com base nos dados. Contato externo exige instrução específica do usuário.

**Verificação/saída:** relatório desta **avaliação exploratória de descoberta na cópia isolada** tem denominadores, amostras, decisões e erros; nenhuma recomendação positiva depende apenas de resultado histórico. **G5, G6 e G7 permanecem pendentes**: eventual corte operacional e ativação gradual exigem a sequência de gates de `16-plano-de-migracao.md`, e G7 exige a jornada Must completa das fases 1–5. O restante do MVP (documentos/logística, fornecedores/matching, finanças/ranking e operação) segue o roadmap existente e recebe plano próprio após os achados.

**Reversão:** parar o recorte e o novo módulo, manter dados e decisões auditáveis; não alterar a coleta recorrente por falha de UX ou hipótese comercial.

## Revisão e mudança do plano

Revisar ao fim de cada entrega: cobertura e frescor reais, custo de API/armazenamento, regressão de identidade/versão, proteção do principal e efeitos em operações legadas. Uma mudança de escopo deve registrar motivo, novas dependências, orçamento, critérios de saída e reversão neste arquivo. O `STATUS.md` da coleta permanece a fonte de verdade da S08 e só deve ser alterado quando houver mudança efetiva naquela missão.

Fontes locais de partida: `plans/coleta-historica-periodica-2026-10-27/STATUS.md`, `PROMPT_MESTRE.md`, `docs/licitacoes/15-roadmap-backlog.md`, `16-plano-de-migracao.md`, `23-especificacao-piloto-fornecedores.md` e `docs/licitacoes/operacao-coleta-historica-prefect.md`. Essas especificações de produto são propostas datadas; verificar o código e o ambiente antes da implementação.
