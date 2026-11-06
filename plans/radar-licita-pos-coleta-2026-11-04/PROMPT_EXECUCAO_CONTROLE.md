# Radar Licita — prompt de execução e controle após a coleta

Atualizado em 04/11/2026 (America/Sao_Paulo). Este prompt pode ser usado em outra sessão enquanto a missão histórica S08 continua. Ele não declara concluídos os pontos 1–4. Em 04/11, o teste de até 60 tentativas/minuto e o teto cumulativo de 100.000 já haviam sido implementados e autorizados; não os reproponha como trabalho pendente sem verificar o estado novo.

## Prompt para o agente executor

Você está no repositório `motor_decisao`, dentro de `C:\Users\luisg\OneDrive\Documentos\ChatGPT\motor publico`. Construa o Radar Licita progressivamente, preservando a coleta PNCP dedicada e o sistema SILLO existente. O objetivo do produto é encontrar oportunidades públicas e demonstrar, com evidências atuais, se o operador consegue atendê-las com produto compatível, logística, capital e margem. A decisão e qualquer participação permanecem humanas.

### 0. Retomada obrigatória e controle

1. Leia o `AGENTS.md` aplicável, `STATUS.md` da missão em `plans/coleta-historica-periodica-2026-10-27/`, `docs/licitacoes/00-prd.md`, `01-visao-produto.md`, `07-arquitetura-to-be.md`, `15-roadmap-backlog.md`, `16-plano-de-migracao.md`, `18-glossario.md` e o runbook `operacao-coleta-historica-prefect.md`. Confira o checkout e o estado real, pois horários, orçamento, cobertura e serviços mudam.
2. Antes de trabalhar, registre em um relatório datado: branch/diff preexistente, estado do Prefect dedicado, resultado de negócio da última execução, estado da missão e da fonte, contadores do ledger, auditoria de cobertura, lacunas dos quatro recursos e localização do banco C2. Diferencie evidência observada de hipótese.
3. Para consultas frequentes, use comandos somente de leitura descritos no runbook: `radar_pncp_schedule status`, `radar_pncp_history status`, `audit --summary-only` e `radar_pncp_s08_24h_report`. Não interprete `Completed`, HTTP 200/204 ou partição `complete` como prova de cobertura integral.
4. Preserve `backups/pncp-control/exclusive.lock`, o banco principal, o destino C2, Bronze, ledger, recibos e backups. Este é o plano da trilha de produto. Mudanças futuras da coleta, orçamento, agenda ou serviços pertencem à trilha operacional, com análise e recibo próprios; a proibição neste prompt não as proíbe globalmente. Não altere esses componentes nesta trilha.
5. Antes de editar código, isole a trilha de produto em branch/worktree próprio. O worker dedicado monta `app/`, `pipelines/`, `scripts/` e `config/` do checkout operacional: mudanças nesses diretórios podem afetar uma execução em curso mesmo sem deploy. O checkout atual tem alterações não commitadas; estabeleça uma base reproduzível para o novo worktree sem sobrescrevê-las. Se o isolamento não estiver demonstrado, limite-se a documentação, desenho e fixtures fora dos diretórios montados. Não permita dois agentes editando o mesmo checkout ao mesmo tempo.
6. Faça uma mudança pequena por etapa, com teste significativo, evidência de saída e atualização do relatório. Se uma premissa mudar, registre motivo, impacto, dependências e novo critério antes de reordenar o plano.

### Gate A — pontos 1–4 do mapa conceitual

Mantenha uma tabela de estado `comprovado / parcial / pendente / bloqueado` para cada ponto:

1. **Coleta oficial:** backfill histórico, sincronização de publicações e atualizações, identificação de propostas abertas e continuidade após falhas. Exija ledger íntegro, limites e janela respeitados, worker/schedules observados e resultado de negócio correlacionado.
2. **Cobertura e qualidade:** auditoria separada do marco histórico e da recorrência; reconciliação; lacunas, rejeições, versões/retificações e frescor visíveis. Cobertura total só com prova independente suficiente, não apenas respostas válidas ou paginação aparentemente completa.
3. **Base organizada:** identidade de contratação, versão, item e unidade de julgamento; rastreabilidade até payload Bronze, hash e data. Valide contagens, duplicidade, integridade referencial e ausência de promoção otimista.
4. **Enriquecimento:** detalhe, itens, resultados e documentos validados por recurso, inclusive paginação, escopo, custo de API, armazenamento e extração segura. Exigências ambíguas seguem para revisão humana. Não marque este ponto concluído enquanto os quatro gaps da S08 estiverem pendentes.

Na trilha operacional, acompanhe especialmente a recorrência: compare a hora da última atualização oficial observada com a última sincronização bem-sucedida, a idade das partições pendentes e o resultado de negócio dos runs de 00h/06h/12h/18h. A contagem de partições pendentes sozinha não prova atraso da carteira. Se o frescor necessário à Etapa B falhar, proponha prioridade para sync antes de backfill com evidência de impacto, teste isolado e alteração auditada **fora deste prompt**. Não mude a agenda durante a janela de 24 horas apenas por essa inferência.

Enquanto os critérios específicos do módulo e recorte ainda não estiverem comprovados, desenvolva e teste com fixtures versionadas em ambiente descartável segregado. Não copie dados do C2 para o banco principal nem apresente um protótipo como análise de licitação real. Libere integração real por capacidade e recurso, com matriz que mostre qual evidência do Gate A cada saída usa e quais lacunas continuam visíveis. B exige identidade, versões, abertura e frescor validados no recorte; C exige itens/resultados comparáveis validados; D exige identidade, documentos/exigências e fornecimento atual suficientes para a unidade analisada. A conclusão global da coleta histórica e o histórico de resultados (Should) não são pré-requisitos ocultos para as capacidades Must que usam dados atuais suficientes.

Antes de migrations ou integração operacional, cumpra G0–G4 de `16-plano-de-migracao.md`: destino explícito, inventário, backup/restore ensaiado e expansão/backfill em cópia. Desenvolvimento e testes não escrevem no C2 nem no principal. A passagem operacional segue G5–G6 e autorização aplicável, em janela própria; este prompt não autoriza pausar a coleta dedicada, alterar serviços ou executar o corte.

### Etapa B — radar consultável (depende do ponto 3; pode começar offline)

Contexto: a busca e a carteira devem usar as versões oficiais normalizadas, sem consulta PNCP durante o GET da interface. Implemente filtros por prazo, local, categoria, objeto e estado; detalhes com fonte, versão e frescor; lista de alterações; alertas locais sem envio externo automático. Projete a interface com Jinja2/FastAPI conforme a arquitetura vigente.

Saída: fluxo de descoberta utilizável em dados de teste, estados de vazio/erro/desatualizado e prova de que uma retificação muda a atualidade exibida. Teste busca, paginação, duplicação e fonte indisponível. Integração real exige Gate A pertinente e revisão das regras de classificação de oportunidade aberta.

### Etapa C — inteligência histórica básica (depende do ponto 3 e dos resultados validados do ponto 4)

Contexto: compare apenas itens tecnicamente comparáveis e preserve data, unidade, quantidade, etapa, órgão, fornecedor e origem. Produza faixas ou referências contextualizadas e candidatos a fornecedor para investigação manual. Histórico público não vira cotação atual, disponibilidade, capacidade de fornecimento ou bônus no score.

Saída: comparação explicável, indicação de amostra insuficiente e casos de não comparabilidade; teste contra mistura de unidades, etapas, versões e fornecedores homônimos. Pode ser desenvolvida em paralelo à Etapa B após contratos de identidade estáveis. Integração real depende da validação de resultados do Gate A.

### Etapa D — motor de viabilidade (depende de exigências e identidade; pode iniciar com fixtures)

Contexto: cadastro de fornecedor e cotação válida; comparação técnica produto–item; modelo de entrega, prazos e obrigações; custos, tributos parametrizados, frete, capital de giro e caixa da unidade completa de julgamento. Use `Decimal` no cálculo. Aplique impedimentos e pendências antes de ordenar as oportunidades viáveis. Guarde snapshot, versões, premissas e justificativas.

Saída: cenários reproduzíveis `bloqueada / pendente / viável`, sem positivo quando falta evidência obrigatória, com revisão humana. Teste lote completo, cotação vencida, produto incompatível, retificação concorrente, falta de capital e atraso de recebimento. Não calcule margem com preço histórico como custo atual. Antes do piloto real, o operador define categoria, recorte geográfico, capital disponível e modelo de fornecimento; até lá, prototipe D com fixtures genéricas explicitamente sintéticas. Papel A4 consta da documentação apenas como hipótese, não como escolha aprovada.

### Etapa E — carteira e acompanhamento (depende da Etapa D)

Contexto: fila de revisão, registro de decisão, participação externa informada manualmente, resultado, execução, entrega e recebimento. Preserve eventos e snapshots anteriores; mudança de fonte exige reanálise, não apaga atos ocorridos. Meça tempo de triagem, falsos descartes, qualidade do matching e diferença entre cenário e realizado.

Saída: jornada de ponta a ponta revisável, transições comerciais válidas, trilha de auditoria e piloto medido. Não automatize proposta/lance, contato, compra ou pagamento.

### Ordem e dependências

`Gate A(1–3) -> B`; `Gate A(3–4 resultados) -> C`; `Gate A(3–4 exigências) + fornecedores atuais -> D`; `D -> E`. B e C podem avançar separadamente; D pode ser prototipada com fixtures antes de concluir o Gate A. A inteligência histórica C é opcional para uma análise D com insumos atuais suficientes. Em cada etapa, exija diff revisado, testes relevantes verdes e nenhum achado crítico aberto.

### Revisão adversarial e encerramento de cada etapa

Antes de declarar uma etapa pronta, peça revisão independente do diff e dos critérios: procure dados antigos apresentados como atuais, falso positivo, falsa comparabilidade, violação de isolamento C2/principal, cálculo financeiro incompleto, duplicação e mudança de fonte perdida. Corrija achados críticos. Entregue: o que foi feito, testes executados, evidência, riscos restantes, dependências ainda abertas e próximo passo. `MVP concluído` exige a jornada E e os critérios Must/RNF do PRD, não apenas coleta ou interface.
