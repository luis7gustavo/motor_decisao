# Sessão 06 Jornada visual da demonstração

Tempo alvo 110 minutos. Dependências S03 e contratos congelados; integração final depende de S04 e S05.
Propriedade dos arquivos frontend/src/App.tsx; frontend/src/api.ts; frontend/src/types.ts; frontend/src/styles.css.

## Prompt para copiar

Você está no workspace <checkout-local-radar_licita>. A apresentação é em 16/10/2026, America/Sao_Paulo. O Radar Licita executável desta missão fica em backend/ e frontend/. motor_decisao/ é o legado SILLO e a especificação ampla, preservados. Estamos preparando um MVP local de apoio à decisão por item; não é operação comercial nem implementação integral do PRD.

Leia AGENTS.md aplicáveis, plans/mvp-apresentacao-2026-10-16/README.md, STATUS.md, CONTRATO_DEMO.md, ESCOPO_ORCAMENTO_E_PARCEIROS.md e os repasses das dependências antes de alterar arquivos. Confira o estado atual: documentação de conclusão anterior não é prova de funcionamento. Execute somente esta sessão e sua validação. Use subagentes para trabalho independente se ajudar, com propriedade de arquivos clara.

Preserve o banco backend/radar_licita.db e todos os dados preexistentes. Use destino explícito de demo/teste e scheduler desativado; confirme o caminho resolvido antes de qualquer migração ou escrita de dados. Não exponha .env ou credenciais. Não faça push, deploy, contato com fornecedores, propostas, exclusão de dados, atualização ampla de pacotes ou migração do legado. A raiz está inicialmente sem commits e sem remote; não dependa de worktree ou PR.

Dados oficiais mantêm datas, origem e texto originais; sintéticos são identificados na UI e no manifesto. Dinheiro usa Decimal no backend, taxas como fração e ausência como null. Produto possivelmente compatível, cotação sem vigência comprovada, logística D ou premissas críticas incompletas mantêm pendência. Viabilidade refere-se ao item e às premissas, com revisão humana.

No fim, escreva repasses/SXX.md na pasta do plano com alterações, comandos, resultados, decisões, limitações e próxima ação. Atualize STATUS.md sem inventar conclusão. Se houver execução paralela, entregue seu repasse e deixe ao coordenador a atualização compartilhada. Não marque concluída com critério de saída pendente. Não peça confirmação para correções reversíveis já incluídas no escopo.

Nesta revisão 1.1, selecionar orçamento e buscar possíveis parceiros por resultados de pregão eletrônico são requisitos obrigatórios. Não remeter a busca inicial para uma fase futura. Histórico indica candidatos; cotação atual sustenta aquisição. A S05 pode fazer leitura pública limitada e documentada para validar o adapter; suites automatizadas permanecem offline. Preservar fonte e não coletar CPF ou contatar empresas.

## Contexto desta sessão

A interface existente já tem as páginas principais. Faltam vínculo de cotação ao item, atualização de status, confirmação de premissas e tratamento consistente de erros. Todo o App.tsx deve ter um único editor. Agora orçamento e parceiros fazem parte do percurso obrigatório; usar os contratos de S04/S05 e preservar contexto de orçamento ao abrir o detalhe.

Arquivos de referência: frontend/src/App.tsx; frontend/src/api.ts; frontend/src/types.ts; frontend/src/styles.css; frontend/vite.config.ts; contrato e manifesto da demo.

## Trabalho autorizado

1. Colocar Quanto posso investir no radar, com campo monetário/presets e busca contextual. Mostrar elegíveis ordenadas, capital, saldo e razões, com seções de pendências/excesso. Debounce e cancelamento/sequência impedem resposta atrasada; alterar orçamento não muda configuração global.

2. Adicionar Buscar possíveis parceiros no detalhe do item: termo/janela/UF, andamento, resultado/cache e erros. Mostrar empresa/CNPJ, item/data/natureza/evidência, razões e botão cadastrar candidata. Não mostrar preço histórico como aquisição ou contato/capacidade confirmados.

3. Preservar navegação e estilo. Mostrar banner de demonstração, origem de cada registro, referência dos dados e identificação de sintéticos. Distinguir arquivo oficial encerrado e janela sintética.

4. No detalhe, selecionar item e associar produto/cotação por procurement_item_id; permitir selecionar supplier_quote_id no simulador e preencher aquisição a partir da cotação. Mostrar preço/frete de referência, divergências manuais e sua pendência. Exibir validade e compatibilidade com razões.

5. Trocar taxes_confirmed=true fixo por confirmação explícita. Expor frete conhecido, capital e premissas; usar assumptions_complete e sensitivity. Taxas da UI em porcentagem devem ser convertidas para fração só no transporte.

6. Exibir elegibilidade do item, pendências, bloqueios e componentes; tratar null sem converter para zero. Não usar status comercial como resultado calculado nem score de outro item.

7. Ao trocar de item, atualizar campos e limpar resultado anterior. Depois de salvar cotação, simular ou alterar status, atualizar detalhe/lista/resumo sem recarregar toda a aplicação.

8. Conectar api.updateStatus ao detalhe e refletir os estados escolhidos no acompanhamento. Preservar enum existente e indicar claramente estados fora do recorte visível; não criar fato comercial realizado.

9. Tratar erro, carregamento e vazio nas operações usadas na demo. Mostrar mensagem e nova tentativa, manter dados digitados e evitar ações duplicadas. No modo demo, desabilitar sincronização externa e leitura remota de documento como pré-requisito.

10. Executar pnpm build sem atualizar dependências. Verificar em janela de notebook e visualização estreita, sem redesign amplo. Se trabalhar em paralelo, usar exemplos do contrato, sem apresentar fixtures como integração concluída.

## Verificação e critério de saída

Build passa. Percurso permite escolher item/cotação, confirmar premissas, calcular e mudar status. Trocar item não reaproveita resultado anterior; erro de API fica visível. Se S04/S05 ainda não terminaram, registrar integração como pendente e deixar o aceite para S07. Alterar orçamento atualiza seleção sem resposta obsoleta; busca mostra evidência e converte candidata em cadastro. Integração final depende de S04/S05 e será concluída em S07.

Comandos existentes de validação: no diretório backend/, usar .\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider, com APP_ENV=test e AUTO_START_SCHEDULER=false; no diretório frontend/, usar pnpm build. Não repetir testes amplos sem nova alteração ou falha que justifique. Comandos novos de demo só podem ser citados como existentes depois de implementados e executados.

## Entrega e continuidade

Frontend consolidado; repasses/S06.md com telas verificadas, comandos e dependências restantes. Não atualizar arquivo compartilhado simultaneamente com S04 e S05.

Use repasses/S06.md, no lugar de SXX no contrato comum. Registre em STATUS.md o próximo passo real.

## Ajuste ao prazo

Cortar gráficos, animações, filtros extras e ajustes de layout secundários. Se necessário, manter cadastro geral de fornecedores e um caminho simples no detalhe, preservando seleção de item e rótulos de origem.
