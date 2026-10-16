# Sessão 08 Pacote final e apresentação

Tempo alvo 40 minutos. Dependências S07 com ensaio completo ou parcial documentado.
Propriedade dos arquivos scripts de inicialização; documentação final; roteiro e materiais de apoio.

## Prompt para copiar

Você está no workspace <checkout-local-radar_licita>. A apresentação é em 16/10/2026, America/Sao_Paulo. O Radar Licita executável desta missão fica em backend/ e frontend/. motor_decisao/ é o legado SILLO e a especificação ampla, preservados. Estamos preparando um MVP local de apoio à decisão por item; não é operação comercial nem implementação integral do PRD.

Leia AGENTS.md aplicáveis, plans/mvp-apresentacao-2026-10-16/README.md, STATUS.md, CONTRATO_DEMO.md, ESCOPO_ORCAMENTO_E_PARCEIROS.md e os repasses das dependências antes de alterar arquivos. Confira o estado atual: documentação de conclusão anterior não é prova de funcionamento. Execute somente esta sessão e sua validação. Use subagentes para trabalho independente se ajudar, com propriedade de arquivos clara.

Preserve o banco backend/radar_licita.db e todos os dados preexistentes. Use destino explícito de demo/teste e scheduler desativado; confirme o caminho resolvido antes de qualquer migração ou escrita de dados. Não exponha .env ou credenciais. Não faça push, deploy, contato com fornecedores, propostas, exclusão de dados, atualização ampla de pacotes ou migração do legado. A raiz está inicialmente sem commits e sem remote; não dependa de worktree ou PR.

Dados oficiais mantêm datas, origem e texto originais; sintéticos são identificados na UI e no manifesto. Dinheiro usa Decimal no backend, taxas como fração e ausência como null. Produto possivelmente compatível, cotação sem vigência comprovada, logística D ou premissas críticas incompletas mantêm pendência. Viabilidade refere-se ao item e às premissas, com revisão humana.

No fim, escreva repasses/SXX.md na pasta do plano com alterações, comandos, resultados, decisões, limitações e próxima ação. Atualize STATUS.md sem inventar conclusão. Se houver execução paralela, entregue seu repasse e deixe ao coordenador a atualização compartilhada. Não marque concluída com critério de saída pendente. Não peça confirmação para correções reversíveis já incluídas no escopo.

Nesta revisão 1.1, selecionar orçamento e buscar possíveis parceiros por resultados de pregão eletrônico são requisitos obrigatórios. Não remeter a busca inicial para uma fase futura. Histórico indica candidatos; cotação atual sustenta aquisição. A S05 pode fazer leitura pública limitada e documentada para validar o adapter; suites automatizadas permanecem offline. Preservar fonte e não coletar CPF ou contatar empresas.

## Contexto desta sessão

A apresentação é em 16/10/2026. O objetivo agora é facilitar início, repetição e explicação. Congelar funcionalidades e usar somente comportamentos ensaiados; não prometer validação comercial.

Arquivos de referência: repasses/S07.md; output/demo/evidencias/; manifesto; README.md raiz; frontend/pnpm-lock.yaml; scripts de S02.

## Trabalho autorizado

1. Entregar procedimento simples de preparação e inicialização com portas, ambiente e caminhos verificados; preferir execução local já ensaiada. Criar scripts PowerShell se ajudarem, sem abrir janelas visíveis para helpers em segundo plano; validar falha de porta ocupada.

2. Entregar README_DEMO.md com requisitos, comando real de carga, início, verificação, parada e recuperação. Preservar lock e evitar reinstalação no momento da apresentação. Docker é opcional; não alegar teste de Compose sem executá-lo.

3. Produzir roteiro de sete minutos e ficha de apoio centrados em informar orçamento, receber oportunidades, abrir item, buscar parceiro por evidência pública e cadastrar para cotação. Distinguir casos financeiros sintéticos, histórico oficial e dados ainda indisponíveis.

4. Reunir capturas do ensaio e preparar apoio offline identificado como demonstração registrada. Excluir segredos e cópias desnecessárias do banco original do pacote.

5. Atualizar README da raiz e STATUS.md com o que foi de fato implementado, resultado dos testes/build, limitações e instrução para a próxima sessão após a apresentação. Não apagar histórico de divergências.

6. Fazer uma última abertura pelos comandos finais, ensaiar ordem dos cliques e congelar. Manter reserva para incidente. Informar com clareza se o resultado é MVP funcional, recorte com pendências ou demonstração registrada.

## Verificação e critério de saída

Outro chat consegue iniciar pelo README_DEMO.md; captura e roteiro condizem com o estado real; não há dependência de sincronização externa. Arquivos finais e suas localizações são informados ao usuário.

Comandos existentes de validação: no diretório backend/, usar .\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider, com APP_ENV=test e AUTO_START_SCHEDULER=false; no diretório frontend/, usar pnpm build. Não repetir testes amplos sem nova alteração ou falha que justifique. Comandos novos de demo só podem ser citados como existentes depois de implementados e executados.

## Entrega e continuidade

README_DEMO.md; scripts validados; roteiro e ficha de apoio; evidências; repasses/S08.md; STATUS.md final. Não publicar nem enviar material a terceiros.

Use repasses/S08.md, no lugar de SXX no contrato comum. Registre em STATUS.md o próximo passo real.

## Ajuste ao prazo

Sem tempo para PDF ou embalagem extra, entregar roteiro Markdown e capturas existentes. A abertura ensaiada e a descrição honesta do estado são prioritárias.
