# Sessão 07 Integração e ensaio completo

Tempo alvo 75 minutos. Dependências S04 S05 e S06; ensaio parcial permitido com critérios faltantes explicitados.
Propriedade dos arquivos Integração, testes do percurso e pequenas correções no backend/frontend após fim do paralelo.

## Prompt para copiar

Você está no workspace <checkout-local-radar_licita>. A apresentação é em 16/10/2026, America/Sao_Paulo. O Radar Licita executável desta missão fica em backend/ e frontend/. motor_decisao/ é o legado SILLO e a especificação ampla, preservados. Estamos preparando um MVP local de apoio à decisão por item; não é operação comercial nem implementação integral do PRD.

Leia AGENTS.md aplicáveis, plans/mvp-apresentacao-2026-10-16/README.md, STATUS.md, CONTRATO_DEMO.md, ESCOPO_ORCAMENTO_E_PARCEIROS.md e os repasses das dependências antes de alterar arquivos. Confira o estado atual: documentação de conclusão anterior não é prova de funcionamento. Execute somente esta sessão e sua validação. Use subagentes para trabalho independente se ajudar, com propriedade de arquivos clara.

Preserve o banco backend/radar_licita.db e todos os dados preexistentes. Use destino explícito de demo/teste e scheduler desativado; confirme o caminho resolvido antes de qualquer migração ou escrita de dados. Não exponha .env ou credenciais. Não faça push, deploy, contato com fornecedores, propostas, exclusão de dados, atualização ampla de pacotes ou migração do legado. A raiz está inicialmente sem commits e sem remote; não dependa de worktree ou PR.

Dados oficiais mantêm datas, origem e texto originais; sintéticos são identificados na UI e no manifesto. Dinheiro usa Decimal no backend, taxas como fração e ausência como null. Produto possivelmente compatível, cotação sem vigência comprovada, logística D ou premissas críticas incompletas mantêm pendência. Viabilidade refere-se ao item e às premissas, com revisão humana.

No fim, escreva repasses/SXX.md na pasta do plano com alterações, comandos, resultados, decisões, limitações e próxima ação. Atualize STATUS.md sem inventar conclusão. Se houver execução paralela, entregue seu repasse e deixe ao coordenador a atualização compartilhada. Não marque concluída com critério de saída pendente. Não peça confirmação para correções reversíveis já incluídas no escopo.

Nesta revisão 1.1, selecionar orçamento e buscar possíveis parceiros por resultados de pregão eletrônico são requisitos obrigatórios. Não remeter a busca inicial para uma fase futura. Histórico indica candidatos; cotação atual sustenta aquisição. A S05 pode fazer leitura pública limitada e documentada para validar o adapter; suites automatizadas permanecem offline. Preservar fonte e não coletar CPF ou contatar empresas.

## Contexto desta sessão

Backend e interface precisam produzir o mesmo resultado em dados persistidos. Teste unitário ou build isolado não prova que a apresentação funciona. No caminho completo, repetir o percurso após reinício e sem serviços externos. No caminho reduzido, executar apenas os passos compatíveis com a simulação pendente ou com capturas verificadas; não aplicar migração incompleta nem exigir caso viável. Registrar cada critério completo não atendido.

Arquivos de referência: repasses/S03.md a S06.md; módulos recommendations/partners; frontend/src/; manifesto e evidência oficial; STATUS.md.

## Trabalho autorizado

1. Conectar os routers de recommendations e partners em main.py, registrar modelos e validar OpenAPI da aplicação real. Resolver diferenças de contrato uma vez; nenhum recurso pode ficar disponível somente em app de teste.

2. Aplicar migração apenas na base de demo verificada e repetir a carga após S03. Conferir que dados sintéticos são calculados pelo produto, não por atualização direta do score. Validar JSON e schema da API.

3. Executar a suíte do backend e pnpm build. Cobrir roteiro de API com registro persistido/reaberto, cotação, cenário, análise, status e resumo vigente. Registrar resultados reais.

4. Ensaiar orçamento R$ 5.000/10.000/12.000, redução e resposta atrasada na UI, item atual versus histórico e custo do edital versus capital. Confirmar que orçamento da busca prevalece somente no contexto da leitura.

5. Ensaiar busca de parceiro no item histórico de arado importado em S05, com cadeia oficial real e identidade correta. Consultar evidência, promover candidato e manter cotação manual separada. Repetir offline com cache identificado; conferir ausência de duplicatas/preço de aquisição automático. Não usar o notebook sintético para exibir fornecedor de arado.

6. No navegador, percorrer radar, item, evidência local, seleção de cotação, simulação, motivos e status. Capturar telas dos três estados; verificar ausência de erros no console nas ações usadas.

7. Com a aplicação mantida acessível em localhost, simular falha de fonte externa sem desativar a rede local. Repetir todo o roteiro sem PNCP e sem downloads externos. A UI deve usar base e documentos locais.

8. Reiniciar API e frontend e repetir consulta de análise/status. Conferir caso encerrado fora do filtro de abertas e atualização do resumo depois de substituir resultado positivo por bloqueado.

9. Parar a API, realizar backup da base demo e testar restauração para arquivo novo. Documentar recuperação sem tocar no original. Corrigir só falhas que impedem o aceite.

## Verificação e critério de saída

Aceite completo exige S04/S05/S06 integradas na aplicação real, orçamento reativo correto e busca executável com cadeia real/cache. Regressões de S03 continuam obrigatórias. Se faltar recurso central, produzir matriz parcial e manter MVP incompleto; não concluir sessões faltantes por causa do prazo.

Comandos existentes de validação: no diretório backend/, usar .\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider, com APP_ENV=test e AUTO_START_SCHEDULER=false; no diretório frontend/, usar pnpm build. Não repetir testes amplos sem nova alteração ou falha que justifique. Comandos novos de demo só podem ser citados como existentes depois de implementados e executados.

## Entrega e continuidade

repasses/S07.md; matriz dos aceites com dados reais/sintéticos identificados; capturas em output/demo/evidencias/; limites e resultado do ensaio. S08 só relata o que foi verificado.

Use repasses/S07.md, no lugar de SXX no contrato comum. Registre em STATUS.md o próximo passo real.

## Ajuste ao prazo

Ensaiar as partes funcionais e registrar critérios não atendidos; liberar S08 para material parcial sem declarar MVP completo. Manter os dois novos requisitos como pendências explícitas se falharem.
