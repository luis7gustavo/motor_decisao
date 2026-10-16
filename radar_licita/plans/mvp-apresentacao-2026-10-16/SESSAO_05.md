# Sessão 05 Busca inicial de parceiros por pregão

Tempo alvo 110 minutos. Dependências S03 e contrato congelado.
Propriedade dos arquivos backend/app/partners/ e testes próprios; usar modelos compartilhados de S03 sem editá-los no paralelo.

## Prompt para copiar

Você está no workspace <checkout-local-radar_licita>. A apresentação é em 16/10/2026, America/Sao_Paulo. O Radar Licita executável desta missão fica em backend/ e frontend/. motor_decisao/ é o legado SILLO e a especificação ampla, preservados. Estamos preparando um MVP local de apoio à decisão por item; não é operação comercial nem implementação integral do PRD.

Leia AGENTS.md aplicáveis, plans/mvp-apresentacao-2026-10-16/README.md, STATUS.md, CONTRATO_DEMO.md, ESCOPO_ORCAMENTO_E_PARCEIROS.md e os repasses das dependências antes de alterar arquivos. Confira o estado atual: documentação de conclusão anterior não é prova de funcionamento. Execute somente esta sessão e sua validação. Use subagentes para trabalho independente se ajudar, com propriedade de arquivos clara.

Preserve o banco backend/radar_licita.db e todos os dados preexistentes. Use destino explícito de demo/teste e scheduler desativado; confirme o caminho resolvido antes de qualquer migração ou escrita de dados. Não exponha .env ou credenciais. Não faça push, deploy, contato com fornecedores, propostas, exclusão de dados, atualização ampla de pacotes ou migração do legado. A raiz está inicialmente sem commits e sem remote; não dependa de worktree ou PR.

Dados oficiais mantêm datas, origem e texto originais; sintéticos são identificados na UI e no manifesto. Dinheiro usa Decimal no backend, taxas como fração e ausência como null. Produto possivelmente compatível, cotação sem vigência comprovada, logística D ou premissas críticas incompletas mantêm pendência. Viabilidade refere-se ao item e às premissas, com revisão humana.

No fim, escreva repasses/SXX.md na pasta do plano com alterações, comandos, resultados, decisões, limitações e próxima ação. Atualize STATUS.md sem inventar conclusão. Se houver execução paralela, entregue seu repasse e deixe ao coordenador a atualização compartilhada. Não marque concluída com critério de saída pendente. Não peça confirmação para correções reversíveis já incluídas no escopo.

Nesta revisão 1.1, selecionar orçamento e buscar possíveis parceiros por resultados de pregão eletrônico são requisitos obrigatórios. Não remeter a busca inicial para uma fase futura. Histórico indica candidatos; cotação atual sustenta aquisição. A S05 pode fazer leitura pública limitada e documentada para validar o adapter; suites automatizadas permanecem offline. Preservar fonte e não coletar CPF ou contatar empresas.

## Contexto desta sessão

O usuário antecipou a busca inicial de candidatos para este MVP. Há rota oficial documentada de resultados por item e uma cadeia real PJ na evidência do pacote. A tarefa é criar o adapter executável e a busca limitada, não exibir uma lista fixa de empresas.

Arquivos de referência: ESCOPO_ORCAMENTO_E_PARCEIROS.md; evidencias/PNCP_VERIFICACAO_2026-10-15.json; backend/app/ingestion/pncp_client.py; backend/app/models.py; repasses/S03.md.

## Trabalho autorizado

1. Implementar adapter público com rotas/tipos confirmados, consulta de publicação em modalidade de pregão eletrônico, itens e resultados por item. Respeitar limite total de tentativas/bytes/tempo e Retry-After; não coletar nacionalmente nem inserir dados em APIs oficiais.

2. Implementar busca por item/termo, unidade e atributos com filtro local quando o endpoint não tiver busca textual. Guardar janela consultada e cobertura parcial. Situação do resultado, cancelamento e tipo PJ são verificações explícitas.

3. Normalizar empresa e evidências nos modelos de S03, com deduplicação por documento completo e evento/versão. Preservar descrição, unidade, natureza do preço e URL. Não coletar CPF ou contatos pessoais e não presumir validade de dado faltante.

4. Expor POST partner-searches, GET partner-candidates e POST promote em router próprio. Cadastro conserva a origem e não cria cotação ou disponibilidade. Explicar comparabilidade e separar candidatos pendentes.

5. Fazer uma tentativa real limitada com o adapter e registrar ledger sanitizado. Importar a compra/item histórico de arado correspondente à amostra e executar a pesquisa nesse item, completando metadados pela fonte sem inventar campos. Usar a amostra também como replay offline de parser/persistência/UI; não associar arado a notebook. Só fixtures sintéticas não cumprem o aceite.

6. Testar duplicatas, retificação/cancelamento, mesmo nome com CNPJ diferente, resultado vazio, timeout/429, preço ausente e preservação do cache. Uma categoria observada não prova utilidade em outra.

7. Entregar integração de router ao coordenador S07 sem editar arquivos compartilhados durante o paralelo. Informar à S06 schemas concretos e exemplos com dados identificados.

## Verificação e critério de saída

Adapter executável e limitado; ao menos uma cadeia real PJ processada com origem completa; busca gera candidatos pertinentes e cache persistente; repetições não duplicam; falha difere de vazio; promote não cria cotação. Declarar separadamente alcance da chamada real e do replay.

Comandos existentes de validação: no diretório backend/, usar .\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider, com APP_ENV=test e AUTO_START_SCHEDULER=false; no diretório frontend/, usar pnpm build. Não repetir testes amplos sem nova alteração ou falha que justifique. Comandos novos de demo só podem ser citados como existentes depois de implementados e executados.

## Entrega e continuidade

Módulo partners/; testes; ledger de validação; fixture oficial sanitizada; repasses/S05.md. Não declarar parceria confirmada ou cobertura nacional.

Use repasses/S05.md, no lugar de SXX no contrato comum. Registre em STATUS.md o próximo passo real.

## Ajuste ao prazo

Reduzir a um item/termo e uma janela pequena, usando cache oficial real no ensaio. Sem adapter executável ou sem processamento de evidência real, registrar requisito incompleto e não trocar por empresas inventadas.
