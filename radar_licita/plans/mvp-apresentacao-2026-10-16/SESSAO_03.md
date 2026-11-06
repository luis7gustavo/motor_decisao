# Sessão 03 Consistência do backend e decisão por item

Tempo alvo 120 minutos. Dependências S02 e contrato congelado S01.
Propriedade dos arquivos backend/app/; backend/alembic/; backend/tests/; nenhuma edição de frontend/src/.

## Prompt para copiar

Você está no workspace <checkout-local-radar_licita>. A apresentação é em 16/10/2026, America/Sao_Paulo. O Radar Licita executável desta missão fica em backend/ e frontend/. motor_decisao/ é o legado SILLO e a especificação ampla, preservados. Estamos preparando um MVP local de apoio à decisão por item; não é operação comercial nem implementação integral do PRD.

Leia AGENTS.md aplicáveis, plans/mvp-apresentacao-2026-10-16/README.md, STATUS.md, CONTRATO_DEMO.md, ESCOPO_ORCAMENTO_E_PARCEIROS.md e os repasses das dependências antes de alterar arquivos. Confira o estado atual: documentação de conclusão anterior não é prova de funcionamento. Execute somente esta sessão e sua validação. Use subagentes para trabalho independente se ajudar, com propriedade de arquivos clara.

Preserve o banco backend/radar_licita.db e todos os dados preexistentes. Use destino explícito de demo/teste e scheduler desativado; confirme o caminho resolvido antes de qualquer migração ou escrita de dados. Não exponha .env ou credenciais. Não faça push, deploy, contato com fornecedores, propostas, exclusão de dados, atualização ampla de pacotes ou migração do legado. A raiz está inicialmente sem commits e sem remote; não dependa de worktree ou PR.

Dados oficiais mantêm datas, origem e texto originais; sintéticos são identificados na UI e no manifesto. Dinheiro usa Decimal no backend, taxas como fração e ausência como null. Produto possivelmente compatível, cotação sem vigência comprovada, logística D ou premissas críticas incompletas mantêm pendência. Viabilidade refere-se ao item e às premissas, com revisão humana.

No fim, escreva repasses/SXX.md na pasta do plano com alterações, comandos, resultados, decisões, limitações e próxima ação. Atualize STATUS.md sem inventar conclusão. Se houver execução paralela, entregue seu repasse e deixe ao coordenador a atualização compartilhada. Não marque concluída com critério de saída pendente. Não peça confirmação para correções reversíveis já incluídas no escopo.

Nesta revisão 1.1, selecionar orçamento e buscar possíveis parceiros por resultados de pregão eletrônico são requisitos obrigatórios. Não remeter a busca inicial para uma fase futura. Histórico indica candidatos; cotação atual sustenta aquisição. A S05 pode fazer leitura pública limitada e documentada para validar o adapter; suites automatizadas permanecem offline. Preservar fonte e não coletar CPF ou contatar empresas.

## Contexto desta sessão

O protótipo já calcula custos e mantém entidades, mas falha em datas persistidas, resposta de sync e exigência de cotação/premissas. O objetivo é concluir corretamente uma análise simples por item e evitar indicação positiva sem suporte.

Arquivos de referência: backend/app/api/routes.py; backend/app/ranking/service.py; backend/app/finance/service.py; backend/app/suppliers/service.py; backend/app/core/time.py; backend/app/models.py; backend/app/schemas.py; backend/tests/.

## Trabalho autorizado

1. Corrigir normalização de datas e comparação em ranking, listagem, contadores e prazo. Testar SQLite após commit, fechamento e nova sessão. Datas oficiais permanecem intactas.

2. Corrigir id/job_id na serialização de sincronização e validar a rota com serviço PNCP substituído por fixture; não executar coleta externa para testar o contrato.

3. Implementar os acréscimos congelados: vínculo da cotação e snapshot do preço/frete no cenário e análise por item; usar migração aditiva com inspeção das estruturas já criadas pela 0001. Testar upgrade de cópia do schema antigo e de SQLite vazio, sem colunas duplicadas. Preservar registros antigos pendentes; aplicar só em demo/teste.

4. Implementar modelos compartilhados congelados de observações de resultados, candidatos e buscas, com dados de origem/versão e chaves únicas. Criar migração de fundação compatível com metadata vivo da 0001, testando banco vazio e cópia antiga. S05 implementa o adapter e usa estes modelos.

5. Extrair uma função comum de avaliação do item sem gravação, com capital explícito de contexto e retorno de componentes/razões. Manter persistência de OpportunityScore na camada chamadora. S04 depende desta função para consultar orçamento sem gravar scores por digitação; demonstrar por teste que consulta de avaliação não muda o banco.

6. Exigir cotação selecionada vigente, vinculada ao item, quantidade/unidade compatíveis, match compatible sustentado por atributos, premissas completas e logística conhecida para positivo. Aquisição deve coincidir com o preço normalizado da cotação e frete com escopo confirmado; override sem confirmação rastreável permanece pendente. Ausências são pendências e impedimentos conhecidos bloqueiam.

7. Unificar capital, margem e pesos entre criação de cenário, cotação e recálculo. Persistir premissas efetivas. Invalidar análise positiva quando cotação/premissa muda ou expira. Manter lote/grupo bloqueado no recorte por item.

8. Corrigir o preço de equilíbrio considerando tributos sobre receita. Verificar aritmética dos casos; revisar o preço alvo e não mostrar zero como preço viável quando a fórmula não tem solução.

9. Usar estado vigente por item em resumo e filtros, com contagem distinta. Não representar margem de qualquer item como margem integral da contratação. Retornar razões explícitas e score=null quando pending/blocked.

10. Adicionar testes de regressão pertinentes e executar suíte. Cobrir regressão de positivo para bloqueado/pendente, validade desconhecida/expirada e alteração de capital. Priorizar essas correções ao polimento de score.

## Verificação e critério de saída

Suíte passa; upgrade de schema antigo e de SQLite vazio funciona; cotação/cenário não gera 500; A/B/C produzem viable/pending/blocked; preço livre divergente da cotação não gera positivo; nova sessão SQLite funciona; sync retorna job_id; original preservado.

Comandos existentes de validação: no diretório backend/, usar .\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider, com APP_ENV=test e AUTO_START_SCHEDULER=false; no diretório frontend/, usar pnpm build. Não repetir testes amplos sem nova alteração ou falha que justifique. Comandos novos de demo só podem ser citados como existentes depois de implementados e executados.

## Entrega e continuidade

Backend e migração aditiva; testes de regressão; repasses/S03.md com exemplos de resposta reais da execução e comandos. Informar a S04 S05 e S06 qualquer divergência de contrato antes de mudar a API.

Use repasses/S03.md, no lugar de SXX no contrato comum. Registre em STATUS.md o próximo passo real.

## Ajuste ao prazo

Se uma regra crítica não couber no tempo, manter pendência e registrar MVP incompleto. S07 pode ensaiar partes funcionais e S08 produzir material parcial, sem eliminar orçamento ou busca de parceiros do aceite completo.
