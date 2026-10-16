# Sessão 01 Linha de base isolamento e contrato

Tempo alvo 35 minutos. Dependências Nenhuma.
Propriedade dos arquivos Configuração de validação e documentos do plano; sem reescrita do produto.

## Prompt para copiar

Você está no workspace <checkout-local-radar_licita>. A apresentação é em 16/10/2026, America/Sao_Paulo. O Radar Licita executável desta missão fica em backend/ e frontend/. motor_decisao/ é o legado SILLO e a especificação ampla, preservados. Estamos preparando um MVP local de apoio à decisão por item; não é operação comercial nem implementação integral do PRD.

Leia AGENTS.md aplicáveis, plans/mvp-apresentacao-2026-10-16/README.md, STATUS.md, CONTRATO_DEMO.md, ESCOPO_ORCAMENTO_E_PARCEIROS.md e os repasses das dependências antes de alterar arquivos. Confira o estado atual: documentação de conclusão anterior não é prova de funcionamento. Execute somente esta sessão e sua validação. Use subagentes para trabalho independente se ajudar, com propriedade de arquivos clara.

Preserve o banco backend/radar_licita.db e todos os dados preexistentes. Use destino explícito de demo/teste e scheduler desativado; confirme o caminho resolvido antes de qualquer migração ou escrita de dados. Não exponha .env ou credenciais. Não faça push, deploy, contato com fornecedores, propostas, exclusão de dados, atualização ampla de pacotes ou migração do legado. A raiz está inicialmente sem commits e sem remote; não dependa de worktree ou PR.

Dados oficiais mantêm datas, origem e texto originais; sintéticos são identificados na UI e no manifesto. Dinheiro usa Decimal no backend, taxas como fração e ausência como null. Produto possivelmente compatível, cotação sem vigência comprovada, logística D ou premissas críticas incompletas mantêm pendência. Viabilidade refere-se ao item e às premissas, com revisão humana.

No fim, escreva repasses/SXX.md na pasta do plano com alterações, comandos, resultados, decisões, limitações e próxima ação. Atualize STATUS.md sem inventar conclusão. Se houver execução paralela, entregue seu repasse e deixe ao coordenador a atualização compartilhada. Não marque concluída com critério de saída pendente. Não peça confirmação para correções reversíveis já incluídas no escopo.

Nesta revisão 1.1, selecionar orçamento e buscar possíveis parceiros por resultados de pregão eletrônico são requisitos obrigatórios. Não remeter a busca inicial para uma fase futura. Histórico indica candidatos; cotação atual sustenta aquisição. A S05 pode fazer leitura pública limitada e documentada para validar o adapter; suites automatizadas permanecem offline. Preservar fonte e não coletar CPF ou contatar empresas.

## Contexto desta sessão

Já foram observados 19 testes passando, dados oficiais locais encerrados em 09/10, erro de fuso no ranking SQLite, divergência id/job_id na sincronização e falhas de cotação/premissas na UI. Revalidar somente o necessário; não repetir uma auditoria ampla.

Arquivos de referência: backend/tests/conftest.py; backend/app/core/config.py; backend/app/db.py; backend/app/schemas.py; backend/app/models.py; frontend/src/types.ts; frontend/src/api.ts; .env.example; contrato e status deste plano.

## Trabalho autorizado

1. Registrar estado de Git nos dois repositórios sem alterar história; conferir ferramentas e ambientes existentes. Não copiar configuração Compose para uso SQLite.

2. Definir destinos resolvidos separados para teste e demo; desativar scheduler, hidratação e rede externa nas verificações. Estabelecer backup somente de leitura do original se necessário.

3. Executar pytest pelo Python de backend/.venv e pnpm build, registrando saída resumida. Usar APP_ENV=test, AUTO_START_SCHEDULER=false e PYTHONDONTWRITEBYTECODE=1. Se faltar dependência, resolver só a necessária no ambiente de projeto.

4. Conferir a proposta CONTRATO_DEMO.md contra modelos e rotas. Congelar cotação selecionada, snapshot de preço/frete, análise por item, eligibility, pending_checks, capital efetivo e origem. Publicar exemplos JSON e estratégia aditiva compatível com a 0001, que usa metadata atual; prever verificação de colunas existentes e dois ensaios de upgrade. Este congelamento é documental, não implementação.

5. Congelar os contratos adicionais de orçamento e busca de parceiros, modelos de observações/candidatos/search_runs, limite de consulta e cache. Ler a evidência PNCP e definir a unidade da busca. Distribuir propriedade de arquivos para S04/S05/S06, com integração única em S07.

6. Registrar orçamento restante, dono de cada pasta e ordem das etapas. Especificar guarda de destino que deve ser executada antes de escrita pela futura carga de demo.

## Verificação e critério de saída

Os comandos de baseline executam em destinos isolados; o build tem resultado registrado; o contrato tem versão congelada e exemplos; nenhuma alteração de banco original. Descrever corretamente como datas sem offset serão tratadas após checar a semântica da origem.

Comandos existentes de validação: no diretório backend/, usar .\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider, com APP_ENV=test e AUTO_START_SCHEDULER=false; no diretório frontend/, usar pnpm build. Não repetir testes amplos sem nova alteração ou falha que justifique. Comandos novos de demo só podem ser citados como existentes depois de implementados e executados.

## Entrega e continuidade

repasses/S01.md; STATUS.md atualizado; CONTRATO_DEMO.md congelado; comandos de baseline e esquema do manifesto. S02 fica liberada se isolamento e contrato estiverem resolvidos.

Use repasses/S01.md, no lugar de SXX no contrato comum. Registre em STATUS.md o próximo passo real.

## Ajuste ao prazo

Se Docker não funcionar, manter Python/SQLite e Vite existentes. Se um erro de baseline impedir teste, registrar reprodução e encaminhar correção a S03; não gastar a sessão reconstruindo toda a infraestrutura.
