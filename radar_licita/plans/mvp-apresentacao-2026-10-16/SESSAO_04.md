# Sessão 04 Melhores oportunidades por orçamento

Tempo alvo 75 minutos. Dependências S03 e contrato congelado.
Propriedade dos arquivos backend/app/recommendations/ e testes próprios; sem editar arquivos compartilhados no paralelo.

## Prompt para copiar

Você está no workspace <checkout-local-radar_licita>. A apresentação é em 16/10/2026, America/Sao_Paulo. O Radar Licita executável desta missão fica em backend/ e frontend/. motor_decisao/ é o legado SILLO e a especificação ampla, preservados. Estamos preparando um MVP local de apoio à decisão por item; não é operação comercial nem implementação integral do PRD.

Leia AGENTS.md aplicáveis, plans/mvp-apresentacao-2026-10-16/README.md, STATUS.md, CONTRATO_DEMO.md, ESCOPO_ORCAMENTO_E_PARCEIROS.md e os repasses das dependências antes de alterar arquivos. Confira o estado atual: documentação de conclusão anterior não é prova de funcionamento. Execute somente esta sessão e sua validação. Use subagentes para trabalho independente se ajudar, com propriedade de arquivos clara.

Preserve o banco backend/radar_licita.db e todos os dados preexistentes. Use destino explícito de demo/teste e scheduler desativado; confirme o caminho resolvido antes de qualquer migração ou escrita de dados. Não exponha .env ou credenciais. Não faça push, deploy, contato com fornecedores, propostas, exclusão de dados, atualização ampla de pacotes ou migração do legado. A raiz está inicialmente sem commits e sem remote; não dependa de worktree ou PR.

Dados oficiais mantêm datas, origem e texto originais; sintéticos são identificados na UI e no manifesto. Dinheiro usa Decimal no backend, taxas como fração e ausência como null. Produto possivelmente compatível, cotação sem vigência comprovada, logística D ou premissas críticas incompletas mantêm pendência. Viabilidade refere-se ao item e às premissas, com revisão humana.

No fim, escreva repasses/SXX.md na pasta do plano com alterações, comandos, resultados, decisões, limitações e próxima ação. Atualize STATUS.md sem inventar conclusão. Se houver execução paralela, entregue seu repasse e deixe ao coordenador a atualização compartilhada. Não marque concluída com critério de saída pendente. Não peça confirmação para correções reversíveis já incluídas no escopo.

Nesta revisão 1.1, selecionar orçamento e buscar possíveis parceiros por resultados de pregão eletrônico são requisitos obrigatórios. Não remeter a busca inicial para uma fase futura. Histórico indica candidatos; cotação atual sustenta aquisição. A S05 pode fazer leitura pública limitada e documentada para validar o adapter; suites automatizadas permanecem offline. Preservar fonte e não coletar CPF ou contatar empresas.

## Contexto desta sessão

Já existe max_capital, mas ele filtra qualquer cenário histórico, mistura itens e não ordena pelas melhores análises. Esta sessão implementa a avaliação contextual por orçamento real do operador, sem gravar preferências globais por digitação.

Arquivos de referência: ESCOPO_ORCAMENTO_E_PARCEIROS.md; backend/app/api/routes.py; backend/app/finance/service.py; backend/app/ranking/service.py; repasses/S03.md.

## Trabalho autorizado

1. Criar POST /api/v1/recommendations/search em router próprio, usando o motor comum de decisão com contexto de available_capital. Não criar motor paralelo com regras divergentes.

2. Selecionar cenário base vigente por item/cotação e revalidar as demais premissas. Aplicar orçamento da requisição mesmo que o cenário salvo tenha outro capital. Não herdar bloqueio de capital calculado para outro orçamento.

3. Retornar recommendations, needs_analysis, outside_budget e blocked, com valor aplicado, capital necessário, saldo, razões, origem e versões. Impedimento não financeiro não é excesso de orçamento. Só recommendations afirma encaixe sustentado; desconhecido não vira zero.

4. Ordenar deterministicamente por score, resultado estimado, prazo e IDs. Paginação e contagem são por item distinto. Não comparar orçamento ao valor global do edital.

5. Implementar casos R$ 4.000/7.000/11.000 e orçamento R$ 5.000/10.000/12.000, além de zero/null/negativo/limite exato/um centavo abaixo, cenário antigo e cotação vencida.

6. Testar o router em app isolada se main.py ainda não estiver conectado. Entregar ao coordenador a importação necessária para S07; não alterar routes.py/main.py ao mesmo tempo que outro agente.

## Verificação e critério de saída

Casos de orçamento produzem listas e saldos esperados; não gravam SystemSetting ou scores por consulta; resultados são do mesmo item; regra de elegibilidade é a de S03. Interface será aceita em S07.

Comandos existentes de validação: no diretório backend/, usar .\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider, com APP_ENV=test e AUTO_START_SCHEDULER=false; no diretório frontend/, usar pnpm build. Não repetir testes amplos sem nova alteração ou falha que justifique. Comandos novos de demo só podem ser citados como existentes depois de implementados e executados.

## Entrega e continuidade

Router/service/schemas em recommendations/; testes e repasses/S04.md com respostas reais e instrução de integração para S07.

Use repasses/S04.md, no lugar de SXX no contrato comum. Registre em STATUS.md o próximo passo real.

## Ajuste ao prazo

Cortar filtros opcionais e paginação visual avançada. Conservar orçamento, atualização da seleção, capital/saldo, ordem e razões. Se faltar custo sustentado, resposta correta é seção pendente, sem afirmar que cabe.
