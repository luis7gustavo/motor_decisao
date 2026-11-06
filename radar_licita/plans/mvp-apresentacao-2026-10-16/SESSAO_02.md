# Sessão 02 Dados e demonstração reproduzível

Tempo alvo 60 minutos. Dependências S01 e contrato congelado.
Propriedade dos arquivos examples/demo/; scripts de preparação de demo; manifesto e testes da carga.

## Prompt para copiar

Você está no workspace <checkout-local-radar_licita>. A apresentação é em 16/10/2026, America/Sao_Paulo. O Radar Licita executável desta missão fica em backend/ e frontend/. motor_decisao/ é o legado SILLO e a especificação ampla, preservados. Estamos preparando um MVP local de apoio à decisão por item; não é operação comercial nem implementação integral do PRD.

Leia AGENTS.md aplicáveis, plans/mvp-apresentacao-2026-10-16/README.md, STATUS.md, CONTRATO_DEMO.md, ESCOPO_ORCAMENTO_E_PARCEIROS.md e os repasses das dependências antes de alterar arquivos. Confira o estado atual: documentação de conclusão anterior não é prova de funcionamento. Execute somente esta sessão e sua validação. Use subagentes para trabalho independente se ajudar, com propriedade de arquivos clara.

Preserve o banco backend/radar_licita.db e todos os dados preexistentes. Use destino explícito de demo/teste e scheduler desativado; confirme o caminho resolvido antes de qualquer migração ou escrita de dados. Não exponha .env ou credenciais. Não faça push, deploy, contato com fornecedores, propostas, exclusão de dados, atualização ampla de pacotes ou migração do legado. A raiz está inicialmente sem commits e sem remote; não dependa de worktree ou PR.

Dados oficiais mantêm datas, origem e texto originais; sintéticos são identificados na UI e no manifesto. Dinheiro usa Decimal no backend, taxas como fração e ausência como null. Produto possivelmente compatível, cotação sem vigência comprovada, logística D ou premissas críticas incompletas mantêm pendência. Viabilidade refere-se ao item e às premissas, com revisão humana.

No fim, escreva repasses/SXX.md na pasta do plano com alterações, comandos, resultados, decisões, limitações e próxima ação. Atualize STATUS.md sem inventar conclusão. Se houver execução paralela, entregue seu repasse e deixe ao coordenador a atualização compartilhada. Não marque concluída com critério de saída pendente. Não peça confirmação para correções reversíveis já incluídas no escopo.

Nesta revisão 1.1, selecionar orçamento e buscar possíveis parceiros por resultados de pregão eletrônico são requisitos obrigatórios. Não remeter a busca inicial para uma fase futura. Histórico indica candidatos; cotação atual sustenta aquisição. A S05 pode fazer leitura pública limitada e documentada para validar o adapter; suites automatizadas permanecem offline. Preservar fonte e não coletar CPF ou contatar empresas.

## Contexto desta sessão

O banco original contém 50 contratações antigas e só uma com detalhes, sem cotações nem cenários. A demonstração precisa de casos sintéticos completos e acervo histórico identificado, sem reabrir uma licitação real alterando datas.

Arquivos de referência: repasses/S01.md; backend/app/models.py; backend/alembic/versions/0001_initial.py; examples/supplier_quotes.example.csv; backend/app/core/config.py; backend/app/finance/service.py.

## Trabalho autorizado

1. Criar carga determinística e idempotente em banco separado sob backend/data/demo/, com guarda que rejeite caminho do banco original e qualquer destino não aprovado para demo. Migrações só nesse arquivo; não chamar PNCP.

2. Gerar DEMO A completo, DEMO B pendente e DEMO C sem capital, usando valores sintéticos do plano, atributos técnicos suficientes e documentos locais com a palavra SINTÉTICO. Não preencher resultado final do ranking manualmente.

3. Adicionar casos sintéticos de capital R$ 4.000/7.000/11.000 e empacotar a evidência real PNCP como fixture oficial identificada. Nesta sessão só preparar dados e ponto de integração; o parser/importação de resultados e candidatos será executado em S05/S07. Não depender desse parser para concluir S02 nem preencher candidato manualmente. Reservar o item histórico de arado para o ensaio oficial; não vincular sua evidência ao notebook sintético.

4. Registrar data de referência explícita, versão e hashes dos arquivos no manifesto. Datas futuras pertencem exclusivamente aos sintéticos. Uma repetição com a mesma referência não duplica registros.

5. Se acessível, copiar de modo seletivo registros e documentos oficiais já existentes para uma área de arquivo histórico na demo, com chaves remapeadas e vínculos íntegros. Preservar fonte, hash, data de coleta e encerramento original; nunca copiar contatos ou segredos desnecessários.

6. Preparar cálculo e cenários só até o que o schema atual suportar. Não implementar regras ou migrações pertencentes a S03 para fazer a carga passar. Deixar a carga preparada para rodar novamente após as migrações de S03.

7. Documentar comando exato de preparação, verificação de contagens e modo de limpeza restrito à base de demo. A demonstração deve iniciar sem sincronização automática.

## Verificação e critério de saída

Carga repetida na mesma referência conserva contagens e chaves. Original mantém hash/contagens esperadas ou evidência de que não recebeu escrita. A/B/C são identificáveis; arquivo oficial está encerrado e marcado histórico. A aceitação de elegibilidade A/B/C ocorrerá em S07 após S03.

Comandos existentes de validação: no diretório backend/, usar .\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider, com APP_ENV=test e AUTO_START_SCHEDULER=false; no diretório frontend/, usar pnpm build. Não repetir testes amplos sem nova alteração ou falha que justifique. Comandos novos de demo só podem ser citados como existentes depois de implementados e executados.

## Entrega e continuidade

Dados e manifesto em examples/demo/; script de preparação com guarda; repasses/S02.md. Registrar comandos ainda provisórios; nenhuma regra pendente é declarada pronta.

Use repasses/S02.md, no lugar de SXX no contrato comum. Registre em STATUS.md o próximo passo real.

## Ajuste ao prazo

Se copiar o arquivo oficial consumir o tempo, manter uma amostra histórica menor ou apenas os sintéticos. Informar a ausência de caso oficial reproduzível. Não inventar origem para preencher a lacuna.
