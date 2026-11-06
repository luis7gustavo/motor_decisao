# Continuidade do MVP Radar Licita

Atualizado em 15/10/2026, 22:52 BRT. Estado geral: **MVP local de demonstração pronto** na revisão 1.1, com S01 a S08 concluídas e acervo ativo ampliado para dados históricos exclusivamente oficiais. A apresentação está prevista para 16/10/2026; nenhuma apresentação externa, operação comercial ou validação de fornecedor foi registrada.

## Estado vigente da apresentação — 15/10/2026, 22:52 BRT

- O acervo ativo foi ampliado para **7 pregões eletrônicos, 19 itens com resultado, 19 resultados PJ e 11 empresas distintas**, capturados do PNCP em 33 requisições. A seleção, URLs e hashes constam em `examples/demo/fixtures/official/PNCP_TECNOLOGIA_HISTORICA_EXPANDIDA_2026-10-15.json`, SHA-256 `503CE560E9A0EF8CA755CEC706AC0BE2A3170A78878777E337FA661BDA9F064D`.
- Novo snapshot exclusivo de dados reais: `backend/data/demo/radar_real_history_snapshot.db`, SHA-256 `A1BD7EC47AC0F07D448470848CA26D6011FB546DE7CA2C1FD6B6A612BAEB09F9`. `check_demo_db.py` conferiu `quick_check=ok`, Alembic `0002`, compras/itens/CNPJs/hashes exatos e **zero fornecedores comerciais, produtos, cotações, cenários, scores e documentos locais**.
- A instância em `127.0.0.1:5173/?demo=historico` foi reiniciada sobre `backend/data/demo/radar_real_show_20261015-224951-1974.db`, cópia validada do novo snapshot. O endpoint histórico e a interface exibiram 7/19/19/11. `start-demo.ps1` agora cria cópia real automaticamente e rejeita base com registros divergentes da captura.
- Backend: **44 testes passaram**, com um aviso de depreciação Starlette/httpx. Frontend: `pnpm build` passou. O banco original manteve SHA-256 `08EFB22E55C79266D4DE2F90D03F8BF207185772B9751D76479C4AE48AC2F6DA`.
- Os bancos, fixtures e imagens sintéticos das sessões anteriores permanecem apenas como arquivo de desenvolvimento. `README_DEMO.md`, roteiro, ficha e índice de evidências descrevem exclusivamente a instância real atual. Histórico de participação não valida fornecimento atual.
- A aba Radar também lista as sete contratações reais do acervo quando não há propostas abertas, com acesso aos detalhes e candidatas; a tela foi conferida no navegador e registrada em `output/demo/evidencias/s09_radar_acervo_real.png`.

## Linha de base

- Backend: 44 testes passaram em 15/10/2026 após o complemento histórico; um aviso Starlette/httpx. Os 41 testes do fechamento original S08 ficam no histórico.
- Frontend: build TypeScript e Vite concluído em 15/10/2026 no fechamento S08.
- Banco original: `backend/radar_licita.db`, SHA-256 `08EFB22E55C79266D4DE2F90D03F8BF207185772B9751D76479C4AE48AC2F6DA`; inspeção somente leitura registrou 50 contratações, 39 itens, 2 documentos, 14 extrações/evidências e nenhum cenário, cotação ou score.
- Todos os prazos do acervo inspecionado encerram em 09/10/2026.
- Na versão 1.0 não houve coleta PNCP. A revisão 1.1 verificou contrato e amostra pública limitada, documentados em evidencias/PNCP_VERIFICACAO_2026-10-15.json; o módulo do produto continua por implementar.
- Repasses S01 a S07 existem. Os routers de orçamento e parceiros estão conectados na aplicação real desde S07; a busca real, o cache sob falha e a escrita pela UI foram verificados em cópia isolada.
- Revisão 1.1 validada documentalmente; PDF atualizado com 34 páginas conferidas por extração e inspeção visual. O pacote S08 contém início/parada verificados, roteiro, ficha e nove capturas identificadas, incluindo duas do histórico real de tecnologia. A instância local usa cópia de `backend/data/demo/radar_demo_s07_final_backup.db`, sem scheduler. O banco original manteve o SHA-256 de linha de base.
- A aba Tecnologia agora apresenta primeiro um recorte capturado do PNCP com dois pregões de tecnologia, quatro itens, quatro resultados e três empresas distintas, vinculado por número PNCP e item, com hashes e links de fonte. Ele funciona offline. A reconsulta dirigida ao vivo foi verificada em oito requisições e retornou as mesmas contagens. A pesquisa adicional de propostas abertas e comparação de orçamentos continua limitada e dependente de rede.

## Controle das sessões

| Sessão | Estado | Evidência de conclusão |
| --- | --- | --- |
| S01 | concluída | [repasses/S01.md](repasses/S01.md): baseline, isolamento, contrato v0.3 congelado e propriedade definidos |
| S02 | concluída | [repasses/S02.md](repasses/S02.md): carga isolada, manifesto, casos A/B/C e fixture oficial preparados |
| S03 | concluída | [repasses/S03.md](repasses/S03.md): avaliação por item, contrato aditivo, migração 0002 e regressões |
| S04 | concluída | [repasses/S04.md](repasses/S04.md): router isolado, 4 testes próprios e suíte com 30 testes |
| S05 | concluída | [repasses/S05.md](repasses/S05.md): cadeia PNCP direcionada, candidata PJ, cache, promoção sem cotação e testes offline |
| S06 | concluída | [repasses/S06.md](repasses/S06.md): busca/evidência, cotação, simulação e status percorridos na UI em 1280 px; build passou |
| S07 | concluída | [repasses/S07.md](repasses/S07.md): routers ligados, busca real na UI, falha PNCP com cache, corrida de orçamento, persistência e restauração verificadas |
| S08 | concluída e complementada | [repasses/S08.md](repasses/S08.md): início/parada isolados; histórico real de tecnologia e empresas; 44 testes, build, roteiro, ficha e capturas |

Próxima ação: em 16/10/2026, executar [README_DEMO.md](../../README_DEMO.md) e seguir [ROTEIRO_7_MIN.md](../../docs/demo/ROTEIRO_7_MIN.md), começando pela aba Tecnologia e pelo acervo histórico real. A instância local está em `127.0.0.1:5173`; `output/demo/run.json` registra a cópia isolada ativa. Se reiniciar, criar nova cópia do snapshot. Após a apresentação, registrar feedback e planejar ampliação de cobertura PNCP, validação de equivalência técnica e obtenção de cotações atuais. Não interpretar empresa histórica como fornecedora apta.

## Modelo obrigatório do repasse

Criar repasses/SXX.md com:

1. Data e hora local, objetivo, estado e dependências.
2. Arquivos alterados e decisões de escopo/contrato.
3. Destino de banco e ambiente, sem segredos.
4. Comandos executados, resultados e evidências.
5. Critérios de saída atendidos e pendências reproduzíveis.
6. Próxima ação exata e arquivos que a próxima sessão precisa ler.
7. Procedimento de recuperação das alterações e dados da sessão.

## Histórico de mudanças

15/10/2026: plano inicial criado com seis sessões. Escolha de execução local no protótipo raiz, preservando o legado e banco original. Nenhuma mudança de comportamento do produto foi feita por esta entrega documental.

15/10/2026 revisão 1.1: orçamento e busca inicial de parceiros por pregão promovidos a obrigatórios. Oito sessões; S04/S05/S06 paralelas após S03. Versão anterior de seis sessões substituída. A estimativa anterior de três horas não cobre o novo escopo. Nenhum comportamento do produto foi implementado nesta atualização documental.

15/10/2026 execução S01: baseline local reproduzida (19 testes e build frontend); scheduler desativado nos testes; banco original confirmado em `backend/radar_licita.db` e não alterado; contrato v0.3 congelado; S02 liberada. O caminho de demonstração continua sem banco próprio e sem implementação de orçamento/parceiros.

15/10/2026 execução S02: criada preparação idempotente em `examples/demo/prepare_demo.py`, banco separado `backend/data/demo/radar_demo.db`, manifesto e fixture oficial identificado. A carga contém 1 arquivo histórico encerrado com IDs remapeados, 3 casos sintéticos, 3 cenários financeiros e nenhum score manual. Testes atuais: 21 passaram; S07 ainda precisa aceitar a elegibilidade A/B/C.

15/10/2026 execução S03: adicionados contrato de cotação/snapshot e análise por item, avaliação sem escrita, normalização de instantes, migração aditiva 0002 e fundação de buscas/candidatos/observações. Demo revalidada como A=viable, B=pending e C=blocked por capital. Suite: 26 testes passaram; banco original preservado; endpoints novos de orçamento/parceiros ainda não integrados.

15/10/2026 execução S04: busca contextual por orçamento implementada em módulo próprio, sem integração ainda na aplicação real. Testes próprios e suíte completa passaram; conexão permanece para S07.

15/10/2026 execução S05: adapter PNCP limitado, persistência/deduplicação de candidatos e evidências, promoção idempotente e replay oficial preparados. Cadeia real PJ do arado processada em leitura; 35 testes passaram; conexão permanece para S07.

15/10/2026 execução S06: frontend de orçamento, parceiros, cotação, simulador e status implementado; `pnpm build` passou. Inspeção temporária com routers incluídos em memória mostrou casos A/B/C no radar. A API de candidatas ainda não expõe observações para a UI e os routers seguem desconectados da aplicação real; S06 não está aceita. Banco original preservado. Uma mudança acidental de status no banco demo durante a inspeção foi revertida com verificação de estado e histórico.

15/10/2026 execução S07 parcial: routers conectados, observações oficiais expostas, demo com recorte histórico do arado e orçamento reativo 5.000/10.000/12.000. Suíte final 35 testes e build passaram; API persistida, resumo vigente, reinício, falha de fonte com cache e restauração de backup verificados. Três estados calculados por item e capturados. Capital salvo voltou a prevalecer no ranking persistido; orçamento explícito prevalece na busca e na criação de cenário. Falta validar busca real pela UI e percurso completo em notebook; S06/S07 e MVP continuam em andamento. Banco original conservou SHA-256 de linha de base.

15/10/2026 fechamento S05/S06/S07: consulta direta pelo número PNCP do item mostrou candidata PJ real pela UI, com janela e UF gravadas; promoção não criou produto ou cotação. Em largura 1280 px, a UI criou cotação vinculada, preservou validade, simulou cenário e mudou status; uma corrida com resposta atrasada conservou o orçamento mais recente. Falha PNCP simulada manteve cache e evidência. Corrigidos parsing de quatro casas decimais e falso conflito técnico em `16 GB RAM, SSD 512 GB`. Suíte 38 testes e build passaram; banco original conservou o hash de linha de base. S08 segue pendente.

15/10/2026 fechamento S08: scripts PowerShell passaram por falha controlada de porta ocupada, início, verificação, parada, nova cópia e nova abertura em 8000/5173. O ensaio na UI confirmou 1/1/1 com R$ 5.000, 2/1/0 com R$ 10.000, detalhes do item sintético, candidata histórica do arado e cadastro que deixou zero produtos e zero cotações. A cópia usada nesse ensaio de escrita foi descartável; a instância final usa cópia nova e validada. `pnpm build` e 41 testes passaram. O proxy do Vite passou a usar configuração explícita para evitar um `vite.config.js` local antigo. O banco original manteve SHA-256 `08EFB22E55C79266D4DE2F90D03F8BF207185772B9751D76479C4AE48AC2F6DA`.

15/10/2026 complemento solicitado após S08: apresentação redirecionada para dados históricos reais. O PNCP foi consultado em recorte dirigido de pregões eletrônicos de tecnologia: compras `00394452000103-1-015447/2026` e `03238755000117-1-000031/2026`; quatro itens têm resultados PJ, três CNPJs distintos. Captura de oito requisições com payloads selecionados, hashes, URLs de fonte e data foi salva em `examples/demo/fixtures/official/PNCP_TECNOLOGIA_HISTORICA_2026-10-15.json`. A aba Tecnologia mostra o acervo offline; reconsulta ao vivo retornou as mesmas contagens. Roteiro e ficha agora começam pelo histórico real, deixando A/B/C sintéticos como complemento. Nove capturas verificadas, 44 testes e build passaram. “Prazo encerrado e resultado publicado” não prova contrato executado ou fornecimento atual.
