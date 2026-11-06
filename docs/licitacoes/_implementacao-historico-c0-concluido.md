# Radar Licita — controle de implementação

Estado atual: **Ciclo 0 concluído**. Fundação validada em ambiente isolado; nenhuma funcionalidade pública ativada. Última atualização: 2026-10-13, America/Sao_Paulo. Histórico das interrupções em [_implementacao-historico-c0.md](_implementacao-historico-c0.md).

## 1. Linha de base e parâmetros efetivos

- Repositório: luis7gustavo/motor_decisao. Commit de código inicial: 261b32107ff7b053c68c1df98e88de329c41c0f5; branch de implementação: feat/radar-licita-implementacao.
- Alterações anteriores: documentação de docs/licitacoes não rastreada, preservada fora dos commits; repositório pai não é alvo. Controle de implementação anterior staged foi retomado com autorização de execução.
- Nome/email Git configurados localmente conforme resposta explícita: luis7gustavo / luis.galvao@sempreceub.com.
- Ciclo alvo 0; categoria não escolhida; janela últimos 7 dias sem coleta; teto 50; intervalo 1 segundo; aplicar migração principal não; schedules Prefect novos não; push não. Permissões não ampliadas nas mensagens de continuação.
- Docker Engine 29.8.2, Compose v5.5.1, contexto desktop-linux. `docker compose config --quiet` e variante com compose.radar-ops.yml passaram.
- Principal motor_decisao; revisão observada 20260817_0009; servidor PostgreSQL identificado por system_identifier 7696140971977625639. Nenhuma migração principal executada.
- Serviços já ativos antes da retomada: api/postgres/redis/selenium/prefect-postgres/prefect-server com healthchecks saudáveis; prefect-services e três workers ativos sem healthcheck; prefect-init já encerrado 0. Nenhum serviço legado iniciado/parado/reconfigurado pela execução. Runner efêmero usa somente postgres e --no-deps, saída como critério de sucesso.
- Imagem runner construída: sha256:20f8d176a64e67bfceccbf8ae411baf49ff76bde98fbf3d98addcbbc60b27515, derivada da imagem API local. Não alegar lock reprodutível de todas as dependências legadas.

## 2. Comandos canônicos

Executados na raiz do checkout; runtimes somente em Docker. Procedimento de backup e detalhes em [operacao-ciclo-0.md](operacao-ciclo-0.md).

```powershell
docker compose config --quiet
docker compose -f docker-compose.yml -f compose.radar-ops.yml config --quiet
docker compose -f docker-compose.yml -f compose.radar-ops.yml build radar-ops
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps radar-ops tests --origin 20260817_0009 --baseline
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps radar-ops tests --origin 20260817_0009
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps radar-ops rehearse --origin 20260817_0009 --dump /evidence/radar-c0-20261013-r2.dump
docker compose ps -a
```

Logs canônicos: `docker compose logs --tail 50 postgres` (não foi necessário ler logs operacionais nesta validação); runner grava manifestos JSON e saída sanitizada de pytest em backups/. Migração de ensaio real futura: modo rehearse com `--target REVISAO_EXATA_NOVA`, somente após revisão manual do caminho e testes estruturais específicos. Não há comando de aplicação principal no runner; `head`, `base`, `-1` e destinos fora do caminho de novas revisões são recusados no ensaio.

## 3. Backup e restauração

- Novo dump: backups/radar-c0-20261013-r2.dump; **854363876 bytes**; SHA256 **b4a21504874a0a3613536c48ece4a81ac714aa3df7007a2a6a62e7ccd6a80439**. Última escrita 2026-10-13 16:28:14 UTC (13:28:14 America/Sao_Paulo).
- `docker compose exec -T postgres` executou pg_dump custom e pg_restore --list, retornou `DUMP_AND_LIST_OK`, código 0. Transferência binária via docker compose cp concluída. Arquivo ignorado pelo Git. Credenciais vêm do ambiente, sem impressão.
- Restauração completa inicial confirmada em 20261013T163109Z_ae802465.json: pg_restore --exit-on-error --single-transaction --no-owner, código 0; origem restaurada 20260817_0009; upgrade/downgrade/upgrade sintético; contagens iguais em todas as etapas; cópia removida.
- Referência consistente: **contagens da cópia restaurada do mesmo dump**, nunca comparações de consultas independentes ao principal ativo. Escritas operacionais não foram suspensas; aplicação no principal segue desabilitada.
- Dump anterior radar-c0-20261013.dump preservado, 853802596 bytes, hash 4ad78541fef2de7e091043db0fada66e35a3795c8072157e7f8411a24216c057; saída final perdida na interrupção, não validado. Dumps em /tmp do container também preservados; nenhuma retenção/remoção automática de backups.

Contagens do snapshot restaurado (somente identificadores e totais):

| Tabela | Linhas |
|---|---:|
| bronze.market_web_listings_raw | 127858 |
| bronze.mercado_livre_items_raw | 16040 |
| bronze.mercado_livre_products_raw | 59360 |
| bronze.price_history_raw | 15198 |
| bronze.supplier_products_raw | 229784 |
| control.collection_alerts | 83 |
| control.data_quality_checks | 5936 |
| control.pipeline_runs | 6211 |
| control.source_runs | 6444 |
| dw.dim_confianca | 4 |
| dw.dim_execucao_motor | 24 |
| dw.dim_fonte_coleta | 16 |
| dw.dim_fornecedor | 3 |
| dw.dim_modelo_ml | 8 |
| dw.dim_produto_fornecedor | 77675 |
| dw.dim_recomendacao | 4 |
| dw.dim_tempo | 27 |
| dw.fato_execucao_fonte | 3839 |
| dw.fato_oportunidade_compra | 5957 |
| dw.fato_score_ml | 22159 |
| feedback.opportunity_reviews | 2 |
| gold.decision_engine_runs | 41 |
| gold.decision_opportunities | 6878 |
| gold.decision_opportunity_snapshots | 232590 |
| gold.ml_model_runs | 8 |
| gold.ml_opportunity_scores | 123266 |
| public.alembic_version | 1 |
| repositorio.market_web_listings_raw | 33385 |
| repositorio.mercado_livre_items_raw | 16040 |
| repositorio.mercado_livre_products_raw | 59360 |
| repositorio.pipeline_runs_raw | 3649 |
| repositorio.price_history_raw | 9313 |
| repositorio.source_runs_raw | 3830 |
| repositorio.supplier_products_raw | 77675 |
| silver.mercado_livre_product_prices | 4272 |
| silver.supplier_products_normalized | 226643 |

## 4. Ciclo e escopo

| Ciclo | Escopo fixado antes de codificar | Estado | Início | Fim | Commits |
|---|---|---|---|---|---|
| 0 | Baseline, runner isolado, backup/restauração, ensaio sintético e limpeza, evidências e documentação | concluído | 2026-10-13 | 2026-10-13 | 88c0186, bd12387, 9f49a33; commit de encerramento informado no relatório |

Nenhuma história de produto selecionada. Documentos lidos: prompt, progresso documental, 15/16/19/21 e seções relevantes 14/17; código Docker/Alembic/settings/testes existentes. Critérios permanecem os originais, sem redução de escopo.

## 5. Checklist e evidências

- [x] C0-01 (RT-011/015): configuração/daemon/banco/branch verificados, principal identificado e preservado.
- [x] C0-02 (RT-015): baseline 54 aprovados; suíte final 70 aprovados, 0 falhas, 0 erros, 0 ignorados, 1 warning. Execução final: 20261013T163830Z_2f97bed0.json, 8,70 s no pytest; zero privilégios do papel temporário nas tabelas principais; banco e papel removidos. Nenhum teste legado editado.
- [x] C0-03 (RT-014; RNF-014): backup com restauração completa validada no primeiro ensaio; hash/36 contagens registrados acima.
- [x] C0-04 (RT-014/015): ensaio final 20261013T163840Z_c8802a74 aprovado, 36 tabelas correspondem ao TOC, contagens e hashes de colunas/constraints/índices iguais após as três etapas; banco removido.
- [x] C0-05 (RT-014/015): manifestos, ADR/rastreabilidade, comandos e verificações finais registrados; implementação no commit 9f49a33; este controle e histórico compõem o commit de encerramento.

16 testes novos em tests/test_radar_ops.py: alvo não criado, principal mesmo com registro, nome inválido, revisão simbólica/legada/divergente/merge e índice do dump com TABLE DATA/ATTACH. Teste inicial escrito antes do componente: ModuleNotFoundError esperado; resolvido pela implementação. Detalhes de alcance em [17-rastreabilidade-ciclo-0.md](17-rastreabilidade-ciclo-0.md).

## 6. Migrações e limpeza

Produção: nenhuma revisão nova; principal 20260817_0009. Ensaio inicial usa revisão vazia c0_empty_a983794dc4, criada só em cópia de Alembic no /tmp do runner, fora da cadeia versionada. Caminho: origem → sintética → origem → sintética. Nunca reverte revisão legada. Nenhum upgrade/downgrade no principal. Aplicação principal: **não**; janela de escrita: não necessária para ensaio do snapshot, não estabelecida para aplicação.

Guarda de limpeza: nome exato gerado, criação bem-sucedida nesta execução, OID e identidade de servidor. Não exclui alvos preexistentes; termina apenas conexões do banco próprio validado. Papéis próprios verificados por OID. Manifestos registram limpeza inclusive nas falhas. SIGTERM e exceções tentam limpeza; encerramento abrupto pode exigir inspeção manual, sem exclusão por prefixo.

## 7. Coletas reais

Todas as retomadas do Ciclo 0: **0 requisições consumidas / 50 restantes**. Nenhuma coleta, janela absoluta de fonte, filtro, cursor ou agendamento novo. Instalação de ferramentas de build não é coleta de fontes. Saldo não autoriza coleta neste ciclo.

## 8. Desvios, ADR e falhas observadas

[ADR-021](19-adr-021-fundacao.md): serviço efêmero aditivo; bancos temporários no mesmo PostgreSQL conforme autorização explícita do prompt; documentação/rastreabilidade em adendos novos para não incluir trabalho alheio não rastreado nos commits. Nenhum serviço Compose existente alterado. Sem variável externa nova: usa MOTOR_POSTGRES_* já existente; variáveis internas do runner não são configuração comercial.

- B0-01 ambiente ausente, B0-02 identidade Git ausente e B0-03 perda do daemon: resolvidos; histórico preservado no arquivo específico.
- Primeira tentativa de baseline (20261013T163100Z_bc7a21d0): audit hook bloqueou ldconfig -p usado pelo Trio. Corrigida exceção exata para consulta local; banco/papel removidos. Não é falha preexistente da suíte.
- Baseline aprovado: 20261013T163132Z_ed6c1498. Duas execuções intermediárias de 69 testes aprovadas; teste adicional de parser TOC elevou a suíte a 70.
- Segundo ensaio (20261013T163444Z_0961f833): restauração completa passou, mas novo leitor TOC confundia TABLE DATA com TABLE e recusou inventário. Corrigido com teste específico; cópia removida, cleanup_errors vazio. Ensaio final corrigido aprovado em 20261013T163840Z_c8802a74.
- Falhas preexistentes da suíte: nenhuma observada. Warning preexistente FastAPI/Starlette/httpx: depreciação, sem mudança de dependências.
- Correção de rastreabilidade: backup/migração pertencem a RT-014; RT-013 citado nos registros/commits iniciais trata observabilidade. Adendo 17 usa vínculo correto.

## 9. Limitações e pendências

CONNECT ao catálogo PostgreSQL é herdado de PUBLIC, preservado; papel de teste não tem permissões nas tabelas operacionais. Audit hook controla sockets Python/subprocessos, não é firewall contra código nativo malicioso. Runner é para suíte revisada, não conteúdo não confiável. Logs pg_restore suprimidos para não expor linhas. Locks completos de dependências, segundo host, recuperação de Prefect/documentos, RPO operacional e produto não foram validados. Contagens iguais não provam conteúdo idêntico linha a linha. Migrações reais futuras exigem revisão de aditividade e asserts de estrutura próprios.

## 10. Próximo passo

1. Não iniciar outro ciclo nesta sessão.
2. Na próxima solicitação, usar CICLO_ALVO automático; o alvo explícito 0 agora já está concluído.
3. Selecionar escopo elegível pelo roadmap; nenhuma autorização de aplicar no principal, coletar ilimitadamente ou ativar schedules é herdada.
4. Gerar backup novo e ensaiar cada migração real com verificação estrutural específica; categoria comercial continua pendente quando necessária.
5. Documentação preexistente não rastreada permanece fora dos commits; adendos 17/19 registram as evidências da fundação.

### Evidências de encerramento

Ensaio final: 20261013T163840Z_c8802a74.json, término 2026-10-13 16:41:57 UTC (13:41:57 America/Sao_Paulo). Revisão sintética c0_empty_e550615df3; origem 20260817_0009. Três etapas com legacy_counts_equal e synthetic_structure_equal verdadeiros. Restauração integral e operation_passed verdadeiros; cleanup_errors vazio. O arquivo permaneceu com o mesmo SHA256. Hashes estruturais no manifesto ignorado, sem conteúdos de linhas na documentação.

Verificação final via docker compose exec -T postgres / psql: revisão principal 20260817_0009; zero bancos com prefixos rehearsal_/test_ e zero papéis radar_test_ presentes. Consulta de inventário apenas; nenhuma exclusão por prefixo. Todos os manifestos próprios registram limpeza, incluindo as tentativas com falha. Configuração Compose base/aditiva validada novamente; ps -a confirma todos os healthchecks existentes saudáveis, tarefas efêmeras encerradas/removidas. Serviços sem healthcheck mantêm a limitação da baseline.

Arquivos novos de runtime conferidos com LF; git diff --check sem erros. Commit de implementação 9f49a33 contém somente arquivos novos próprios. Nenhuma alteração em app, pipelines, testes legados, revisões Alembic ou Compose operacional; nenhuma inclusão de .env/dump/segredo. A documentação alheia não rastreada foi preservada. Histórico de interrupções movido para arquivo versionado próprio, mantendo o controle atual sem estados contraditórios. Commit final não é inserido em seu próprio conteúdo; consultar relatório desta sessão ou git log.

Conclusão limitada à fundação: runner e ensaio disponíveis via Compose; sem RF de produto entregue, sem migração ou ativação Radar no principal. Nenhum bloqueio aberto do Ciclo 0; limitações da seção 9 e gates futuros permanecem.
