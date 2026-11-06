# S06 — validação integrada e prova controlada

Data: 28/10/2026, America/Sao_Paulo. **Estado: validação offline concluída; critério de saída da S06 não atendido, pois a prova PNCP real permanece bloqueada.** S05 foi a dependência de implementação; a aplicação no Prefect compartilhado ainda depende de reparo e verificação próprios. O pedido desta sessão abrangeu todas as fases restantes, mas os gates de S06 impedem a ativação S07 e o aceite S08. Não houve GET PNCP, job histórico real, migração, reset de quota, agenda real ou escrita no principal/C2.

## Trabalho concluído nesta sessão

- `tests/test_pncp_history.py`: prova de `Retry-After` de 240 segundos e data HTTP superior à duração do lote, persistência de `not_before` após novo worker, uma única reserva/Bronze durante a espera e teto total de três tentativas HTTP 503 entre reinícios. A passagem artificial do tempo ocorre apenas no banco descartável dos testes.
- `scripts/radar_pncp_s06_restore.py`: backup atual do C2, verificação do TOC, restauração integral em banco descartável, comparação de revisão, contagens e impressões digitais estáveis de Bronze, identidades, versões e observações. Confere hashes dos corpos Bronze, servidor/OID/role e proteção do principal antes/depois; recusa sobrescrever evidência. Ensaio limpo por OID.
- `scripts/radar_pncp_history.py`, `pipelines/procurement/history_flow.py` e `tests/test_pncp_history_audit.py`: resumo conservador da auditoria (`not_started`, `partial`, `pagination_consistent_unverified`) e execução local de `audit` depois de cada lote Prefect habilitado, inclusive lote parcial. O artefato de negócio mostra a cobertura, sem converter `Completed` em aceite integral.
- Runbook atualizado em `docs/licitacoes/operacao-coleta-historica-prefect.md`.

## Evidências e comandos executados

| Verificação | Resultado |
|---|---|
| `docker compose -f docker-compose.yml -f compose.radar-ops.yml --profile radar-ops run --rm -T radar-ops tests --origin 20261027_0012` | **186 aprovados, 1 aviso**, pytest exit 0, banco/role descartáveis removidos; `backups/20261028T162438Z_378918e6.json` e `.tests.txt`. O runner bloqueou socket externo e subprocessos nos testes. |
| `... radar-ops -m scripts.radar_pncp_s06_restore` | Exit 0; backup de 854.469.951 bytes, SHA-256 `b4509f88b8859b0d7a132f585cca3d2c0bbb0413311f3a42c3d2e3ac0a25f517`; `pg_restore --list` e restauração integral passaram; revisão/contagens/fingerprints iguais; banco de ensaio removido. Relatório `backups/c2/c2-20261013/s06-restore-report.json`. |
| `... radar-ops -m scripts.radar_pncp_history plan` | Exit 3 de bloqueio, 13 gates, `network_calls=0`, `database_writes=0`; `backups/s06-policy-plan.txt`. |
| `... radar-ops -m scripts.radar_pncp_s00_inventory` e `... radar-ops -m scripts.radar_pncp_live_validation verify` | Ambos exit 0; `backups/s06-c2-inventory.txt` e `s06-c2-verify.txt`. Integridade válida, estado de negócio `partial`, cobertura não verificada, zero issues/warnings. |
| `... radar-ops -m scripts.radar_pncp_history status` e `audit` | Ambos exit 3, `not_started`, sem missão histórica/partição; `backups/s06-history-status.txt` e `s06-history-audit.txt`. |
| Prefect de ensaio isolado, flow `run_history('sync')` | Run `4fc57f17-f06f-4e5f-975c-d1977b8fbee7`: estado Prefect `Completed`, negócio `blocked_contract`, 13 gates, `commands=[]`, `network_calls_when_blocked=0`. Pilha de ensaio parada depois, volumes conservados; `backups/s06-prefect-flow-blocked.txt`. |
| `docker compose ... config --quiet`; `git diff --check` | Exit 0 na execução desta sessão. |

O backup S06 contém dados do C2 e fica em `backups/` protegido; o relatório expõe apenas IDs, contagens e hashes. Não foi restaurado sobre o C2 nem sobre o principal.

## Estado protegido e limites da prova

Inventário final: principal `motor_decisao`, revisão `20260817_0009`, sem schema `procurement`; C2 `test_20261020_ba894ce71405`, OID 2508600, revisão `20261027_0012`, role sem superusuário. C2 mantém 3 jobs, 4 Bronze, 10 identidades, 10 versões e 10 observações. O job `e47c6fe2-2cc9-4b58-803a-49a4aa44a443` continua `partial/SourceChanged`, `next_page=2`, `records_seen=10`, `request_count=2`. Ledger: 11 reservas, DOCUMENTAÇÃO 7/15, DADOS 4/50, calibração 2/3; hashes Bronze conciliados com recibos. Missão nova: zero tentativas externas, zero manifestos persistidos, zero partições e nenhuma cobertura histórica adicional.

A política ainda tem `enabled=false`, orçamento externo zero, cobertura inicial e catálogo de modalidades desconhecidos, semântica de datas, contrato atual, limites/escopo do limitador e janela efetiva não validados. Faltam contratos de detalhe/itens/resultados/documentos; `docs/licitacoes/fontes/` continua ausente e nenhum manual foi buscado pela rede. O Redis compartilhado permanece reiniciando por AOF inválido; `prefect-services` não está saudável de forma estável. `schedule status` retornou pool/fila/deployment/runs/workers da missão vazios (`backups/s06-prefect-status.txt`). A pilha Prefect de ensaio demonstra execução de flow bloqueado, não execução PNCP real. Testes sintéticos e restauração comprovam lógica local e integridade dos dados já capturados, não enumeração nacional nem recorrência real.

## Decisão para S07 e S08

S06 permanece **parcial/bloqueada para prova real**. S07 não foi ativada: seus critérios exigem S06 aprovada, política habilitada, destino e Prefect saudáveis, seguido de disparo automático PNCP real. S08 não pode auditar o marco histórico porque nenhum manifesto/partição real foi iniciado. A auditoria local está integrada ao pipeline para uso futuro, mas o aceite histórico e a completude absoluta continuam pendentes. A solução futura deve conservar todas as partições e recursos obrigatórios, sem reduzir escopo para encerrar o plano. Sem snapshot ou controle independente, o melhor estado demonstrável pode ser `reconciliado_sem_snapshot`, com risco residual explícito.

Próxima ação concreta: obter os contratos/evidências oficiais e orçamento de missão, resolver o Redis compartilhado sem perda de AOF e validar o scheduler/worker real; então repetir a prova controlada S06. Só depois executar S07 e acompanhar S08. Rollback agora: nenhum fluxo real a pausar; manter política desabilitada, C2/ledger, backup e volumes. Não apagar evidências nem alterar outras automações.

## Complemento de pesquisa oficial autorizado em 28/10/2026

Após a redação do relatório acima, o usuário autorizou buscar a documentação na internet. O snapshot OpenAPI atual da API Consulta foi recuperado por Docker com um GET documental registrado, hash `b2c1fb8874861f63ebf7c065756f82c04dc4a84c8def63e5b4aa641d5edb4430`; um segundo GET documental arquivou o OpenAPI de Integração, hash `d721ad1c59a82f5dea3ced7ce4e92dc51da91096c2041382a567aa5773a57aa2`. O primeiro contém as mesmas operações e parâmetros do snapshot de 13/10, mudando apenas 35 valores de exemplo de datas. A tentativa de baixar o PDF do manual de consultas falhou (exit 1; nenhum PDF/recibo salvo); a ferramenta web informou 401 ao abrir o PDF. `manual-failure.json` preserva o resultado sem alegar conteúdo recebido.

As rotas GET para detalhe, itens, resultados por item, documentos, histórico e catálogo de modalidades constam do contrato atual, mas não foram chamadas. `config/contracts/pncp-history-mission-policy.json` foi atualizada para apontar ao novo contrato de **Consulta** e marcar somente sua estrutura como revalidada. `enabled=false` e orçamento de dados zero foram mantidos. `plan` passou de 13 para **12 gates**, exit 3, `network_calls=0` e `database_writes=0` em `backups/s06-docs-policy-plan.txt`. Limites oficiais numéricos, data inicial, catálogo completo, semântica das bordas, janela efetiva, orçamento e Prefect compartilhado continuam pendentes. Relatório detalhado e URLs oficiais: `docs/licitacoes/fontes/pncp-consulta-2026-10-28.md`. O estado de S06/S07/S08 acima não mudou.

## Complemento de sondas reais autorizadas em 28/10/2026

Após o complemento documental, o usuário autorizou **até 12 tentativas reais de dados** conforme `S06_SONDAS_PROPOSTAS.md`. `scripts/radar_pncp_s06_probe.py` verificou o destino segregado e reservou tentativas no controle persistente antes das conexões. Executou quatro: catálogo HTTP 200 com 19 modalidades atuais; publicação HTTP 200 e atualização global HTTP 200 em uma única página da modalidade 6; detalhe sob a base de Integração HTTP 301. O corpo do 301 apontou porta não prevista e não foi seguido. O orçamento foi pausado em **4/12**, sem renovação ou novas chamadas. Quatro arquivos de corpo e recibos tiveram SHA-256 conferidos. Evidência e limitações detalhadas em `S06_SONDAS_RESULTADO.md`.

O inventário posterior confirmou C2 e principal intactos. Essas sondas não preencheram Bronze nem foram promovidas ao histórico; permanecem evidência operacional separada. A missão principal segue `enabled=false` e orçamento zero. A divergência operacional da rota de detalhe bloqueia o enriquecimento até nova validação; S06 real ainda não satisfaz seu critério de saída, S07/S08 continuam pendentes.

**Retomada autorizada posteriormente:** o detalhe pela base oficial de Consulta, prevista no OpenAPI arquivado, retornou 200 com identidade conferida; itens, lista de arquivos e histórico também retornaram 200. O processo observado tinha `existeResultado=false`, então não houve GET de resultado por item, nem download dos bytes de anexos. Três janelas antigas da modalidade 6 retornaram 204; elas não estabelecem a primeira data recuperável. O controle foi pausado ao final em **11/12** tentativas (7 HTTP 200, 3 HTTP 204, 1 HTTP 301), com uma tentativa não usada. Onze outcomes, corpos e recibos foram reconciliados; SHA-256 e tamanho dos onze corpos conferiram. Inventário posterior manteve principal e C2 intactos. Ver `S06_SONDAS_RESULTADO.md` e `S06_SONDAS_RETOMADA_PROPOSTA.md`. S06 real continua sem aceite, e S07/S08 não foram ativadas.
