# Radar Licita — controle de execução, 04/11/2026

Fotografia observada em 04/11/2026, cerca de 14h23 (America/Sao_Paulo). A janela de 24 horas está em curso, de 04/11 às 09h42 até 05/11 às 09h42. Os comandos de agenda, missão, auditoria e relatório da janela foram executados somente para leitura. Os números abaixo são uma fotografia, não um aceite de cobertura.

## Baseline e isolamento

- Checkout operacional: branch `codex/radar-licita-mvp-sacy-2026-10-16`, HEAD `4a9b91d34ff0d2bf38033cd085f501918f19b501`, à frente do remoto por 2 commits. Havia 64 entradas em `git status --porcelain` antes desta edição: 12 arquivos rastreados modificados e 52 entradas não rastreadas. Nenhuma foi sobrescrita.
- Só havia um worktree registrado, o checkout operacional. O worker dedicado monta `app/`, `pipelines/`, `scripts/` e `config/` dele. Nenhuma edição de produto nesses diretórios foi feita nesta verificação.
- Destino C2 segregado identificado no registro de credenciais: banco `test_20261020_ba894ce71405`. O banco principal é `motor_decisao`, conforme o contrato do runner. Esta verificação não copiou dados nem escreveu em nenhum dos dois bancos.
- Trava exclusiva `backups/pncp-control/exclusive.lock` presente (145 bytes; não modificada desde 29/10). Bronze, ledger, backups e recibos devem permanecer preservados. O conteúdo da trava não foi revalidado nesta fotografia.

## Operação da missão S08

- O `docker compose ps` mostrou PostgreSQL, Redis, servidor, serviços e worker dedicados em execução; PostgreSQL, Redis e servidor dedicados estavam `healthy`. O Redis compartilhado continuava reiniciando, fora da pilha da missão.
- O deployment dedicado `9183361f-fe0e-4a10-8469-d348f765fdb7` estava ativo, com pool/fila sem pausa e concorrência 1. O worker `pncp-history-dedicated-pc1` estava `ONLINE`. Três schedules estavam ativos com `mode` correto: backfill a cada 60 segundos, sync 00/06/12/18 e reconciliação segunda 03h30, ambos no fuso de São Paulo.
- Missão e fonte estavam habilitadas. Ledger: missão 4.764/100.000 e fonte 4.863/100.099 tentativas; saldo compartilhado 95.236. Intervalo mínimo efetivo 1.000 ms. Zero outcomes faltantes na fonte, segundo o relatório de 24 horas. O limite diário de 90.000 e o teto de até 60 tentativas/minuto são política registrada no `STATUS.md`; esta fotografia não auditou o pico por minuto.
- Último run de negócio correlacionado nos logs até esta fotografia: `expert-loon`, backfill iniciado por agenda, publicação com páginas persistidas, atualização `deferred` por `http_500`; `business_outcome=partial_or_deferred` às 14h23 locais. Prefect o registrou como `Completed`, que não equivale a lote integralmente concluído. O sync das 12h locais teve `partial_or_deferred`. Há também runs de backfill agendados antigos descartados por idade; a contagem de `Completed` inclui esses descartes.

## Janela de 24 horas — progresso parcial

Relatório observado às 14h23min35s locais, cerca de 4h41 após o início:

| Indicador | Valor observado |
|---|---:|
| Tentativas da missão na janela | 2.651 |
| Média desde o início | 9,415 tentativas/minuto |
| HTTP 5xx | 33 |
| HTTP 403 / 429 / 200 malformado | 0 / 0 / 0 |
| Outcomes ausentes | 0 |
| Identidades históricas únicas | 92.542 |
| Aumento contra baseline de 62.953 | 29.589 |
| Partições históricas completas / pendentes / reconciliação | 154 / 349 / 33 |
| Partições recorrentes completas / pendentes | 11 / 293 |

As contagens por hora UTC foram 177 (12h, hora parcial), 665 (13h), 764 (14h), 392 (15h), 406 (16h) e 247 (17h, hora parcial). São reservas da janela, não uma garantia de enumeração completa. O relatório registrou zero chamadas PNCP e zero escritas provocadas pela própria consulta.

## Gate A e lacunas

| Ponto | Estado | Evidência e limite |
|---|---|---|
| 1. Coleta oficial | parcial | Agenda/worker ativos, ledger íntegro na fotografia e avanço de identidades; runs recentes com `http_500` adiado e descarte de agendamentos antigos. Janela de 24 horas ainda não terminou. |
| 2. Cobertura e qualidade | parcial | Auditoria `historical_coverage_status=partial`, 349 partições históricas pendentes e 33 em reconciliação; recorrência 293 pendentes. Não há prova independente de enumeração integral. |
| 3. Base organizada | parcial | C2 segregado e 92.542 identidades únicas observadas; esta fotografia não revalidou hashes Bronze, duplicidade, vínculos ou versões no recorte de produto. |
| 4. Enriquecimento | pendente | Detalhe: fila/identidade e orçamento; itens: paginação e orçamento; resultados: validação de runtime e semeadura por item; documentos: política de armazenamento binário e download. |

O frescor da carteira requer comparação entre a última atualização oficial observada e o último sync bem-sucedido. Essa medida não consta dos quatro relatórios consultados; a idade de partições pendentes não deve ser usada como substituto. Não houve mudança de agenda ou orçamento nesta trilha.

## Próxima decisão

Continuar medições somente de leitura até 05/11 às 09h42, correlacionando o resultado de negócio de cada run relevante com ledger e auditoria. O Gate A ainda não libera integração real de B, C ou D. Desenvolvimento de produto pode avançar isoladamente com fixtures sintéticas; qualquer migração ou integração real depende de G0–G4 e depois de janela operacional G5–G6.

## Trilha de produto iniciada após a fotografia

Foi criado o worktree `motor_decisao_produto`, branch `codex/radar-licita-produto-2026-11-04`, a partir do HEAD operacional identificado acima. A entrega B offline está descrita em `motor_decisao_produto/docs/licitacoes/radar-demo-offline.md`: busca, detalhe, alterações e estados de erro/desatualização em fixture sintética. Quatro testes focados passaram no container sem rede; `git diff --check` passou. Integração real, alertas persistentes, RNF e revisão independente seguem pendentes.

## Atualização de acompanhamento — 14h33 locais

Nova leitura somente de relatório: 2.784 tentativas na janela (média 9,539/min), missão 4.897/100.000, fonte 4.996/100.099, 34 HTTP 5xx, zero 403/429 e zero outcomes faltantes. Identidades históricas únicas 94.618, aumento de 31.665 contra o baseline. Partições históricas 157 completas, 347 pendentes e 34 em reconciliação; recorrentes 11 completas e 293 pendentes. Consulta SQL somente leitura do ledger da missão mediu pico de 27 reservas em minuto de calendário e 29 em janela móvel de 60 segundos até a observação, dentro do teto local de 60/min. A medida é sobre reservas da missão, não sobre completude de dados nem uma auditoria global da fonte. Último resultado de negócio visível nos logs até 14h31 foi `backfill` `partial_or_deferred`; a janela permanece em curso. A entrega de produto foi preservada no commit `a15558c`.

Consulta SQL adicional no C2, também somente leitura: as 94.632 linhas de `procurement.procurement_versions` tinham `source_time_text` preenchido, `source_updated_at` nulo e `timezone_status='unknown'`. Portanto, esta fotografia não consegue datar com segurança a última atualização oficial observada. O último sync de 12h locais foi `partial_or_deferred`; não há nesta verificação prova de um sync integralmente bem-sucedido na janela. O frescor exigido para B real permanece sem demonstração, mesmo com avanço no backfill.

## Atualização de acompanhamento — 16h55 locais

O relatório de 24 horas, somente leitura, marcou 4.410 tentativas na janela (média 10,172/min), missão 6.523/100.000 e fonte 6.622/100.099; zero outcomes faltantes, zero HTTP 429/200 malformado e 42 HTTP 5xx. Identidades históricas únicas 100.562, aumento de 37.609 contra o baseline. Partições históricas 192 completas, 333 pendentes e 35 em reconciliação; recorrentes 11 completas e 293 pendentes. Nos logs do worker, os backfills das 16h46 e 16h54 tiveram `business_outcome=batch_completed`, enquanto o das 16h39 foi `partial_or_deferred`. A janela prossegue sem aceite S08.

## Trilha de produto — Etapa D sintética

O worktree isolado recebeu no commit `e2e5dea` um núcleo puro de viabilidade com `Decimal`, unidade completa, guardas de fonte/versão/fornecimento/logística, margem e fluxo de caixa. A fixture, o CLI, as oito verificações focadas e os limites estão em `motor_decisao_produto/docs/licitacoes/radar-viability-offline.md`. A execução no container sem rede retornou `viável` apenas para o cenário inventado, lucro R$ 1.622,00 e capital requerido R$ 12.178,00. A garantia continua vinculando capital até o dia 75 da fixture. Não houve integração real, migração, escrita no C2/principal ou mudança na agenda. Revisão independente e Etapa E permanecem pendentes.

## Atualização de acompanhamento — 17h04 locais

O relatório marcou 4.510 tentativas na janela, média 10,201/min, missão 6.623/100.000, fonte 6.722/100.099, zero outcomes faltantes, zero 429 e 42 HTTP 5xx. Identidades históricas únicas 100.562 (37.609 acima do baseline); a diferença estável após 100 novas tentativas não foi interpretada como falha ou sucesso de cobertura. Partições históricas 194 completas, 331 pendentes e 35 em reconciliação. O backfill das 17h03 terminou com `business_outcome=batch_completed`. O sync integral e o frescor oficial seguem sem prova; janela ainda em andamento.
