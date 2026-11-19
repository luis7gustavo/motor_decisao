---
id: EVID-P01
tipo: evidencia
estado: pronto_para_revisao
classe_evidencia: acervo_real_local_nao_aprovado
atualizado_em: 2026-11-19
---

# P01 — auditoria dos manifestos existentes

## Recorte e decisão proposta

Em 19/11/2026, foi verificada **somente a integridade do backup local do piloto de 20 licitações de 12/11**. Os 20 manifestos por licitação coincidem com as entradas do manifesto raiz; os 186 arquivos listados existem, têm o tamanho e o SHA-256 declarados, e não há arquivo de resposta sem entrada. As contagens registradas são 2.748/2.748 itens e 40/40 documentos. Isso permite propor a Carlos o fechamento **do pacote de inspeção do piloto**, limitado ao índice observado. Não constitui aprovação humana, E3, cobertura da fonte nem fechamento da janela operacional de 12–13/11.

O backup foi recebido entre **12/11/2026 11h55min56s e 12h03min02s de São Paulo** (horários UTC registrados nos manifestos: `14:55:56.873667` a `15:03:02.510627`). O recibo `20261112-enrichment-rewindow.json` marca a nova janela de **12/11 12h38 a 13/11 12h38**. Portanto, o pacote inspecionado é anterior à janela e não pode ser usado como seu fechamento.

## Fontes locais examinadas

| Fonte | Identificação e integridade | Papel e limite |
| --- | --- | --- |
| `motor_decisao/backups/pncp-enrichment-inspection-20261112/manifest.json` | SHA-256 `62172be9b4ae314b4eb1e3eaab4aa28c025e06d2415b75db273cdd3d516f6106` | Manifesto raiz do piloto; 20 entradas. Arquivo local não publicado neste PR. |
| `motor_decisao/backups/pncp-enrichment-inspection-20261112/LEIA-ME.md` | SHA-256 `632f88de2ec0c70e3b1445c3d95a2bbc24653405b6a6d468095cc8a441a73ab0` | Resumo original; seu texto limita a completude ao índice observado. |
| `motor_decisao/backups/pncp-history-budget/20261112-enrichment-rewindow.json` | SHA-256 `4ed4a6a41d50eb02c1d50af0f9babad4678b88b447f9240c241e474763624603` | Recibo da janela; estado `rewindowed_paused` no instante do registro, não recibo de conclusão. |
| `motor_decisao_coleta_20261111/plans/pncp-enrichment-20261111/STATUS.md` | SHA-256 `a82de8bc4742a1d60844aeeb9666c1a2ecd60cdbac514e4163544a42848a73f5` | Relato operacional de 12/11: início da janela, HTTP 500 em atualização e lotes documentais parciais/adiados. Não atesta o encerramento em 13/11. |

Os caminhos acima são relativos à pasta local `motor publico/`. Os manifestos, respostas e identidades reais não foram copiados ao repositório. O uso local para inventário segue o limite de G0; publicação e classificação E3 dependem da decisão de Luis.

## Verificação reproduzível

Executar `node scripts/audit_pncp_inspection_manifest.mjs <caminho-do-backup>` com o backup original. A ferramenta lê arquivos locais, sem rede nem banco, compara cada manifesto filho ao raiz, confere existência, tamanho e SHA-256 de cada resposta, detecta arquivos não listados e reconcilia as contagens do cabeçalho. Resultado desta execução: **20 entradas, 186 arquivos, zero erro**, com 20 detalhes, 20 contagens de itens, 66 páginas de itens, 20 contagens de documentos, 20 índices de documentos e 40 anexos binários. Todos os 186 registros estavam em estado `complete`; os campos de inspeção registraram zero hash de corpo inválido ou ausente. A ferramenta retorna código diferente de zero se encontrar divergência.

O script verifica a consistência do backup, não a verdade externa dos dados, o funcionamento atual do coletor, a completude nacional, o frescor ou a adequação comercial. O piloto contém leilões/alienações, conforme a linha de base P00, e não valida a busca de compras de periféricos.

## Pendência da janela 12–13/11 e aceite

No diretório local de backups examinado, os manifestos de inspeção localizados pertencem ao piloto anterior à janela; não foi encontrado manifesto de fechamento de 13/11. Para fechar a janela operacional, Carlos precisa entregar ou indicar o manifesto/ledger final com intervalo, versão, páginas, falhas e recursos parciais/adiados, reconciliar os recursos esperados e os hashes Bronze, registrar a lacuna da rota de resultados se aplicável e aprovar explicitamente o fechamento real com data e versão. Luis valida proveniência e permissão de uso dos dados para o produto. **G1 permanece pendente.**

Arquivos deste PR: este relatório, o verificador offline e os registros de estado P01. Desfazer: reverter este PR; nenhum dado, banco ou processo de coleta foi alterado. A revisão por sessão independente e a decisão humana ainda precisam ser registradas.
