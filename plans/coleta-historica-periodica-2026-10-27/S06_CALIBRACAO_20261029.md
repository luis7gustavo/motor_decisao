# S06 — segunda campanha de calibração PNCP em 29/10/2026

## Autorização, proteção e preparação

O usuário autorizou ajustar orçamentos e executar diversas sondas até avançar a prova. A campanha preservou um teto finito: a chave compartilhada `pncp:public-read:shared-docker-egress` foi ampliada de 12 para 36 tentativas cumulativas, **sem zerar as 11 já consumidas**. A missão antiga ficou pausada em 11/12. A missão nova `pncp-s06-calibration-2026-10-29` recebeu teto próprio 24 e foi pausada ao final em 22/24. O orçamento compartilhado terminou desabilitado em 33/36. Preparação e resultado da ampliação estão em `backups/pncp-probe/20261029/budget-amendment-*.json`.

Antes da alteração, o backup `backups/c2/c2-20261013/calibration-20261029-current-0012.dump` foi criado com SHA-256 `c46c79aa5c25fb63e1f7b9efd09c3895c1ef19d0b2e175383e83d2fb73f59450`. `pg_restore --list` e restauração integral em banco descartável passaram; contagens e fingerprints de Bronze/identidades/versões/observações conferiram, zero hashes Bronze inválidos, banco de ensaio limpo. Relatório protegido `calibration-20261029-restore-report.json`. O principal manteve revisão `20260817_0009` e nenhum schema `procurement`; C2 manteve OID 2508600/revisão `20261027_0012`.

A suíte final do runner Docker passou com **196 testes, 1 aviso**, exit 0; manifesto `backups/20261029T190517Z_7c4a1dac.json`. O teste novo impede ampliar orçamento se houver missão habilitada, exige limite e consumo esperados e confirma que contador, tempos de espera e tentativas antigas ficam intactos. Outro teste separa o teto cumulativo da fonte do teto da missão; a CLI exige ambos antes de iniciar a carga real. A campanha usou uma conexão, intervalo mínimo de 1 segundo, sem retry ou redirecionamento automáticos. O único timeout consumiu uma tentativa e pausou a campanha; uma repetição manual posterior com timeout de 60 segundos consumiu nova reserva e retornou 200.

## Resultados da campanha

Foram **22 reservas e 22 outcomes**: 20 HTTP 200, um HTTP 204 e um timeout. Há 21 pares de corpo/recibo e um recibo de erro; todos os 21 corpos conferiram em tamanho e SHA-256. Nenhum corpo foi promovido a Bronze ou a registro histórico da missão principal. Os arquivos protegidos estão em `backups/pncp-probe/20261029/`.

| Operação e amostra | Resultado observado | Limite da conclusão |
|---|---|---|
| Catálogo `/v1/modalidades?statusAtivo=false` | HTTP 200, array vazio | Não prova que códigos históricos nunca foram removidos. |
| Publicação, modalidade 6, 13/10/2026 | Páginas 1 e 2 HTTP 200, 10 registros cada, total atual 1.494/150 páginas. Em 28/10 o total era 1.495; página 1 conservou suas 10 identidades e não se sobrepôs à página 2 atual. | A fonte mudou entre datas; duas páginas não provam cobertura das 150. |
| Atualização global, modalidade 6, 13/10/2026 | Páginas 1 e 2 HTTP 200, total atual 3.054/306 páginas; em 28/10 era 3.160/316. A primeira tentativa da página 2 terminou em timeout, foi registrada e não devolvida; a repetição respondeu 200. | Fonte mutável; não extrapolar de duas páginas para o universo. |
| Atualização, modalidade 1, 13/10/2026 | 11 registros em duas páginas, 11 identidades únicas; página 1 estável na releitura. | Partição pequena completa apenas neste instante. |
| Publicação, modalidade 1, 14/10/2026 | 14 registros em duas páginas, 14 identidades únicas; página 1 estável na releitura. | Partição pequena completa apenas neste instante. |
| Publicação, modalidade 1, 12–13/10 e 13–14/10 | 12–13 trouxe as mesmas seis identidades do dia 13; o dia 12 sozinho retornou 204. 13–14 trouxe exatamente a união das 20 identidades dos dias 13 e 14, sem duplicatas. | Evidência empírica de inclusão das bordas nessa amostra; não estabelece fuso ou regra oficial universal. |
| Atualização, modalidade 1, 13–14/10 | Três páginas com 30 identidades únicas, exatamente a união dos 11 registros do dia 13 e 19 do dia 14. | Evidência empírica das bordas nessa amostra; dados podem mudar posteriormente. |

Para dimensionamento, **apenas** a janela observada de 13/10 na modalidade 6 exigiria no mínimo 30 páginas de publicação e 62 de atualização com `tamanhoPagina=50`, se os totais 1.494 e 3.054 permanecessem estáveis. Isso não é extrapolação nacional nem inclui detalhes, itens, documentos, retries ou reconciliação.

## Estado após as sondas

O inventário somente leitura confirmou principal `20260817_0009` sem `procurement`, C2 OID 2508600/revisão `20261027_0012`, 3 jobs, 4 Bronze e 10 identidades/versões/observações. O job legado permanece `partial/SourceChanged`, página 2, com ledger próprio 11 reservas (DADOS 4/50). O verificador C2 saiu 0. As sondas desta campanha foram guardadas no controle de fonte e em arquivos protegidos, sem iniciar backfill/partições da missão histórica principal.

Backup **posterior** `backups/c2/c2-20261013/calibration-20261029-post-current-0012.dump`, 854.474.041 bytes, SHA-256 `a2a31eacb56b6caa66605eb2491e440d11e1b5ea11a8873ee44fef50a1380cbc`: TOC e restauração integral em banco descartável passaram, 33 reservas/outcomes, duas missões de controle, os mesmos quatro Bronze e dez identidades/versões/observações, zero hash Bronze inválido, fingerprints iguais ao estado pré-campanha para os dados de domínio e banco de ensaio removido. Relatório `calibration-20261029-post-restore-report.json`.

No encerramento da campanha, os gates de início histórico, catálogo histórico, fuso/semântica oficial das datas, orçamento do backfill, coordenação com outros consumidores PNCP e ativação permaneciam. A política mantinha `enabled=false` e `external_attempts_authorized_now=0`; os gates de rota ainda não haviam sido habilitados.

**Decisão posterior do usuário:** `S06_DECISAO_ESCOPO_ACESSIVEL_20261029.md` redefine o alvo como o universo acessível pela API. Com base nas sondas, a política passou a aceitar o catálogo atualmente exposto e a semântica diária observada na amostra, habilitando os gates das duas listagens. Isso não ativa a missão principal: início operacional, limitador compartilhado e orçamento continuam pendentes.

O backend da raiz `motor publico` estava parado durante a campanha, mas seu cliente PNCP usa espera/retries em memória e não compartilha o orçamento persistente do C2. A ausência atual de tráfego desse backend é observação operacional, não garantia para uma carga histórica longa. Antes de ativar a missão principal, é necessário um caminho comum de reserva para todos os consumidores conhecidos ou isolamento verificável durante toda a operação.
