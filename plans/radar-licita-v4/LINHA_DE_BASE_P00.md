---
id: BASE-P00
tipo: evidencia
estado: aprovada_com_limites
classe_evidencia: mista_por_recurso
atualizado_em: 2026-11-18
base_commit: b46aec77a3448f51ac212676216c675822fb12f3
---

# P00 — linha de base e lacunas

Esta é uma fotografia documental e do código da branch de produto no commit acima, examinada em 17/11/2026. As contagens do piloto são do relatório offline de 16/11, não uma medição de hoje. Os estados e critérios propostos estão em [políticas](POLITICAS_E_CRITERIOS_P00.md); o [cartão P00](prs/P00.md) registra o aceite.

Em 18/11/2026, Luis validou o uso local das fontes e do inventário aqui descritos e aprovou G0 documental com limites, conforme [decisões](DECISOES.md). As lacunas e estados de evidência abaixo permanecem; esta aprovação não comprova cobertura, frescor, rótulos E3 ou qualidade comercial.

## Capacidades por recurso

| Recurso | Implementação ou observação verificável | Lacuna para aceite real | Estado |
| --- | --- | --- | --- |
| Descoberta histórica | A prévia C2 possui busca por objeto, filtros brutos, paginação e detalhe com IDs, versão e hash; `app/api/radar_c2_preview.py` e `app/radar_c2_reader.py`. | Cobertura nacional, sincronização e abertura atual não demonstradas; a prévia de 05/11 apontou sete ponteiros de versão em quarentena. | parcial; histórico |
| Detalhe e versões PNCP | O checkout de coleta registra detalhe no piloto de 20 processos; a prévia C2 preserva identidade e versão. | Reconciliação de todas as versões por janela e com a fonte atual. | parcial |
| Itens e unidade de julgamento | Relatório offline de 16/11: 2.748 itens projetados do índice observado. | Todos são de leilão/alienação; falta amostra de compras e composição confirmada dos lotes. | estrutura observada; uso comercial pendente |
| Documentos e exigências | 40 PDFs íntegros e 4.402 páginas com texto no relatório offline; 3.041 trechos sugeridos. | Revisão humana, anexos retificados, OCR real e vínculo de exigência ao lote. | sugestão, não confirmação |
| Resultados | A exportação de 16/11 não contém resultados. | Coleta, versão e validação por item/lote. | desconhecido na exportação |
| Cobertura e frescor | O piloto da coleta registra 20/20 índices observados, 2.748 itens e 40 anexos. | Falta referência independente, fechamento auditado da janela 12–13/11, recursos e atraso por janela. | não medidos |
| Triagem comercial | Há demonstrações sintéticas de busca, viabilidade e fila em `app/radar_demo.py`, `app/viability_preview.py` e `app/review_queue_preview.py`. | Rótulos positivos/negativos de compras, precisão e revocação por categoria e lote. | demonstração E2; qualidade desconhecida |
| Catálogo, cotação e caixa | Há regras e telas de simulação no produto; a decisão comercial inclui 3.095 registros de origem de MegaMix/Coletek. | Reconciliação versionada, cotação/estoque/frete atuais, premissas financeiras e caixa por data revisados. | referência/estimativa |
| Decisão e operação | O produto expõe fila e ações de revisão; documentos existentes descrevem controle de coleta. | Aceite de responsáveis, auditoria persistente da nova ficha, gates G3–G6 e decisão humana por oportunidade. | pendente |

**Denominador do piloto:** 20 identidades selecionadas e uma versão observada por identidade; 60 recursos esperados *dentro desse índice* para detalhe, itens e documentos. Essa fração não é cobertura de publicações nacionais nem frescor. Os 20 casos e seus 2.748 itens não entram no denominador de precisão ou revocação para compras de bens. O recurso resultados não foi coletado nessa exportação.

## Inventário preliminar das amostras disponíveis em 17/11/2026

| Fonte local examinada sem novas chamadas ao PNCP | O que está comprovado | Uso para a amostra de periféricos |
| --- | --- | --- |
| Exportação enriquecida de 12/11, processada no relatório de 16/11 | 20 identidades de leilão/alienação, 2.748 itens, 40 PDFs; nenhum resultado coletado. | Teste estrutural e exclusão de contexto; nenhum positivo comercial comprovado. |
| Consulta ao Bronze descrita em `tratamento-coletas-com-descricao-20261116.md` | 55 detalhes e 100 páginas de itens com 3.489 ocorrências; zero menção literal a mouse, webcam ou headset e duas menções incidentais a teclado em serviços. | Não comprova compra de periférico elegível; contagens datadas de 16/11, sem inventário atual de todos os casos. |
| Cópia C2 isolada de 10/11 e `amostra-alvo-compras-para-revisao-20261116.md` | Cinco identidades candidatas pelo texto do objeto. Consulta somente de leitura à cópia C2 em 17/11 mostrou apenas recursos de listagem `/v1/contratacoes/publicacao` e `/v1/contratacoes/atualizacao` no Bronze dessa cópia. | Sem itens, lotes e documentos conferidos nessa cópia; nenhum dos cinco IDs é rótulo positivo confirmado. |

**Resultado:** nenhuma compra de periférico com item, lote e documentos revisados foi demonstrada **nessas fontes examinadas**. Isso não prova ausência de casos em outras partes da coleta. P31 deve inventariar metadados e disponibilidade de recursos dos casos coletados, com proveniência e permissão de uso, antes de separar desenvolvimento e aceite cego. Até lá não há denominador comercial E3 nem base para fixar um tamanho mínimo viável a partir do acervo.

## Atualização da coleta isolada em 18/11/2026

Esta seção registra uma consulta **somente de leitura** ao banco C2 isolado da coleta, entre 01h52 e 02h08 de 18/11 (America/Sao_Paulo). É uma fotografia posterior e de fonte diferente da cópia de 10/11 descrita acima. O worker dedicado estava online, a missão e os agendamentos ativos. Na janela iniciada em 17/11 às 17h54 havia 395 tentativas até 01h52, 25 respostas HTTP 5xx, zero 403/429 e zero tentativas sem desfecho registrado naquele instante. A janela termina em 18/11 às 17h54. Execução e respostas recebidas não demonstram cobertura nem frescor nacionais.

Cinco contratações previamente selecionadas pelo texto do objeto tinham índices estruturais completos: 23 registros de itens, 18 documentos baixados com hash presente e válido e 20 registros de resultado esperados/recebidos. A descrição de itens desses cinco casos já havia sido examinada antes do congelamento desta seleção; por isso, **os cinco ficam somente na partição de desenvolvimento**, inclusive os aparentes descartes. Integridade e contagem dos recursos não equivalem a rótulo de periférico, revisão do edital/lote ou elegibilidade comercial E3.

Na fila de detalhes já coletados, um filtro de **metadados do objeto** por mouse, teclado, webcam ou headset encontrou oito identidades, incluindo as cinco anteriores. As outras três entraram inicialmente em reserva provisória. Uma delas tinha índices estruturais completos; duas ainda tinham binário de documento pendente, e uma destas também resultados pendentes. A conferência de disponibilidade usou uma rotina que **processou corpos de respostas** para contar registros e verificar hashes antes do congelamento; somente contagens e estados foram exibidos à autoria, sem descrições, títulos ou rótulos. Portanto essas três **não são tratadas como conjunto cego estrito** até uma revisão independente da elegibilidade. A separação por contratação se aplica a qualquer versão ou item. Nas oito versões correntes consultadas, `source_updated_at` era nulo; isso não comprova frescor.

Uma segunda busca, limitada à fila de detalhes ainda pendentes e usando apenas metadados de objeto e versão, encontrou **mais uma identidade**, sem leitura de corpos por esta sessão. O manifesto privado v2, congelado em 18/11 às 02h14, registra nove identidades: cinco em desenvolvimento, três em reserva provisória com a ressalva de processamento estrutural e uma nova reserva baseada somente em metadados, ainda sem recursos conferidos. SHA-256 do manifesto v2: `2cf5dede886e29b79f6c08cf10eed01da3e9cbab86cd5e07b8366cf95e262431`. O v2 preserva referência ao v1 (`22f5230d89d6809447ea1f62592e175229c3aba6f460ed2778ca151afb95df58`) para auditoria. Os manifestos locais contêm apenas identificadores, hashes de versão, sinais de metadados e disponibilidade, sem corpos ou rótulos, e não integram o PR. **Nenhuma amostra E3 foi aprovada**: proveniência, permissão de uso e elegibilidade dependem da validação de Luis, e o conjunto reservado é insuficiente para métricas por categoria.

## Fontes congeladas para esta fotografia

| Fonte consultada | Versão e SHA-256 local | Uso e limite |
| --- | --- | --- |
| `Radar Licita/ESTADO_ATUAL.md` | 16/11; `7221cea4dbe4dad05d222138cb0860d0df498311cf16c3e0cd1ccb55a05be829` | Síntese datada, não estado ao vivo. |
| `Radar Licita/DECISOES.md` | consultado em 17/11; `e3ac882f87413a64fd1a8b226c6e1918d7735f26fa29553be09f34600d505062` | Decisões de produto registradas. |
| `Radar Licita/plans/PLANO_VERSAO_FINAL_20261116.md` | 16/11; `81c5e14ab5998a95848b8ebf82078421d687c1f84c5202003496df7b489773f7` | Plano I00 anterior; critérios propostos, não execução. |
| `motor_decisao_produto/docs/licitacoes/execucao-metas-2-3-4-piloto-20261116.md` | 16/11; `342d4940701ce2c3c432c9722cbe20cf5fd7418a136d0929658470370679f311` | Medidas da exportação isolada. Documento local ainda não versionado na branch base. |
| `motor_decisao_produto/docs/licitacoes/tratamento-coletas-com-descricao-20261116.md` | 16/11; `b0065aff0a82bc79eb82d1935fca803df3e3483e1fbf4fe71046319e431b46dd` | Consulta datada ao Bronze de enriquecimento e limites das menções a periféricos; não é inventário atual. |
| `motor_decisao_produto/docs/licitacoes/amostra-alvo-compras-para-revisao-20261116.md` | 16/11; `11b68f3f302fbf0b814dbd018c8a872655b04b808be6d0ed5043fcf0a598d6b1` | Cinco candidatos pelo objeto; nenhum rótulo de item confirmado. Documento local ainda não versionado na branch base. |
| `motor_decisao_coleta_20261111/plans/pncp-enrichment-20261111/STATUS.md` | atualização documentada de 12/11; `a82de8bc4742a1d60844aeeb9666c1a2ecd60cdbac514e4163544a42848a73f5` | Piloto e início da janela; não comprova fechamento. |
| `docs/licitacoes/radar-real-isolado-20261105.md` | commit base `b46aec77` | Limites da prévia C2. |
| `docs/licitacoes/radar-review-queue-offline.md` | commit base `b46aec77` | Demonstração sintética de revisão. |

As fontes locais fora deste repositório são referências por caminho e hash, não fixtures publicadas ou autorização para distribuir dados. O manifesto do cofre `RADAR LICITA/.biblioteca-manifest.json` foi consultado para localizar as cópias; para fatos mutáveis, foram lidos os arquivos originais acima. O código e os dados não foram modificados nesta fotografia.
