# S00 — inventário, contrato e política operacional

Data: 27/10/2026, America/Sao_Paulo. Resultado: **concluída documentalmente; rede da missão histórica bloqueada**. Este relatório usa somente o contrato arquivado e evidência local. Não houve requisição externa nesta etapa.

## Inventário comprovado

| Item | Observação desta sessão |
|---|---|
| Git | `codex/radar-licita-mvp-sacy-2026-10-16`, dois commits à frente do remoto; alterações locais anteriores preservadas |
| Docker / Compose | 29.8.2 / 5.5.1; configuração Compose aceita |
| PostgreSQL | 15.19; mesmo `system_identifier` do relatório preservado de 20/10 |
| Prefect no runner | 3.8.8; servidor e workers em execução no Compose; não se verificou schedule nesta etapa |
| Principal | `motor_decisao`, revisão `20260817_0009`, zero schemas `procurement` |
| Destino C2 | `test_20261020_ba894ce71405`, OID 2508600, revisão `20261013_0010`, role própria sem superusuário |
| Banco C2 | 3 jobs, 4 respostas Bronze, 10 identidades, 10 versões e 10 observações |
| Job preservado | `e47c6fe2-2cc9-4b58-803a-49a4aa44a443`: `partial`, `error_code=ValueError`, `next_page=2`, duas requisições e 10 registros vistos |
| Ledger C2 | 11 reservas sequenciais; DOCUMENTAÇÃO 7/15, DADOS 4/50, calibração 2/3; 9 corpos recebidos e verificados por hash/tamanho, sendo 4 de dados; 4 hashes Bronze coincidem com os recibos de dados |
| Armazenamento | C: com 30.907.371.520 bytes livres no momento da inspeção; o `df` dentro do volume PostgreSQL mostrou 1.001.257.848.832 bytes, mas esse valor virtual não substitui o espaço físico disponível no host |
| Manual local | `docs/licitacoes/fontes/` ausente; nenhum manual foi buscado pela rede |

`scripts/radar_pncp_s00_inventory.py` reproduz a verificação de identidade, revisão, contagens, ledger, hashes e job em modo somente leitura. A role de ingestão C2 não pode ler `alembic_version`; por isso a revisão e as contagens são lidas pela conexão administrativa em transação somente leitura, enquanto a identidade da role é conferida pela própria conexão restrita. Não houve migração, criação de banco, reset nem alteração de quota.

## Contrato e limites

Fonte do que está confirmado: `config/contracts/pncp-consulta-20261013.json`, OpenAPI 3.0.1, `info.version=1.0`, observado em 13/10/2026 e hash SHA-256 `a5d87b60a54acfd681e84e0a77d541b0db7065401171508c53c911d12bdd121d`, novamente conferido nesta sessão. O arquivo contém 12 caminhos. Seu hash confirma integridade do arquivo arquivado, não vigência do contrato hoje.

| Tema | Evidência e data | Decisão S00 |
|---|---|---|
| Publicação `/v1/contratacoes/publicacao` e atualização global `/v1/contratacoes/atualizacao` | GETs no contrato de 13/10; exigem `dataInicial`, `dataFinal`, modalidade e página | Operações identificadas; coleta histórica bloqueada |
| Formato das datas | Descrição `yyyyMMdd` no contrato de 13/10 | Inclusão das bordas, fuso e cobertura antiga não identificados |
| Paginação | Página mínima 1, tamanho 10–50 no contrato de 13/10 | Tamanho 50 é teto contratual para estas listas, não prova de paginação estável |
| Modalidades | Parâmetro inteiro obrigatório, sem enumeração completa no contrato arquivado | Catálogo integral pendente; código 6 visto em amostra C2 não cobre o domínio |
| Taxa, concorrência, quota por período, janela máxima, horários e retenção | Nenhuma regra oficial confirmada no snapshot local | `não identificado`; revisão da política de uso é gate para rede |
| Política local candidata | `docs/licitacoes/10-integracoes-pncp.md`, 11/10/2026 | 1 tentativa/s e concorrência 1 são propostas locais, sem execução nesta missão; 6 h de sincronização e revisão semanal de 30 dias também são propostas locais |
| Manual e itens, resultados, documentos | Diretório local ausente; contrato de consulta arquivado não valida rotas de leitura desses recursos | Recursos bloqueados; não inventar endpoints |
| Exportação oficial em lote | Não validada nesta sessão | Descoberta e contrato próprios pendentes |

## Escopo C2 × missão histórica

| Dimensão | C2 preservado | Missão histórica proposta |
|---|---|---|
| Finalidade | Ensaio manual de ingestão | Backfill, sincronização e reconciliação recorrentes |
| Janela | 06–14/10/2026 | Início mais antigo comprovado: pendente; marco final fixo proposto: 26/10/2026, dia civil completo anterior à criação do manifesto; semântica do filtro pendente |
| Modalidade | Código 6 observado | Todas as modalidades comprovadas pelo catálogo oficial; catálogo pendente |
| Filtros | Sem UF; página de 10 | Cobertura nacional sem filtros que excluam o universo; partições ainda não geradas |
| Orçamento | Ledger 7/15 documental e 4/50 dados; 50 é teto do ensaio | Política própria em `config/contracts/pncp-history-mission-policy.json`: **0 tentativas externas autorizadas agora**; orçamento futuro e teto efetivo dependem de dimensionamento e limites da fonte |
| Limitador | Ledger C2 persistente | Exigido limitador persistente e compartilhado por fonte/credencial ou IP entre todas as missões; ainda não implementado |
| Estado | Parcial na página 2, com Bronze retido | Manifesto de planejamento; nenhuma partição executada e nenhum schedule criado |

Separar os orçamentos contábeis não permite ultrapassar um limite compartilhado da PNCP. O manifesto e a política nova são legíveis por máquina, mas **não são consumidos por um coletor ainda**. O estado `enabled=false` e o orçamento externo zero impedem que esta documentação seja tomada como autorização técnica para uma carga real.

## Dimensionamento e decisão por recurso

Sem data inicial comprovada, total por modalidade, catálogo de recursos, quota oficial e volume de anexos, não há estimativa nacional defensável de páginas, disco ou duração. A fórmula a preencher após levantamento é `Σ ceil(registros da partição / tamanho de página aprovado) + detalhes + itens + documentos + reconciliações + margem de retries`; o tempo mínimo depende do menor teto aplicável. As quatro respostas de dados C2 somam 83.525 bytes e não são amostra representativa do histórico. A mudança do total de 9.577/958 para 9.562/957 na página 2 invalida usar esse total como universo estável. O espaço livre físico de 30,9 GB exige planejamento antes de backfill/anexos.

Gates por recurso estão no JSON: publicação e atualização bloqueadas por contrato vigente, termos/limites, bordas, modalidades, cobertura e orçamento; detalhe bloqueado por contrato vigente e orçamento; itens, resultados e documentos sem contrato validado; lote oficial pendente de descoberta. Assim S01–S05 podem avançar offline, cada uma com suas pré-condições. S06 real e S07 continuam bloqueadas.

## Alteração controlada da política C2

O arquivo preexistente continha duas chaves JSON `operator_authorization` idênticas. Foi removida somente a segunda ocorrência redundante. `app/procurement/source_policy.py` rejeita qualquer chave duplicada, inclusive aninhada, e `live_transport` usa esse carregador. Quatro testes isolados em Docker passaram: política C2 continua desabilitada com grant restrito, missão histórica continua com orçamento externo zero, duplicata de topo rejeitada e duplicata aninhada rejeitada. O grant C2 não foi ampliado.
