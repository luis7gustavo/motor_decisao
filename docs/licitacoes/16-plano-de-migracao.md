# 16 — Plano de migração, implantação e recuperação

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: rascunho para revisão do operador; [PROPOSTO], nenhum banco, volume, configuração ou migration alterado.

## 1. Princípios e linha de partida

A migração evolui o SILLO no PostgreSQL atual da aplicação. O PostgreSQL de metadados Prefect também é preservado, sem virar banco comercial. Manter `motor_postgres_data`, `prefect_postgres_data` e `motor_redis_data`; sua existência declarada no Compose é [CONFIRMADO], mas conteúdo/saúde instalados são [NÃO-CONFIRMADO]. Inspeção do código não substitui inventário operacional futuro. Não interpretar a branch documental como release executável do Radar.

O head estático Alembic `20260817_0009` é referência de código, não evidência da revisão aplicada. `alembic/env.py` tem `target_metadata = None`: autogenerate não é inventário seguro do banco. Migrations futuras exigem revisão manual de SQL, dependências e locks. Seguir [08](08-modelo-de-dados.md), [21](21-infraestrutura-docker.md) e ADR-003/008/014/016; o procedimento não autoriza executar comandos nesta missão.

## 2. Mapeamento e preservação

| Origem | Tratamento futuro | Verificação de preservação |
|---|---|---|
| Bronze comercial e `silver.supplier_products_normalized` | Manter; importar candidatos em `sourcing` somente com identidade/unidade/atributos validados | Manifesto origem→destino por PK/hash; nenhum produto automaticamente confirmado |
| `control.pipeline_runs` e observabilidade atual | Reusar semântica compatível; acrescentar `control.ingestion_jobs` | FK válida, escopo/checkpoint/fencing independentes, leitura antiga funciona |
| Gold de revenda/runs/snapshots e feedback de revisão | Preservar em suas tabelas e significados | IDs, conteúdos e auditoria iguais; aprovação de compra nunca vira E07 |
| Estruturas ML e exportação BI | Preservar como legado opcional | Sem dependência do MVP; nenhuma exclusão ou promessa de modelo treinado |
| Nova fonte PNCP | Criar `bronze.pncp_responses` e schema `procurement` aditivamente | Identidades oficiais, versões/observações, PK/FK e manifesto coerente |
| Novos cenários/decisões/estados | Criar `commercial`, `sourcing` e projeção Gold recuperável | Snapshot íntegro; projeção pode ser refeita sem perder autoridade transacional |
| Originais documentais | Volumes novos de documentos/spool, vinculados por hash/UUID | Banco e arquivos recuperados no mesmo ponto consistente; sem órfãos publicados |

Backfill não inventa dado: desconhecidos permanecem NULL com motivo. Guardar mapeamento versionado contendo tabela/PK/hash de origem, ID de destino, transformação, autor/run, estado e rejeição. Processar lotes pequenos com checkpoint; replay não altera IDs já reconciliados. Conflito de identidade/embalagem fica em relatório de rejeição, não desaparece da contagem. Cada lote satisfaz `lidas = aceitas + rejeitadas + pendentes`, com duplicadas identificadas no subconjunto correspondente. Não somar categorias sobrepostas sem definição.

## 3. Gates de implantação

| Gate | Ação futura e responsável | Evidência exigida / condição para avançar |
|---|---|---|
| G0 — inventário real | Técnico identifica revisão instalada, extensões, schemas, PK/FK, contagens, tamanhos, imagens, volumes e jobs ativos | Manifesto sem credenciais; divergência da baseline resolvida antes de mudar |
| G1 — recuperação | Técnico e operador suspendem novas mutações/coletas, drenam tarefas ou registram parciais; produzem backup consistente | Ensaio isolado de restore aprovado, RPO/RTO medidos; sem isso, não migrar |
| G2 — expansão em cópia | Técnico aplica migration aditiva única com lock, destino explícito e recursos limitados | Constraints/índices e leitura legada aprovados; nenhuma remoção/renomeação destrutiva |
| G3 — backfill e reconciliação | Técnico executa transformação em cópia; operador resolve exceções | IDs/contagens/hashes preservados; rejeições explicadas; reexecução idempotente |
| G4 — aprovação da release | Operador revisa relatório; técnico registra imagens/revisão e compatibilidade de retorno | TST aplicáveis passam, backup vigente, rollback ensaiado e pendências de contrato resolvidas para funções liberadas |
| G5 — corte controlado | Técnico repete procedimento aprovado no destino operacional, com janela e escrita suspensa | Uma migration; `prefect-init` não concorre; schema expandido e aplicação anterior compatível |
| G6 — ativação gradual | Operador habilita recorte pequeno, depois módulos conforme roadmap | Contagens, frescor, erros e consumo monitorados; sem recomendação positiva de contrato/insumo incompleto |
| G7 — aceite do piloto | Operador verifica jornada Must das fases 1–5 | Evidência integral de positivos e metas medidas/pendências explícitas; não prometer ganho sem amostra |

O gate de aprovação operacional pertence à missão de implementação. Nesta execução apenas se documenta o procedimento. Schedules iniciam suspensos; habilitação do PNCP é única em Prefect, sem daemon concorrente. Revisão humana mínima acompanha cada fase, sem esperar a carteira completa da fase 5. Nenhum corte exige apagar dados ou desligar histórico de revenda.

## 4. Backup, consistência e retenção

Proposta: cópia diária consistente, com RPO de até 24 h, mais cópia antes de mudança relevante; ensaio mensal e após alteração do procedimento. RTO de até 4 h começa com destino Docker e cópias disponíveis, conforme RNF-014. Metas ainda não medidas. Retenção operacional inicial: 7 diárias, 4 semanais e 12 mensais, com deduplicação documentada; não é prazo legal de guarda de registros de negócio. Revisar capacidade e obrigações antes de automatizar expiração, sem apagar única cópia válida ou evidência sob retenção necessária (13/ADR-016).

Após suspender escrita e registrar jobs em voo, produzir dump lógico consistente do banco de domínio e metadados Prefect; finalizar arquivos antes do ponto de corte; incluir originais/hash, manifesto de referências, revisões, imagens, parâmetros e checkpoint. Não copiar diretório PostgreSQL ativo como backup consistente. Manter segredos em canal de recuperação separado. Cópia independente protegida fora do volume operacional; cifrar quando fora do host controlado.

Restaurar em instância com nomes de volume e credenciais próprios, sem herdar `name:` globais. Manter schedules desligados. Conferir esquema/PK/FK, contagens, hashes e todos os IDs legados incluídos no manifesto; reconstruir amostra documentada de análises de cada tipo e todos os casos críticos sintéticos. Restore que termina sem erro não prova recuperação de negócio. Reconciliar leases e jobs antes de habilitar Prefect; Redis não é autoridade de decisões e AOF antigo não pode redisparar trabalho sem deduplicação.

## 5. Rollback e falhas

| Situação | Resposta futura | Condição de retomada |
|---|---|---|
| Migration falha antes de commit | Interromper release, manter escrita suspensa e avaliar transação/DDL parcial | Schema real reconciliado com manifesto; não repetir cegamente |
| Backfill incompleto | Pausar novo módulo, conservar origem/rejeições e retomar último lote durável | Replay idempotente e contagens conciliadas |
| Nova API falha com schema aditivo compatível | Retornar imagem conhecida e desativar função nova, preservando dados novos | Leitura legada e jobs existentes aprovados; evitar worker antigo consumindo job novo |
| Schema/dados incompatíveis com imagem anterior | Suspender escrita e preparar recuperação em destino isolado | Validar ponto restaurado e reconciliar eventos posteriores antes de troca controlada |
| Documento ausente ou hash divergente | Não publicar evidência/análise afetada; restaurar original pelo manifesto | Hash exato verificado ou nova obtenção versionada, nunca substituição silenciosa |
| Duplicidade ou revisão perdida no piloto | Suspender positivos, registrar incidente e reconciliar com trilha | Causa corrigida, regressão comprovada, decisão afetada revista |

Não usar downgrade destrutivo como retorno padrão. Restore de backup antigo pode perder fatos ocorridos depois dele: manter ledger/eventos e exportação protegida do período, identificar atos externos com operador e reconciliar antes de abrir escrita. Não sobrescrever automaticamente o volume ativo. Encerramento e rollback não apagam participação, contrato ou pagamento já ocorrido.

## 6. Critérios de conclusão futura

100% dos IDs, registros e históricos da baseline de migração permanecem preservados; mapeamentos/rejeições estão explicados; nova aplicação e leitura legada funcionam; teste não acessa operacional; restore e retorno de imagem têm relatório; versões/fórmulas reproduzem decisões. TST-039/042/046/048/051/053/054 e RNF-008/012/014/017/019/020 governam o aceite. Esta documentação resolve QA-006 no nível de procedimento; ensaios e dados reais permanecem para implementação.

## 7. Migração do incremento 2-H — revisado em R1

Revisão 0.3-R1-E2, 2026-10-13. [PROPOSTO], especialização dos gates G0–G7, sem novo gate com responsabilidade duplicada. G7 do MVP completo permanece distinto do aceite exploratório restrito de 23.

1. G0 inventaria se as tabelas propostas em 08 sequer existem; inspeção de código não prova schema instalado. Em missão futura, conferir tipos, índices e validadores de CNPJ em órgão/fornecedor/importação/UI/API, inclusive regex só numérica ou coerção a inteiro. Registrar versão oficial do DV antes de alterar validação. Nenhuma restrição defeituosa implementada foi afirmada nesta revisão.
2. G1 ensaia cópia/restore com todos os UUIDs legados, cotações, snapshots, jobs, políticas e conteúdo Bronze referenciado. A amostra R1 não é origem de backfill: nenhum registro foi coletado. Guardas e credenciais do destino independentes, schedules suspensos.
3. G2 expande colunas textuais original/canônico/tipo/país/validação; não modifica o valor original para satisfazer novo índice. Acrescenta eventos/links/comparações/candidatos/ledger de 08 no mesmo banco. Migração explícita com lock; substituir restrições numéricas apenas após leitura antiga compatível demonstrada. Nenhum nome pessoal de caminho ou banco torna o procedimento reproduzível.
4. G3 mapeia cada PK de origem para mesmo supplier_id quando identidade completa foi comprovada; registros duplicados entram em reconciliação, não merge automático. Raiz gera só coluna analítica. ID ausente/inválido conserva UUID e motivo. Mapear historical_prices antigas para event_id apenas com evidência da etapa/identidade; sem ela, quality_state pendente. Cotação antiga fica em supplier_quotes e projeção aponta quote_id, sem copiar autoridade. Preservar naturezas, centavos/frações e zeros significativos do documento fiscal. Gerar manifesto origem/PK/hash→destino/PK/regra/versão/estado.
5. Validar cada lote: lidas=utilizáveis+rejeitadas+pendentes, possível duplicidade como subconjunto; checar chaves oficiais por fonte, FK/escopo, dez replays e retorno a conteúdo anterior. Constraint UNIQUE parcial fiscal só passa após exceções resolvidas/segregadas; reexecução conserva IDs. TST-056–071 e 039/042/046/048/051/054 futuros em Docker.
6. G4–G6 habilitam recurso a recurso após QA-R1-02/03/04, revisão de retenção e decisão do operador. Se fonte não provar item/unidade/vínculo, manter recurso desabilitado; alternativa manual com evidência continua. Não mover cotação/fornecedor existente para dependência do histórico.
7. Retorno seguro: desligar incremento e seus schedules, retornar imagem compatível com schema expandido, preservando eventos novos e histórico. Nunca downgrade/fusão destrutiva como retorno automático. Incompatibilidade exige pausa de escrita, restore em destino independente e reconciliação de fatos posteriores conforme seção 5. Reconstituir Gold a partir das autoridades, não restaurá-lo como única história.

Aceite de preservação: manifesto inclui todos os registros da baseline, 100% dos IDs/hashes legados preservados, todas as exceções explicadas, nova validação aceita ambos os formatos por fixtures oficiais e não desloca cotação entre filiais. Nenhum ensaio foi executado na R1; bloqueia implementação/corte, não conclusão documental.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: expansão fiscal/eventos/candidatos, mapeamento e retorno aditivos; gates existentes preservados, migração não executada.

