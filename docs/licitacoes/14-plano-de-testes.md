# 14 — Plano de testes

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: rascunho para revisão do operador; [PROPOSTO], sem implementação ou testes de aplicação executados.

## 1. Estratégia e isolamento

Todos os casos TST abaixo são especificações futuras. Nada deste plano foi executado contra a aplicação. Usar testes unitários puros para regras/Decimal, testes de contrato com HTTP simulado, integração em PostgreSQL descartável e jornadas Jinja2 em container. Relógio fixo, UUIDs determinísticos e dados sintéticos; datas de vencimento são relativas ao relógio do teste. Não consultar PNCP nas suítes determinísticas. As fixtures não afirmam contrato oficial de itens/anexos ainda pendente em [10](10-integracoes-pncp.md).

Antes de qualquer escrita, conferir identidade do servidor/banco, volume e credenciais contra allowlist exclusiva de teste. Projeto Compose distinto não basta: os volumes atuais têm nomes globais fixos. Credencial de teste não acessa operacional. Apontar configuração errada deve falhar antes de limpeza/migration. Não executar a suíte legada com a guarda atual de substring no nome do banco como único controle. Asserts são derivados destas entradas/saídas e das regras, não da implementação a testar.

Responsável técnico prepara fixtures e relatório; operador revisa casos de decisão; especialista valida premissas fiscais/jurídicas quando necessárias. Bloqueios, centavos, perda de histórico ou vazamento são falhas de aceite, independentemente da cobertura de linhas.

## 2. Casos funcionais sintéticos

Cada caso cobre o RF correspondente e seus critérios completos em [04](04-requisitos-funcionais.md); adicionar variações de limites/nulos/concorrência pertinentes. Identificadores TST-001–034 são estáveis.

| Teste / requisito | Entrada e fixture | Saída esperada / critério de aceite | Fase |
|---|---|---|---|
| TST-001 / RF-001 | Duas páginas sintéticas, três identidades e fonte HTTP simulada | Três contratações com origem/bruto; executar dez replays sem novas versões lógicas | 1 |
| TST-002 / RF-002 | Prazo encerrado, prazo futuro com offset e prazo sem fuso; filtros UF/modalidade | Somente caso verificavelmente aberto sinaliza abertura; ambíguo permanece pendente; filtros não omitem o motivo | 1 |
| TST-003 / RF-003 | Mesmo source_id/hash dez vezes; chave alternativa conflitante | Uma identidade e conteúdo; conflito em revisão, sem fusão por título | 1 |
| TST-004 / RF-004 | Versão v1 analisada, depois v2 e retorno a conteúdo v1 | Histórico preservado; nova observação invalida atualidade inclusive no retorno; participação factual não desaparece | 1 |
| TST-005 / RF-005 | Fonte retorna timeout após versão válida local | Consulta local disponível, último sucesso visível e nenhuma lista local apagada | 1 |
| TST-006 / RF-006 | Mesma URL com dois hashes; documento acima de 50 MiB | Duas versões para bytes diferentes; excesso recusado com motivo, sem sobrescrever original | 2 |
| TST-007 / RF-007 | PDF textual com cláusula na página 2; PDF somente de imagem | Primeiro produz página/trecho/hash; segundo fica pendente, sem inferir ausência de obrigação | 2 |
| TST-008 / RF-008 | Anexo define dois destinos, três parcelas e instalação | Requisitos separados com escopo, evidência e campos ausentes explícitos | 2 |
| TST-009 / RF-009 | Edital e anexo divergem no prazo | Conflito preservado e pendência; resolução exige autor, fundamento e nova interpretação | 2 |
| TST-010 / RF-010 | Quatro casos: envio convencional; parcelas remotas; presença comprovada; lacuna crítica | Classes A/B/C/D respectivamente; D prevalece sobre presença conhecida, mantendo alerta | 2 |
| TST-011 / RF-011 | Prazo null, demais dimensões logísticas iguais a 1 | L=75, cobertura=0,75, intervalo [75;100]; classe D, sem global; não renormalizar | 2 |
| TST-012 / RF-012 | Envio direto sem autorização; alternativa por transportadora fundamentada | Primeiro plano pendente; segundo é cenário independente com custos/obrigações próprios | 2 |
| TST-013 / RF-013 | Dois cadastros com mesma identificação fiscal e contatos divergentes | Identidade não duplicada silenciosamente; reconciliação e autoria da alteração | 3 |
| TST-014 / RF-014 | CSV com SKU repetido, decimal local e embalagem desconhecida | Preview de válidas/rejeitadas; replay idempotente; embalagem desconhecida não normalizada como uma unidade | 3 |
| TST-015 / RF-015 | Cotação vence antes da decisão; estoque informado de 10 para demanda 20 | Vencimento/insuficiência conhecidos bloqueiam; estoque informado não vira reservado | 3 |
| TST-016 / RF-016 | Cotações v1 e v2 em moedas/unidades distintas | Histórico preservado; comparação exige conversão fundamentada, sem substituir v1 | 3 |
| TST-017 / RF-017 | Item exige 500 W, produto fornece 400 W apesar de título igual | Conflito obrigatório identificado por atributo/unidade, sem confirmação lexical | 3 |
| TST-018 / RF-018 | Atributo crítico ausente; conflito conhecido em outro atributo | Incompatível prevalece; sem conflito e com ausência crítica retorna insuficiente | 3 |
| TST-019 / RF-019 | Par técnico confirmado em versões v1; item muda para v2 | Confirmação antiga preservada, sem validade automática para par novo; revisor e motivo obrigatórios | 3 |
| TST-020 / RF-020 | Cenário sem tributo obrigatório, com custo frete já incluso | Salvar incompleto; não duplicar frete; saídas afetadas não calculáveis | 4 |
| TST-021 / RF-021 | Quantidade 3 e unitário 0,335000; rateio 100 entre três IDs ordenados | Receita 1,01; rateio 33,34/33,33/33,33; nenhuma conversão float | 4 |
| TST-022 / RF-022 | Caixa do hardware do documento 12, garantia 500 e reserva 200 | G_base=10.400, G=10.600; garantia recuperável não vira despesa; atraso reavalia déficit | 4 |
| TST-023 / RF-023 | Lote obrigatório com receitas 12.000 e 7.000; custos 10.100 e 8.420 | Lucro 480; capital 19.020 quando picos simultâneos; não omitir item B | 4 |
| TST-024 / RF-024 | Hardware base/conservador/otimista do documento 12 | Lucros 1.900/646/2.195; capital 10.600/11.254/10.305; ausência desta extensão não bloqueia ranking básico | 4 |
| TST-025 / RF-025 | Lote acima com capital 15.000, margem mínima didática 5% | Bloqueada por capital e margem; S=NULL e ambos motivos presentes, mesmo com L=100 | 4 |
| TST-026 / RF-026 | Hardware do caso 1 de 09, F=79,1666..., L=T=C=100, E=48,2, P=3 | S=82,98; desempate usa precisão interna persistida; não gera E07 automaticamente | 4 |
| TST-027 / RF-027 | Snapshot completo com versões de regras, fontes, parâmetros e relógio | Replay idêntico; alteração de parâmetro gera versão, não reescreve resultado anterior | 4 |
| TST-028 / RF-028 | Mesmo nome em caixa de 12 e unidade; preço estimado versus contratado | Não fundir natureza/unidade; só comparar após fator e equivalência comprovados | 2-H básico; 6 ampliado |
| TST-029 / RF-029 | Fila contém viável, bloqueada e pendente com alto componente logístico | Grupos e motivos distintos; ação humana não remove trava; evidência alcançável em até duas ações | 5 |
| TST-030 / RF-030 | Matriz das 45 transições de RN-032; demais pares entre E01–E16 | Transições válidas passam guardas; todas não autorizadas são recusadas; evento sem avanço não cria transição nova | 5 |
| TST-031 / RF-031 | Vitória informada sem contrato; participação com documento/data do ato | Registrar somente fato comprovado com autoria, sem presumir contrato, entrega ou envio de proposta | 5 |
| TST-032 / RF-032 | Entrega parcial e recebimento parcial, depois saldo conciliado | Parciais são eventos; E14/E15 somente com conclusão aplicável; previsto e realizado permanecem distintos | 5 |
| TST-033 / RF-033 | Carteira com estados mistos, pendentes e denominador zero em MN-07 | Totais conciliam; denominador zero mostra não calculável; sem transformar valor estimado em receita | 5 |
| TST-034 / RF-034 | Retificação após E08 e duas revisões concorrentes | Participação preservada, reanálise aberta; versão obsoleta recusada atomicamente | 5 |

## 3. Casos transversais, segurança e falhas

| Teste | Entrada/ação controlada | Aceite | RNF / fase inicial |
|---|---|---|---|
| TST-035 | Origem não permitida, token CSRF inválido e mutação por GET | Recusa sem efeito; portas acessíveis apenas localmente no perfil padrão | RNF-001 / 1 |
| TST-036 | Sentinelas sensíveis em URL/header/exceção/exportação | Zero sentinelas em logs, imagem, resposta e exportação não autorizada | RNF-002 / 1 |
| TST-037 | Redirect para loopback/link-local, DNS rebinding, traversal, streaming sem tamanho | Zero acesso interno/proibido; limite real 50 MiB e três redirects; recusa auditada | RNF-003 / 2 |
| TST-038 | PDF ativo, malformado, cifrado, expansivo e acima de recursos | Sem rede/segredos/escrita indevida; término em 120 s/1 GiB/1.000 páginas; API continua e evidência fica pendente | RNF-004 / 2 |
| TST-039 | Retificação concorrente à publicação, FK de outra versão, worker com lease expirado | Positivo obsoleto rejeitado; nenhum vínculo cruzado; fencing recusa confirmação tardia | RNF-005, RNF-008 / 1 |
| TST-040 | Banco indisponível versus PNCP indisponível | Readiness falha em até 60 s para DB; PNCP falho mantém leitura local e indica frescor | RNF-006 / 1 |
| TST-041 | 429/Retry-After longo, timeout, 500, 400 e JSON inválido | No máximo três tentativas totais por requisição transitória; reagendar espera longa; permanente sem retry cego; dado válido preservado | RNF-007 / 1 |
| TST-042 | Crash antes/depois de commit de página e evento, dez replays | Checkpoint durável apenas após dados; estado/histórico atômicos; nenhuma duplicação lógica | RNF-008, RNF-018 / 1 |
| TST-043 | Perfil completo de 05 seção 1.1, 500 requisições após 20 de aquecimento, três rodadas | p95 busca<=2 s, painel<=3 s, simulação 100 itens/3 cenários<=5 s; registrar erros e recursos de todas rodadas | RNF-009 / 1 e 4 |
| TST-044 | NaN, infinito, seis casas excedidas, taxa+meta>=1, receita zero e frete duplicado | Rejeição ou não calculável com razão; nenhum zero favorável; meta após quantização do exemplo 12 exige unitário 118,734250 | RNF-010 / 4 |
| TST-045 | Alterar adapter/parser mantendo contratos internos | Núcleo financeiro sem HTTP e sem mudança; contratos tipados, falhas e versões documentados | RNF-011 / 1 |
| TST-046 | Configuração de teste aponta ao servidor/volume operacional | Guarda falha antes da primeira escrita; nenhuma credencial de teste tem permissão operacional | RNF-012 / 1 |
| TST-047 | Erro de job e fila crescente; sentinelas presentes no erro original | Correlação ponta a ponta e contagens conciliadas; alerta local em até 60 s; sanitização | RNF-013 / 1 |
| TST-048 | Cópia consistente restaurada em destino isolado com schedules desativados | Hashes/PK/FK e replay coincidem; RPO<=24 h e RTO<=4 h após destino/cópias disponíveis; tempos registrados | RNF-014 / 1 |
| TST-049 | Cinco tarefas: localizar, abrir evidência, corrigir matching, simular lote, decidir | Pelo menos quatro sem SQL/ajuda técnica; teclado/foco/erros textuais; críticos exigem confirmação e motivo | RNF-015 / 2 |
| TST-050 | Caso 3 de 09, presença mal definida e pós-venda null | D, L=85, cobertura 0,85, intervalo [85;100]; financeiro incompleto, S=NULL; sem mensagem de lucro confirmado | RNF-016 / 2 |
| TST-051 | Cópia legada antes/depois de expansão/backfill/reversão de imagem | 100% dos IDs/contagens/históricos baseline preservados; leitura legada íntegra | RNF-017 / 1 |
| TST-052 | 1.000 contratações/10 mil itens locais; atualização obtida e fonte indisponível | Processamento<=10 min no perfil; 95% visível<=30 min da obtenção; recorte/frescor explícitos; não prometer cobertura nacional | RNF-018 / 1 |
| TST-053 | Host Windows Docker em caminho com espaços/acentos e segundo host compatível | Sem runtime Python/Node/DB no host; scripts LF, mounts/permissões funcionais; dados sobrevivem substituição de container | RNF-019 / 1 |
| TST-054 | Parâmetros inválidos, peso zero em dimensão crítica, dois scores distintos além da quarta casa | Rejeitar versão inválida; não eliminar guarda por peso zero; ordenação idêntica antes/depois de persistir | RNF-020 / 2 e 4 |
| TST-055 | Disco próximo do limite, fila saturada, saída do parser truncada/hostil | Pausar trabalho conforme limite de 05, preservar último dado válido e pendências; não marcar sucesso nem executar saída | RNF-021 / 1 e 2 |

## 4. Execução futura em containers

Os comandos a seguir são **propostos; não executados nesta missão**. O arquivo isolado `compose.test.yml` e o serviço `tests` ainda precisam ser implementados conforme [21](21-infraestrutura-docker.md); não usar o Compose operacional como substituto. O runner futuro deve aplicar a guarda de destino antes do pytest. Logs exigem revisão de sanitização antes de compartilhamento.

```text
docker compose -f compose.test.yml config --quiet
docker compose -f compose.test.yml run --rm tests pytest tests/ -q
docker compose -f compose.test.yml logs --tail 200 tests
docker compose logs --tail 200 api worker-http-pc1
docker compose exec api python -m pip check
```

Não executar coletor, build, migration, restauração ou suíte da aplicação nesta etapa documental. Testes de fonte ao vivo serão opt-in, limitados a amostra autorizada em missão posterior, após fechamento do contrato e sem alterar dados oficiais. Falha externa deve distinguir quebra contratual de indisponibilidade, sem mascarar resultado como sucesso.

## 5. Critérios de liberação e evidências

Cada fase exige todos os Must aplicáveis passando, incluindo negativos e falhas injetadas; Should pode ser postergado com registro (RF-024/028). Anexar release/imagens, fixture/hash, relógio, parâmetros, destino isolado, comando sanitizado, resultado e duração. Rerun não substitui falha anterior sem causa/correção. Validar todas as 45 transições autorizadas e pares proibidos de RN-032, inclusive E05 sem aptidão final, parcial sem E15 e reconciliação excepcional sem aprovação retroativa.

Testes de propriedades: aumento de déficit não melhora E; bloqueio sempre impede global; replay é determinístico; soma do rateio iguala total; mudança de prazo/cotação invalida análise; retorno a conteúdo antigo conserva observação; dois scores próximos continuam ordenados após persistência. Relatório de piloto usa MN-01–10 com tamanho de amostra e desconhecido quando insuficiente. RNF-006 mede disponibilidade>=99% apenas na janela declarada, excluindo somente host desligado/manutenção prévia; RNF-021 usa limites e alertas de 05. Nenhuma meta foi medida nesta missão.

Convenção de contagem de estados: os 45 arcos de RN-032/D10 incluem a entrada inicial Início→E01; são 44 transições entre os 16 estados. Eventos sem avanço não acrescentam transições.


## 6. Fixtures e casos R1 — revisado em R1

Revisão 0.3-R1-E2, 2026-10-13. **Todos os TST abaixo são planejados, não executados.** Validação de texto/diagramas nesta revisão não é execução destes casos nem teste de aplicação. Fixtures futuras versionadas no pacote de testes do produto, com schema, hash, relógio fixo, configurações e resultado esperado independente da implementação. Não usar dados pessoais/empresas reais. Rótulos E-A, E-B, R-A e C-A são sintéticos, não CNPJs válidos. Para testes de DV, obter vetores oficiais verificados antes de declarar números/letras como válidos.

Pacotes sintéticos: `eventos-v1` (chaves A/B, resultado/ata/contrato, retificação); `identidades-v1` (estabelecimentos/raiz, formatos fiscais/erros); `comparabilidade-v1` (atributos, caixas, globais, outliers); `relacionamento-v1` (cinco dimensões, cotação/validade e silêncio); `falhas-v1` (HTTP simulado/checkpoint/quotas); `isolamento-v1` (destino falso marcado operacional, sem conexão/escrita real). São nomes de fixtures a implementar, não arquivos existentes da aplicação.

| Teste / origem | Entrada/ação sintética | Resultado esperado verificável | Fase |
|---|---|---|---|
| TST-056 / RF-003/035; RN-034; RT-017 | Mesmo resultado R-A/item/seq em PNCP e Compras com relação oficial confirmada; dez replays | Uma classe de equivalência para contagem por etapa, duas proveniências e mesmos IDs; nome igual sem chave fica apenas sugestão | 1 identidade; 2-H integração |
| TST-057 / RF-035; RN-034; RT-017 | Resultado R-A e contrato C-A com vínculo oficial; contrato com dois resultados relacionados | Etapas separadas, vínculo N:M preservado; nenhuma soma como faturamento/execução nem remoção de evento | 2-H |
| TST-058 / RF-028/035; RN-005/034; RT-009/017 | Ata registra quantidade 100, sem entrega/contrato executado | Quantidade registrada=100; entregue/receita continuam desconhecidas, sem preenchimento por inferência | 2-H |
| TST-059 / RF-003/013/037/038; RN-033; RT-004/018/019 | E-A matriz e E-B filial com raiz compartilhada; quote de E-A | Dois supplier_ids, grupo analítico comum; cotação E-A não valida E-B; conversão candidato→cadastro reutiliza E-A | 1/2-H/3 |
| TST-060 / RF-003/013/035; RN-033; RT-017/004 | Vetores oficiais futuros de DV numérico/alfanumérico; alterar DV, omitir campo, null, espaços/máscara e zero inicial | Válidos preservam letras/zeros/original; inválido/ausente/null diferenciados, UUID provisório sem ID fiscal fabricado; sem suporte da fonte o recurso continua pendente | 1/2-H/3 |
| TST-061 / RF-036; RN-005/015/035; RT-018 | Caixa com fator 10 evidenciado, caixa sem fator, CATMAT igual com especificação incompatível, código ausente com atributos suficientes | Primeiro converte com trilha; segundo pendente; terceiro incompatível; último pode ser comparável após revisão, sem exigir código | 2-H |
| TST-062 / RF-028/035/036; RN-005; RT-009/017/018 | Contrato global de vários itens sem valor discriminado, quantidade só de um item | Preserva preço total contratado; não cria unitário nem compara total a unitário | 2-H |
| TST-063 / RF-028/036; RN-035; RT-009/018 | Grupo pequeno, anomalia sinalizada; depois grupo suficiente conforme política de 22 | Primeiro só observações qualificadas e agregado não calculável; exclusão exige autor/motivo, original mantido; N/exclusões/política explícitos no suficiente | 2-H |
| TST-064 / RF-028/035; RN-005/034; RT-009/017 | Lance, homologado, registrado, contratado e cotação do mesmo item | Cinco naturezas separadas, nenhum agregado misto; cotação continua em RF-015; resultado não prova execução | 2-H/3 |
| TST-065 / RF-004/035/036; RN-028; RT-017/018 | Referência v1 selecionada, v2 retificada/cancelada, retorno ao conteúdo v1 | Preserva versões e observações; invalida comparável/cenário dependente; não apaga cotação independente nem fato comercial | 1/2-H |
| TST-066 / RF-037/038; RN-036/037; RT-018/019 | Concorrente sem interesse no escopo A, contato B sem resposta e novo ciclo com resposta | Disposição e contato independentes; silêncio não vira recusa/capacidade negativa universal; novo ciclo mantém anterior, nenhuma mensagem automática | 2-H/3 |
| TST-067 / RF-015/038; RN-006/037; RT-004/019 | Cotação expirada, outra filial, outro produto, quantidade 10 frente a 20; confirmação válida apenas no escopo correto | Nenhuma das quatro variações confirma fornecimento; versão válida só naquele escopo e sem reserva automática; expirada fica fora da métrica de vigentes | 3 |
| TST-068 / RF-005/020/021/025/026/027/037; RN-005/036; RT-006/008/018 | Casos adicionais 09 §8 e 12 §8: histórico sem custo obrigatório; mesmo caso hardware com histórico ausente | Primeiro pendente e indicadores afetados NULL; segundo mantém 82,98 e regras atuais. Alterar somente histórico/candidatos não altera componentes ou penalidades. Origem pública rejeitada como aquisição | 3/4; guardas desde 2-H |
| TST-069 / RF-004/005/035; RNF-007/008/018/021; RT-017 | HTTP simulado 429, redirect, timeout e crash entre persistência/checkpoint; orçamento de job próprio pequeno | Tentativas/redirects contam antes de envio, Retry-After respeitado; falha não apaga válido, cursor só após commit; retomada não multiplica retry; teto para e relata parcial | 1/2-H |
| TST-070 / RNF-012/019; RT-011/015 | Configuração synthetic aponta identidade/volume na denylist operacional; runner futuro inicia | Guarda termina antes de conexão de mutação/migration/limpeza; auditoria zero escrita. Não acessar banco real para realizar o teste | 1 e todos incrementos |
| TST-071 / RF-038; RNF-002/005/012/013/019/020; RT-012/015/018/019 | Mesmas fixtures/configuração/relógio em dois containers compatíveis, sentinelas pessoais/segredos em erro, casos sem resposta e N=0 | Resultados determinísticos e hashes iguais após excluir metadados de execução; logs sem sentinelas; métricas incluem censurados/insucessos e N=0 não calculável; registro manual não chama rede | 2-H/3 |

Extensões dos testes anteriores sem duplicar IDs: TST-043 aplica R1-P1/p95 de 05; TST-048/051 abrangem autoridades R1 e retorno aditivo; TST-049 inclui localizar candidato, abrir evidência e registrar cotação sem SQL; TST-054 mantém pesos/precisão e versiona políticas de comparabilidade; TST-055 cobre saturação do histórico; TST-030 continua matriz RN-032/D10 sem estados novos. Produto não precisa da amostra E1 para ter fixtures determinísticas, mas ativação de fonte precisa de validação de contrato/dados separada.

Execução futura exclusivamente em Docker/Compose isolado de 21, sem dados operacionais. Responsável técnico prepara/asserts; operador revisa resultados/denominadores e especialista valida escopo fiscal quando necessário. Todos os 16 casos acima são controles obrigatórios para recurso habilitado; Should pode ficar desligado sem bloquear o MVP. Casos herdados Must continuam obrigatórios independentemente de histórico.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: TST-056–071, fixtures sintéticas especificadas e extensões dos testes existentes; nenhum TST de produto executado.

