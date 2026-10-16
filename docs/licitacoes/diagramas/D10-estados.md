# D10 — Estados comerciais da oportunidade

Versão: 0.1. Data base: 2026-10-10 (America/Sao_Paulo). Retomada: 2026-10-11.  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: [PROPOSTO] — ciclo comercial futuro; não implementado nesta missão.

## Fonte normativa e legenda

[RN-032](../06-regras-de-negocio.md#rn-032--máquina-de-estados-comerciais) é a fonte normativa dos **16 estados E01–E16** e das guardas. As três vistas Mermaid abaixo representam partes da mesma máquina, com os mesmos identificadores. Uma repetição de E07 ou E16 em outra vista identifica o mesmo estado, sem duplicá-lo nem criar submáquinas.

Retângulo arredondado = estado comercial; seta = transição permitida mediante evento e guarda; círculo inicial = entrada em E01. A legenda de cada seta resume a guarda, cujo detalhe permanece em RN-032. Os nomes são fixados; em particular, **vencida** significa resultado comercial vencedor. Expiração do prazo é condição de elegibilidade, nunca transição para E11. E16 pode ser reaberto nas condições expressas; por isso não há pseudestado final que elimine essa possibilidade.

Situação oficial, elegibilidade, classe logística, score, pendências e estado comercial são dimensões distintas. Nenhum score dispara participação, vitória, contrato, entrega ou recebimento.

## Vista 1 — Descoberta, estudo e aptidão

```mermaid
stateDiagram-v2
    direction TB
    state "E01 — oportunidade descoberta" as E01
    state "E02 — em enriquecimento" as E02
    state "E03 — aguardando cotação" as E03
    state "E04 — aguardando revisão" as E04
    state "E05 — pré-aprovada para estudo" as E05
    state "E06 — descartada" as E06
    state "E07 — apta para decisão humana de participação" as E07

    [*] --> E01 : identidade suficiente e origem registrada
    E01 --> E02 : iniciar enriquecimento
    E02 --> E03 : solicitar cotação ou confirmação com lacuna definida
    E02 --> E04 : submeter dados ou conflito à revisão
    E03 --> E02 : receber cotação ou revisar escopo
    E03 --> E04 : revisar falta de resposta ou condição comercial
    E04 --> E02 : pedir complemento, corrigir premissa ou reabrir estudo
    E04 --> E05 : revisão preliminar favorável ao estudo
    E05 --> E02 : complementar estudo, matching ou simulação
    E05 --> E04 : estudo pronto ou conflito para revisão
    E04 --> E07 : revisão final favorável e todas as guardas atuais atendidas
    E01 --> E06 : descarte manual com motivo, sem participação
    E02 --> E06 : descarte manual com motivo, sem participação
    E03 --> E06 : descarte manual com motivo, sem participação
    E04 --> E06 : descarte manual com motivo, sem participação
    E05 --> E06 : descarte manual com motivo, sem participação
    E07 --> E06 : descarte manual com motivo, sem participação
    E06 --> E02 : reabrir com justificativa e nova evidência
    E07 --> E04 : mudança, expiração ou desfazimento exige revalidação

    note right of E05
        Autoriza aprofundar o estudo.
        Lacunas continuam visíveis.
        Não libera participação.
    end note
    note right of E07
        Guarda inclui prazo, dados, produto,
        logística, habilitação e capital.
        Decisão humana não envia proposta.
        E07 para E08 está na vista 2.
    end note
```

Reabrir E06 não restaura aprovação anterior: o estudo retorna a E02. O bloqueio analítico também não descarta automaticamente a oportunidade; a transição para E06 exige ação manual com motivo. Pré-aprovação, descarte e aptidão guardam autor, instante, versão, evidências e decisão anterior conforme RN-030.

## Vista 2 — Participação, resultado e execução comprovados

```mermaid
stateDiagram-v2
    direction TB
    state "E07 — apta para decisão humana de participação" as E07
    state "E08 — participação registrada manualmente" as E08
    state "E09 — em disputa" as E09
    state "E10 — resultado conhecido" as E10
    state "E11 — vencida" as E11
    state "E12 — perdida" as E12
    state "E13 — contrato em execução" as E13
    state "E14 — entrega concluída" as E14
    state "E15 — recebimento financeiro" as E15

    E07 --> E08 : ato externo comprovado e aptidão reavaliada na decisão
    E08 --> E09 : registrar início de disputa externa
    E08 --> E10 : resultado sem etapa de disputa aplicável
    E09 --> E10 : resultado oficial conhecido e evidenciado
    E10 --> E11 : resultado vencedor fundamentado, provisório ou definitivo
    E10 --> E12 : resultado não vencedor fundamentado, provisório ou definitivo
    E10 --> E09 : reabertura oficial da disputa comprovada
    E11 --> E10 : resultado retificado, anulado ou reaberto oficialmente
    E12 --> E10 : resultado retificado, anulado ou reaberto oficialmente
    E11 --> E13 : fundamento contratual e início de execução comprovados
    E13 --> E14 : todas as entregas e condições de aceite atendidas
    E14 --> E15 : total devido recebido e conciliado

    note right of E08
        A ferramenta registra atos externos.
        Não envia lance ou proposta.
        Retificação posterior abre revisão
        da análise sem apagar participação.
    end note
    note right of E13
        Vitória não cria contrato.
        Eventos parciais mantêm saldos.
        Entrega física não equivale a aceite.
        Faturamento não equivale a recebimento.
    end note
    note right of E15
        Recebimento integral conciliado.
        Encerramento está na vista 3.
    end note
```

E10 → E11/E12 registra a interpretação do resultado com fundamento e sua condição provisória/definitiva; não presume adjudicação nem contratação. E11 → E13 exige fundamento contratual aplicável e início comprovado. As transições registram fatos; nenhum evento representa compra, assinatura, envio de proposta, pagamento ou comunicação automática pelo Radar (RN-031).

Entregas parciais, recebimentos parciais, adiantamentos comprovados, recusa de entrega, saldo contestado, atraso e revisão posterior à participação são eventos/pendências, sem avanço automático. Entrega parcial mantém E13; recebimento parcial mantém o estado factual e o saldo. Se o total já tiver sido efetivamente recebido antes da entrega completa, conservar a evidência em E13; após o aceite final, E13 → E14 e E14 → E15 podem ser registrados na mesma operação auditada, com dois eventos. O segundo usa o recebimento já comprovado, sem criar pagamento fictício.

Retificação depois de E08 conserva o estágio factual e abre revalidação da análise em paralelo. Um bloqueio novo impede nova recomendação favorável; não força retorno comercial a E04 nem apaga o ato externo. Mudança comprovada do próprio resultado segue E11/E12 → E10; correção de um registro errado exige invalidação/substituição auditada do evento, sem exclusão de histórico.

## Vista 3 — Encerramento e reabertura

```mermaid
stateDiagram-v2
    direction TB
    state "E04 — aguardando revisão" as E04
    state "E06 — descartada" as E06
    state "E08 — participação registrada manualmente" as E08
    state "E09 — em disputa" as E09
    state "E10 — resultado conhecido" as E10
    state "E11 — vencida" as E11
    state "E12 — perdida" as E12
    state "E13 — contrato em execução" as E13
    state "E14 — entrega concluída" as E14
    state "E15 — recebimento financeiro" as E15
    state "E16 — encerrada" as E16

    E06 --> E16 : fechar estudo descartado com motivo e pendências residuais
    E12 --> E16 : fechar oportunidade perdida com motivo e pendências residuais
    E15 --> E16 : fechar normalmente com obrigações residuais tratadas
    E08 --> E16 : encerramento excepcional com evidência, saldos e plano
    E09 --> E16 : encerramento excepcional com evidência, saldos e plano
    E10 --> E16 : encerramento excepcional com evidência, saldos e plano
    E11 --> E16 : encerramento excepcional com evidência, saldos e plano
    E13 --> E16 : encerramento excepcional com evidência, saldos e plano
    E14 --> E16 : encerramento excepcional com evidência, saldos e plano
    E16 --> E04 : reabrir estudo com justificativa, se nunca houve participação
    E16 --> E10 : reabrir excepcionalmente no resultado factual comprovado
    E16 --> E13 : reabrir excepcionalmente na execução factual comprovada
    E16 --> E14 : reabrir excepcionalmente na entrega factual comprovada
    E16 --> E15 : reabrir excepcionalmente no recebimento factual comprovado

    note right of E16
        Encerramento não apaga saldo ou obrigação.
        Cancelamento, rescisão ou outro fato
        comprovado exige motivo, evidência,
        saldos, plano e responsável pelas pendências.
        Garantias residuais mantêm acompanhamento.
    end note
```

Encerramento normal E15 → E16 exige obrigações residuais e garantias resolvidas ou acompanhamento formal registrado. Encerramento excepcional distingue cancelamento, rescisão ou outro fato comprovado e explicita saldos e responsável/plano para pendências; não transforma perdas, dívidas ou obrigações em zero. Reabertura exige nova evidência e o destino factual permitido; uma oportunidade com participação prévia não pode usar E16 → E04 para reconstituir um estudo como se nenhum ato tivesse ocorrido.

## Exceções controladas e integridade

As setas representam as transições comerciais comuns e os encerramentos/reaberturas expressamente previstos. A **reconciliação excepcional** de RN-032 é um caminho de registro distinto: admite fato externo comprovado ocorrido sem uso prévio da ferramenta, inclusive participação feita apesar de bloqueio. Ela não é desenhada como seta irrestrita de qualquer estado para qualquer destino.

| Condição da reconciliação | Registro obrigatório e limite |
|---|---|
| Fato externo ocorrido fora da sequência | Estado de origem, destino factual E08–E15, autor, data real, data de registro, motivo, evidência e sinalização de desvio |
| Participação feita apesar de bloqueio | Preservar bloqueio e histórico; não inventar passagem por E07 nem aprovação retrospectiva |
| Correção de fato registrado incorretamente | Invalidar/substituir evento com justificativa e vínculo ao original; não excluir o histórico |
| Salto sem evidência ou fora das condições | Rejeitar sem alterar estado nem criar fato comercial |

Toda alteração de estado e seu histórico formam uma operação atômica; submissão duplicada não cria dois eventos ativos conflitantes e versão obsoleta exige resolução explícita (RN-030, RNF-005 e RNF-008). A reconciliação existe para registrar realidade comprovada, sem liberar controles de uma nova recomendação.

## Rastreabilidade e validação documental

**Rastreabilidade:** [01 — visão](../01-visao-produto.md), [03 — DAN](../03-analise-negocio-dan.md), [RF-029 a RF-034 e ciclo comercial de 04](../04-requisitos-funcionais.md), [RN-001, RN-003, RN-006, RN-024 e RN-028 a RN-032](../06-regras-de-negocio.md), [RNF-005, RNF-008, RNF-015, RNF-016 e RNF-018](../05-requisitos-nao-funcionais.md). [D07](D07-atividades.md) detalha as atividades que antecedem a decisão de participar.

Verificação documental na retomada de 2026-10-11: comparação textual automatizada dos nomes E01–E16 e dos pares de transição das três vistas contra a tabela de RN-032 encontrou **16 estados com nomes idênticos e 45 transições, incluindo a entrada inicial, sem pares ausentes ou excedentes**. Guardas foram revistas separadamente, incluindo retorno de resultado, pagamento antes da entrega, preservação de fatos após retificação e reconciliação excepcional. Exemplos de coerência: E02 → E13 sem prova é rejeitado; entrega parcial não alcança E14; recebimento parcial não alcança E15; prazo expirado não produz E11. São verificações da especificação, não testes de aplicação.

Não foi localizado `mmdc` no PATH. Não houve renderização nem validação por parser Mermaid; a validação automatizada de sintaxe e a revisão visual permanecem para a consolidação da Execução 3. As três fontes são editáveis e usam `stateDiagram-v2` para visualização em ambiente com suporte a Mermaid.
