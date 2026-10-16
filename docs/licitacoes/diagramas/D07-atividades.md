# D07 — Atividades da descoberta à decisão de participar

Revisão: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Revisado em R1.

Versão: 0.1. Data base: 2026-10-10 (America/Sao_Paulo). Retomada: 2026-10-11.  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: [PROPOSTO] — jornada futura do Radar Licita; nenhuma atividade executada nesta missão.

## Fonte editável

```mermaid
flowchart TD
    INICIO(["Início da análise"])
    DESCOBRIR["Consultar fonte oficial e preservar origem<br/>Normalizar identidade e versões<br/>E01 — oportunidade descoberta | RF-001 a RF-005"]
    ENRIQUECER["Iniciar ou retomar enriquecimento<br/>E02 — em enriquecimento"]
    DOCUMENTOS["Obter documentos com controles de segurança<br/>Extrair texto, exigências, evidências e lacunas<br/>RF-006 a RF-009; RNF-003 e RNF-004"]
    UNIDADE["Identificar unidade de julgamento e itens obrigatórios<br/>Conferir prazo, regime e checklist de habilitação<br/>RN-001 a RN-003, RN-007, RN-022 a RN-025"]
    ORIENTACAO{"Precisa de revisão preliminar<br/>ou resolução de conflito?"}
    LOGISTICA["Avaliar modelos de fornecimento e obrigações<br/>Classificar A/B/C/D e calcular prontidão com cobertura<br/>RF-010 a RF-012; RN-010 a RN-014"]
    PRECISA_COTAR{"Cotação ou confirmação<br/>comercial necessária?"}
    COTAR["Registrar item e lacuna; operador consulta fornecedor fora do app<br/>E03 — aguardando cotação | RF-013 a RF-016"]
    RESPOSTA{"Cotação ou dado recebido?"}
    REGISTRAR_COTACAO["Registrar evidência, validade, quantidade e condições<br/>Retornar ao enriquecimento com a nova versão"]
    MATCHING["Conferir cada produto, quantidade, unidade e atributo obrigatório<br/>Registrar revisão humana e compatibilidade por versão<br/>RF-017 a RF-019; RN-015 e RN-024"]
    FINANCAS["Simular todos os itens da unidade de julgamento<br/>Custos, preço hipotético, cronograma, capital e incertezas<br/>RF-020 a RF-024; RN-004 a RN-009, RN-016 a RN-021"]
    CONTROLES["Avaliar elegibilidade e viabilidade atuais<br/>Guardar todos os bloqueios e pendências com evidência<br/>RF-025 e RF-027; RN-026 a RN-029"]
    BLOQUEIO{"Há falha impeditiva conhecida?"}
    BLOQUEADA["Resultado analítico: bloqueada<br/>Manter também pendências; fora do ranking positivo"]
    LACUNA{"Há pendência obrigatória?"}
    PENDENTE["Resultado analítico: pendente<br/>Lacuna não gera condição favorável"]
    RANKING["Resultado analítico: viável para priorização<br/>Calcular ranking explicável e preservar snapshot<br/>RF-026 e RF-027; RN-029"]
    REVISAO["Operador confere evidências, versões e consequências<br/>E04 — aguardando revisão | RF-029"]
    ESCOLHA{"Ação humana fundamentada"}
    ESTUDO["E05 — pré-aprovada para estudo<br/>Autorizar aprofundamento; lacunas continuam visíveis"]
    DESCARTE["E06 — descartada<br/>Registrar motivo e histórico; sem participação prévia"]
    APTA["E07 — apta para decisão humana de participação<br/>Somente revisão final favorável e guardas atendidas"]
    ATUAL["Conferir novamente prazo, cotação, dados e demais guardas<br/>Imediatamente antes da decisão de participar<br/>RN-001, RN-003, RN-006, RN-028; RNF-018"]
    MUDOU{"Análise mudou, venceu<br/>ou deixou de atender guardas?"}
    DECISAO{"Operador decide participar?"}
    DECIDIDA(["Decisão humana favorável registrada<br/>Ato externo e comprovante seguem D10: E07 para E08"])
    ADIAR["Adiar decisão enquanto as condições forem atuais<br/>Manter E07; conferir novamente antes de decidir"]

    INICIO --> DESCOBRIR --> ENRIQUECER --> DOCUMENTOS --> UNIDADE --> ORIENTACAO
    ORIENTACAO -->|"Sim"| REVISAO
    ORIENTACAO -->|"Não"| LOGISTICA --> PRECISA_COTAR
    PRECISA_COTAR -->|"Sim"| COTAR --> RESPOSTA
    RESPOSTA -->|"Sim"| REGISTRAR_COTACAO --> ENRIQUECER
    RESPOSTA -->|"Sem resposta ou condição a revisar"| REVISAO
    PRECISA_COTAR -->|"Não; condições disponíveis"| MATCHING --> FINANCAS --> CONTROLES --> BLOQUEIO
    BLOQUEIO -->|"Sim"| BLOQUEADA --> REVISAO
    BLOQUEIO -->|"Não"| LACUNA
    LACUNA -->|"Sim"| PENDENTE --> REVISAO
    LACUNA -->|"Não"| RANKING --> REVISAO
    REVISAO --> ESCOLHA
    ESCOLHA -->|"Pedir complemento com lacuna e responsável"| ENRIQUECER
    ESCOLHA -->|"Pré-aprovar somente o estudo"| ESTUDO
    ESTUDO -->|"Complementar estudo, matching ou simulação"| ENRIQUECER
    ESTUDO -->|"Estudo pronto ou conflito a resolver"| REVISAO
    ESCOLHA -->|"Descartar com motivo"| DESCARTE
    ESCOLHA -->|"Revisão final; todos os controles atuais atendidos"| APTA
    DESCARTE -->|"Reabertura justificada e nova evidência"| ENRIQUECER
    APTA --> ATUAL --> MUDOU
    MUDOU -->|"Sim; invalidar atualidade e preservar histórico"| REVISAO
    MUDOU -->|"Não"| DECISAO
    DECISAO -->|"Sim; decisão auditada"| DECIDIDA
    DECISAO -->|"Não prosseguir; motivo"| DESCARTE
    DECISAO -->|"Adiar"| ADIAR --> ATUAL

    COTAR -.->|Pesquisa opcional| HISTORIA["Comparáveis, identidade e candidatos<br/>Contato manual e cotação por escopo: D15"]
    HISTORIA --> COTAR

    classDef humana fill:#fff1d6,stroke:#8a5a00,color:#241b0b;
    classDef impedida fill:#fff1f2,stroke:#be123c,color:#4c0519;
    classDef apta fill:#dcfce7,stroke:#15803d,color:#052e16;
    class REVISAO,ESCOLHA,ESTUDO,DECISAO,ADIAR humana;
    class BLOQUEADA,PENDENTE,DESCARTE impedida;
    class RANKING,APTA,DECIDIDA apta;
```

## Leitura e limites

O fluxo de atividades usa a notação Mermaid `flowchart`: retângulos são atividades, losangos são condições/decisões e nós arredondados delimitam início e resultado da jornada. Amarelo destaca intervenção humana; vermelho identifica resultado impedido ou descarte; verde indica priorização/decisão condicionada às guardas. Os textos e formas preservam o significado sem depender de cor. Os códigos E01–E08 referenciam os estados de [RN-032](../06-regras-de-negocio.md#rn-032--máquina-de-estados-comerciais) e [D10](D10-estados.md); atividades internas não criam estados comerciais adicionais.

O percurso cobre descoberta, documentos, julgamento, logística, fornecedores, matching, finanças, bloqueios, ranking e decisão humana. Revisão preliminar permite orientar o estudo, sem liberar participação. Todos os ramos que chegam à revisão compartilham a mesma guarda para E04 → E07: resultado atual sem bloqueio nem pendência obrigatória, verificações suficientes e revisão final favorável. Chegar à tela de revisão por um ramo incompleto não satisfaz essa guarda. Pré-aprovação E05 pode conduzir a complementação ou voltar a E04; não substitui a análise completa.

O sistema registra a necessidade de cotação e os dados recebidos. O operador consulta fornecedor/assessor fora do aplicativo; nenhuma seta representa envio automático de mensagem. A revisão especializada, quando necessária, é registrada com responsável, escopo, fonte e data. Recusa/erro de download, extração incompleta, fonte indisponível, prazo ambíguo e atributo técnico ausente permanecem como pendências; falha impeditiva conhecida prevalece no resultado analítico, sem esconder outras lacunas.

Todos os itens obrigatórios precisam ser avaliados. Classe A ou score elevado não dispensam verificação de edital, produto, cotação, capital e habilitação. Classe C admite alternativa local comprovada; classe D conserva a falta crítica. A simulação distingue referência oficial de preço hipotético e calcula o caixa da unidade completa, sem converter ausência em custo zero. Sensibilidade ampliada (RF-024, Should) é opcional; os controles obrigatórios e o cenário de atraso continuam exigidos pelas regras de capital.

Ao decidir participar, o operador ainda precisa realizar o ato fora do Radar. O estado permanece E07 até existir comprovação suficiente para registrar E08; a decisão por si só não cria participação. Se a informação mudar ou vencer antes desse ato, a análise volta à revisão. Participação realizada apesar de bloqueio ou fora da sequência prevista exige a reconciliação excepcional auditada de RN-032/D10, sem aprovar retrospectivamente a análise. O acompanhamento posterior à participação está em D10.

Para manter a leitura, o fluxo mostra o descarte na revisão e na decisão final; D10 também explicita os descartes manuais autorizados a partir de E01, E02, E03 e E05. A ausência dessas setas transversais aqui não restringe nem amplia as transições normativas de RN-032.

**Rastreabilidade:** [01 — visão](../01-visao-produto.md), [03 — DAN](../03-analise-negocio-dan.md), [RF-001 a RF-027 e RF-029 a RF-034](../04-requisitos-funcionais.md), [RN-001 a RN-032](../06-regras-de-negocio.md), [RNF-003 a RNF-005, RNF-008, RNF-015, RNF-016, RNF-018 e RNF-020](../05-requisitos-nao-funcionais.md). Histórico básico RF-028/035–037 é incremento opcional 2-H (ampliação fase 6), contato RF-038 junto de 3 e não é dependência da decisão no MVP.

**Validação na retomada de 2026-10-11:** revisão textual de nomes, guardas, ramos e referências frente aos documentos 01, 03, 04, 05 e 06. Não foi localizado `mmdc` no PATH; não houve renderização nem validação por parser Mermaid. A validação automatizada de sintaxe e a conferência visual ficam pendentes para a consolidação da Execução 3, com fonte editável preservada.


## Histórico de revisões

2026-10-13 — 0.3-R1-E2: Pesquisa de candidatos é ramo opcional de cotação, D15, sem dispensar insumos. Validação efetiva no relatório E2; notas de validação antigas referem-se às datas originais.

