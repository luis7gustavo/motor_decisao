# D09 — Sequência da análise de oportunidade

Revisão: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Revisado em R1.

Versão: 0.1. Data: 2026-10-12 (America/Sao_Paulo).  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: [PROPOSTO]; fonte editável, sem implementação.

```mermaid
sequenceDiagram
    actor O as Operador
    participant A as Serviço de aplicação
    participant DB as PostgreSQL de domínio
    participant M as Matching
    participant L as Logística
    participant F as Finanças Decimal
    participant R as Motor de decisão
    O->>A: Solicitar análise da unidade de julgamento
    A->>DB: Ler versões de itens, documentos e evidências
    DB-->>A: Manifesto, requisitos e lock_version
    A->>DB: Ler produtos, cotações e parâmetros versionados
    DB-->>A: Insumos com origem e validade
    A->>M: Comparar item e produto por atributos
    M-->>A: Compatibilidade, conflitos e lacunas
    A->>L: Avaliar obrigações e plano de fornecimento
    L-->>A: A/B/C/D, L, cobertura e evidências
    A->>F: Simular todos os itens, custos e caixa
    F-->>A: Cenários ou saídas não calculáveis com motivo
    A->>R: Snapshot coerente, checks e instante
    R-->>A: Elegibilidade e componentes, global condicionado
    A->>DB: Iniciar transação e comparar versões e guardas atuais
    alt Versão obsoleta ou guarda temporal mudou
        DB-->>A: Recusar publicação positiva
        A-->>O: Reanálise necessária, histórico preservado
    else Snapshot ainda atual
        A->>DB: Persistir cenário, análise, score e manifesto atomicamente
        DB-->>A: Commit confirmado
        A-->>O: Exibir resultado, evidências e pendências
        O->>A: Registrar revisão com motivo e versão esperada
        A->>DB: Revalidar guardas e gravar revisão e evento atomicamente
        DB-->>A: Confirmação ou conflito de versão
        A-->>O: Resultado da revisão
    end
    Note over O,R: Bloqueio ou pendência implica global NULL. E07 exige revisão e RN-032.
    Note over O,DB: Não há envio de proposta nem contato automático com fornecedor.
```

Participantes representam módulos de 07, não novos microsserviços. Documentos e cotações entram por versões já obtidas; análise não faz download oculto. Fonte: [08](../08-modelo-de-dados.md), [09](../09-motor-de-decisao.md), RF-017–027/029/034, RN-028–032.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: Sem impacto na sequência obrigatória: referência de venda selecionada entra no manifesto; ausência de histórico não bloqueia insumos suficientes. Validação efetiva no relatório E2; notas de validação antigas referem-se às datas originais.

