# D08 — Sequência de coleta PNCP

Revisão: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Revisado em R1.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: [PROPOSTO] — sem implantação. Fonte editável; validação formal de sintaxe prevista na Execução 3.


```mermaid
sequenceDiagram
    participant P as Prefect
    participant W as Worker HTTP
    participant A as API publica por recurso validado
    participant D as PostgreSQL dominio
    P->>W: Executar escopo e janela fixos
    W->>D: Obter lease e fencing token
    D-->>W: Checkpoint duravel ou pagina 1
    loop Paginas da janela
        W->>A: Reservar tentativa na quota e GET validado
        alt Falha transitoria
            A-->>W: Erro ou timeout
            W->>W: Ate 3 tentativas dentro da quota do job e Retry-After
            W->>D: Falha parcial sem avancar checkpoint
        else Resposta recebida
            A-->>W: Envelope e dados
            W->>W: Validar pagina, schema, identidade e etapa
            alt Inconsistencia
                W->>D: Preservar bruto e registrar quarentena
            else Valida
                W->>D: BEGIN e validar token
                W->>D: Preservar bruto e deduplicar
                opt Conteudo ou atualidade mudou
                    W->>D: Versao, observacao e invalidacoes
                end
                W->>D: Checkpoint e COMMIT
            end
        end
    end
    alt Janela coerente e persistida
        W->>D: Concluir janela
        W-->>P: Sucesso com escopo e contagens
    else Falha ou instabilidade
        W-->>P: Parcial, retomar checkpoint confirmado
    end
```

[10](../10-integracoes-pncp.md), RF-001–005, RNF-007/008/018. Falha esgotada interrompe a janela e sai do loop; não repetir indefinidamente. Retomada repete unidade não confirmada, com UNIQUE/fencing contra duplicação. Itens/documentos dependem de contrato ainda pendente; ausência em lista não exclui registros. Setas contínuas=ações, tracejadas=respostas; alt=alternativas, opt=condição. Nenhuma coleta executada.



## Histórico de revisões

2026-10-13 — 0.3-R1-E2: Mesma sequência por recurso validado, quota total e identidade/etapa; E2 não executa coleta. Validação efetiva no relatório E2; notas de validação antigas referem-se às datas originais.

