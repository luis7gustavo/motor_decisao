# D03 — Componentes do motor

Revisão: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Revisado em R1.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: [PROPOSTO] — sem implantação. Fonte editável; validação formal de sintaxe prevista na Execução 3.


```mermaid
flowchart TD
    S["Snapshot da unidade completa<br/>Itens, versões e evidências"] --> G["Guardas de prazo, julgamento e habilitação"]
    S --> M["Matching por atributo"]
    S --> L["Logística A/B/C/D<br/>L, cobertura e intervalo"]
    S --> F["Simulador Decimal<br/>Margem, caixa e cenários"]
    G --> C["Consolidar todos os checks"]
    M --> C
    L --> C
    F --> C
    C --> B{"Falha impeditiva conhecida?"}
    B -->|Sim| X["Bloqueada: global NULL"]
    B -->|Não| P{"Pendência obrigatória?"}
    P -->|Sim| Y["Pendente: global NULL"]
    P -->|Não| N["Priorizar viáveis<br/>F, L, T, C, E e penalidades"]
    N --> V["Verificar atualidade concorrente<br/>Publicar snapshot imutável"]
    X --> H["Revisão humana fundamentada"]
    Y --> H
    V --> H
    H --> A["Complementar, descartar ou declarar aptidão<br/>Guardas RN-032; sem envio automático"]
```

Retângulos são componentes/saídas; losangos são condições. Revisão não supera trava: nova evidência exige reanálise. Fórmula em [09](../09-motor-de-decisao.md), logística em [11](../11-logistica-e-fornecimento.md), finanças em [12](../12-simulacao-financeira.md). RF-017–027/029; ADR-006/015.


## Histórico de revisões

2026-10-13 — 0.3-R1-E2: Sem impacto no núcleo/fórmula: histórico é contexto fora dos componentes de score; TST-068 verifica invariância. Validação efetiva no relatório E2; notas de validação antigas referem-se às datas originais.

