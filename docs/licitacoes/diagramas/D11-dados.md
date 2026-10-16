# D11 — Fluxo Bronze/Silver/Gold e dados transacionais

Revisão: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Revisado em R1.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: [PROPOSTO] — sem implantação. Fonte editável; validação formal de sintaxe prevista na Execução 3.


```mermaid
flowchart LR
    PNCP["JSON oficial PNCP / Compras"] --> B[("Bronze: pncp_responses<br/>Bytes, hash e recebimento")]
    CSV["Catálogo e cotação com origem"] --> S[("sourcing: produtos e cotações versionados")]
    B --> N["Normalização e validação<br/>Papel Silver público"]
    N --> P[("procurement: versões, itens e unidades")]
    PDF["Documentos autorizados"] --> V[("Originais em volume<br/>Metadados no banco")]
    V --> X["Extração isolada e revisão"]
    X --> P
    P --> A["Matching, logística e finanças"]
    S --> A
    A --> C[("commercial: cenários e análises")]
    U["Operador e fatos comprovados"] --> H[("commercial: revisões e eventos")]
    C --> G["Gold público: projeções reconstruíveis"]
    H --> G
    G --> UI["Carteira e explicações"]
    N --> EV["Eventos por etapa e correspondencias entre fontes"]
    EV --> HP["historical_prices e comparacoes versionadas"]
    HP --> CA["Candidatos por estabelecimento e item"]
    U --> REL["Registro manual de contato e disposicao"]
    CA --> REL --> S
    HP -.->|Hipotese de venda selecionada| A
    EV --> G
    CA --> G
    LEG[("Bronze/Silver/Gold legados preservados")] -.-> S
```

Seta pontilhada=importação opcional validada, nunca conversão automática de histórico. Silver é função de normalização, sem duplicar tabelas procurement/sourcing. Gold não é única cópia das decisões: commercial é autoridade. Backup cobre banco e documentos. [08](../08-modelo-de-dados.md), ADR-014, RF-003/004/014/027/034.



## Histórico de revisões

2026-10-13 — 0.3-R1-E2: Histórico/candidatos no domínio e projeções, Bronze multirrecurso; JSON não exige volume novo. Validação efetiva no relatório E2; notas de validação antigas referem-se às datas originais.


