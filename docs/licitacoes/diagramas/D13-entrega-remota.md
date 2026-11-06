# D13 — Decisão de entrega remota

Versão: 0.1. Data: 2026-10-12 (America/Sao_Paulo).  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: [PROPOSTO]; fonte editável, sem implementação.

```mermaid
flowchart TD
    I["Unidade completa: itens, requisitos e evidências"] --> Q{"Lacuna ou conflito crítico<br/>de obrigação, destino, prazo ou plano?"}
    Q -->|Sim| D["Classe D<br/>Guardar também presença já conhecida"]
    D --> P["Pedir evidência e revisão<br/>Sem ranking positivo"]
    Q -->|Não| C{"Presença local relevante comprovada?"}
    C -->|Sim| CL["Classe C<br/>Validar prestador, permissão, prazo e custo"]
    C -->|Não| B{"Restrição remota relevante conhecida?"}
    B -->|Sim| BL["Classe B<br/>Validar parcelas, agendamento e compromissos"]
    B -->|Não| AL["Classe A<br/>Envio convencional suficientemente demonstrado"]
    CL --> V["Verificar modelo escolhido e todas as obrigações"]
    BL --> V
    AL --> V
    V --> E{"Envio direto selecionado?"}
    E -->|Sim| ED["Exigir compromisso do fornecedor<br/>e validação contratual e fiscal aplicável"]
    E -->|Não| T["Custear operador e transportadora<br/>Não aplicável exige fundamento"]
    ED --> G["Consolidar bloqueios e pendências<br/>com produto, habilitação, prazo e capital"]
    T --> G
    G --> R["Revisão humana<br/>Classe não equivale a autorização"]
    I -.-> S["L e cobertura são indicadores separados<br/>Não renormalizar desconhecidos"]
```

Losangos são condições; a precedência classificatória é D→C→B→A, conforme RN-011 e [11](../11-logistica-e-fornecimento.md). Falha conhecida de execução pode bloquear qualquer classe; presença atendida não torna C automaticamente inviável. L é o limite inferior de v0.1-logistica, não determina classe. RF-010–012/025; TST-010–012/025/050.
