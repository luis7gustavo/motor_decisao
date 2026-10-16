# D12 — Implantação Docker local

Revisão: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Revisado em R1.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: [PROPOSTO] — sem implantação. Fonte editável; validação formal de sintaxe prevista na Execução 3.


```mermaid
flowchart TB
    U["Navegador do operador"] -->|Loopback host:8010| API
    U -->|Administração loopback:4200| PF
    subgraph HOST["Host com Docker e Compose"]
        subgraph NET["Redes propostas de aplicação e controle"]
            API["api: FastAPI + Jinja2"] -->|5432 interno| DB["postgres existente"]
            W["worker-http-pc1: fontes oficiais e coordenador"] --> DB
            W -->|4200 interno| PF["prefect-server / prefect-services"]
            PF --> PDB["prefect-postgres existente"]
            PF -->|6379 interno| R["redis existente"]
            LEG["Workers comerciais opcionais"] --> DB
            LEG --> PF
            LEG --> SEL["selenium opcional"]
        end
        PAR["document-parser novo<br/>Sem rede, não root, sem segredos"]
        DB --> VD[("motor_postgres_data preservado")]
        PDB --> VP[("prefect_postgres_data preservado")]
        R --> VR[("motor_redis_data preservado")]
        W --> DOC[("radar_documents novo")]
        DOC -->|Leitura de liberados| API
        W --> IN[("radar_parser_input novo")]
        IN -->|Somente leitura| PAR
        PAR --> OUT[("radar_parser_output novo")]
        OUT --> W
    end
    W -->|HTTPS e política de saída| PNCP["PNCP / Compras / origens aprovadas"]
    BK["Cópia independente protegida"] -.-> VD
    BK -.-> VP
    BK -.-> DOC
```

Desenho futuro, sem redes/portas alteradas. Detalhes por serviço em [21](../21-infraestrutura-docker.md). Jinja2 no container API; sem frontend separado. Setas contínuas=acesso/fluxo; tracejadas=cobertura de backup/restauração. Redis requer reconciliação de jobs. Parser sem socket Docker. Migração/testes são tarefas efêmeras em 21, com destinos explícitos; testes nunca montam volumes operacionais. ADR-003/004/012/013; RNF-003/004/014/017/019.



## Histórico de revisões

2026-10-13 — 0.3-R1-E2: Sem nova topologia/porta/volume para histórico; rótulo de origem agora inclui Compras, detalhes em 21 §12. Validação efetiva no relatório E2; notas de validação antigas referem-se às datas originais.

