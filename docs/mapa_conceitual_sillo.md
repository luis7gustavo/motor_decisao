# Mapa conceitual do SILLO

Mapa integrando o motor do SILLO, a arquitetura Medalhão, fontes, orquestração, fornecedores, finanças, ranking e revisão humana.

Os marcadores ajudam a separar o que está documentado como atual do que pertence à arquitetura TO-BE proposta.

```mermaid
flowchart TD
    SILLO(("SILLO<br/>Motor de inteligência de compras<br/>para revenda"))

    SILLO --> OBJ["Objetivo<br/>Transformar dados e evidências em oportunidades<br/>priorizadas para decisão humana"]

    subgraph FONTES["1. Fontes e evidências"]
        PNCP["PNCP / Compras oficiais<br/>[TO-BE]"]
        MARKET["Marketplaces e varejo<br/>Mercado Livre, Amazon, Kabum,<br/>Shopee, Pichau, Magalu"]
        HISTPRICE["Histórico de preços<br/>Zoom, Buscapé e comparadores"]
        CATALOG["Catálogos de fornecedores<br/>MegaMix, Mirão e outros"]
        QUOTE["Cotações e contatos<br/>registrados manualmente"]
        DOCS["Documentos autorizados<br/>editais, atas, anexos e evidências"]
    end

    FONTES --> BRONZE

    subgraph MEDALHAO["2. Arquitetura Medalhão"]
        CONTROL["Control<br/>execuções, status, checks,<br/>quotas, ledger, freshness e auditoria"]
        BRONZE[("Bronze<br/>payload bruto, bytes, hash,<br/>origem e recebimento")]
        SILVER["Silver<br/>normalização, validação,<br/>chaves, atributos e comparabilidade"]
        GOLD[("Gold<br/>projeções, recomendações,<br/>snapshots e carteira consumível")]

        CONTROL -.-> BRONZE
        BRONZE --> SILVER
        SILVER --> GOLD
    end

    subgraph DOMINIO["3. Domínio operacional TO-BE"]
        PROCUREMENT["procurement<br/>fontes públicas, versões,<br/>contratações, itens e unidades"]
        SOURCING["sourcing<br/>produtos, fornecedores,<br/>cotações e condições"]
        COMMERCIAL["commercial<br/>cenários, análises,<br/>revisões e eventos humanos"]
        HISTORY["historical_prices<br/>preços históricos e comparações<br/>versionadas"]
        CANDIDATES["candidatos por estabelecimento<br/>e item comparável"]
    end

    SILVER --> PROCUREMENT
    SILVER --> SOURCING
    SILVER --> HISTORY
    PROCUREMENT --> COMMERCIAL
    SOURCING --> COMMERCIAL
    HISTORY --> CANDIDATES
    CANDIDATES --> SOURCING
    COMMERCIAL --> GOLD

    subgraph EXECUCAO["4. Orquestração e infraestrutura"]
        API["API FastAPI + Jinja2<br/>consulta, revisão e domínio"]
        WORKER["Worker HTTP + Prefect<br/>ingestão, coordenação e resultados"]
        PREFECT["Prefect Server + Services"]
        REDIS["Redis<br/>controle do Prefect"]
        POSTGRES[("PostgreSQL<br/>domínio, dados e histórico")]
        SPOOL["Spool de entrada e saída"]
        PARSER["Parser documental isolado<br/>sem rede, banco ou segredos"]
        BROWSER["Workers comerciais opcionais<br/>Crawlee, Selenium ou browser"]
    end

    API <--> POSTGRES
    WORKER --> POSTGRES
    WORKER --> PREFECT
    PREFECT --> REDIS
    WORKER --> PNCP
    WORKER --> DOCS
    DOCS --> SPOOL
    SPOOL --> PARSER
    PARSER --> SPOOL
    SPOOL --> WORKER
    BROWSER --> POSTGRES
    BROWSER --> PREFECT
    BROWSER --> MARKET

    subgraph MOTOR["5. Motor de decisão"]
        SNAPSHOT["Snapshot da unidade completa<br/>itens, quantidades, versões e evidências"]
        GUARDAS["Guardas<br/>prazo, julgamento, habilitação,<br/>documentação e consistência"]
        MATCH["Matching técnico<br/>marca, modelo, atributos,<br/>unidade, quantidade e compatibilidade"]
        LOGISTICA["Logística A/B/C/D<br/>cobertura, entrega,<br/>origem e incerteza"]
        FINANCEIRO["Simulador financeiro Decimal<br/>aquisição, frete, taxas, impostos,<br/>capital, caixa, margem e ROI"]
        CONSOLIDA["Consolidar verificações"]
        IMPEDITIVA{"Falha impeditiva<br/>conhecida?"}
        PENDENCIA{"Pendência obrigatória?"}
        RANKING["Ranking de viáveis<br/>F: financeiro<br/>L: logístico<br/>T: técnico<br/>C: confiabilidade<br/>E: exposição"]
        PUBLICAR["Publicar snapshot imutável<br/>com versão, evidências e justificativas"]
    end

    PROCUREMENT --> SNAPSHOT
    SOURCING --> SNAPSHOT
    COMMERCIAL --> SNAPSHOT
    SNAPSHOT --> GUARDAS
    SNAPSHOT --> MATCH
    SNAPSHOT --> LOGISTICA
    SNAPSHOT --> FINANCEIRO
    GUARDAS --> CONSOLIDA
    MATCH --> CONSOLIDA
    LOGISTICA --> CONSOLIDA
    FINANCEIRO --> CONSOLIDA
    CONSOLIDA --> IMPEDITIVA
    IMPEDITIVA -->|Sim| BLOQUEADA["Bloqueada<br/>resultado global NULL"]
    IMPEDITIVA -->|Não| PENDENCIA
    PENDENCIA -->|Sim| PENDENTE["Pendente<br/>resultado global NULL"]
    PENDENCIA -->|Não| RANKING
    RANKING --> PUBLICAR
    PUBLICAR --> GOLD

    subgraph FORNECEDORES["6. Inteligência de fornecedores"]
        PUBLIC_HISTORY["Histórico público<br/>homologação, ata, preço e fornecedor"]
        COMPARABILITY["Revisar comparabilidade<br/>atributos, unidade, quantidade,<br/>datas, etapa e contexto"]
        CANDIDATE["Candidato explicável<br/>por estabelecimento e item"]
        CONTACT["Contato externo manual"]
        DISPOSITION["Disposição atual<br/>desconhecida, aceita sob condições<br/>ou recusada"]
        CURRENT_QUOTE["Cotação atual<br/>produto, quantidade, condições e validade"]
        CURRENT_CHECK["Verificação atual<br/>estoque, prazo, logística, capital,<br/>preço e disponibilidade"]
    end

    PUBLIC_HISTORY --> COMPARABILITY
    COMPARABILITY --> CANDIDATE
    CANDIDATE --> CONTACT
    CONTACT --> DISPOSITION
    DISPOSITION --> CURRENT_QUOTE
    CURRENT_QUOTE --> CURRENT_CHECK
    CURRENT_CHECK --> SOURCING
    CURRENT_CHECK --> SNAPSHOT
    HISTPRICE -.-> PUBLIC_HISTORY
    PUBLIC_HISTORY -.->|"Contexto, nunca custo automático"| HISTORY
    PUBLIC_HISTORY -.->|"Não prova capacidade,<br/>estoque, vontade ou cotação atual"| CURRENT_CHECK

    subgraph GOVERNANCA["7. Guardrails e governança"]
        EVIDENCE["Rastreabilidade<br/>origem, versão, hash,<br/>data, etapa e evidência"]
        NULLS["Ausência de evidência<br/>= não identificado, pendente<br/>ou bloqueado"]
        NOAUTO["Sem compra automática<br/>o sistema recomenda e explica;<br/>o humano decide"]
        REANALYSIS["Nova evidência ou mudança<br/>exige nova análise"]
    end

    CONTROL -.-> EVIDENCE
    BRONZE -.-> EVIDENCE
    SILVER -.-> EVIDENCE
    GOLD -.-> EVIDENCE
    EVIDENCE --> NULLS
    NULLS --> BLOQUEADA
    NULLS --> PENDENTE
    BLOQUEADA --> NOAUTO
    PENDENTE --> NOAUTO
    REANALYSIS -.-> SNAPSHOT

    subgraph SAIDAS["8. Saídas do SILLO"]
        REVIEW["Revisão humana fundamentada"]
        BUYTEST["comprar_teste<br/>vale testar em pequena escala"]
        REVISE["revisar<br/>potencial com validações pendentes"]
        IGNORE["ignorar<br/>risco, margem ou evidência insuficiente"]
        UI["Carteira, filtros,<br/>explicações, histórico e exportações"]
        AUDIT["Auditoria e reconstrução<br/>por rodada e snapshot"]
    end

    BLOQUEADA --> REVIEW
    PENDENTE --> REVIEW
    PUBLICAR --> REVIEW
    REVIEW --> BUYTEST
    REVIEW --> REVISE
    REVIEW --> IGNORE
    BUYTEST --> UI
    REVISE --> UI
    IGNORE --> UI
    GOLD --> UI
    GOLD --> AUDIT
    REVIEW --> AUDIT

    classDef central fill:#263238,color:#ffffff,stroke:#111111,stroke-width:2px;
    classDef medal fill:#e8f5e9,color:#1b5e20,stroke:#4caf50;
    classDef engine fill:#e3f2fd,color:#0d47a1,stroke:#2196f3;
    classDef guard fill:#fff3e0,color:#e65100,stroke:#ff9800;
    classDef proposal fill:#f3e5f5,color:#4a148c,stroke:#9c27b0;
    classDef output fill:#ede7f6,color:#311b92,stroke:#673ab7;

    class SILLO central;
    class BRONZE,SILVER,GOLD medal;
    class SNAPSHOT,GUARDAS,MATCH,LOGISTICA,FINANCEIRO,CONSOLIDA,RANKING,PUBLICAR engine;
    class IMPEDITIVA,PENDENCIA,BLOQUEADA,PENDENTE,NOAUTO,NULLS guard;
    class PROCUREMENT,SOURCING,COMMERCIAL,WORKER,PREFECT,REDIS,SPOOL,PARSER proposal;
    class REVIEW,BUYTEST,REVISE,IGNORE,UI,AUDIT output;
```

## Como interpretar

1. As fontes entram como evidências brutas na Bronze.
2. A Silver normaliza e torna os dados comparáveis.
3. `procurement`, `sourcing` e `commercial` preservam responsabilidades diferentes.
4. O motor combina guardas, matching, logística e finanças.
5. Falhas ou pendências impedem um ranking positivo.
6. A Gold publica projeções e snapshots reconstruíveis.
7. A decisão final continua humana; `comprar_teste` não significa compra automática.
8. Histórico público pode ajudar a descobrir fornecedores, mas não prova capacidade atual, estoque, disponibilidade, disposição de vender ou cotação.

## Documentos de referência

- `docs/arquitetura_tecnica.md`
- `docs/licitacoes/diagramas/D03-motor.md`
- `docs/licitacoes/diagramas/D11-dados.md`
- `docs/licitacoes/diagramas/D15-descoberta-fornecedores.md`
