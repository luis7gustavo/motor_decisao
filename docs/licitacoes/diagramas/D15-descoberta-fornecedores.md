# D15 — Descoberta de fornecedores a partir do histórico público

Revisão: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Revisado em R1.
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.
Status: [PROPOSTO], marco 2-H e fase 3, condicionado aos gates de fonte/ambiente/piloto.

```mermaid
flowchart TD
    I["Item versionado: especificacao, quantidade e prazo"] --> Q["Pesquisar resultados no recorte e contrato validados"]
    Q --> E["Preservar fonte, etapa, natureza, situacao e versao"]
    E --> ID["Identificar estabelecimento completo<br/>Deduplicar mesmo evento; vincular etapas distintas"]
    ID --> C["Revisar qualidade e comparabilidade<br/>Atributos, unidade, fator, datas e contexto"]
    C --> OK{"Comparavel com evidencia suficiente?"}
    OK -->|Nao| P["Rejeitado ou pendente com motivo<br/>Sem agregado enganoso"]
    OK -->|Sim| CA["Candidato explicavel por item<br/>Historico nao confirma capacidade"]
    CA --> O["Operador avalia e registra contato externo manual"]
    O --> R{"Resposta no escopo?"}
    R -->|Ainda nao| ESP["Aguardar ou encerrar ciclo<br/>Registrar duracao e ausencia de resposta"]
    R -->|Sim| D["Disposicao para vender: desconhecida,<br/>aceita sob condicoes ou recusada"]
    D -->|Aceita cotar| CO["Registrar cotacao por estabelecimento,<br/>produto, quantidade, condicoes e validade"]
    D -->|Recusa no escopo| ALT["Registrar motivo e investigar alternativa"]
    CO --> V["Verificar validade, disponibilidade, produto,<br/>logistica, capital e demais requisitos atuais"]
    V --> SUF{"Insumos obrigatorios suficientes?"}
    SUF -->|Nao| PEN["Analise pendente; sem margem ou capacidade afirmadas"]
    SUF -->|Sim| A["Analise pelas regras atuais<br/>Formula global preservada e revisao humana"]
    I -.->|Fornecedor e cotacao ja disponiveis| V
    Q -.->|Historico indisponivel| SEM["Sinalizar cobertura; manter cadastro e cotacao"]
    SEM --> V
    CA -.->|Hipotese explicita de venda, nunca custo| V
```

Legenda: retângulos são atividades/estados informativos, losangos são decisões; setas tracejadas são caminhos opcionais/contexto. Todo contato ocorre fora do sistema e é apenas registrado manualmente. Pendente de comparação não equivale a inaptidão universal do fornecedor. CNPJ completo distingue estabelecimento, raiz apenas agrupa; equivalência de fontes não funde etapas ou cotações de filiais. Não há transição nova de RN-032/D10.

A cotação não reserva estoque. A seta histórico indisponível preserva a análise somente se houver insumos suficientes, não cria informação faltante. Hipótese histórica de venda exige seleção, proveniência, comparabilidade e sensibilidade; nunca vira aquisição. Sem resposta permanece na população e duração observada de MN-14–18. Nenhum passo foi implementado ou executado como piloto nesta revisão.

Fontes normativas: [22](../22-inteligencia-de-mercado-fornecedores.md), [23](../23-especificacao-piloto-fornecedores.md), RF-028/035–038, RN-005/006/033–037, RT-017–019, UC-09, TST-056–071, ADR-018/019/020. Modelo em 08; orçamento/cobertura explícitos, sem coleta nova na E2.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: D15 criado como fonte Mermaid editável, legenda e limites; validação efetiva registrada no relatório E2.
