# R1 — Resultados e limitações

Versão 0.2-R1. 2026-10-12, America/Sao_Paulo. Revisado em R1-E1.

## Resultado efetivo

Foram lidos documentos oficiais, comparado Git e inspecionado código. Não houve amostragem, normalização experimental, cálculo programático, teste da aplicação ou validação automatizada de documentos/diagramas. Docker Engine não estava acessível; CLI/Compose presentes não suprem esse requisito. Nenhum processo operacional foi iniciado para contornar a limitação. Amostragem: 0/30 tentativas, zero registros/bytes de dados obtidos. Digest/arquitetura do ambiente de execução não existem nesta revisão.

A documentação Compras descreve recursos úteis; a matriz separa essa evidência de resposta real. Não foram medidos preenchimento, utilidade dos vínculos, cobertura, diversidade de estabelecimentos, defasagem, conversão comercial, tempo para cotar ou margem. Não recomendar categoria comercial definitiva com base somente no manual.

## Casos exclusivamente sintéticos, para revisão textual

| Caso criado para especificação | Consequência esperada, não teste executado |
|---|---|
| Resultado R em fonte A e B, mesma chave oficial/seq/item | Uma observação lógica para contagem, duas proveniências |
| Resultado R e contrato C relacionado | Duas etapas, totais não somados como negócios independentes |
| Estabelecimentos E1/E2 com raiz igual | Grupo analítico comum; cotação E1 não valida E2 |
| Caixa com fator desconhecido e item em unidade | Comparação pendente; não dividir preço por fator inventado |
| Resultado homologado, sem cotação atual | Candidato para investigar; análise sem custo obrigatório permanece pendente |
| Fonte histórica indisponível, cotação e demais insumos suficientes | Análise segue regras atuais, sem bloqueio novo por histórico |

R/E são rótulos fictícios; nenhum CNPJ real foi persistido. Fixtures executáveis, inclusive DV numérico/alfanumérico, ficam para missão posterior em Docker. Os casos não demonstram algoritmo implementado.

## Gates e alternativa

- **QA-R1-01:** Docker, imagem oficial por digest/arquitetura, runner contido e escrita não root precisam ser demonstrados. Ver procedimento em ferramentas-r1.
- **QA-R1-02:** obter contrato machine-readable legível, hash, formato de datas, tipos, security, paginação e vínculos; resolver divergências do manual. Responsável: implementação/integrador.
- **QA-R1-03:** termos específicos de reuso, quotas, cobertura e política de atualização por recurso; ausência de informação não equivale a proibição nem autorização ilimitada. Responsável: operador/integrador.
- **QA-R1-04:** amostra controlada e perfil de campos críticos, sem CPFs/brutos. Como E1 será concluída documentalmente, nova amostragem não entra em E2: requer missão posterior que preserve ledger cumulativo quando retomar esta investigação.
- **QA-R1-05:** operador escolhe categoria/janela/região e finalidade/base de tratamento; especialista quando necessário; métricas comerciais só no piloto futuro.

Até resolver gates, antecipação é especificação condicionada. Alternativa: pesquisa humana de referências com origem e cadastro/cotação manual pelos RF existentes, sem alegar ingestão automática aprovada. Histórico ausente não interrompe oportunidades suficientemente fundamentadas.

Relatórios `validacao-documental.json` e `diagramas/validacao-mermaid.json` no pacote original continuam evidência de 12/10 anterior à R1; não cobrem estes novos documentos ou requisitos. Nenhuma renderização nova aprovada.

## Histórico de revisões

2026-10-12 — 0.2-R1: evidências, casos sintéticos e gates; revisão documental não libera piloto.

## Adendo E2 — revisado em R1

2026-10-13, versão 0.3-R1-E2. As limitações de Engine/imagem/scripts acima descrevem a execução E1 de 12/10. E2 executou somente validação documental offline com ferramentas fixadas; evidências em [relatório E2](e2/relatorio-validacao.md). Não houve amostragem: ledger permanece 0/30, nenhuma fonte aprovada por dados observados. Runtime de validação disponível não resolve contrato, runner de amostra, termos/quota ou escolha comercial. Histórico de revisões: adendo de escopo temporal, sem modificar conclusões originais.
