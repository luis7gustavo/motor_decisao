# 00 — PRD: requisitos de produto do Radar Licita

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-12 (America/Sao_Paulo).  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: rascunho para revisão do operador; capacidades do Radar [PROPOSTO], sem implementação nesta missão.

## 1. Resumo e proposta de valor

O Radar Licita ajuda o operador a responder: quais oportunidades públicas posso atender com produto compatível, execução viável, capital suficiente e margem fundamentada, preferencialmente sem deslocamento próprio? Reúne fonte oficial, edital, fornecedor e cenário numa decisão explicável. Um valor estimado alto não implica oportunidade boa. A pergunta é respondida por evidências e guardas antes do ranking, com revisão humana final (RF-025–027; RN-027/029).

## 2. Problema e contexto

O SILLO atual apoia compra para revenda; sua heurística e matching lexical não verificam exigências públicas, unidade de julgamento ou caixa contratual. O trabalho manual de cruzar edital, cotação, logística e custos é disperso e pode produzir falso lucro ou descarte equivocado. [01](01-visao-produto.md), [02](02-diagnostico-as-is.md) e [03 — DAN](03-analise-negocio-dan.md) detalham contexto e reaproveitamento. Não há tamanho de mercado, receita ou ganho comprovado nesta especificação.

## 3. Objetivos e não objetivos

Objetivos: reduzir tempo de triagem, conservar evidências, evitar recomendação com impedimento e produzir cenário reproduzível por item/lote/grupo/global (RN-007–009). Preferência por remoto não significa excluir classe C comprovadamente atendível.

Não objetivos da V1: envio automático de lances/propostas, compra/pagamento/mensagens a terceiros, parecer jurídico/contábil/fiscal, multiusuário complexo, ML sem dados, cobertura nacional garantida ou previsão de vitória. Dados oficiais continuam externos; o sistema não os altera.

## 4. Personas e jornadas

Operador individual acumula papéis de analista, revisor e responsável por parâmetros. Fornecedor e especialista entregam informações fora do aplicativo; o operador registra origem e validação. Jornada principal: descobrir → enriquecer → conferir produto e execução → simular unidade completa → resolver pendências → revisar → registrar eventual participação externa e resultados. Jornada de retificação: detectar nova versão → invalidar atualidade → reanalisar preservando atos já ocorridos. Jornada de falha: consultar última versão local com aviso de frescor; falha não gera aprovação. UC-01–UC-08 em D04.

## 5. Escopo e definição do MVP

MVP = 32 requisitos Must das fases 1–5 mais todos os RNF aplicáveis. Fase 1 isolada entrega fundação, não MVP comercial. RF-024 (sensibilidade ampliada), RF-028 (apresentação histórica) e RF-035–038 (descoberta/contato) são Should (revisado em R1); cenários de atraso obrigatórios de caixa continuam no Must RF-022. React, ML, BI externo e operação distribuída não são pré-requisitos.

## 6. Capacidades de produto

Todas as capacidades públicas abaixo são [PROPOSTO]. A coluna AS-IS classifica somente o investimento reutilizável, sem afirmar que RF público já está implementado. MoSCoW/fase seguem 04.

| ID / capacidade | Descrição | RF associados | MoSCoW | Fase | AS-IS e adaptação |
|---|---|---|---|---|---|
| CP-01 — Descoberta e atualização | Consultar, filtrar e versionar oportunidades com origem, atualidade e falhas visíveis | RF-001, RF-002, RF-003, RF-004, RF-005 | Must | 1 | [CONFIRMADO] coleta comercial/controle; adapter público novo |
| CP-02 — Documentos e evidências | Obter documentos, localizar trechos e revisar exigências/conflitos | RF-006, RF-007, RF-008, RF-009 | Must | 2 | [PARCIAL] extração comercial; pipeline de editais novo |
| CP-03 — Logística e execução remota | Classificar A/B/C/D e comparar planos com evidências | RF-010, RF-011, RF-012 | Must | 2 | [PROPOSTO] nova |
| CP-04 — Fornecedores e cotações | Manter identidade, catálogo, validade e condições de fornecimento | RF-013, RF-014, RF-015, RF-016; RF-038 opcional | Must no cadastro/cotação; Should no contato de candidato | 3 | [CONFIRMADO] ingestão de catálogos; cotação pública versionada nova |
| CP-05 — Matching técnico | Comparar atributos e registrar revisão por versões | RF-017, RF-018, RF-019 | Must | 3 | [PARCIAL] recuperação lexical; confirmação técnica nova |
| CP-06 — Finanças e capital | Calcular custo, preço, margem e caixa da unidade inteira | RF-020, RF-021, RF-022, RF-023 | Must | 4 | [PARCIAL] heurística em float; Decimal e caixa novos |
| CP-07 — Sensibilidade ampliada | Comparar variações adicionais sem bloquear cálculo básico | RF-024 | Should | 4 | [PROPOSTO] nova |
| CP-08 — Elegibilidade e ranking | Aplicar guardas, priorizar viáveis e explicar resultados reproduzíveis | RF-025, RF-026, RF-027 | Must | 4 | [CONFIRMADO] padrão heurístico/snapshot; regras públicas novas |
| CP-09 — Histórico e descoberta básica (revisado em R1) | Resultados, comparáveis, preços contextualizados e candidatos explicáveis | RF-028, RF-035, RF-036, RF-037 | Should | 2-H básico; 6 ampliado | [PROPOSTO] nova; gates da E1 pendentes |
| CP-10 — Operação comercial | Revisar, registrar fatos, acompanhar execução/recebimento e carteira | RF-029, RF-030, RF-031, RF-032, RF-033, RF-034 | Must | 5 | [CONFIRMADO] revisão auditável Jinja2; estados públicos novos |

## 7. Fluxos principais

[D07](diagramas/D07-atividades.md) descreve a jornada; [D10](diagramas/D10-estados.md) representa os 16 estados e 45 transições de RN-032; [D13](diagramas/D13-entrega-remota.md) separa classe de autorização; [D14](diagramas/D14-fluxo-financeiro.md) conecta custo, capital e revisão. E05 é pré-aprovação para estudo, E07 aptidão após guardas/revisão, E08 fato externo. “Vencida” (E11) significa vitória, não prazo expirado. Retificação após participação não apaga o fato.

## 8. Métricas de sucesso

Definições normativas e protocolo estão no DAN, MN-01–10; esta síntese não muda denominadores. Linha de base de todas: **desconhecida**. Metas são provisórias, sujeitas a calibração registrada. Piloto inicial: quatro semanas e pelo menos 50 unidades; amostra insuficiente não demonstra eficácia.

| ID | Definição operacional resumida / fonte | Meta provisória |
|---|---|---|
| MN-01 | Mediana de minutos ativos de triagem; eventos/cronômetro comparados ao processo manual equivalente | Redução de 30% |
| MN-02 | Descartes corretos / descartes revisados; amostra estratificada humana | >=95%, reportar incerteza |
| MN-03 | Viáveis descartadas / viáveis na amostra, incluindo mantidas e descartadas | <=5% |
| MN-04 | Pares confirmados corretos / confirmados; ficha/atributo revisado | >=95% em pelo menos 50 pares quando disponíveis; zero conflito obrigatório conhecido aceito |
| MN-05 | Mediana de abs(previsto-real)/real para real>0; cenário e custos realizados | <=10% em pelo menos dez operações concluídas |
| MN-06 | Unidades atendíveis / triadas; checklist com produto, capacidade, prazo e execução | Hipótese exploratória >=20%; não afrouxar guardas |
| MN-07 | Positivos E07 com trilha completa / positivos E07; snapshots | 100% |
| MN-08 | Positivos E07 com pendência impeditiva / positivos E07; histórico de checks | 0% |
| MN-09 | Aptas sem deslocamento do operador / aptas; evidências de logística | Hipótese exploratória >=70%; D separado |
| MN-10 | Pico real menos pico previsto; eventos de caixa | Nenhuma falta de caixa ignorada; limite monetário a calibrar |

Denominador zero é não calculável. E05 não entra como positivo em MN-07/08. RNF mede desempenho técnico separadamente; estimativa de margem não é lucro realizado.

## 9. Premissas, restrições e dependências

Operação local individual em Docker/Compose; preservar banco/volumes/histórico; manter FastAPI, PostgreSQL, Prefect e Jinja2 (ADR-002–005/013). Contratos PNCP de itens/anexos/resultados ainda precisam validação; campos desconhecidos permanecem pendentes. Categorias/UFs, capital, fornecedores, margens e políticas fiscais dependem do operador. Documentação não autoriza execução de migration ou coleta.

## 10. Riscos de produto

Retificação, equivalência falsa, cotação vencida e extração incompleta podem produzir decisão incorreta; versões, checks e revisão mitigam. Caixa/frete/tributo subestimados exigem cenário completo e especialista quando aplicável. Score excessivamente confiável é mitigado por global NULL em bloqueio/pendência e evidência visível. [13](13-seguranca-riscos-conformidade.md) registra probabilidade, impacto e residual, inclusive atraso do órgão que o software não controla.

## 11. Aceite verificável do MVP

Todos os Must e RNF aplicáveis têm evidências de teste em container; RF-001–005 preservam fonte e replay; RF-006–019 explicam documento, logística e matching; RF-020–027 reproduzem os três casos de 09/12; RF-029–034 preservam eventos e impedem transição indevida. Positivo exige fontes atuais, cotação válida, matching suficiente, unidade completa, capital e revisão. TST-001–023/025–027/029–055 são obrigatórios conforme fase; TST-024/028 cobrem Should. Backup/restore e preservação do legado devem ser demonstrados. Metas ainda sem amostra ficam não medidas, nunca aprovadas por ausência de dados.

## 12. Plano de entrega

Fase 0 especificação; 1 fundação/PNCP; 2 documentos/logística; 3 fornecedores/matching; 4 finanças/ranking; 5 operação comercial; 2-H descoberta histórica opcional após identidade/ingestão, junto da preparação de 3; 6 inteligência ampla condicionada (revisado em R1). [15](15-roadmap-backlog.md) contém dez épicos, 38 histórias, dependências e aceites; [16](16-plano-de-migracao.md) contém gates de preservação e recuperação.

## 13. Questões abertas

QA-001: fechar contratos oficiais de recursos/datas por leitura e amostra controlada futura. QA-002: comprovar runtime em missão de implementação. QA-004: operador define recorte/capital/fornecedores. QA-008: especialista valida condições contratuais/fiscais do piloto. QA-010: medir metas com amostra e registrar calibração. Validação de diagramas e procedimento de migração são entregas documentais, não ensaios operacionais. Estado atualizado no [progresso](_progresso.md).


## 14. Descoberta e piloto restrito — revisado em R1

[PROPOSTO] A capacidade antecipada pergunta quem vale investigar, não quem comprovadamente vende ao operador hoje. Fluxo: item → resultado comparável → estabelecimento/candidato → contato manual → cotação por escopo → verificação atual → análise. Histórico não muda fórmula, não satisfaz custo obrigatório e sua ausência não impede análise com insumos suficientes. Identidade completa numérica/alfanumérica, raiz analítica, equivalência entre fontes e etapas relacionadas seguem ADR-019.

CP-09/EP-09 usam RF-035 para ingestão, RF-036 para comparação, RF-037 para candidato e RF-028 para apresentação. CP-04/EP-04 preserva cadastro/cotação como autoridades e usa RF-038 só para registrar relacionamento/vínculos. Não duplicar obrigação nem transformar Should em pré-requisito do MVP; Must revisados de identidade/atualidade/fornecimento/decisão estão em 17. Mercado amplo/CNAE em escala/lances completos/ML permanecem fase 6.

[23](23-especificacao-piloto-fornecedores.md) especifica um piloto de pesquisa/cotação separado do piloto integral do MVP: proposta de uma categoria, até 20 itens/quatro semanas, MN-11–18 e seguimento de casos sem resposta. Categoria e UF ainda pendentes; papel A4 só hipótese por atributos, sem amostra. Baselines desconhecidas, metas propostas calibráveis e zero conversão observada. Operador decide recorte e esforço; contratos/quota/dados/ambiente do produto são gates. Conclusão documental não significa piloto pronto.

UC-09 e D15 acrescentam jornada sem alterar UC-01–08 ou RN-032/D10. Totais R1: 38 RF (32 Must/6 Should), 21 RNF, 37 RN, 10 CP, 19 RT, 20 ADR, 71 testes planejados, 15 diagramas, 18 MN; nenhum RF implementado nesta missão.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: CP-09/04, escopo/fases e métricas do piloto alinhados à decomposição RF-028; preservado MVP e indepedência do histórico.

