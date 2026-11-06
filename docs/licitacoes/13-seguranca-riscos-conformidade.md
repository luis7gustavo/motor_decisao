# 13 — Segurança, riscos e conformidade

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-11 (America/Sao_Paulo).  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: rascunho para revisão do operador; controles [PROPOSTO], sem certificação de conformidade ou teste operacional.

## 1. Fronteiras e responsabilidades

O Radar é apoio comercial à decisão; não substitui assessoria jurídica, contábil ou fiscal. O operador responde pela evidência registrada e decisão; o mantenedor técnico, por isolamento, integridade e recuperação; o especialista pertinente, pela validação externa do caso. Uma pessoa pode acumular responsabilidades, sem eliminar o registro de autoria. Fontes oficiais, cotações, CSV, PDFs e saídas do parser são entradas não confiáveis. Conteúdo externo não pode alterar regras, executar comandos, conceder acesso ou aprovar uma oportunidade (ADR-012/013).

As fronteiras são navegador→API local, downloader→internet, coordenador→spool, parser→saída validada e aplicação→PostgreSQL. Conforme [07](07-arquitetura-to-be.md) e [21](21-infraestrutura-docker.md), parser não recebe rede, banco, credenciais nem socket Docker. Identificador de arquivo nunca se converte diretamente em caminho enviado pelo usuário. PDF suspeito permanece em quarentena; texto extraído é exibido escapado, sem HTML ativo. Links exigem política explícita e cada redirect/DNS/conexão efetiva é verificado contra destinos internos.

## 2. Matriz de riscos

Probabilidades qualitativas são hipóteses de planejamento: baixa, média, alta; não frequências medidas. Impacto alto compromete capital, obrigação, dados ou decisão; médio exige retrabalho sem perda crítica presumida. Residual é avaliação após controles, ainda sujeita ao piloto. Nenhum risco residual alto autoriza ultrapassar bloqueio obrigatório.

| ID | Risco | Probabilidade / impacto | Mitigação proposta | Detecção / responsável | Residual |
|---|---|---|---|---|---|
| RSK-01 | Fonte indisponível ou desatualizada | Alta / alto | Consulta local com frescor, retry limitado e reconciliação | Último sucesso, checkpoint incompleto; técnico | Médio: publicação oficial pode atrasar |
| RSK-02 | Edital retificado após análise | Alta / alto | Versões, invalidação e conferência transacional antes do positivo | Hash/versão e validade; técnico/operador | Médio: intervalo entre consultas |
| RSK-03 | Especificação ambígua | Alta / alto | Pendência, trecho e revisão especializada | Conflito entre anexos/atributos; operador | Médio: interpretação exige caso concreto |
| RSK-04 | Produto incompatível | Média / alto | Matching por atributo obrigatório e revisão | Certificado/unidade/modelo divergente; operador | Médio: documentação do fornecedor falha |
| RSK-05 | Cotação vencida/estoque indisponível | Alta / alto | Validade, reconfirmação e alternativas | Relógio e quantidade comprometida; operador | Médio: volatilidade após confirmação |
| RSK-06 | Frete subestimado | Alta / alto | Cotar destinos/parcelas/seguro e cenário adverso | Previsto versus cotado/real; operador | Médio: adicionais não previstos |
| RSK-07 | Erro tributário | Média / alto | Bases, vigência e responsável fiscal por cenário | Parâmetro sem fundamento, custo não calculável; especialista | Médio: mudança de regra/operação |
| RSK-08 | Prazo insuficiente | Média / alto | Cronograma completo, fuso e buffer explícitos | Folga negativa ou desconhecida; operador | Médio: atraso externo |
| RSK-09 | Falta de capital | Média / alto | Caixa datado, compromissos e atraso obrigatório | Déficit + reserva acima de capital livre; operador | Médio: recebimento tardio |
| RSK-10 | Entrega direta não autorizada | Média / alto | Evidência contratual/fiscal e compromisso específico; cenário alternativo | Autorização/condição ausente; operador/especialista | Médio: vedação/interpretação particular |
| RSK-11 | Habilitação não atendida | Média / alto | Checklist por regime/edital, validade e revisão | Requisito faltante ou documento vencido; operador | Médio: diligência/análise externa |
| RSK-12 | Recusa no recebimento ou atraso no pagamento | Média / alto | Evidência de entrega/aceite e caixa adverso; saldos separados | Divergência, parcela vencida e cobrança registrada; operador | Alto: sistema não controla o órgão |
| RSK-13 | PDF malicioso/SSRF/exaustão | Média / alto | Downloader restrito, quarentena, parser sem rede e limites | Recusa de destino, timeout, recursos excedidos; técnico | Médio: vulnerabilidade inédita |
| RSK-14 | Extração omite tabela/anexo | Alta / alto | Cobertura explícita, página/trecho e revisão do original | PDF de imagem, texto vazio, cláusula conflitante; operador | Médio: omissão difícil de perceber |
| RSK-15 | Exposição de segredos/dados pessoais | Média / alto | Minimização, redaction, ACL, backup protegido e escopo local | Sentinelas sintéticas e revisão de exportação; técnico | Médio: erro humano/host comprometido |
| RSK-16 | Recomendação excessivamente confiante | Alta / alto | Bloqueios antes do score, NULL e revisão final | Positivo com lacuna, cobertura ou versão omitida; operador | Médio: viés de automação |
| RSK-17 | Teste ou restore atinge dados reais | Média / alto | Destino/volume/credencial isolados e guarda antes da escrita | Inventário contra allowlist; técnico | Baixo após ensaio, nunca presumido |
| RSK-18 | Concorrência ou backup inconsistente | Média / alto | Transações, lock_version, fencing e manifesto banco/arquivos | Versão obsoleta, órfão/hash divergente; técnico | Médio: falha de procedimento |

Rastreabilidade: RSK-01/02→RF-001/004/005/034; 03/11/14→RF-006–009/025; 04/05→RF-013–019; 06/08/10→RF-010–012; 07/09/12→RF-020–023/032; 13/15/17/18→RNF-001–008/012/014/017/021; 16→RF-025–027 e RNF-016. Casos verificáveis estão em [14](14-plano-de-testes.md); riscos são revisados a cada fase e incidente.

## 3. Fontes oficiais, vigência e aplicabilidade

Consulta em 2026-10-11: os três textos oficiais abaixo foram abertos no navegador de pesquisa. Leitura do texto disponível e de suas disposições de vigência não constitui monitoramento legislativo contínuo. Antes de parametrizar uma contratação, reconferir redação aplicável, data do ato, regime, ente, regulamento e instrumento. Não aplicar trecho revogado/riscado como vigente. Valores, prazos de benefício e alíquotas não são hardcoded nesta especificação.

| Fonte oficial consultada | Escopo verificado e consequência de desenho |
|---|---|
| [Lei 14.133/2021](https://www.planalto.gov.br/ccivil_03/_ato2019-2022/2021/lei/l14133.htm), arts. 1–4, 62, 140, 174, 190–194 | Vigência desde publicação (194); âmbito e exceções nos arts. 1–3. Registrar regime e preservar contratos anteriores segundo disposições transitórias. Habilitação possui dimensões próprias (62); recebimentos provisório/definitivo e conformidade não equivalem a pagamento (140). PNCP é fonte institucional (174), sem garantir completude de cada registro. Aplicação de ME/EPP depende também do art. 4. |
| [LC 123/2006, texto atualizado](https://www.planalto.gov.br/ccivil_03/leis/lcp/lcp123.htm), arts. 42–49 | Regras diferenciadas de aquisições públicas dependem de enquadramento, condições e exceções. Texto contém redações históricas e remissões a produção de efeitos: selecionar a redação aplicável ao ato. Não converter identificação ME/EPP em benefício automático. Checklist e confirmação especializada, RN-025. |
| [LGPD, Lei 13.709/2018, compilada](https://www.planalto.gov.br/ccivil_03/_ato2015-2018/2018/lei/l13709compilado.htm), arts. 6, 7, 15–16, 46 e 65 | Texto vigente consultado; princípios, hipótese de tratamento, término/conservação e medidas de segurança orientam inventário e minimização. Dados públicos ou operação local não resolvem sozinhos a base legal. Registrar finalidade, categoria, fundamento, acesso e revisão de retenção por conjunto. Não assumir consentimento universal. |

As consequências de desenho acima são [PROPOSTO]. Normas tributárias, regras estaduais/municipais, documentos de habilitação e permissão de envio direto dependem do caso; QA-008 permanece aberta para o piloto. Não há conclusão de que o art. 140 fixa prazo universal de pagamento, nem de que envio direto é sempre permitido. O sistema registra fundamento e pendência, sem emitir parecer.

## 4. Dados, acesso e retenção

| Conjunto | Acesso / minimização | Retenção proposta e revisão |
|---|---|---|
| JSON/documentos oficiais e evidências de decisões | Operador e processos necessários; manter original protegido, extrações escapadas | Preservar enquanto sustentar análise/obrigação; revisão anual e no encerramento, sem prazo legal universal presumido |
| Contatos e cotações de fornecedores | Somente dados comerciais necessários; exportar campos estritamente úteis | Revisar semestralmente finalidade/validade; cessado uso, avaliar anonimização/eliminação com responsável e dependências históricas |
| Logs técnicos | IDs/contagens/códigos; sem corpo de PDF, senha ou token | Janela operacional inicial de 30 dias, calibrável; incidentes relevantes preservados em registro segregado |
| Backups | ACL restritiva e cópia independente protegida | Proposta operacional: 7 diárias, 4 semanais e 12 mensais; retenção final depende de capacidade e obrigações, conforme 16 |
| Segredos | Fora do repositório/imagem/manifesto de análise | Rotação em incidente e mudança de acesso; recuperação por canal separado |

Não implementar exclusão automática de evidências referenciadas. Retenção histórica não significa guardar dados pessoais para sempre: pedido ou prazo aplicável exige análise de base legal, bloqueio de descarte quando houver obrigação e eventual anonimização auditada que preserve integridade referencial. ADR-016 resolve a tensão entre preservação e minimização. Política é proposta para futura operação; nenhuma limpeza ocorre nesta missão.

## 5. Resposta a incidentes e aceite

Ao detectar incidente: conter o componente afetado e suspender novos positivos; preservar trilha sanitizada/hash e instante; delimitar dados/versões; corrigir e rotacionar acesso quando pertinente; restaurar somente em destino isolado e reconciliar; reavaliar análises afetadas. Avaliar com responsável competente obrigações legais de comunicação conforme norma vigente do incidente, sem inventar prazo universal aqui. Nenhuma mensagem externa é enviada automaticamente.

Aceite futuro: nenhuma entrada crítica desconhecida gera E07; corpus adverso não acessa rede/arquivo proibido; sentinelas não vazam; restauração recompõe vínculos; confirmação concorrente obsoleta falha; decisão registra responsável e fundamento. RNF-001–005/008/014/016/020/021 e [plano de testes](14-plano-de-testes.md) são os critérios executáveis a implementar.

## 6. Riscos da descoberta histórica — revisado em R1

Revisão 0.3-R1-E2, 2026-10-13. [PROPOSTO]. Probabilidades são qualitativas de planejamento, baseline desconhecida. Reusar IDs existentes quando a obrigação é a mesma; novos riscos tratam mecanismos adicionais. Revisão pelo operador/técnico a cada incremento e incidente; controles não garantem interesse de vender.

| ID / extensão ou novo risco | Probabilidade / impacto | Mitigação | Detecção e responsável | Residual |
|---|---|---|---|---|
| RSK-01/02 — histórico velho ou cancelado | Alta / alto | Atualização por recurso, situação/consulta separadas, invalidação de referência, último válido preservado | Referência selecionada com versão nova/sem frescor; técnico | Médio: atraso da fonte e intervalo de consulta |
| RSK-04 — filial/CNPJ/CATMAT/unidade incorretos | Média / alto | Texto completo original/canônico, DV oficial nos dois formatos, UUID independente; raiz não funde; atributos/fator evidenciados | TST-059–062, conflitos fiscais ou unidade sem fator; técnico/operador | Médio: dado oficial incompleto e revisão humana |
| RSK-05 — cotação/disponibilidade fora do escopo | Alta / alto | Conferir produto/filial/quantidade/prazo/condições, expiração e reconfirmação; preço não reserva | TST-067; evidência vencida ou de outro escopo; operador | Médio: estoque muda após confirmação |
| RSK-15 — dado público pessoal desnecessário | Média / alto | Minimização, base/finalidade/retencão por conjunto, sem enriquecimento de sócios/CPF/contato pessoal; logs por allowlist | Sentinelas e auditoria de exportação TST-071; técnico e responsável de tratamento | Médio: campos livres podem conter pessoas |
| RSK-16 — candidato classificado como capacidade | Alta / alto | Razões explicáveis, dimensões independentes, nenhuma probabilidade/score novo; concorrente/silêncio não significam incapaz | TST-066/068 e revisão das telas; operador | Médio: viés de automação e falsa segurança |
| RSK-19 — preço público usado como aquisição | Média / alto | Contrato de cenário admite histórico somente como hipótese explícita de venda; custo exige evidência atual/premissa admitida | Campo acquisition_cost com origem pública, TST-068; técnico/operador | Baixo após guardas; erro manual ainda requer revisão |
| RSK-20 — homologação/ata/contrato tratados como execução | Alta / alto | Etapas e naturezas de quantidade separadas; comprovação específica de entrega/aceite/pagamento | TST-057/058/064; rótulo ou agregado sem stage; operador | Médio: cobertura de execução desconhecida |
| RSK-21 — mesmo negócio duplicado entre fontes/etapas | Alta / alto | event_links com relação e identidade/grão oficiais; equivalência conta uma vez, etapas não somadas; possível duplicidade fora do agregado | TST-056/057; contagens e conflitos entre fontes; técnico | Médio: vínculos insuficientes, cobertura reduzida explicitamente |
| RSK-22 — seleção/sobrevivência e conversão presumida | Alta / alto | Exibir população observada, amostra insuficiente, desconhecido de perdedores/desertos; incluir insucessos/sem resposta nas métricas | MN-11–18 e TST-063/066; operador revisa coorte completa | Alto para extrapolação: piloto não representa mercado nacional |
| RSK-23 — termos/quota/contrato não resolvidos | Média / alto | Gate por recurso, quota produtiva versionada própria, retries/redirects contados, pausa em bloqueio, sem contorno | TST-069 e QA-R1-02/03; integrador/operador | Médio: alterações externas; sem quota publicada não é ilimitado |

RSK-19–23 ligam RF-028/035–038, RN-005/033–037, RT-017–019 e testes 056–071; os controles transversais RNF existentes continuam. Cenário de concorrente que não vende para revenda: registrar disposição recusada para o escopo, buscar alternativa; não inferir papel por nome/CNAE nem criar cadastro universalmente inapto. Aplicabilidade do art. 83 e minimização permanecem fundamentadas na matriz E1 F6/F7, com consulta de 12/10 e verificação pertinente antes de operação. Nenhuma nova conclusão jurídica foi produzida na E2.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: cinco novos riscos e particularizações dos existentes, sem estatísticas de frequência ou certificação de controles.

