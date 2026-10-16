# Radar Licita plano do MVP para apresentação

Preparado em 15/10/2026 para apresentação em 16/10/2026. Fuso de referência America/Sao_Paulo.
Versão 1.1. Documento de execução em oito sessões. Orçamento e busca inicial de parceiros são obrigatórios neste MVP. Implementação ainda não iniciada por este plano.

## 1 Objetivo e decisão de escopo

Entregar uma demonstração local do Radar Licita em que o operador informa quanto pode investir, recebe oportunidades ordenadas que cabem nesse orçamento, abre um item, consulta evidências e busca possíveis parceiros a partir de resultados públicos de pregões eletrônicos. Depois, associa uma cotação atual, simula a operação, entende a análise e registra o próximo passo. A apresentação deve mostrar uma decisão explicável e suas limitações, sem depender de uma coleta ao vivo.

O público foi assumido como avaliadores de uma apresentação de projeto, com demonstração de aproximadamente sete minutos. Categoria, UF e modelo comercial definitivo continuam em aberto. Para os casos sintéticos, informática é apenas uma escolha didática, não uma recomendação comercial.

A base de execução é backend/ e frontend/ da raiz atual. O diretório motor_decisao/ contém o legado SILLO e a especificação ampliada em docs/licitacoes/. Será referência de domínio, preservada nesta entrega. A documentação ampla prevê outra arquitetura e regras adicionais; este recorte de apresentação não satisfaz todos os requisitos do produto completo.

Escolha para o prazo: reaproveitar FastAPI, React e SQLite local, mantendo a interface existente. Não migrar para Jinja2, PostgreSQL, Prefect ou outra infraestrutura antes da apresentação. O banco original será preservado e a demonstração terá arquivo de banco próprio.

## 2 Linha de base verificada

Inspeção do código e validações locais em 15/10/2026. Os dados do banco abaixo foram consultados em leitura na linha de base da versão 1.0. Nesta revisão 1.1 foi feita verificação limitada do contrato e de uma cadeia real de pregão/item/resultado PJ; a evidência está em evidencias/PNCP_VERIFICACAO_2026-10-15.json. Isso não implementa o coletor do produto nem representa coleta nacional.

| Área | Evidência atual | Consequência para o plano |
| --- | --- | --- |
| Backend | 19 testes passaram, com um aviso de depreciação Starlette/httpx | Há base reaproveitável, mas a cobertura atual não garante o fluxo completo |
| Interface | Radar, detalhes, itens, evidências, simulador, fornecedores, acompanhamento e configurações existem | Melhorar o percurso existente |
| Dados locais | 50 contratações, 39 itens, 2 documentos, 14 evidências e 1 classificação logística D | O acervo atual serve de referência histórica |
| Prazo do acervo | As 50 contratações estão marcadas abertas, mas encerram em 09/10/2026 | Corrigir o filtro temporal e identificar o snapshot |
| Dados comerciais | Nenhuma cotação, cenário financeiro ou score no banco inspecionado | Preparar casos de demonstração em banco separado |
| Persistência SQLite | Comparação entre data sem fuso e data UTC reproduziu TypeError no ranking | Testar registro persistido e recarregado |
| Sincronização | SyncResult exige job_id, enquanto IngestionJob expõe id | Corrigir resposta sem repetir coleta por causa de erro de serialização |
| Integração visual | Cotação não envia procurement_item_id e acompanhamento não chama updateStatus | Fechar os vínculos e a mudança de status |
| Premissas | Interface força taxes_confirmed=true; ranking ignora assumptions_complete | Exigir confirmação e manter análise incompleta como pendente |
| Histórico | Resumos podem usar score elegível antigo e cenário de outro item | Usar análise vigente e identificar o item |

O build TypeScript e Vite do frontend passou em 15/10/2026, em 10,58 segundos. O README descreve verificações anteriores; somente os resultados novamente executados e registrados podem ser apresentados como atuais.

Na raiz, o Git está sem commits, na branch master, com arquivos preexistentes não rastreados e sem remote configurado. Há um repositório separado em motor_decisao/. A execução padrão será local, sem exigir PR, commit ou publicação. Se o estado mudar, registrar a mudança antes de prosseguir; nunca usar git add geral neste workspace.

## 3 O que o MVP precisa demonstrar

1. Informar orçamento disponível em reais e atualizar a lista de melhores oportunidades por item, com capital necessário, saldo, margem simulada, prazo e motivo da posição.
2. Recalcular a seleção quando o orçamento mudar, sem alterar silenciosamente as configurações globais. Mostrar oportunidades elegíveis, pendências e itens que excedem o orçamento em estados separados.
3. Abrir um item e iniciar a busca de possíveis parceiros por termo ou atributos, recuperando resultados anteriores de pregão eletrônico, no recorte consultado.
4. Exibir empresa, CNPJ completo, item, unidade, data, natureza do resultado e link da evidência. Permitir cadastrar a candidata para avaliação e incluir cotação atual manualmente.
5. Consultar documento, página e trecho, associar cotação ao item e simular aquisição, frete, tributos, custos adicionais e capital.
6. Mostrar análise viável sob premissas, pendente ou bloqueada, com razões. Registrar mudança de status comercial e reencontrar a oportunidade.
7. Repetir o percurso após reinício e sem fonte externa, usando o cache identificado da busca já realizada. O coletor de parceiros deve ter sido testado antes contra dado oficial real.

Orçamento significa capital disponível para executar o item inteiro, somando aquisição, frete, tributos e custos adicionais. Não é o valor estimado total do edital. Oportunidade de venda elevada pode caber em orçamento menor se o capital necessário estiver comprovado; item com valor público pequeno pode exigir capital superior.

A lista oferece alternativas individuais. Duas oportunidades que cabem separadamente não cabem necessariamente juntas. Não otimizar carteira nem prometer lucro ou vitória. Sem cotação atual e premissas suficientes, mostrar item em Precisam de análise, com capital desconhecido ou estimado explicitamente identificado; nunca custo zero presumido.

Possíveis parceiros são empresas identificadas como fornecedoras/arrematantes em resultados públicos de pregões de itens pertinentes. Podem ser concorrentes ou não querer revender. O MVP encontra candidatos e suas evidências; contato, interesse, cotação e capacidade são dimensões a confirmar.

O simulador aceita custo manual, mas só promove indicação positiva com cotação vigente selecionada e rastreável. Aquisição e frete devem corresponder à cotação e ao seu escopo. Preço homologado histórico permanece referência pública de venda; não vira custo de aquisição nem cotação atual.

## 4 Limites do recorte

Ficam para depois da apresentação: integração com o legado, cumprimento integral dos Must do PRD amplo, operação comercial real, análise completa de lotes/grupos, integração ampla do Compras.gov.br, descoberta nacional de fornecedores, lances de todos os participantes, otimização automática de carteira, OCR, LLM, ML, autenticação multiusuário, pagamentos, integrações fiscais, deploy público e envio de propostas.

O MVP trabalha por item. Julgamento por lote/grupo permanece bloqueado para recomendação por item isolado. A classificação logística A/B/C/D continua independente da elegibilidade. Existência de participação eletrônica não prova entrega remota nem envio direto.

Regras e limiares são critérios do protótipo, não validação estatística de decisão nem parecer jurídico, fiscal ou contábil. Não inventar métricas de mercado, receita, economia de tempo ou precisão.

## 5 Casos de demonstração

Usar um banco dedicado, por exemplo backend/data/demo/radar_demo.db. O nome é proposto; a sessão 2 criará e verificará o procedimento. Nunca substituir backend/radar_licita.db.

| Caso | Preparação | Resultado esperado |
| --- | --- | --- |
| DEMO A | Item sintético com atributos, cotação vigente selecionada, logística conhecida, premissas completas e capital de R$ 5.000 | Viável sob as premissas registradas |
| DEMO B | Mesmos valores, mas frete ou tributos não confirmados, ou cotação sem validade suficiente | Pendente e sem score positivo |
| DEMO C | Caso completo com capital disponível de R$ 500 | Bloqueado por capital insuficiente |
| ARQUIVO | Cópia seletiva do acervo oficial existente, com origem, coleta e prazo original | Histórico encerrado, separado dos casos sintéticos |

Valores exclusivamente sintéticos do DEMO A: quantidade 10; venda unitária R$ 200; aquisição unitária R$ 100; frete R$ 100; embalagem R$ 50; seguro R$ 20; custo financeiro R$ 30; instalação R$ 0; contingência R$ 100; tributos de 10% da receita. Receita R$ 2.000; custo total R$ 1.500; resultado estimado R$ 500; margem de 25%; capital necessário R$ 1.500, segundo o cálculo simplificado atual.

Esses números são um exemplo aritmético para verificar o produto, sem relação com preço comercial observado. O preço de equilíbrio precisa considerar tributos incidentes sobre a receita: para esse caso, R$ 144,4444 por unidade. Para margem alvo de 15%, o preço é R$ 173,3333 por unidade. Não chamar custo total dividido pela quantidade de equilíbrio se os tributos variam com o preço.

Datas sintéticas devem derivar de uma data de referência explícita, registrada no manifesto. A mesma referência deve reproduzir os casos. Se for regenerada para outro dia, criar nova versão do manifesto. Datas e texto de documentos oficiais não podem ser alterados para parecer atuais.

Cada caso sintético deve ter rótulo visível na interface e no documento de apoio. Links e documentos sintéticos não podem aparentar validação pelo PNCP. Documento sintético usado como evidência deve trazer essa identificação no próprio conteúdo.

## 6 Sessões e dependências

O novo escopo exige mais trabalho que a versão anterior. Estimativa de planejamento: 10h25 sequenciais mais 45 minutos de reserva. Com S04, S05 e S06 em paralelo, o caminho principal fica em cerca de 7h20 mais a reserva. Tempos são alvos, não garantia. Congelar funcionalidades 90 minutos antes da apresentação.

| Sessão | Tempo alvo | Entrega | Depende de |
| --- | --- | --- | --- |
| S01 | 35 min | Isolamento e contratos de orçamento e parceiros congelados | Nenhuma |
| S02 | 60 min | Dados sintéticos e amostra oficial real identificada | S01 |
| S03 | 120 min | Consistência por item e fundação de dados compartilhados | S02 |
| S04 | 75 min | Seleção e ranking por orçamento | S03 |
| S05 | 110 min | Coletor inicial e busca de parceiros por pregão | S03 |
| S06 | 110 min | Jornada visual de orçamento e parceiros | S03 |
| S07 | 75 min | Integração e ensaio dos dois requisitos | S04 S05 S06 |
| S08 | 40 min | Inicialização roteiro e materiais finais | S07 |

S01 → S02 → S03 → [S04 em paralelo com S05 e S06] → S07 → S08.

S03 é a única responsável por models.py, schemas.py, rotas compartilhadas e migrações de fundação. S04 trabalha em backend/app/recommendations/ e testes próprios, sem mudar o legado compartilhado. S05 trabalha em backend/app/partners/ e testes próprios, usando o modelo congelado/criado em S03. S06 é a única editora de frontend/src/. O coordenador conecta os routers e resolve ajustes compartilhados em S07. Não manter um router implementado sem integrá-lo ao produto ao final.

STATUS.md é atualizado pelo coordenador; agentes paralelos escrevem repasses individuais. S04 e S05 podem montar aplicações de teste locais para validar suas rotas antes da conexão final, sem tocar em main.py simultaneamente.

O caminho reduzido preserva orçamento e parceiros como requisitos obrigatórios. Pode cortar polimento, gráficos, filtros extras e automação de carga; pode usar cache oficial real validado no ensaio. Se um requisito central não ficar funcional, registrar MVP incompleto, concluir S07 e S08 como ensaio/material parcial e manter as etapas técnicas incompletas. Não rebaixar parceiros para futuro nem concluir o MVP só com uma lista sintética.

## 7 Contrato mínimo de integração

CONTRATO_DEMO.md e ESCOPO_ORCAMENTO_E_PARCEIROS.md contêm a proposta concreta e os critérios de congelamento de S01. A base atual continua /api/v1, com rotas de oportunidades, fornecedores, cotações, cenários, ranking e status. Não criar uma segunda API.

Mudanças aditivas propostas: expor o item na cotação e na análise; selecionar e persistir supplier_quote_id no cenário; expor eligibility e pending_checks na análise. Registros antigos sem esse vínculo continuam consultáveis e pendentes. A implementação exata será validada contra os modelos na S01 e congelada antes de S03 e das sessões paralelas.

Elegibilidade de API: viable, pending ou blocked. A interface traduz para Viável sob premissas, Pendente e Bloqueada. Manter eligible e provisional por compatibilidade; o status comercial manual é outra dimensão. Score fica null para pendente ou bloqueada. Razões e componentes podem permanecer disponíveis.

Valores monetários usam Decimal no backend e strings decimais no transporte. null significa indisponível. Taxa de 10% viaja como 0.10. O frontend não faz a regra de decisão e não converte ausência em zero.

Para o recorte, cada execução de cenário usa capital informado explicitamente; na ausência, usa a configuração global; na ausência dos dois, permanece pendente. Margem mínima e pesos vêm da configuração versionada para o cálculo. Recalcular o mesmo cenário não pode mudar o critério por usar outra rota.

O cenário conserva a cotação, um snapshot do preço e escopo de frete, o capital efetivo e as premissas usados. A análise se refere a um item identificado. Dashboard e filtros consideram a análise mais recente por item e o prazo atual; nunca qualquer resultado histórico positivo. Não agregar viabilidade de um item em viabilidade integral da contratação.

A migração inicial existente usa Base.metadata.create_all com os modelos atuais. A próxima revisão precisa inspecionar o schema, adicionar apenas estruturas ausentes e verificar as já presentes, sem tentar criar colunas duplicadas. O aceite inclui atualização de uma cópia do schema antigo e instalação desde um SQLite vazio. Não modificar silenciosamente a migração inicial para fazer um caso passar.

## 8 Como iniciar e continuar em outro chat

Todos os arquivos ficam em plans/mvp-apresentacao-2026-10-16/. Comece por PROMPT_MESTRE.md. Ele orienta o agente a ler o plano, STATUS.md, o contrato e o prompt da próxima sessão.

Abra uma conversa no mesmo workspace para cada sessão e cole o prompt mestre ou o conteúdo da sessão desejada. Não é necessário levar todo o histórico do chat anterior. Se a sessão foi interrompida, leia o repasse e continue a parte pendente antes de abrir a etapa seguinte.

No fim de cada sessão, criar repasses/SXX.md com objetivo, arquivos alterados, comandos e resultados, decisões, limitações e próxima ação. Atualizar STATUS.md somente com evidência. Manter cada etapa como planejada, em andamento, concluída ou bloqueada. Concluída exige critério de saída atendido.

Uma dependência bloqueada não autoriza marcar a etapa como concluída. Registrar o bloqueio, executar trabalho independente e aplicar o ensaio parcial de S07 e S08 se necessário. Os aceites completos continuam pendentes. Não pedir confirmação para correções reversíveis dentro deste escopo.

## 9 Execução e validação

Comandos existentes para validação local, no PowerShell:

~~~powershell
Set-Location "<checkout-local-radar_licita>\backend"
$env:APP_ENV = "test"
$env:AUTO_START_SCHEDULER = "false"
$env:PYTHONDONTWRITEBYTECODE = "1"
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
~~~

~~~powershell
Set-Location "<checkout-local-radar_licita>\frontend"
pnpm build
~~~

A sessão 1 deve verificar o destino de banco, documentos e tarefas antes dos testes. Os testes existentes usam SQLite em memória, mas novas suítes também precisam de isolamento. Nunca herdar automaticamente o destino operacional de DATABASE_URL.

Inicialização atual para desenvolvimento, a adaptar somente para o banco separado:

~~~powershell
Set-Location "<checkout-local-radar_licita>\backend"
$env:AUTO_START_SCHEDULER = "false"
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000
~~~

Em outro terminal, executar pnpm dev dentro de frontend/ e abrir http://localhost:5173. Saúde da API em http://127.0.0.1:8000/api/v1/health. Esses comandos atuais não criam a base de demonstração; usar os procedimentos que S02 e S08 entregarem.

Não copiar cegamente .env.example para uso local: há valores direcionados ao Compose, como host db e caminho /app/data/documents. Docker é alternativa opcional e não será requisito para o ensaio. Preservar pnpm-lock.yaml e evitar atualização de dependências.

## 10 Critérios de aceite final

O aceite técnico exige testes pertinentes passando e build atualizado, além de demonstração no navegador. A antiga contagem de 19 testes é a linha de base; a entrega deve registrar sua nova contagem real.

- Uma contratação encerrada fica fora do filtro de abertas, inclusive após reinício.
- Um item recarregado do SQLite pode ser cotado e analisado sem erro de fuso.
- A resposta de sincronização controlada inclui job_id e não exige nova chamada externa para validar o contrato.
- Os três casos sintéticos retornam os estados esperados pela API e pela interface.
- Cotação expirada, validade desconhecida, produto apenas possivelmente compatível, frete/tributos não confirmados ou logística indeterminada não resultam em análise positiva.
- Capital insuficiente e incompatibilidade comprovada bloqueiam; lote/grupo não recebe recomendação de item isolado.
- Cotação e cenário pertencem ao mesmo item; trocar de item limpa resultado anterior.
- Aquisição coincide com o preço da cotação selecionada e o frete tem escopo compatível; alteração manual sem confirmação impede indicação positiva.
- A migração funciona tanto no schema antigo copiado quanto em SQLite vazio, sem duplicar colunas.
- Resultado atual substitui o anterior nos resumos, sem apagar o histórico.
- Status comercial persiste e aparece no acompanhamento após recarga.
- Falha de API gera erro legível; falha de fonte externa não apaga a base local.
- O percurso funciona sem rede externa, com fontes locais, rótulos e documentos acessíveis.
- Nenhum banco original, credencial ou arquivo de motor_decisao/ foi alterado.


- Ao informar R$ 5.000, só itens com capital necessário conhecido de até R$ 5.000 aparecem como elegíveis; alterar para R$ 10.000 recalcula a lista e o saldo.
- Orçamento não mistura itens nem reutiliza cenário antigo barato, e uma resposta atrasada não sobrescreve a busca mais recente.
- Orçamento ausente, zero, negativo, limite exato e um centavo abaixo têm respostas verificadas.
- A ordem da lista segue score explicável, resultado estimado, prazo e identificadores, usando o mesmo contexto de orçamento.
- Busca de parceiros tem adapter implementado, leitura pública limitada testada e cache real; não é somente uma lista fixa.
- Um resultado real PJ chega à UI com compra, item, CNPJ, razão social, data, natureza e URL da fonte, sem virar cotação atual.
- Repetir a busca não duplica candidato/resultado; erro externo difere de nenhum resultado e explicita o recorte incompleto.
- Cadastrar uma candidata não cria estoque, disponibilidade, parceria ou cotação automaticamente.

## 11 Roteiro de apresentação

| Duração | Ação e mensagem |
| --- | --- |
| 40 s | Explicar a decisão comercial e a origem dos dados |
| 70 s | Informar R$ 5.000 e depois R$ 10.000; mostrar mudança da lista e capital por item |
| 70 s | Abrir uma oportunidade e conferir evidência e premissas |
| 100 s | Buscar possíveis parceiros em pregão, abrir resultado público e cadastrar candidata |
| 80 s | Mostrar cotação atual, simulação, margem e pendência ou bloqueio |
| 30 s | Alterar status e mostrar acompanhamento |
| 30 s | Expor limites e próximos passos |

Fala sugerida: Informo quanto posso investir e o Radar Licita mostra as oportunidades analisadas que cabem nesse capital. Para buscar fornecimento, o sistema localiza empresas em resultados de pregões de itens pertinentes e mostra a evidência. Histórico orienta a pesquisa; cotação atual e condições da operação sustentam a decisão.

A demonstração de orçamento usa exemplos financeiros sintéticos identificados. A busca de parceiros também precisa mostrar ao menos uma cadeia oficial real; se usar uma categoria diferente da financeira, explicar essa diferença e nunca relacionar produtos incompatíveis.

## 12 Plano reduzido e recuperação

Priorizar os dois novos requisitos. Manter um campo de orçamento, lista ordenada por item e justificativas; manter busca por item/termo em recorte pequeno, evidência oficial real e conversão para cadastro. Cortar gráficos, animações, preferências avançadas, busca nacional e filtros secundários.

Uma janela curta exige paralelismo e redução dos detalhes, não a antiga promessa de três horas para o novo escopo. Se faltar dado financeiro, a resposta de orçamento continua funcional, mas o item fica pendente e a lista de elegíveis pode estar vazia. Se a fonte de parceiros falhar durante a apresentação, consultar cache oficial da busca previamente testada, com data e aviso de desatualização.

Fixtures sintéticas testam erros e limites. Não substituem o aceite do coletor com resposta oficial real. Se o adapter não for implementado ou não houver teste real, marcar parceiros incompleto. Um retorno vazio correto comprova tratamento de ausência, não utilidade da descoberta; a cadeia real verificada no pacote deve ao menos atravessar parser, armazenamento e apresentação.

Se houver bloqueio técnico, S07 pode ensaiar as partes funcionais e S08 entregar material parcial, sem declarar as sessões faltantes concluídas. Preservar o banco original; parar a API antes de copiar a demo e testar restauração em outro arquivo. Não executar downgrade/exclusão no original.

## 13 Regras para alterar o plano

Toda mudança recebe data, motivo, impacto, dependências e aceite no histórico de STATUS.md. Pode reduzir escopo, dividir sessão ou inserir correção indispensável. Não pode remover critério de integridade para fabricar sucesso. Mudança de contrato após o congelamento exige atualização do contrato e comunicação aos agentes antes da retomada.

Não criar worktrees a partir da raiz sem commits nem inicializar o legado em outra história. Não fazer commit, push, deploy, contato com fornecedores ou envio de propostas por causa deste plano. A S05 executa leitura pública limitada para validar o adapter de parceiros, registrando limites e resultados. O ensaio da apresentação também precisa funcionar com cache local, sem fonte externa.

## 14 Fontes locais e arquivos de execução

Fontes de diagnóstico: README.md da raiz; backend/app/api/routes.py; backend/app/schemas.py; backend/app/models.py; backend/app/core/config.py; backend/app/core/time.py; backend/app/ranking/service.py; backend/app/finance/service.py; backend/tests/; frontend/src/App.tsx; frontend/src/api.ts; frontend/src/types.ts; frontend/package.json; frontend/pnpm-lock.yaml.

Contexto ampliado, sem impor implantação integral para amanhã: motor_decisao/docs/licitacoes/00-prd.md, 06-regras-de-negocio.md, 09-motor-de-decisao.md, 12-simulacao-financeira.md, 20-resumo-executivo.md e _progresso.md.

Entregas deste planejamento: README.md, PROMPT_MESTRE.md, CONTRATO_DEMO.md, STATUS.md ESCOPO_ORCAMENTO_E_PARCEIROS.md, evidencias/PNCP_VERIFICACAO_2026-10-15.json e SESSAO_01.md a SESSAO_08.md. Repasses, scripts de demo, testes novos e materiais finais são entregas futuras das sessões, ainda inexistentes neste momento.
