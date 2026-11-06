# Consulta técnica ao PNCP — cobertura histórica e limites da API

**Estado:** minuta pronta; não enviada. Em 29/10/2026 o usuário informou que fará o encaminhamento. Após a decisão `S06_DECISAO_ESCOPO_ACESSIVEL_20261029.md`, a resposta é complementar e não bloqueia a coleta do universo acessível pela API. Canal oficial localizado: [Canais de Atendimento do PNCP](https://www.gov.br/pncp/pt-br/acesso-a-informacao/canais-de-atendimento/), que direciona à Central de Atendimento/Portal de Serviços.

## Texto proposto

Prezados(as),

Estamos planejando uma coleta auditável de todas as contratações recuperáveis pela API pública de Consulta do PNCP, preservando retificações e respeitando limites operacionais. Para definir um intervalo histórico verificável e um orçamento de requisições, solicitamos esclarecimentos sobre as rotas `/api/consulta/v1/contratacoes/publicacao` e `/api/consulta/v1/contratacoes/atualizacao`:

1. Qual é a primeira data recuperável em cada rota? Há contratações inseridas posteriormente cuja data de publicação precede o início de operação do portal? Existem períodos antigos não disponíveis ou retenção limitada?
2. `dataInicial` e `dataFinal` incluem os dois dias? Qual fuso governa a classificação de uma contratação em cada dia e qual é a amplitude máxima aceita para uma janela de consulta?
3. O GET `/api/pncp/v1/modalidades` sem `statusAtivo` contém também modalidades inativas e todos os códigos já usados historicamente? Se algum código foi removido do catálogo, onde obter sua lista histórica?
4. Quais limites de taxa, concorrência e quota se aplicam às consultas públicas sem autenticação? A restrição é por IP de saída, aplicação, credencial ou outro escopo? Há cabeçalhos de quota e `Retry-After` para 429?
5. Há exportação oficial integral de contratações e respectivas atualizações/retificações, com documentação de cobertura, que possa ser usada para reconciliar a API paginada? Como o consumidor deve lidar com páginas que mudam durante a varredura?

Uma referência a documento oficial ou resposta com número de chamado ajudará a manter a rastreabilidade. Não solicitamos credenciais nem acesso de manutenção.

Atenciosamente,

## Uso da resposta

Arquivar a resposta e seu identificador, registrar data e versão do contrato, atualizar somente os gates sustentados pela evidência e manter pendências explicitamente não identificadas. A resposta não altera automaticamente o orçamento externo nem ativa a missão.
