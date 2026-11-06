# S06 — decisão de escopo: histórico acessível pela API

Em 29/10/2026 o usuário esclareceu que “todo o histórico” e “todo o disponível” significam os dados que seja possível acessar. Aquilo que a fonte não permitir acessar pode ficar fora da carga. Esta decisão substitui, para o aceite operacional, a exigência anterior de obter do atendimento do PNCP prova da primeira data absoluta ou de códigos de modalidades apagados do catálogo. O usuário encaminhará a consulta técnica se desejar; a resposta é complementar e **não é dependência para prosseguir**.

## Critério operacional

- Percorrer as duas listagens oficiais validadas, publicação e atualização global, em todas as modalidades atualmente expostas e em todas as janelas do intervalo acessível definido por sondas controladas. Não filtrar UF, categoria, situação ou palavras-chave na aquisição.
- Registrar cada partição tentada, total/páginas/identidades observados, tentativas e códigos HTTP. HTTP 204 numa janela aceita é evidência de resultado vazio; não é falha de acesso.
- Retentar com limites persistentes os erros transitórios, como timeout, 429 e 5xx. Até resolver ou esgotar a política, a partição permanece pendente. Uma rejeição estável de requisição validada, rota inexistente ou recurso indisponível fica como lacuna de fonte, com evidência; erro de parâmetro ou implementação deve ser corrigido e consultado novamente. Nenhum desses casos vira registro coletado ou “cobertura integral”.
- Preservar Bronze, identidades, observações e versões, inclusive retificações recuperáveis. Enriquecimentos de itens/resultados/documentos só contam como completos por recurso quando efetivamente coletados; indisponibilidade documentada pode ser excluída do universo acessível daquele recurso.
- Sem snapshot ou exportação independente, classificar o resultado como cobertura do universo **observado na API**, com risco residual de paginação mutável e de conteúdo não exposto. Não declarar todas as licitações brasileiras nem ausência absoluta de omissões.

## Evidência que sustenta a alteração

A campanha `S06_CALIBRACAO_20261029.md` observou o catálogo atual 1–19, consulta de inativas vazia, HTTP 200 nas duas listagens e equivalência das uniões de dias consecutivos numa amostra da modalidade 1. Isso sustenta habilitar apenas os **gates de rota e catálogo acessível**, e marcar as bordas diárias como validadas empiricamente para planejamento. Não prova o primeiro dia recuperável nem as regras oficiais de fuso para todos os períodos.

## Dependências, orçamento, saída e reversão

S06 passa a exigir descoberta limitada por orçamento do início operacional acessível, coordenação comprovável de todos os consumidores PNCP conhecidos sob a mesma restrição efetiva, dimensionamento e teto cumulativo da missão. A política principal permanece `enabled=false`, orçamento externo zero, e nenhuma partição histórica foi criada. O orçamento da calibração continua em 33/36 na fonte, desabilitado; a nova missão de sonda permanece pausada em 22/24. Nenhuma tentativa consumida é renovada.

S07 só pode ativar a recorrência após prova real S06. S08 só pode aceitar a coleta depois de percorrer e auditar todas as partições e recursos **acessíveis** do manifesto, resolver erros transitórios e classificar lacunas de fonte e risco residual. Se uma resposta oficial ou sondas futuras mostrarem que o catálogo, datas ou limites usados estavam incompletos, ampliar o manifesto e reabrir as partições afetadas; pausar a missão e reverter os gates de rota/catálogo se a evidência de acesso se invalidar. Não apagar dados ou ledger já gravados.
