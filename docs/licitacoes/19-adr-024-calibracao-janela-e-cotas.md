# ADR-024 — Cotas separadas e calibração por janela sobreposta

Decisão do operador recebida em 2026-10-15; retomada em 2026-10-18. Complementa
ADR-023 para o mesmo Ciclo 2. Esta autorização não permite migração principal,
ativação de schedules ou push.

## Cotas

Manter um ledger durável único para serializar envios, com limites independentes:
DOCUMENTAÇÃO=15 e DADOS=50. As sete tentativas já registradas (quatro em 13/10 e
três em 15/10) são classificadas como documentação conforme decisão expressa;
nenhuma foi consulta de contratação. Não apagar nem reescrever recibos. A migração
do ledger deve arquivar o estado anterior, conservar todos os sete recibos e fazer
as contagens reconciliarem. O limite da sonda é no máximo três das chamadas DADOS,
com contador persistente separado e teto imposto antes do envio.

No código, a validação do formato do ledger e a primeira versão da separação já
estão no commit `df6320e`. O ponto de entrada `--split-budget` e seleção de categoria
foram preparados nesta retomada, mas a migração real do arquivo ainda não foi
executada. O ledger continua no formato anterior até o runner Docker voltar.

## Fonte e janela

`docs/licitacoes/fontes/` não existe neste checkout; nenhum manual local foi usado.
Não fazer outras buscas de manual por rede. A ambiguidade temporal deixa de bloquear
por decisão do operador. Janela original 2026-10-07 a 2026-10-13 (America/Sao_Paulo)
fica fixa. A coleta principal usa sobreposição de um dia em cada lado:
2026-10-06 a 2026-10-14. Deduplicação por identidade/chave de conteúdo evita
duplicar versões, mantendo recibos e observações.

Calibração autorizada: até três requisições DADOS. Plano inicial usa 2026-10-13 a
2026-10-13 e, na segunda consulta, 2026-10-13 a 2026-10-14, mesmo código de
modalidade e filtros. Comparar as datas de publicação retornadas e registrar os
hashes/contagens sem expor dados pessoais. A comparação de uma única modalidade e
dois intervalos é evidência amostral, não prova completa da semântica temporal. A
terceira tentativa fica reservada para resposta vazia/ambígua ou falha transitória
documentada; não é chamada obrigatória.

O ledger contém quatro tentativas em 13/10 e três tentativas adicionais em 15/10:
uma URL de manuais retornou 404, outra central de conteúdo retornou 302 e `/app`
do domínio PNCP encerrou a conexão. Esses registros já existiam quando o operador
reclassificou as sete tentativas como DOCUMENTAÇÃO. Desde essa decisão não houve
consulta HTTP de documentação nem de dados. Os recibos e resultados originais são
mantidos no ledger.

## Evidência ainda necessária

Em 18/10/2026 o Docker Desktop abriu com erro interno e o daemon não criou o socket
`npipe:////./pipe/dockerDesktopLinuxEngine`; WSL `docker-desktop` permanece ativo,
mas `docker info` falha. Não foi possível migrar o ledger, criar/validar um destino
PostgreSQL isolado, rodar a suíte após `df6320e`, sondar ou coletar dados. Nenhum
novo envio foi feito em 18/10. A tentativa de testes anterior ao commit de correção
terminou com 8 falhas específicas de estado corrompido e 128 casos aprovados; o
fix foi incluído em `df6320e`, mas ainda não pôde ser verificado depois dele.
Retomar na mesma sessão e janela quando Docker estiver operacional, executar a
migração do ledger antes de qualquer chamada e preservar o principal na revisão
20260817_0009.
