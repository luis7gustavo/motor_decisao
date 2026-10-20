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

## Interrupção de 18/10, resolvida na retomada de 20/10

Em 18/10/2026 o Docker Desktop abriu com erro interno e o daemon não criou o socket
`npipe:////./pipe/dockerDesktopLinuxEngine`; WSL `docker-desktop` permanece ativo,
mas `docker info` falhou naquele turno. Nenhuma sonda ou coleta ocorreu em 18/10.
A tentativa de testes anterior ao commit de correção terminou com oito falhas
específicas de estado corrompido e 128 casos aprovados; o fix foi incluído em
`df6320e`. Na retomada de 20/10, Docker voltou, o ledger foi migrado e a suíte
passou integralmente, conforme evidência abaixo.

## Evidência final — retomada 2026-10-20

Docker 29.8.2 / Compose 5.5.1 operacionais. O ledger foi migrado sem descartar
recibos: DOCUMENTAÇÃO 7/15; DADOS 3/50; calibração 2/3. Testes finais: 140
aprovados em 19,33 s, uma depreciação Starlette/httpx; manifesto
`backups/20261020T152553Z_78c9da2b.json`. A falha anterior de oito testes ocorreu
antes do fix de `df6320e`; a suíte final passou com o fix aplicado.

Destino isolado criado do dump C1, restaurado e validado:
`test_20261020_ba894ce71405`, OID 2508600. Dump SHA256
`ad9ad83e61b8db747dfaed022ffa2f077a3340e3abe24c7084825f3ae1763fef`.
Manifesto `backups/20261020T152131Z_e6215d7c.json`: nove tabelas C1, 19 FKs
RESTRICT, inventário legado preservado, zero privilégios da role fora do conjunto
de ingestão. Alvo e role ficam persistentes para retomada, sem limpeza automática.

Calibração: duas respostas 200, dez registros cada, datas `2026-10-13` em ambas,
nenhuma sem data; hashes `ab65ad557fdc3dcbd65e0284d624025ef8606268fa69c59461e7d3cf4a23b9ad`
e `5c27f4b1321b0fbde7ed3b290d18651c00d8182d19a0d374be9f987c193f364`. Não houve
retorno publicado em 14/10; portanto, o teste não resolve se a borda superior é
incluída. Resposta identificou o código 6 como “Pregão - Eletrônico”; isso comprova
somente a modalidade observada neste recorte, não valida o catálogo inteiro.

Coleta real mínima: um GET 200, 18.894 bytes, hash
`f41cc2aa777f65aca5e5e2094877b5d89ee64838ebc1813c8c94e7ce7890df8e`; dez
contratações com publicação 2026-10-06, dez versões e dez observações. Job
`e47c6fe2-2cc9-4b58-803a-49a4aa44a443` fica parcial em `next_page=2` por limite
intencional de uma página; pode retomar no mesmo destino. Nenhum item foi inferido.
Janelas de calibração, job, contagens, hashes e quota estão preservados em
`backups/c2/c2-20261013/`.

Banco principal verificado novamente em `20260817_0009`; nenhum schema
`procurement` ou `commercial` aplicado ali. Nenhuma schedule ou push executados.
Termos/quota oficial, borda superior, cobertura nacional e flow pelo motor Prefect
seguem pendentes; a entrega real é uma amostra de uma página/modalidade. C2 segue
em andamento até o operador decidir a continuação da coleta limitada.
