# ADR-023 — Adapter PNCP isolado e coleta condicionada ao contrato

2026-10-13. Escopo HU-001/TEC-04, RT-001/015, RN-026/027/028. Complemento de
ADR-010/014/021/022. Ciclo permanece em andamento enquanto faltar amostra real.

## Contrato e bloqueio de fonte

OpenAPI obtido do endpoint oficial em 2026-10-13 22:49:18 UTC, 37.943 bytes,
SHA256 a5d87b60a54acfd681e84e0a77d541b0db7065401171508c53c911d12bdd121d.
Snapshot em config/contracts/pncp-consulta-20261013.json, bytes preservados pelo
Git como -text. Define GET de publicação, dataInicial/dataFinal yyyyMMdd,
modalidade obrigatória, página >=1, tamanho 10–50 e envelope DTO. Exemplos
contêm datas geradas pela fonte; hash novo pode incluir apenas mudança de exemplo.
Nenhum contrato futuro é aceito automaticamente pela mesma versão textual 1.0.

O schema não esclarece inclusão das bordas ou fuso. Página oficial de manuais
redirecionou e respondeu 401; manual antigo encerrou a conexão. Não houve retry
autenticado ou tentativa de contornar restrição. Catálogo oficial de modalidade,
termos/quota e limites temporais continuam pendentes. Política versionada
pncp-publicacao-policy.json tem enabled=false; live_transport recusa antes de
ler saldo ou enviar dados. Não inferir os limites a partir do nome dos parâmetros.

## Transporte e orçamento

Alternativas: cliente com retries/redirects implícitos, quota em memória ou ledger
persistente com envio explícito. Adotada a terceira: flock no diretório compartilhado
serializa todos os processos da sessão inclusive o envio. Reserva JSON com fsync
e rename antes de DNS/conexão; cada falha continua consumida. Retomada exige arquivo
existente e reconciliação do contador; init explícito recusa diretório existente.
Não há renovação automática do teto. O ledger da sessão C2 inclui documentação.

Somente HTTPS:443 em pncp.gov.br/www.gov.br, sem usuário/senha na URL; rejeita
IPs não públicos em todas as respostas DNS, conecta diretamente um IP validado,
confere peer efetivo e valida TLS/SNI do host. Não usa proxies do ambiente,
retries automáticos, descompressão automática ou redirects automáticos. Redirect
de documentação foi seguido por chamada explícita contabilizada; coleta retorna
parcial no 3xx. Downloader de documentos permanece fora deste incremento.

Máximo 4 MiB por resposta, conexão 10 s, leitura 30 s com deadline do corpo.
Timeout mais curto que o proposto em 10 reduz ocupação nesta validação; não afirma
latência oficial. Intervalo >=1 s, maior Retry-After prevalece. Antes de novo envio,
callback da reserva confirma contador/lease do job; cota zero não incrementa o job.
Uma falha entre reserva global e registro do job pode consumir quota sem envio,
de modo conservador; jamais devolver a reserva. Bytes/status/headers permitidos
ficam no arquivo durável e no Bronze; conteúdo nunca é executado.

## Persistência, retomada e normalização

Reutiliza somente DDL C1; nenhuma revisão nova. Cada job tem janela/filtros/hash
fixos no checkpoint, política versionada e run próprio. A tentativa é reservada
antes do envio; a resposta Bronze é confirmada antes da transformação. Página
normalizada e cursor são uma transação; falha mantém cursor e bruto, com estado
parcial. Essa separação refina a transação única proposta em 07/21 para cumprir
a preservação do bruto inclusive em falhas de transformação. Não promove página
com identidade ausente, repetição ou envelope/contagens inconsistentes.

Fingerprint inclui fonte/recurso/parâmetros da URL e número da tentativa reservada.
Isso mantém registros distintos para bytes idênticos em 500 e 200, com status
correto. O índice existente continua idempotente para reprocessamento da mesma
tentativa. Versão comercial deduplica conteúdo independentemente do recibo.
JSON numérico usa Decimal; zero de estimativa fica no snapshot de origem, não
vira custo/receita ou item válido. Não são fabricados itens/unidades a partir de
cabeçalhos. Manifesto sinaliza itens/documentos pendentes. Datas sem offset
permanecem em texto original; prazo normalizado NULL, sem inferir UTC.

Lease de cinco minutos e fencing_token monotônico verificam posse antes de
tentativa, Bronze, página e encerramento. Trabalhador expirado não confirma
dados nem libera outro proprietário. Limite de três tentativas persistido por
página/job; Prefect retries=0. Falha transitória usa base 2 s, jitter até 0,25 s;
Retry-After >30 s grava not_before e suspende. A retomada não zera tentativas;
após esgotar três, exige decisão explícita em execução futura. Uma nova sessão
não deve apagar ledger/job antigo para fingir que houve menos chamadas.

204 ou envelope zero coerente concluem somente consulta inicialmente vazia;
vazio depois de página populada fica parcial. Nenhum desses resultados apaga
contratação local. Mudança de total ou ID repetido não é contada como completude.

## Isolamento e operação

Store exige banco test_ exato, OID e papel sem superusuário do runner, verificados
a cada transação. Prefixo sozinho não autoriza uso. Não aceita principal. Testes
usam dados sintéticos, com limpeza somente no banco descartável já validado.
Sem .env novo nem credencial em parâmetro Prefect. collect_pncp recebe job_id e
usa sessão já registrada; não cria deployment/schedule e não conecta ao importar.

Corpo manual do flow (.fn) é testado com transporte sintético; motor de orquestração
Prefect e coleta real não foram executados. Não declarar isso como validação de
deployment. Uma amostra futura precisará de destino isolado persistente/recuperável
e evidência de execução manual, além de fechar o gate documental. Nenhum serviço
legado, entrypoint, teste preexistente ou schema de produção foi alterado.
