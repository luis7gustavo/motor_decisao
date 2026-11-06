# S06 — proposta de sondas reais limitadas

Data: 28/10/2026. Esta é uma proposta de validação, **não uma autorização nem um registro de chamadas executadas**. Política vigente: `enabled=false`, teto externo de dados zero; C2 e banco principal preservados.

## Para que servem

As especificações oficiais confirmam a estrutura das consultas, mas não informam quota numérica, primeira data recuperável por modalidade, comportamento das bordas das datas nem resposta real dos recursos de enriquecimento. Uma amostra pequena pode testar transporte, parser, recibos, paginação e retenção; não prova a cobertura nacional nem permite ativar S07 sozinha.

## Teto proposto

- No máximo **12 tentativas HTTP de dados PNCP no total**, incluindo falhas, timeouts, 429 e retries. Nenhum reinício renova esse teto.
- Uma conexão por vez e intervalo local mínimo de 1 segundo entre reservas, subordinados a qualquer limite oficial mais restritivo que venha a ser encontrado. Respeitar `Retry-After` sem reduzi-lo.
- Reservar atomicamente cada tentativa no controle persistente compartilhado da fonte **antes** de DNS/conexão; registrar resposta bruta/erro, hash e resultado. O coletor C2 conserva seu ledger próprio e não recebe novas chamadas.
- Não iniciar o backfill nacional ou a agenda recorrente com esse teto de sondagem. Parar se houver 401/403, 400/422 contratual, 429, redirecionamento inesperado, divergência estrutural ou fonte instável, conforme o resultado tipado e a espera oficial.

## Ordem das sondas, sempre condicionada à anterior

1. Um GET do catálogo `/api/pncp/v1/modalidades` sem `statusAtivo`, para observar códigos ativos e inativos. Máximo acumulado: 1.
2. Até três GETs de publicação e atualização com uma modalidade observada e janelas curtas, para testar formato, bordas, paginação e identidade sem filtros geográficos. Máximo acumulado: 4.
3. Até cinco GETs encadeados a **uma identidade efetivamente retornada**: detalhe, itens, resultados de um item existente, lista de arquivos e histórico. Não inventar item ou URL; omitir a sonda cujo pré-requisito não exista. Máximo acumulado: 9.
4. Até dois GETs de uma janela antiga, com modalidade observada, para investigar cobertura recuperável. Resposta vazia não prova início absoluto da fonte. Máximo acumulado: 11.
5. Uma tentativa de contingência, somente para repetir uma sonda elegível após falha transitória e depois de `Retry-After`/backoff; sem uso automático. Teto total: 12.

Antes de executar, registrar escopo efetivo da restrição (por exemplo, IP de saída), destino segregado/OID, quota já consumida por outras missões, espaço livre, contrato/hash e identificador da missão de sondagem. Uma sondagem satisfatória ainda exigirá orçamento dimensionado, cobertura/modalidades por período, reconciliação da fonte mutável e Prefect compartilhado saudável para a carga e a agenda reais.
