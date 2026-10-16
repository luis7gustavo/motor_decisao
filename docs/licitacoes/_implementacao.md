# Implementação Radar Licita — Ciclo 2

Estado: **em andamento; entrega parcial**. Iniciado em 2026-10-13, retomado em
2026-10-14 por “continue”. A retomada não renova orçamento nem desloca janela.
Branch feat/radar-licita-implementacao. Base af1a5e0353947342462cc5c01d369f0e4c569309.
Código C2: d417ca81b00e8e361251b50b2c9cdbd7167f32e8.

## 1. Histórico e parâmetros efetivos

C0 concluído em 50eeaeb; C1 concluído com implementação 2a7153e e documentação
af1a5e0. Registro C1 preservado em [_implementacao-historico-c1.md](_implementacao-historico-c1.md).
Históricos C0 permanecem nos arquivos próprios. Documentação alheia não rastreada
preservada fora dos commits, conforme ADR-021.

| Parâmetro | Valor mantido nesta retomada |
|---|---|
| Ciclo | 2, EP-01 / HU-001 parcial e TEC-04 |
| Categoria | não escolhida; não impede infraestrutura independente |
| Janela absoluta | 2026-10-07 a 2026-10-13, America/Sao_Paulo |
| Campo previsto | dataPublicacaoPncp; bordas/fuso da consulta não confirmados |
| Sessão HTTP | c2-20261013 |
| Orçamento | 50 tentativas; 4 consumidas, 46 restantes |
| Intervalo | mínimo 1 s, serializado entre processos |
| Principal / schedules / push | não / não / não |

## 2. Entrega e aceites

- C2-01: snapshot OpenAPI oficial, política desabilitada até confirmar semântica,
  transporte HTTPS com validação DNS/peer/TLS, reserva durável antes de envio,
  limite de bytes/tempo e respostas arquivadas. Implementado e testado offline.
- C2-02: paginação, Bronze confirmado antes de transformar, Decimal, idempotência,
  cursor transacional, lease/fencing, tentativas persistidas e Retry-After.
  Implementado e testado com dados sintéticos em banco isolado.
- C2-03: corpo de flow manual com retries=0, sem deployment/schedule. Testado via
  .fn com transporte sintético. **Motor Prefect e amostra real não validados**.
  Aceite integral e encerramento pendentes; HU-001 não declarada concluída.

Não há coleta de itens/documentos/resultados, validação fiscal ou ativação
comercial neste incremento. Cabeçalhos não geram itens inventados. Datas sem
offset permanecem originais, com prazo normalizado desconhecido.

## 3. Testes e evidências

Comando canônico, executado em Docker:

```powershell
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps radar-ops tests --origin 20261013_0010
```

Baseline C2: 95 aprovados (20261013T224649Z_bfe1f10b). Resultado final:
**128 aprovados em 22,88 s**, zero falhas/erros/ignorados, um warning preexistente
Starlette/httpx. São 95 casos anteriores e 33 novos (17 transporte, 16 ingestão).
Manifesto backups/20261013T230028Z_5aee84a9.json, operation_passed=true,
pytest_exit_code=0 e cleanup_errors vazio. Banco test_20261013_2ce11901c2b1 e
papel radar_test_70decda65c0b removidos pelo runner após verificar propriedade.
Nenhum teste legado alterado. Fixtures offline, sem envio a PNCP durante a suíte.

Etapas vermelhas: módulos ausentes antes da implementação; depois três falhas
(quota esgotada, envelope vazio, flow ausente) e uma falha de recibos com corpo
igual/status diferente. Todas corrigidas e cobertas na suíte final. Registro
20261013T225855Z_805714b3 preserva a última falha antes da correção do fingerprint.
Não são falhas preexistentes; não houve exclusão de testes para obter aprovação.

Retomada de 2026-10-14: Compose base e aditivo válidos; SHA256 do contrato igual
ao capturado; banco principal 20260817_0009; zero bancos test_/rehearsal_, zero
papéis radar_test_ e zero schemas procurement/commercial no principal. Inventário
somente de leitura: prefixos não autorizam limpeza. Healthchecks existentes
saudáveis; workers/prefect-services sem healthcheck. Nenhum serviço iniciado ou
parado por esta retomada. Nenhum runtime alterado após a suíte final.

## 4. Migração e restauração

Nenhum DDL novo no C2. Testes aplicam revisão C1 explícita 20261013_0010 somente
no banco sintético próprio. Ensaio de backup/restauração/upgrade/downgrade/upgrade
da revisão C1 permanece registrado no histórico, não repetido sem nova migração.
Principal continua 20260817_0009. Não houve aplicação principal, agendamento ou push.

## 5. Rede e contrato

Ledger durável: backups/c2/c2-20261013/ledger.json; bytes em arquivos .body no
mesmo diretório, ignorados pelo Git. Não recriar, apagar ou executar --init na
retomada. Saldo **4/50 consumidas, 46 restantes; zero consultas de contratações**.

| Tentativa | Recurso oficial | Resultado |
|---|---|---|
| 1 | pncp.gov.br/api/consulta/v3/api-docs | 200, 37.943 bytes |
| 2 | www.gov.br/pncp/pt-br/acesso-a-informacao/manuais | 302, contado separadamente |
| 3 | www.gov.br/pncp/pt-br/pncp/manuais | 401, sem contornar acesso |
| 4 | pncp.gov.br/manual/pt-br/latest/ | RemoteDisconnected, cota consumida |

Contrato recebido em 2026-10-13 22:49:18 UTC; snapshot público versionado em
config/contracts/pncp-consulta-20261013.json, SHA256
`a5d87b60a54acfd681e84e0a77d541b0db7065401171508c53c911d12bdd121d`.
Define formato e parâmetros de publicação, mas não comprova inclusão/exclusão das
bordas nem fuso. Catálogo de modalidades e termos/limites de consumo também não
foram confirmados. Não inferir limites por experimentação com contratações.

## 6. Bloqueios, desvios e retomada

B2-01: sem evidência oficial dos limites temporais, modalidade e regras de consumo,
política pncp-publicacao-policy.json permanece enabled=false; live_transport
recusa antes de qualquer envio. Documentação inacessível não prova inexistência
da regra. Obter referência oficial e registrar evidência antes de liberar a fonte.

B2-02: amostra real e execução pelo motor Prefect pendentes. Store atual só aceita
banco/papel/OID do runner sintético. Antes da amostra, preparar destino isolado
persistente e recuperável com validação equivalente de propriedade, separado das
fixtures; não flexibilizar a guarda para apontar ao principal. Não há cursor real
ou contagem real de contratações a apresentar.

[ADR-023](19-adr-023-adapter-pncp.md) explica decisões: ledger em arquivo com lock,
30 s de leitura, Bronze separado da transação de página, fingerprint por tentativa
e restrição de isolamento. [Rastreabilidade C2](17-rastreabilidade-ciclo-2.md) e
[operação C2](operacao-ciclo-2.md) distinguem implementação de validação operacional.

Próxima retomada continua **este mesmo ciclo**, janela e ledger. Fechar B2-01,
preparar destino de validação e executar amostra mínima dentro das 46 tentativas
restantes; depois validar orquestração manual e registrar evidências. Não iniciar
C3 nem declarar C2 concluído enquanto os aceites reais estiverem pendentes.

## Plano original preservado

## Sessão C2 — plano antes da implementação (2026-10-13)

Nova execução explicitamente autorizada por “faça isso”. Base af1a5e0353947342462cc5c01d369f0e4c569309, encerramento C1; branch feat/radar-licita-implementacao; nenhum diff rastreado inicial. Documentação alheia não rastreada preservada. Docker Compose 5.5.1 e Engine 29.8.2 respondem.

Parâmetros efetivos: ciclo automático → 2; categoria não escolhida; janela 2026-10-07 a 2026-10-13 America/Sao_Paulo; campo filtrado e limites dependem do contrato antes da coleta. Orçamento global 50 tentativas incluindo documentação/validação de contrato/redirects/retries, intervalo mínimo 1 segundo, uma tentativa por vez entre processos. Aplicar principal não; schedules não; push não. Nenhuma ampliação de permissões. Sessão de rede c2-20261013; ledger durável fora do Git, reservado antes do envio, sem reposição após erro/reinício.

Escopo fixado: EP-01, HU-001 (RF-001, RN-026/027, RT-001), com TEC-04 de contrato e suporte parcial à identidade/versionamento C1. C2-01 contrato/transport/ledger, C2-02 adapter paginado com Bronze antes da transformação e checkpoint idempotente/retomável, C2-03 flow Prefect manual sem deployment/schedule, validação real delimitada e encerramento. Não incluir HU-002/003/004/005 completas, itens/documentos/resultados, validação fiscal ou operação comercial. Uma única modalidade explícita de validação não será chamada cobertura nacional.

Aceites: duas páginas sintéticas e três IDs; replay dez vezes sem novas versões; página 2 falha preserva página 1, estado parcial e cursor; 204 sem apagar local; envelopes inválidos/instáveis não concluem; 400/401/403 sem retry cego; até três tentativas totais e Retry-After respeitado; cada envio/redirect reserva cota antes; quota compartilhada e durável, rede serial; bytes/hash preservados antes de transformar; JSON numérico Decimal, prazo sem offset permanece desconhecido; fonte/domínio/porta/IP público efetivamente conectado validados; testes offline e banco isolado; amostra real somente após contrato e semântica da janela confirmados. Falha de gate mantém coleta bloqueada e ciclo em andamento, sem inventar contrato.

Leituras: progresso integral C1, 10 integral, roadmap EP-01, 04/14 e ADR-010/014/018–022; modelos/migração/runner C1 e Prefect existente. Continuação da leitura 08/16/21 e contratos específicos conforme necessidade. Sem mudança de esquema prevista; infraestrutura existente reutilizada e sem aplicação principal. Se DDL se tornar necessário, registrar antes e cumprir backup/ensaio novos. Ambiente: banco temporário próprio na infraestrutura existente, fixtures separadas de eventual amostra real; evidências/bytes e orçamento duráveis em backups/c2, ignorado pelo Git.
