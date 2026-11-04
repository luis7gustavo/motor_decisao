# Radar Licita — entrega B offline, 04/11/2026

Branch/worktree isolado: `codex/radar-licita-produto-2026-11-04` em `motor_decisao_produto`, criado do commit `4a9b91d34ff0d2bf38033cd085f501918f19b501`. O checkout operacional e seus mounts de Prefect não foram editados por esta entrega.

## Escopo demonstrado

- `/radar-demo`: busca local por objeto, UF, categoria, estado e intervalo de prazo; página de resultados com identidade única e versão atual.
- `/radar-demo/{source_id}`: fonte, versão, data do snapshot, prazo e versões anteriores.
- `/radar-demo/alteracoes`: retificações preservadas; aviso local na busca, sem envio externo.
- Fixture versionada e explicitamente sintética, sem consulta PNCP, banco C2 ou banco principal. Resposta vazia, erro de leitura e snapshot desatualizado são estados visíveis. Fonte marcada indisponível conserva os registros locais e perde o selo de frescor.

## Matriz de liberação

| Saída | Evidência usada | Lacuna para dados reais |
|---|---|---|
| Busca/identidade | Fixture com IDs distintos, duplicata idêntica removida e versões ordenadas | Gate A3: identidade, vínculos, versões e integridade no recorte real |
| Estado aberto | Campo sintético de abertura combinado com prazo | Gate A1/A2: classificação oficial e frescor por recorte; prazo ausente é indeterminado |
| Alteração/alerta | Duas versões sintéticas da mesma identidade | Gate A2/A3: retificação real e atualização sem perder evento anterior |
| Fonte/frescor | Timestamp e estado da fixture | Sync bem-sucedido, última atualização oficial e tolerância de frescor ainda não medidos |

## Verificação e limites

Quatro testes focados passaram no container `motor_decisao-api` com `--network none` e mount apenas do worktree. Cobrem retificação, deduplicação, filtros, classificação conservadora, paginação, frescor, páginas HTML, estado vazio, 404 e indisponibilidade. `git diff --check` passou. O runner registrou um aviso de depreciação do `fastapi.testclient`/Starlette, sem falha.

Esta é uma prévia offline da Etapa B, não integração real nem MVP. Não há persistência de alertas, revisão independente do diff, medição de RNF ou liberação de dados PNCP. Antes de integrar com C2/principal, cumprir Gate A pertinente e G0–G4 da migração em cópia. C e D podem usar contratos/fixtures independentes; E depende da jornada de D.
