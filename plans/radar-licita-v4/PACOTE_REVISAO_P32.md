---
id: REV-P32
tipo: pacote_revisao
estado: aguardando_verificacao
atualizado_em: 2026-11-18
---

# Pacote de revisão — P32

## Decisão pedida

Após verificação independente, Luis poderá aprovar, pedir mudanças, rejeitar ou registrar incerteza sobre a integração da P32. Nenhuma decisão ou merge foi presumido.

## Branch e entrega

- Base integrada: `058514f054981669357b6d4083dbb9497883bad7` na branch `codex/radar-licita-produto-2026-11-04`, conferida por `git fetch` em 18/11/2026.
- Branch autora: `codex/radar-p32-lint-20261118`; worktree exclusivo: `motor_decisao_p32_20261118`.
- [PR #6 em rascunho](https://github.com/luis7gustavo/motor_decisao/pull/6), commit inicial publicado `84a7783184410e0e825bdee204cc858a89b95a52`. A verificação deve examinar o HEAD final do PR.
- Entrega: [CLI](../../scripts/lint_radar_vault.py), [testes](../../tests/test_lint_radar_vault.py), instruções no [README](README.md), [cartão](prs/P32.md) e atualização do [controle](CONTROLE_DE_PRS.md).
- A CLI valida estrutura e referências da documentação versionada. Não altera dados nem aplicações em execução.

## Evidência e testes da autoria

- Fonte: [cartão P32](prs/P32.md), [proposta V4](PROPOSTA_V4_20261117.md), [decisões](DECISOES.md) e estado da branch base em 18/11/2026. Classe E1/E2: documentação e testes sintéticos.
- `python scripts/lint_radar_vault.py --no-git` no contêiner `motor_decisao-radar-ops:latest`: OK nas notas atuais.
- `python -m pytest -p no:cacheprovider tests/test_lint_radar_vault.py -q` no mesmo contêiner: 8 testes passaram, incluindo casos inválidos.
- Git do host: 10 referências hexadecimais curtas citadas nas notas resolvem como commits. O contêiner não dispunha de Git; essa verificação foi executada separadamente.
- `git diff --cached --check`: OK após preparar os seis arquivos do PR; diff inspecionado pela autoria. A revisão independente ainda precisa examinar o commit publicado.

## Limites e reversão

O lint valida forma, presença de arquivos e resolução local de commits; não consulta PNCP, não valida SHA-256 contra arquivos externos ou manifestos privados, não autentica decisões humanas e não atesta cobertura, frescor, dados E3/E4 ou qualidade comercial. Para desfazer, reverter somente os arquivos deste PR.

## Verificação e decisão pendentes

Verificador em sessão distinta: **pendente**. Achados e commit examinado: **pendentes**. Decisão humana e merge: **pendentes**.

Na primeira leitura independente do commit `b24d180de39f84a8f88aa7532084f6ff07cea348`, foram identificadas duas lacunas: uma nota nova sem frontmatter e um embed `![[...]]` quebrado passavam sem aviso. A autoria corrigiu ambas e adicionou testes de regressão. A rechecagem independente do novo HEAD ainda está pendente.
