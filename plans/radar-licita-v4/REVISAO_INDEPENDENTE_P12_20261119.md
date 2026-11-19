---
id: REV-P12-INDEPENDENTE
tipo: revisao
estado: sem_bloqueio_escopo_e2
classe_evidencia: E2
atualizado_em: 2026-11-19
---

# Revisão independente da P12 — 19/11/2026

**Versão examinada:** `096e4fa79d9a5627249a19841a12e576c5b73781`, contra a base P31 `e0eaa0b3b8c6034cf08dd4d18f8354efcbdc186f`.

**Parecer técnico:** sem bloqueio no recorte de contrato e casos sintéticos E2. O validador usa `Decimal`, soma receitas, aquisições e custos uma vez, trata a garantia recuperável no caixa, processa saídas antes de entradas no mesmo dia e confere o pico conjunto da carteira. Os valores são propostas para revisão financeira, não casos de ouro aprovados.

## Verificações

- `scripts/validate_financial_cases_p12.py`: 3 casos e 2 cenários de carteira válidos, em contêiner local sem rede e worktree somente leitura.
- `python -m unittest tests.test_financial_cases_p12 -v`: 4/4 passaram, inclusive preço ausente, custo repetido e pico incorreto.
- `scripts/lint_radar_vault.py --no-git`: OK estrutural; `git diff --check e0eaa0b..HEAD`: sem falhas.
- SHA-256 conferido da fixture: `7e0d7e6492cd9e8216fc9038563d433cc6f4dac00a9f69fa9e51b8435063efc7`.
- Nenhum motor de produção, banco, worker, PNCP, dado real ou remoto foi acessado.

## Pendências para o aceite

- O financeiro/contábil deve revisar tratamento tributário, reservas, cronograma e contas ao centavo. O validador exercita ausência do preço de proposta; não há caso para cotação, preço de aquisição, tributo aplicável ou data ausente. P13 deve manter esses resultados como pendentes, sem lucro confiável.
- O plano 1.0 vincula P12 a P07; o controle V4 lista P00 + P31. A revisão de E2 não resolve essa mudança de dependência nem autoriza iniciar P13. P07, decisão humana, G2 e integração permanecem pendentes.
- O código avaliado valida fixtures propostas, não implementa o motor financeiro do produto. E2 não demonstra viabilidade comercial, preço atual ou caixa livre real.

Próximo passo: obter parecer financeiro e alinhamento das dependências antes de decisão humana sobre a integração. Este parecer não aprova gate ou merge.
