---
id: P12-PACOTE
tipo: pacote_revisao
estado: aguarda_revisao_independente
atualizado_em: 2026-11-19
---

# P12 — pacote para revisão independente

**Data:** 19/11/2026. **Autor:** sessão desta automação. **Estado:** autoria offline preparada, sem PR remoto, revisão independente ou aceite humano.

## Base e escopo

- Base local: `e0eaa0b` (`origin/codex/radar-licita-produto-2026-11-04` observada no início). Branch `codex/radar-p12-finance-contract-20261119` em worktree próprio.
- Dependências P00 e P31 constavam integradas no controle e no Git local. P02/P03 não foram usados como dependências.
- Fontes: plano interno 1.0 §5 I06/§5A P12; [cartão](prs/P12.md); [decisões](DECISOES.md); [simulação financeira documental](../../docs/licitacoes/12-simulacao-financeira.md); [protótipo D](../../docs/licitacoes/radar-viability-offline.md).
- Arquivos desta autoria: `CONTRATO_FINANCEIRO_P12.md`, `fixtures/radar_finance_p12/cases.json`, `scripts/validate_financial_cases_p12.py`, `tests/test_financial_cases_p12.py`, este pacote, cartão P12 e linha do controle. Nenhum motor de produção, banco, worker, catálogo real ou configuração operacional foi alterado.

## Evidência e verificações

Os valores dos casos são **sintéticos E2**. O validador recusa entrada decimal não textual, resultado numérico diante de preço ausente, custo duplicado, divergência de fluxo e pico de carteira diferente da linha temporal conjunta. Calcula linha de receita/aquisição com `Decimal` e `ROUND_HALF_UP`; no mesmo dia, processa saída antes de entrada. A garantia recuperável só afeta caixa. A carteira junta os eventos de dois casos simultâneos, sem somar picos individuais.

- Validador em contêiner existente `motor_decisao-radar-ops:latest`, com `--network none` e montagem somente leitura: 3 casos válidos, 2 cenários na carteira.
- `unittest` no mesmo contêiner: 4/4 testes aprovados, inclusive mutações negativas.
- SHA-256 de `fixtures/radar_finance_p12/cases.json`: `7e0d7e6492cd9e8216fc9038563d433cc6f4dac00a9f69fa9e51b8435063efc7`.
- `git diff --cached --check`: sem erros. Lint documental `--no-git`: aprovado após corrigir 12 falhas iniciais de frontmatter nestes dois documentos. A verificação de referências a commits pelo modo Git não foi executada no contêiner.
- O `python` do Windows é apenas o atalho da Microsoft Store; não foi instalada dependência. O contêiner já existente permitiu a validação isolada.

## Limites e decisão requerida

P12 congela apenas contrato e expectativas sintéticas; não implementa P13, não avalia licitação e não valida tributo, preço, estoque, caixa livre nem viabilidade. A política monetária documental é proposta e pode exigir adaptação à regra do edital. O financeiro deve revisar os casos ao centavo e as premissas antes de G2. Uma sessão distinta deve conferir diff, fórmula, fixture, testes, origem E2 e ausência de dados reais. Luis decide eventual integração separadamente; este pacote não aprova G2.

**Como reproduzir:** no worktree P12, executar `python scripts/validate_financial_cases_p12.py` e `python -m unittest tests.test_financial_cases_p12 -v` com Python disponível. Nesta máquina, usar o contêiner já existente, sem rede, montando o worktree como `/src` somente leitura e `--entrypoint python`.
