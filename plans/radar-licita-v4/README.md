# Radar Licita — execução por PR

**Estado:** [P30 integrado no PR #4](https://github.com/luis7gustavo/motor_decisao/pull/4); [P00 integrado no PR #5](https://github.com/luis7gustavo/motor_decisao/pull/5); [P32 integrada no PR #6](https://github.com/luis7gustavo/motor_decisao/pull/6); G0 documental aprovado com limites em 18/11/2026. [P31 está em revisão no PR #7](https://github.com/luis7gustavo/motor_decisao/pull/7); os gates seguintes e o escopo técnico integral da V4 continuam propostos ou pendentes.

Este diretório contém o controle vivo da execução do Radar Licita. Abra `plans/radar-licita-v4/` como cofre no Obsidian para consultar os mesmos arquivos Markdown que entram nos PRs, sem indexar código e dados do restante do repositório. O cofre preexistente `RADAR LICITA` continua como biblioteca de fontes e sínteses datadas; não mantenha duas cópias editáveis da mesma decisão.

## Ordem de leitura

1. [Decisões e pendências](DECISOES.md)
2. [Controle e fila de PRs](CONTROLE_DE_PRS.md)
3. [Instruções das sessões](INSTRUCOES_SESSOES.md)
4. [Cartão do PR](prs/P30.md) ou outro cartão em `prs/`
5. Fontes e evidências indicadas no cartão

## Documentos

- [Proposta V4 recebida em 17/11/2026](PROPOSTA_V4_20261117.md): cópia de referência, **não** aprovação integral do plano.
- [Decisões](DECISOES.md): separa o método aprovado de escolhas ainda pendentes.
- [Controle de PRs](CONTROLE_DE_PRS.md): resumo do que aconteceu e fila sugerida por dependências.
- [Instruções das sessões](INSTRUCOES_SESSOES.md): papéis, handoff, verificação e limites.
- [Permissões e fontes](PERMISSOES_E_FONTES.md): o que pode ser consultado e o que requer autorização específica.
- [Modelo de cartão](MODELO_PR.md) e [modelo de revisão](MODELO_REVISAO.md).
- [Pacote de revisão P30](PACOTE_REVISAO_P30.md): histórico da publicação e verificação; integração registrada no controle.
- [Linha de base P00](LINHA_DE_BASE_P00.md), [políticas e critérios](POLITICAS_E_CRITERIOS_P00.md) e [pacote de revisão](PACOTE_REVISAO_P00.md): contrato documental aprovado com limites e estado atual do PR.
- [Lint P32](../../scripts/lint_radar_vault.py) e [pacote P32](PACOTE_REVISAO_P32.md): verificação offline da estrutura das notas e referências.
- [Inventário P31](INVENTARIO_AMOSTRAS_P31.md), [schema observado](SCHEMA_E_EXEMPLOS.md), [manifesto de hashes](MANIFESTOS_E_FIXTURES.md) e [pacote P31](PACOTE_REVISAO_P31.md): exemplos E0, caso E2 e lacuna de autorização para E1.
- [Cartões de PR](prs/): uma nota por PR, com identificador estável.

Os cartões derivados da V4 são **planejamento**. O início de cada PR exige conferir o código, as fontes, os bloqueios e o escopo efetivamente aprovado. Dependências satisfeitas e testes verdes não elevam uma classe de evidência para outra.

## Lint local das notas

Na raiz do repositório, execute `python scripts/lint_radar_vault.py` para verificar as notas de execução e resolver commits pelo Git local. Use `python scripts/lint_radar_vault.py --no-git` somente quando Git não estiver disponível; esse modo não confirma referências de commit. O comando não acessa a rede e não abre as fontes de dados externas citadas. O código de saída 0 indica integridade estrutural das notas verificadas, não aprovação humana ou validade das fontes.
