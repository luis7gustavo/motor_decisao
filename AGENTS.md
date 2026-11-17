# Radar Licita — sessões de desenvolvimento

Antes de trabalhar em um PR do Radar Licita, leia [o índice de execução](plans/radar-licita-v4/README.md), [as decisões](plans/radar-licita-v4/DECISOES.md), [o controle de PRs](plans/radar-licita-v4/CONTROLE_DE_PRS.md) e o cartão do PR correspondente em `plans/radar-licita-v4/prs/`. Consulte as fontes apontadas pelo cartão e confirme o estado atual do repositório antes de editar.

- Uma entrega por PR e um worktree exclusivo por PR. Preserve alterações preexistentes em outros checkouts.
- A sessão autora e a sessão verificadora devem ser distintas. Uma sessão não aprova o próprio trabalho; ausência de resposta humana não é aprovação.
- Não altere o worker ou a coleta de Carlos, não faça chamadas adicionais ao PNCP e não use dados privados como fixtures sem autorização registrada.
- O produto em execução usa processamento convencional. Não acrescente agente ou chamada a LLM ao runtime.
- Marque exemplos, fixtures, casos sintéticos e dados reais conforme sua origem. Testes offline não comprovam cobertura, atualidade ou viabilidade comercial.
- Registre no cartão e no controle apenas fatos observados; decisões humanas exigem autor, data, versão examinada e referência.

O cofre local `RADAR LICITA` em `../RADAR LICITA/` é biblioteca de referência. As instruções e o controle vivos desta sequência de PRs ficam neste repositório para que código e documentação sejam revisados juntos.
