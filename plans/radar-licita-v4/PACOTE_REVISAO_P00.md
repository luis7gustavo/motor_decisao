---
id: REV-P00
tipo: pacote_revisao
estado: em_revisao
atualizado_em: 2026-11-17
---

# Pacote de revisão — P00

## Decisão pedida

Luis aprovou o método descrito nas decisões P00-01 a P00-07 em 17/11/2026. Carlos/responsável pelos dados ainda precisa examinar a proveniência, os denominadores de cobertura/frescor e os limites de uso das fontes. A aprovação final de P00/G0 deve registrar autor, data, commit examinado e ressalvas; o merge por si não concede G3–G6 nem aprova metas comerciais ainda não fixadas.

**Escolhas registradas:** usar as amostras de periféricos já coletadas, duas revisões independentes, análise exploratória antes da avaliação cega, prioridade para encontrar candidatos na fila humana, referência independente de cobertura, apresentação histórica até auditoria de frescor e consulta local dos dados ([decisões](DECISOES.md)). A elegibilidade de cada caso depende do inventário e da revisão de itens, lotes, documentos e versões. Nomes dos revisores, tamanhos mínimos, limiares numéricos, referência concreta e SLA continuam pendentes.

## Entrega e base

- [PR #5 (pronto para revisão humana)](https://github.com/luis7gustavo/motor_decisao/pull/5); branch `codex/radar-p00-baseline-20261117`; base: `b46aec77a3448f51ac212676216c675822fb12f3`, merge do [PR #4 (P30)](https://github.com/luis7gustavo/motor_decisao/pull/4).
- [Linha de base](LINHA_DE_BASE_P00.md): capacidades por recurso, lacunas, datas e hashes das fontes locais.
- [Políticas e critérios](POLITICAS_E_CRITERIOS_P00.md): classes E0–E4, estados da ficha, seleção futura de compras, denominadores, bloqueios e contrato de runtime.
- [Estado do PR](prs/P00.md) e [controle](CONTROLE_DE_PRS.md): histórico verificável. Não há fixture nem dado privado anexado.
- Commit inicial publicado: `b0097ce38a31ab2ecb94b421ff59ba63b999bfc9`. Conteúdo técnico examinado no commit `06e931df0abe00fede7adcbe5d1f8d42fa7bac4e`; o registro da verificação foi acrescentado depois. Nenhum código, banco, worker ou agendamento faz parte desta entrega.

## Verificação e pendências

- Autoria conferiu os hashes dos seis documentos locais citados na linha de base e os links Markdown locais: **OK** em 17/11/2026.
- Conferir que 20 leilões e 2.748 itens só constam como evidência estrutural e que os cinco IDs por objeto não viram rótulos comerciais.
- `git diff --check`: **OK** em 17/11/2026. Branch exclusiva na base `b46aec77`; o diff contém apenas Markdown. Nenhuma credencial ou dataset foi incluído.
- Uma **sessão verificadora distinta** confrontou diff e fontes e registrou o parecer abaixo antes da decisão humana. A sessão autora não assinou a verificação.
- O método de revisão foi aprovado por Luis; nomes dos dois rotuladores, data, tamanhos mínimos e limiares por categoria, referência concreta de cobertura e SLA de frescor permanecem pendentes. Os parâmetros correspondentes devem ser aprovados antes da avaliação cega ou do gate operacional, sem ajuste retroativo.
- As alterações documentais posteriores ao commit técnico examinado precisam de verificação por outra sessão antes da aprovação final do PR.

### Verificação independente — 17/11/2026

Sessão verificadora Codex distinta da sessão autora. Conteúdo examinado no commit `06e931df0abe00fede7adcbe5d1f8d42fa7bac4e` do PR #5, contra a base `b46aec77a3448f51ac212676216c675822fb12f3`.

- Os seis SHA-256 das fontes locais na linha de base conferem com os arquivos originais. Os sete caminhos de código e documentação citados para as capacidades do produto existem no checkout da base.
- O relatório offline confirma 20 identidades e versões observadas, 2.748 itens, 40 binários de PDF íntegros, 4.402 páginas com texto, 3.041 trechos sugeridos e resultados não coletados. O próprio relatório classifica os itens como leilão/alienação e não mede precisão ou revocação comercial.
- O diff do PR contém somente nove arquivos Markdown. Links locais desses arquivos resolvem no checkout; `git diff --check` não apontou erros; o worktree estava limpo. Nenhum dataset ou credencial foi incluído.
- **Parecer técnico sobre o commit examinado:** sem achados bloqueantes no contrato documental então apresentado. As escolhas posteriores de Luis constam nas decisões P00-01 a P00-07; este parecer anterior não é verificação independente dessas alterações. A validação do responsável pelos dados, G0 e o merge continuam pendentes.

## Risco e reversão

O principal risco é interpretar o método aprovado como qualidade medida. O cabeçalho e as tabelas mantêm esse limite explícito. Para desfazer, rejeitar ou reverter somente os arquivos documentais deste PR; nenhuma operação ou dado bruto é alterado. Decisão de Luis sobre o método: **registrada**. Validação dos dados e G0: **pendentes**.
