---
id: REV-P00
tipo: pacote_revisao
estado: em_revisao
atualizado_em: 2026-11-17
---

# Pacote de revisão — P00

## Decisão pedida

Luis: examinar e aprovar, pedir mudanças, rejeitar ou manter incerto o recorte e as políticas de [P00](prs/P00.md). Carlos/responsável pelos dados: examinar a proveniência, os denominadores de cobertura/frescor e os limites de uso das fontes. A aprovação de P00/G0 deve registrar autor, data, commit examinado e ressalvas; o merge por si não concede G3–G6 nem aprova metas comerciais ainda não fixadas.

## Entrega e base

- [PR #5 (rascunho)](https://github.com/luis7gustavo/motor_decisao/pull/5); branch `codex/radar-p00-baseline-20261117`; base: `b46aec77a3448f51ac212676216c675822fb12f3`, merge do [PR #4 (P30)](https://github.com/luis7gustavo/motor_decisao/pull/4).
- [Linha de base](LINHA_DE_BASE_P00.md): capacidades por recurso, lacunas, datas e hashes das fontes locais.
- [Políticas e critérios](POLITICAS_E_CRITERIOS_P00.md): classes E0–E4, estados da ficha, seleção futura de compras, denominadores, bloqueios e contrato de runtime.
- [Estado do PR](prs/P00.md) e [controle](CONTROLE_DE_PRS.md): histórico verificável. Não há fixture nem dado privado anexado.
- Commit inicial publicado: `b0097ce38a31ab2ecb94b421ff59ba63b999bfc9`. O verificador deve registrar o **commit final que examinar** após esta atualização documental. Nenhum código, banco, worker ou agendamento faz parte desta entrega.

## Verificação e pendências

- Autoria conferiu os hashes dos seis documentos locais citados na linha de base e os links Markdown locais: **OK** em 17/11/2026.
- Conferir que 20 leilões e 2.748 itens só constam como evidência estrutural e que os cinco IDs por objeto não viram rótulos comerciais.
- `git diff --check`: **OK** em 17/11/2026. Branch exclusiva na base `b46aec77`; o diff contém apenas Markdown. Nenhuma credencial ou dataset foi incluído.
- Uma **sessão verificadora distinta** deve confrontar diff e fontes e registrar achados aqui antes da decisão humana. A sessão autora não assina a verificação.
- A escolha de revisores, data de rotulagem, tamanho e limiares por categoria, referência de cobertura e SLA de frescor permanece pendente. Esses números devem ser aprovados antes da avaliação cega, sem ajuste retroativo.

## Risco e reversão

O principal risco é interpretar um contrato proposto como qualidade medida. O cabeçalho e as tabelas mantêm esse estado explícito. Para desfazer, rejeitar ou reverter somente os arquivos documentais deste PR; nenhuma operação ou dado bruto é alterado. Decisão humana: **pendente**.
