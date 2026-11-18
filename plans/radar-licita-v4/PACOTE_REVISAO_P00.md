---
id: REV-P00
tipo: pacote_revisao
estado: aprovado_para_integracao
atualizado_em: 2026-11-18
---

# Pacote de revisão — P00

## Decisão registrada

Luis aprovou o método descrito nas decisões P00-01 a P00-07 em 17/11/2026 e esclareceu em P00-08 que ele mesmo é responsável por validar os dados. Em 18/11/2026, respondeu “Sim, validar e integrar” à decisão apresentada sobre o pacote do PR #5 no commit `a1b8429d2b680bc1efffcbba0975cec477c80fbf`. Assim, validou o **uso local** das fontes e do inventário descritos aqui, aprovou G0 documental com limites e autorizou integrar o PR #5. A decisão não atesta cobertura nacional, frescor ou qualidade comercial e não autoriza D13, publicação de dados como fixtures/anexos, G3–G6 ou metas comerciais ainda não fixadas.

**Escolhas registradas:** usar as amostras de periféricos já coletadas, duas revisões independentes, análise exploratória antes da avaliação cega, prioridade para encontrar candidatos na fila humana, referência independente de cobertura, apresentação histórica até auditoria de frescor e consulta local dos dados ([decisões](DECISOES.md)). A elegibilidade de cada caso depende do inventário e da revisão de itens, lotes, documentos e versões. Nomes dos revisores, tamanhos mínimos, limiares numéricos, referência concreta e SLA continuam pendentes.

## Entrega e base

- [PR #5 (pronto para revisão humana)](https://github.com/luis7gustavo/motor_decisao/pull/5); branch `codex/radar-p00-baseline-20261117`; base: `b46aec77a3448f51ac212676216c675822fb12f3`, merge do [PR #4 (P30)](https://github.com/luis7gustavo/motor_decisao/pull/4).
- [Linha de base](LINHA_DE_BASE_P00.md): capacidades por recurso, lacunas, datas, hashes das fontes locais e inventário preliminar das amostras disponíveis.
- [Políticas e critérios](POLITICAS_E_CRITERIOS_P00.md): classes E0–E4, estados da ficha, seleção futura de compras, denominadores, bloqueios e contrato de runtime.
- [Estado do PR](prs/P00.md) e [controle](CONTROLE_DE_PRS.md): histórico verificável. Não há fixture nem dado privado anexado.
- Commit inicial publicado: `b0097ce38a31ab2ecb94b421ff59ba63b999bfc9`. Conteúdo técnico examinado no commit `06e931df0abe00fede7adcbe5d1f8d42fa7bac4e`; o registro da verificação foi acrescentado depois. Nenhum código, banco, worker ou agendamento faz parte desta entrega.

## Verificação e pendências

- Continuação de 18/11: consulta somente de leitura ao C2 isolado e inventário de nove candidatas por metadados; cinco casos já expostos foram separados para desenvolvimento, três tiveram corpos processados por rotina estrutural antes de reserva provisória e uma quarta foi reservada somente por metadados. Os manifestos ficaram locais, fora do PR, com SHA-256 registrados na [linha de base](LINHA_DE_BASE_P00.md). Os cinco casos expostos somam 23 registros de itens, 18 binários com hash válido e 20 resultados; esses totais são estruturais, não rótulos comerciais.
- Na etapa P00-09, Luis informou que não estaria disponível para validar naquele momento e autorizou seguir com as sugestões; não houve aprovação por silêncio. Sua decisão explícita posterior, em 18/11/2026, está registrada no início deste pacote. A revisão técnica anterior se refere a commits anteriores, não a esta continuação.

- Autoria conferiu os hashes dos seis documentos locais inicialmente citados na linha de base e os links Markdown locais: **OK** em 17/11/2026. A sétima fonte acrescentada no inventário foi conferida na revisão independente posterior.
- Conferir que 20 leilões e 2.748 itens só constam como evidência estrutural e que os cinco IDs por objeto não viram rótulos comerciais.
- `git diff --check`: **OK** em 17/11/2026. Branch exclusiva na base `b46aec77`; o diff contém apenas Markdown. Nenhuma credencial ou dataset foi incluído.
- Uma **sessão verificadora distinta** confrontou diff e fontes e registrou o parecer abaixo antes da decisão humana. A sessão autora não assinou a verificação.
- O método de revisão foi aprovado por Luis; nomes dos dois rotuladores, data, tamanhos mínimos e limiares por categoria, referência concreta de cobertura e SLA de frescor permanecem pendentes. Os parâmetros correspondentes devem ser aprovados antes da avaliação cega ou do gate operacional, sem ajuste retroativo.
- As alterações documentais posteriores ao primeiro parecer técnico receberam a revisão independente descrita abaixo; ela não substitui a validação do responsável pelos dados.
- O inventário preliminar das fontes locais examinadas não demonstrou compra de periférico com item, lote e documentos revisados. O acervo operacional restante ainda precisa de inventário pelo P31; sem esse denominador, o tamanho mínimo e os limiares numéricos não podem ser fixados como aceite do conjunto disponível.

### Verificação independente — 17/11/2026

Sessão verificadora Codex distinta da sessão autora. Conteúdo examinado no commit `06e931df0abe00fede7adcbe5d1f8d42fa7bac4e` do PR #5, contra a base `b46aec77a3448f51ac212676216c675822fb12f3`.

- Os seis SHA-256 das fontes locais na linha de base conferem com os arquivos originais. Os sete caminhos de código e documentação citados para as capacidades do produto existem no checkout da base.
- O relatório offline confirma 20 identidades e versões observadas, 2.748 itens, 40 binários de PDF íntegros, 4.402 páginas com texto, 3.041 trechos sugeridos e resultados não coletados. O próprio relatório classifica os itens como leilão/alienação e não mede precisão ou revocação comercial.
- O diff do PR contém somente nove arquivos Markdown. Links locais desses arquivos resolvem no checkout; `git diff --check` não apontou erros; o worktree estava limpo. Nenhum dataset ou credencial foi incluído.
- **Parecer técnico sobre o commit examinado:** sem achados bloqueantes no contrato documental então apresentado. As escolhas posteriores de Luis constam nas decisões P00-01 a P00-07; este parecer anterior não é verificação independente dessas alterações. Na data desse parecer, a validação do responsável pelos dados, G0 e o merge ainda estavam pendentes; a decisão humana posterior consta no início deste pacote.

### Revisão das decisões posteriores

Uma sessão verificadora distinta examinou `c90e25241e3fea6a7b54fb38478e17e841f22ab9` e encontrou um risco de vazamento: a análise exploratória de todos os casos precedia a separação do conjunto cego. O método foi corrigido para congelar a separação por contratação antes da exploração e restringir a implementação à partição de desenvolvimento.

Na revisão de `9afa9c2`, a mesma sessão confirmou a correção, o SHA-256 da sétima fonte e suas contagens datadas (55 detalhes, 100 páginas de itens e 3.489 ocorrências). Repetiu a consulta somente de leitura à cópia C2 `radar_c2_snapshot_20261110`: o Bronze dessa cópia contém apenas recursos de listagem `/v1/contratacoes/atualizacao` e `/v1/contratacoes/publicacao`, sem detalhe de itens ou documentos. A única observação remanescente foi a redação acima sobre seis fontes iniciais versus sete atuais, corrigida nesta atualização. A validação do responsável pelos dados ainda não foi registrada.

Na revisão independente do commit `4bd03fb`, foi identificado que `inspect_identity` processa corpos de respostas ao calcular contagens e hashes. A redação inicial dizia incorretamente que nenhum conteúdo havia sido aberto nas três reservas. A correção registra o processamento estrutural e deixa a elegibilidade cega dessas três pendente de revisão; uma identidade adicional foi selecionada somente por metadados.

A sessão verificadora distinta reexaminou o commit `48d3a184daff7f58a6d237d4ce17f31cfde9fc33` e não encontrou achados bloqueantes. Conferiu o SHA-256 do manifesto privado v2, as nove identidades únicas e a divisão 5+3+1; verificou que a consulta do candidato adicional lê somente fila e metadados de versão, sem Bronze/corpos. O diff integral do PR continha apenas Markdown, nenhum identificador PNCP reservado, links locais válidos e `git diff --check` sem erros. O parecer não valida proveniência, autorização de uso, G0 ou ausência universal de acesso de conteúdo fora do fluxo examinado.

## Risco e reversão

O principal risco é interpretar o método aprovado como qualidade medida. O cabeçalho e as tabelas mantêm esse limite explícito. Para desfazer, rejeitar ou reverter somente os arquivos documentais deste PR; nenhuma operação ou dado bruto é alterado. Decisão de Luis sobre o método, uso local das fontes e G0 documental: **registrada em 18/11/2026**. Medições de cobertura, frescor e qualidade comercial: **pendentes**.
