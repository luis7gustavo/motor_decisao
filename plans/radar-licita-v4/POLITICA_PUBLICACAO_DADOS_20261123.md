---
id: POL-DADOS-20261123
tipo: politica
estado: aprovado
atualizado_em: 2026-11-23
---

# Política de publicação, repositórios e permissões

Decisão de Luis em 23/11/2026. Esta política resolve D12 e D13 e substitui a
restrição temporária de publicação da P00.

## Duas camadas de publicação

### GitHub público

Pode receber código, contratos, schemas, relatórios, manifestos, hashes,
recibos de coleta, dados originariamente públicos e fixtures reais
controladas. A amostra deve ser limitada, reproduzível e suficiente para
testes; cada arquivo real carrega origem, instante de aquisição, versão e
SHA-256. Não publicar segredos, credenciais, dumps operacionais, backups
integrais, dados pessoais desnecessários, material privado de terceiro ou o
conteúdo do conjunto cego antes de sua abertura.

Branches autorizadas:

- `codex/radar-licita-produto-2026-11-04`: integração por PR, sem push direto
  de entregas de autoria.
- `codex/radar-*`: código, documentação, manifestos e fixtures públicas
  controladas.
- `codex/pncp-enrichment-20261111`: código e operação da coleta, recibos,
  relatórios e manifestos públicos; não recebe banco, dump ou segredo.
- `main`: somente por integração revisada de release; dados novos não entram
  diretamente.

### Kaggle privado

Recebe snapshots integrais versionados do read model, exportações completas,
arquivos grandes, catálogos autorizados, anexos e backups destinados à
análise interna. Cada versão inclui manifesto, hash, contagens, período e
limites. Privado não significa local para segredos: credenciais, tokens e
senhas continuam proibidos.

O snapshot inicial preparado em 23/11/2026 contém 2.855 observações, 1.527
identidades mais recentes e 350 identidades completas no índice observado.
Foi publicado como versão 1 do dataset privado
[`luisgustavo27/radar-licita-pncp-integral-2026-11-23`](https://www.kaggle.com/datasets/luisgustavo27/radar-licita-pncp-integral-2026-11-23),
com sete arquivos e 94,71 MB. A interface e a CLI confirmaram o estado
`Private`/`ready`; o token fica somente no perfil local e não integra o pacote.

## Papéis e permissões

Luis, Filipe e Carlos podem criar branches, enviar commits, abrir e revisar
PRs, publicar versões no dataset privado do Kaggle e executar as rotinas de
entrega previstas nesta política. Para mudanças de gate ou promoção de dados,
uma pessoa atua como autora e outra registra a revisão; testes verdes não são
autoaprovação.

- Luis: responsável por proveniência, política de uso e finanças.
- Filipe e Luis: rotuladores independentes.
- Carlos: adjudicador das divergências de rótulo e aceite operacional da
  coleta conforme os critérios documentados.

## Retenção e conjuntos cegos

O conjunto cego permanece privado até o congelamento de identidades, hashes,
critérios e limiares. Após a abertura formal, seus dados públicos podem entrar
no GitHub; a versão integral permanece no Kaggle privado. Backups grandes são
referenciados no GitHub por manifesto e hash, não adicionados ao histórico Git.

## Revogação e correção

Uma origem com permissão contestada é removida da próxima versão pública e
marcada como indisponível; o histórico Git exige tratamento específico, pois
reverter um commit não apaga seus objetos. Divergência de hash, versão ou
proveniência bloqueia promoção até nova revisão.
