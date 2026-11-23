---
id: EXP-PNCP-20261123
tipo: evidencia
estado: aprovado_com_limites
classe_evidencia: E1
atualizado_em: 2026-11-23
---

# Exportação real PNCP para contrato estrutural

Luis determinou o uso das coletas mais recentes com licitações completas como
exportação real. A fonte foi o read model SQLite atualizado pela coleta
contínua, sem nova chamada ao PNCP e sem escrita no banco operacional.

## Fonte e fotografia

- Read model observado na geração pública: 1.526 identidades mais recentes,
  das quais 350 tinham `observed_resources_complete=true`.
- SHA-256 do read model usado pela exportação pública:
  `06d31eeb192aa1fb1e69ba1e3aeb3d2949f1d142e21b1a06a6909c68cf7d5050`.
- Exportação controlada: 100 identidades completas, ordenadas pela aquisição
  mais recente, em `fixtures/radar_real/pncp_complete_20261123/`.
- Manifesto público SHA-256:
  `14c835a9f8829ee04ce4148b9ae89a141dda96e83dd2a8661f4d0c41c01b7a54`.
- Tamanho público: 101 arquivos e 4.149.479 bytes.

Um snapshot consistente posterior, preparado para o Kaggle privado, contém
2.855 observações, 1.527 identidades mais recentes, 350 completas, 26.424
itens, 1.676 resultados e 3.900 documentos. Seu SHA-256 é
`e736958039464b371d49125849576b722f7613acd0ffe5b697cf49ef0401b185`.
Ele foi publicado como versão 1 do dataset privado
[`radar-licita-pncp-integral-2026-11-23`](https://www.kaggle.com/datasets/luisgustavo27/radar-licita-pncp-integral-2026-11-23),
com estado `ready`, sete arquivos e 94,71 MB.

## Aceite estrutural

A exportação é aceita como E1 para validar envelope, identidade, versão,
hashes, itens, resultados, documentos, ausências e testes de borda. O schema
P31 passa a ser v1 provisório; divergências de uma exportação posterior são
registradas e não convertidas silenciosamente.

## Limites

`observed_resources_complete` significa completude perante o índice observado,
não snapshot independente da fonte. A exportação não prova cobertura nacional,
frescor sob SLA, abertura atual, destino de entrega, elegibilidade comercial,
preço, estoque, frete ou caixa. A coleta contínua segue ativa e a conclusão do
histórico nacional continua separada deste aceite estrutural.
