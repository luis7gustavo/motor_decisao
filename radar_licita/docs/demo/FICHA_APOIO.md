# Ficha de apoio — tecnologia com dados reais

## Números conferidos

| Fato | Leitura correta |
| --- | --- |
| 7 pregões, 19 itens, 19 resultados PJ, 11 CNPJs distintos | Recorte dirigido do PNCP capturado em 15/10/2026, em 33 requisições. O painel funciona sem rede. |
| Exército, `00394452000103-1-015447/2026`, item 1 | Microcomputadores: 12 unidades, estimativa pública total R$ 117.844,08; resultado identifica INFOG LTDA, CNPJ `42145571000164`, com valor unitário informado como homologado R$ 8.223,55. |
| Sorriso, `03238755000117-1-000031/2026` | Item 5363: MEDIATIZE, CNPJ `45369129000100`. Itens 5365 e 5366: CDS SOLUTIONS, CNPJ `61458397000154`. |
| Fundação Municipal de Saúde, `05522917000170-1-000138/2026` | Sete itens de outsourcing de impressão têm resultados que identificam LOGUS COPIADORAS, CNPJ `14926785000132`. |
| Urucânia, `18316281000151-1-000112/2026` | Serviço de software para cesta de preços; resultado identifica RCDNC PARTICIPACOES, CNPJ `27783574000130`. |
| Esteio, `90871831000121-1-000023/2026` | Scanner profissional; resultado identifica HKA Tecnologia do Brasil, CNPJ `19729347000106`. |
| CISBAF, `03681070000140-1-000008/2026` | Integração de sistemas de TIC; resultado identifica SYS4WEB BRASIL, CNPJ `12349666000139`. |
| USP, `63025530000104-1-002341/2026` | Cinco itens selecionados: notebook, duas impressoras, fonte e adaptador. Resultados identificam LICITEI, Vertical Producoes, HIGH LEVEL e Pablo Farias Benicio, com CNPJs e links por item na tela. |

## Respostas rápidas

- **“Todos os dados da aplicação são reais?”** A instância da apresentação foi recriada somente com compras, itens e resultados oficiais do PNCP. A base inicial tem zero fornecedores cadastrados, produtos, cotações, cenários financeiros, scores e documentos locais. Os antigos arquivos de casos sintéticos foram retirados do pacote publicado.
- **“Como a empresa foi encontrada?”** O CNPJ consta no resultado publicado para o mesmo número de controle PNCP e número do item. O nome, o valor histórico e o link são exibidos junto ao item.
- **“Ela pode fornecer agora?”** Isso está pendente. O histórico não confirma especificações atuais, habilitação, estoque, capacidade, interesse ou cotação.
- **“O valor homologado é o custo de compra?”** Não. É preço de resultado de uma compra pública anterior. A estimativa do órgão, o resultado homologado e uma futura cotação de aquisição são valores diferentes.
- **“A busca funciona sem internet?”** O painel histórico e a base local funcionam. Links oficiais, **Reconsultar PNCP** e a consulta adicional de propostas abertas exigem rede.
- **“Por que o Radar não traz oportunidades elegíveis?”** Os pregões selecionados já encerraram e não há cotações atuais nem custos confirmados. A demonstração prova extração e cruzamento históricos, não viabilidade comercial.

## Se algo falhar

1. Execute `scripts/demo/check-demo.ps1`. Veja os logs em `output/demo/logs/` se a API ou a interface não responder.
2. Abra `http://127.0.0.1:5173/?demo=historico`; a captura local é a fonte do painel.
3. Use a [captura de tela atual](evidencias/s09_historico_real_resumo.png) e o [arquivo PNCP capturado](../../examples/demo/fixtures/official/PNCP_TECNOLOGIA_HISTORICA_EXPANDIDA_2026-10-15.json) como reserva.
4. Para restaurar uma cópia operacional limpa, pare a demo e execute `scripts/demo/start-demo.ps1`. O script gera uma nova cópia do snapshot oficial.

**Regra de fala:** diga “empresa identificada em resultado histórico” ou “possível parceira para investigar”. Não afirme contrato executado, capacidade atual ou preço de compra atual.
