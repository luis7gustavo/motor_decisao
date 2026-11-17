---
id: POL-P00
tipo: contrato_proposto
estado: aguardando_aprovacao_humana
atualizado_em: 2026-11-17
base_commit: b46aec77a3448f51ac212676216c675822fb12f3
---

# P00 — políticas, estados e critérios de avaliação

Este contrato é uma **proposta para G0**. Organiza as decisões já registradas em [DECISOES.md](DECISOES.md), a [proposta V4](PROPOSTA_V4_20261117.md) e o [plano I00 de 16/11](LINHA_DE_BASE_P00.md#fontes-congeladas-para-esta-fotografia). Luis e os responsáveis indicados no [pacote de revisão](PACOTE_REVISAO_P00.md) devem aprovar o recorte e os limites antes de tratá-los como aceite. Não altera regras da aplicação nesta revisão.

## Classes de evidência e estado da ficha

| Classe | Origem e uso | Limite |
| --- | --- | --- |
| `E0_exemplo` | Descrição ou registro identificado; teste de formato, busca e apresentação. | Não mede cobertura, acerto ou atualidade. |
| `E1_fixture` | Recorte autorizado, congelado, com versão e hash; regressão. | Não demonstra generalização ou disponibilidade atual. Autorização de origem/uso (D13) pendente. |
| `E2_sintetico` | Caso inventado, assinalado como simulação; bordas e falhas. | Não é licitação nem cotação real. |
| `E3_real_revisado` | Compra de bens, com identidade, itens/lote/documentos versionados e rótulos humanos. | Avaliação somente no recorte e na data revisados. |
| `E4_real_monitorado` | Fonte real com cobertura/frescor auditados e SLA aceito. | Alerta atual apenas nas janelas e recursos medidos. |

**Campos mínimos de toda evidência:** classe, ID de origem, recurso, versão/retificação, URL ou caminho, hash quando existir, horário de publicação/atualização declarado pela fonte, horário de recepção, data da revisão, revisor, lacunas e estado de validade. Ausência de campo é `desconhecido`, nunca valor zero inferido. Um hash verifica bytes de um snapshot, não a atualidade da fonte.

**Estados de apresentação:** `exemplo`, `simulado`, `historico`, `pendente`, `estimado`, `confirmado`, `desatualizado`, `incompativel`. A classe de evidência e o estado são eixos distintos. `confirmado` requer campo e versão conferidos na fonte pertinente e, para julgamento comercial, revisão humana; parser, classificação ou existência de documento sozinhos mantêm `pendente`/`estimado`. Retificação, cotação expirada ou divergência de versão reabre a revisão e pode marcar `desatualizado`. `incompativel` exige motivo e evidência do item/lote inteiro; falta de dados produz `pendente`.

**Estados independentes por recurso:** listagem/publicação, detalhe, itens, unidade de julgamento, índice/binário de documentos, texto/OCR, exigências revisadas e resultados. Recurso não consultado é `não_consultado`; resposta com falha é `falha`; ausência confirmada pela fonte é `ausente_confirmado` com contexto; leitura parcial é `parcial`; aceito com hash e versão é `observado`. A ficha não agrega esses estados em um único “completo”.

## Recorte e amostra de aceite futura

O universo comercial candidato são todos os registros MegaMix/Coletek; o primeiro **recorte de validação** é mouse, teclado de computador, webcam e headset. Computador/notebook fica fora desse primeiro ciclo. A busca pode ser nacional; o local de entrega vem do edital, não da UF do órgão. O capital de R$ 20–30 mil é referência aproximada de teto disponível, não corte de edital. Não há margem mínima universal nem decisão automática de participação. Essas decisões estão registradas em `Radar Licita/DECISOES.md` e nas fontes comerciais indicadas na [linha de base](LINHA_DE_BASE_P00.md).

1. **Quadro de seleção, antes de rotular:** formar compras de bens reais com itens e documentos disponíveis, cobrindo as quatro categorias, positivos, descartes e negativos difíceis. Incluir lote misto, objeto genérico, pacote com acessório não separável, falso positivo lexical, serviço/leilão, retificação, prazo/destino incertos e PDF sem texto. Os cinco IDs do documento local `amostra-alvo-compras-para-revisao-20261116.md` são somente candidatos localizados pelo objeto, não positivos nem negativos confirmados.
2. **Unidade e rótulo:** registrar contratação e versão, item, unidade de julgamento/lote e documento/página. Rotular separadamente contexto de compra, categoria, item ofertável, lote integralmente atendível, especificação suficiente, exigência documental, prazo e destino. Usar `sim`, `não` ou `incerto`, motivo e referência. Um item compatível não torna um lote misto compatível.
3. **Revisão:** duas pessoas rotulam de forma independente; divergências recebem adjudicação documentada. Luis revisa positivos e descartes; Carlos ou responsável pelos dados valida proveniência e recorte. Quem revisa e a data ficam pendentes até confirmação nominal. Rótulos `incerto` não viram negativos silenciosos.
4. **Separação:** congelar lista, hashes e critérios antes de avaliar a triagem. Agrupar por identidade de contratação, incluindo todas as versões e itens, para impedir vazamento entre desenvolvimento, validação e aceite cego. O conjunto cego não guia ajustes. Registrar exclusões e sua razão antes do resultado.

Nenhuma amostra E3 foi congelada neste P00. As 20 contratações de leilão/alienação da exportação de 12/11 entram apenas na avaliação estrutural; seus 2.748 itens não compõem o denominador comercial.

## Denominadores e metas a pactuar antes da avaliação

| Medida | Numerador / denominador e recorte | Condição proposta de aceite |
| --- | --- | --- |
| Precisão por categoria | Verdadeiros positivos / todos os positivos previstos, por categoria, contexto e tipo de lote, no aceite cego E3. | Limiar e tamanho mínimo por estrato **pendentes de Luis e revisores comerciais** antes da rotulagem/avaliação. Publicar contagens, IC de 95% e discordâncias. |
| Revocação por categoria | Verdadeiros positivos / todos os positivos humanos elegíveis do quadro amostrado; divulgar amostragem e limite de inferência. | Limiar e tamanho mínimo **pendentes**. Não extrapolar para compras nacionais se o quadro de seleção não representar o universo. |
| Lote inteiro | Lotes previstos como atendíveis e confirmados / todos os lotes previstos como atendíveis; reportar lotes parcialmente atendidos à parte. | Nenhum positivo sem composição e exigências do lote revisadas; limite quantitativo pendente. |
| Cobertura por janela e recurso | Identidades/páginas/recursos aceitos / universo de referência independente para a mesma janela; ausências e tentativas separadas. | Referência, período e limiar **pendentes de Carlos**; 20/20 do índice observado não serve. |
| Frescor | Distribuição do atraso entre atualização da fonte e recepção/integração, por janela e recurso; relógios e fuso explícitos. | SLA e percentis **pendentes de Carlos**; sem referência atual não há selo de frescor. |
| Finanças | Casos de ouro com mesmas premissas e valores por data; divergências em centavos. | Concordância ao centavo; premissas e casos assinados por financeiro/Luis antes do uso real. |
| Proveniência | Registros críticos com identidade, versão, fonte, hash/URL, observação e lacunas recuperáveis / registros críticos avaliados. | Divergência crítica bloqueia promoção, mesmo com outros indicadores aprovados. |

Para precisão e revocação, publicar também falsos positivos, falsos negativos, casos `incerto`, intervalos e fração excluída. Os limiares **não são escolhidos após ver o resultado**. Se faltar amostra, a capacidade permanece parcial e o quadro é ampliado; não se concede selo por categoria sem denominador suficiente.

## Limites de desenvolvimento e runtime

- O agente atua em desenvolvimento, verificação e documentação. O **produto em execução** usa processamento convencional, regras, NLP/ML convencional e cálculos determinísticos; não chama LLM nem executa agente para classificar, decidir, operar ou contatar terceiros. P28 deverá testar dependências e rede antes de G6.
- Este P00 não autoriza chamadas PNCP, escrita no worker/coleta, promoção de banco, distribuição de dados privados, alertas atuais, contato com fornecedores, compras, propostas ou lances.
- Nenhuma tela ou API usa E0/E1/E2 como oportunidade aberta ou viabilidade confirmada. Preço de catálogo é referência; resultado não consultado não vale zero; orçamento sigiloso não cria receita; estoque, frete, habilitação e caixa exigem fonte específica.
- G0 aprova a governança para PRs em ambiente isolado. G1/G2 são aceites estruturais/offline. G3 requer compras reais revisadas; G4 exige qualidade comercial medida; G5 exige cobertura/frescor e operação auditados; G6 exige piloto real e liberação expressa. A [proposta V4](PROPOSTA_V4_20261117.md#6-gates-e-níveis-de-pronto) detalha as provas propostas.
