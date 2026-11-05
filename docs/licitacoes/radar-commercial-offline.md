# Radar Licita — jornada comercial sintética, 04/11/2026

A Etapa E **offline** reproduz `fixtures/radar_demo/commercial_journey.json` em memória com `python -m scripts.radar_commercial_preview`. A revisão favorável usa o `status` e o hash de snapshot calculados pelo protótipo D para as versões sintéticas 2 e 3. A fixture não representa participação, contrato, entrega ou recebimento reais; todas as referências `synthetic://` são inventadas. O comando não envia proposta, lance, contato, compra ou pagamento.

## Fluxo demonstrado

- A revisão humana exige avaliação D recalculada `viável` **na data local do evento**, versão atual, hash correspondente e motivo; participação é um evento separado com referência externa. Cotação vencida no dia da revisão não reutiliza avaliação antiga. Repetir o mesmo ID/evento é idempotente; conteúdo divergente com o mesmo ID falha.
- A retificação da fonte depois da participação mantém E08 e seu evento anterior, marca revisão pendente e exige nova aprovação com snapshot da versão 3. Antes de participação, uma mudança em E07 retorna a E04. O status comercial e a revisão analítica são dimensões distintas.
- Resultado conhecido, vitória e início de contrato usam eventos separados. Vitória sozinha não cria contrato. O evento contratual declara itens únicos, quantidades positivas e valor com referência; no protótipo, eles devem coincidir com a última análise D e ficam travados. Uma retificação posterior que mude esses termos exige tratamento excepcional ainda não implementado. Entregas A/B são acompanhadas por item e quantidade; aceite exige a unidade completa. Recebimento parcial conserva saldo e E14; E15 exige total conciliado. Fechamento normal exige revisão atual e motivo.
- O replay termina em E16 com 14 eventos preservados, A=100 e B=10 entregues e R$ 13.300,00 registrados como recebidos **somente na ficção**. O evento de participação ainda aponta a versão 2 após a retificação para versão 3.

## Estado e limites

O protótipo cobre apenas um caminho dos 16 estados e 45 transições propostos em RN-032/D10. Não persiste fila, atos, anexos ou snapshots completos; conserva em memória os eventos e hashes de avaliação. A avaliação é recalculada a partir do cenário fornecido, mas o cenário ainda é sintético e sua origem real não é verificada. As referências externas não são verificadas, a identidade do ator é sintética e não há controle de concorrência entre usuários. Entregas e recebimentos da fixture não reconciliam documentos fiscais, aceite real, banco ou cronograma financeiro. Não há indicadores medidos, falso descarte avaliado nem piloto.

| Saída | Evidência offline | Bloqueio para uso real |
| --- | --- | --- |
| Aprovação e reanálise | D sintética com hash por versão; guarda testada | Gate A3/A4, documentos/exigências, fornecimento e capital atuais, revisão humana autenticada |
| Participação e resultado | Eventos `synthetic://`, transições e idempotência | Evidências externas verificadas, atores, autorização e persistência auditável |
| Execução e recebimento | Entrega por item e saldo monetário em memória | Contrato, aceite, cobrança, conciliação e cronograma reais |
| Carteira e métricas | Replay CLI de um caso e fila sintética em memória descrita em `radar-review-queue-offline.md` | Tela/filtros, métricas MN, RNF, persistência e G0–G6 |

E permanece **parcial**. `E16` no replay é apenas o estado da fixture. Não equivale ao MVP, que exige todos os requisitos Must/RNF do PRD e piloto medido.
