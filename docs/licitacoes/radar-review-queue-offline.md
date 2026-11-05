# Radar Licita — fila de revisão sintética, 04/11/2026

Este incremento de E reproduz `fixtures/radar_demo/review_queue.json` com `python -m scripts.radar_review_queue_preview`. O núcleo `app/review_queue_preview.py` mantém a fila e o histórico **somente em memória**, usando casos fictícios e a avaliação D sintética. Não consulta PNCP, C2 ou banco principal; não envia proposta, lance, contato, compra ou pagamento.

## Comportamento demonstrado

- A fila deriva suas linhas pendentes de `Case.review_pending`; o contador usa exatamente a mesma lista. Complemento aberto aparece primeiro e exige lacuna e responsável. Resolução exige motivo e referência de evidência.
- Complemento aberto impede aprovação favorável. Aprovação passa pelo motor E/D, que recalcula o cenário na data local do evento e confere versão, status e hash. Descarte exige motivo e cancela complemento ainda aberto, sem apagar o pedido anterior.
- Cada ação informa revisão **e versão da fonte esperadas** e registra ID, autor, instante, versões antes/depois, estado e pendência antes/depois e dados de motivo/evidência. Repetir a mesma ação é idempotente; reutilizar o ID com outro conteúdo falha. Revisão ou fonte esperada desatualizada é rejeitada antes da decisão. Retificação registrada na fila invalida complemento anterior; nova aprovação exige complemento revisto na versão nova.
- Na fixture, um caso pede e resolve complemento antes da aprovação E04→E07; outro é descartado E04→E06 com motivo. A fila final contém zero pendências e quatro ações no histórico. Isso demonstra o fluxo inventado, não qualidade de análise real.

## Limites e liberação

O histórico é imutável durante o processo, mas **não é persistência auditável**. Reiniciar perde a fila; não há autenticação, anexos, verificação de evidência, transação entre processos, bloqueio concorrente em banco nem tela Jinja2. A mudança da fonte precisa passar pela ação da fila; mutação externa de `Case` fora dela não é suportada. A resolução de complemento só confirma que uma referência foi informada, não que o documento é suficiente. O conjunto de estados permanece parcial frente a RN-032/D10. RNF, métricas do piloto e integração real seguem pendentes.

| Saída | Evidência sintética | Falta para uso real |
| --- | --- | --- |
| Fila e contador | Dois casos inventados; lista e contador derivados da mesma consulta | Persistência transacional, filtros e tela, autorização por usuário |
| Complemento e decisão | Lacuna, responsável, motivo, evidência e revisão testados | Documentos/exigências reais, qualidade da evidência e trilha durável |
| Aprovação | Reavaliação D e snapshot por versão | Gate A3/A4, fornecimento/capital atuais, G0–G4 em cópia |

Próximo incremento de produto: persistir eventos, revisões e snapshots em **ambiente descartável segregado**, com reexecução e conflito concorrente testados. A migração para destino operacional exige os gates do plano de migração e janela própria.
