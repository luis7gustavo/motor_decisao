# Gate A — avaliação agregada do C2 para a descoberta do Radar Licita

Observação: **2026-11-05 16:05 America/Sao_Paulo**. A consulta foi executada em transação `REPEATABLE READ READ ONLY` pelo contêiner dedicado que valida o destino C2. O programa não chamou o PNCP, não exportou linhas individuais e não escreveu no C2 ou no banco principal. Dados agregados: [gate-a-c2-observation-20261105.json](gate-a-c2-observation-20261105.json).

## Evidência

| Indicador | Contagem |
| --- | ---: |
| Licitações PNCP com projeção local | 340.985 |
| Sem versão atual vinculada / identidade PNCP divergente | 0 / 0 |
| Identidades do marco histórico, sem projeção | 338.407 / 0 |
| Objeto, ano bruto, valor estimado e UF brutos presentes | 340.985 cada |
| Prazo bruto de encerramento presente | 340.919 |
| Prazo normalizado (`deadline_at`) | 0 |
| Atualização oficial com instante/fuso (`source_updated_at`) | 0 |
| Versões com fuso desconhecido | 340.985 |
| Modalidades / UFs observadas | 7 / 27 |
| Versões posteriores à primeira | 7 |
| Itens / itens de julgamento persistidos | 0 / 0 |

As situações brutas são `Divulgada no PNCP` (331.368), `Revogada` (6.014), `Anulada` (1.836) e `Suspensa` (1.767). `Divulgada no PNCP` **não é prova de proposta aberta**; é preciso combinar situação, prazo oficial validado e atualização confiável. As 340.919 datas brutas não devem ser tratadas como prazo normalizado sem resolver fuso e semântica. A amostra real de retificações ainda é pequena, pois só sete identidades têm uma segunda versão persistida.

## Decisão de desenvolvimento

O acervo já sustenta a implementação da descoberta por **identidade, objeto, UF e modalidade**, com indicação explícita da origem e de cobertura parcial. A primeira ligação a dados reais deve ser ensaiada em cópia isolada e leitura somente, com busca sem selo de frescor ou de oportunidade aberta. A classificação de aberto, filtros por prazo e alertas dependem da validação temporal e de sync completo. Comparação de itens, resultados e viabilidade específica dependem de pilotos de enriquecimento por recurso; os contadores atuais não os demonstram.

O protótipo `/radar-demo` continua sintético. Antes de migrar ou conectar a aplicação operacional, aplicar os gates pertinentes de A1–A4 e G0–G4 do plano de migração em cópia. A coleta histórica integral não é pré-requisito para desenvolver a busca, mas esta avaliação agregada também não equivale a aceite S08 ou integração de produção.

Um diagnóstico adicional, `scripts/radar_c2_discovery_preview.py`, executou uma busca limitada por objeto `papel` e UF `SP`, retornando três registros diretamente do C2 em transação somente leitura. Sua saída declara `freshness=unknown`, `opportunity_open=null`, `coverage=partial`, zero chamadas PNCP e zero escritas. É uma prova de consulta e mapeamento de metadados para o operador, sem ligação com a interface `/radar-demo`.
