# Benchmark de workers

Execute a mesma fonte com volume comparável em cada worker antes de elevar concorrência. Registre pelo menos três runs por combinação.

```sql
SELECT
    source_name,
    worker_id,
    collection_strategy,
    COUNT(*) AS runs,
    ROUND(AVG(duration_seconds), 2) AS avg_duration_s,
    ROUND(AVG(records_loaded / NULLIF(duration_seconds, 0)), 3) AS products_per_second,
    ROUND(AVG(requests_total / NULLIF(duration_seconds / 60, 0)), 2) AS requests_per_minute,
    ROUND(AVG(peak_memory_mb), 2) AS avg_peak_memory_mb,
    ROUND(AVG(avg_cpu_percent), 2) AS avg_cpu_percent,
    SUM(requests_failed) AS failed_requests
FROM control.source_runs
WHERE status IN ('success', 'partial')
GROUP BY source_name, worker_id, collection_strategy
ORDER BY source_name, worker_id, collection_strategy;
```

## Protocolo

- Mesmas queries, horário próximo e `max_results` igual.
- Separar HTTP, AdaptivePlaywright e Playwright.
- Não executar outros browsers durante a medição.
- Ajustar um nível por vez: primeiro limite do worker, depois concorrência Crawlee da fonte.
- Recuar se memória superar 75% do host, houver paginação degradada, bloqueios ou aumento sustentado de 429.

Valores iniciais: PC1 3 flows HTTP e 1 browser; PC2 4 HTTP e 2 browser. Eles são ponto de partida, não meta permanente.

## Evidencia inicial no PC1

Em 12/08/2026, a coleta integral Kabum com `AdaptivePlaywright` concluiu 21 requests e 420 itens em 75,01 s, sem request falha, pico de 313,16 MB e CPU media de 20,32%. Esse unico run comprova a instrumentacao, mas nao substitui as tres repeticoes por combinacao exigidas pelo protocolo. A comparacao fisica com PC2 permanece pendente.
