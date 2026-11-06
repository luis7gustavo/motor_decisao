# Rastreabilidade — Ciclo 2 (parcial)

Base C1: af1a5e0. Escopo EP-01 / HU-001 e TEC-04; RF-001, RT-001/015,
RN-026/027/028. A tabela registra cobertura sintética; não encerra HU-001.

| Comportamento | Implementação | Evidência sintética |
|---|---|---|
| Cota global persistida antes de envio, sem reposição | pncp_transport.Budget | test_budget_reserved_before_failure_and_survives_reopen; test_shared_ledger_serializes_workers_and_spacing |
| Bytes originais e headers permitidos | Budget / Store.bronze | test_response_archived_before_return; test_two_pages_replay_and_original_bytes |
| Destino HTTPS e IP público efetivo | pncp_transport.send_get | test_unapproved_destinations_rejected; test_nonpublic_dns_rejected; test_connected_peer_is_rechecked_after_dns |
| Duas páginas, três IDs, replay dez vezes | Collector / Store.apply_page | test_two_pages_replay_and_original_bytes; test_new_job_replays_content_without_new_version |
| Falha parcial e retomada no cursor | Collector / checkpoint | test_page_two_failure_then_resume_same_cursor |
| Resposta inválida conserva Bronze | Store.apply_page | test_bad_page_preserves_bronze_without_advancing |
| Retry limitado e Retry-After | Collector / Budget | test_transient_errors_are_counted_and_retry_is_bounded; test_long_retry_after_defers_without_another_request |
| Zero quota não aumenta contador do job | callback da reserva | test_zero_quota_does_not_count_an_unsent_job_request |
| Vazio coerente sem apagar histórico; totais estáveis | Store.apply_page | test_204_does_not_delete_previous_local_data; test_consistent_empty_200_and_changing_totals |
| Lease/fencing e guarda de destino | Store | test_stale_worker_and_wrong_database_refused |
| Contrato não confirmado bloqueia rede | live_transport | test_unverified_live_contract_blocks_before_send |
| Status distintos com bytes iguais mantêm recibos | fingerprint de tentativa | test_same_body_different_http_status_keeps_both_receipts |
| JSON financeiro sem conversão para float | json Decimal / repositório C1 | test_json_decimal_never_roundtrips_through_float |
| Corpo de flow manual sem retry externo | collect_pncp.fn | test_prefect_manual_body_uses_existing_job_and_zero_retries |

Arquivos: app/procurement/pncp.py, app/procurement/pncp_transport.py,
pipelines/procurement/pncp_flow.py, scripts/radar_pncp_contract.py;
testes em tests/test_pncp_ingestion.py e tests/test_pncp_transport.py.

Resultado: 128 aprovados (95 anteriores + 33 novos, incluindo parametrizações),
22,88 s; zero falhas/erros/ignorados, um warning conhecido. Manifesto
backups/20261013T230028Z_5aee84a9.json e log .tests.txt. Limpeza própria confirmada.
Testes inteiramente em Docker, com banco sintético e rede externa bloqueada.

Contrato HTTP público: quatro tentativas documentais reais; nenhuma contratação.
O OpenAPI oficial foi preservado por hash. Sem evidência de bordas/fuso/termos e
modalidade, a política de coleta real segue desabilitada. Não existe evidência de
amostra real, execução pelo motor Prefect, carga concorrente real ou operação
nacional. O teste com dois clientes do ledger usa threads; a implementação usa
flock entre processos, mas não se declara aqui ensaio multiprocesso sob carga.

CNPJ permanece textual pendente de validação; itens/documentos/resultados e demais
histórias não foram implementados neste incremento. Nenhuma migração nova no C2.
