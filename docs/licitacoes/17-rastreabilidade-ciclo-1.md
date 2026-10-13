# Rastreabilidade da persistência pública — Ciclo 1

2026-10-13. Adendo à matriz 17, preservando documento preexistente fora dos
commits conforme ADR-021. Escopo TEC-02 do EP-01, não conclusão de HU funcional.

| Origem | Componente | Evidência e alcance |
|---|---|---|
| RF-003 parcial; RN-026/028; RT-001; TST-003 parcial | app/procurement/repository.py e models.py | replay dez vezes, identidade por source/source_id e conteúdo sem duplicar versão; source diferente da origem Bronze rejeitada; não valida DV fiscal nem contrato PNCP |
| RF-004 parcial; RN-028; TST-004 parcial | ProcurementRepository.record e version_observations | v1→v2→v1 reutiliza versão e conserva observação; manifesto diferente muda hash; observação atrasada não regride ponteiro; invalidação de análise comercial depende de módulo futuro |
| RT-014; RNF-005/008; TST-039 parcial | 20261013_0010, triggers e FKs compostas | testes por SQL rejeitam ponteiro de outro pai, rascunho como atual e composição entre versões; publicação congela snapshot/filhos; não afirma fencing de ingestão implementado |
| RN-026; RNF-010; RT-001 | validação de Item/Snapshot e NUMERIC(24,6) | NULL conservado, Decimal com seis casas retorna exato; float/zero/negativo/NaN/infinito/excesso de casas rejeitados antes de escrita; não implementa simulador financeiro |
| RT-001; RNF-008; TST-042 parcial | savepoint e transação do chamador | falha real de constraint após criação da identidade reverte todo o snapshot; conflito de chave de observação mantém histórico; checkpoint HTTP não implementado |
| RN-026/028; RT-014 | bronze.pncp_responses e gatilhos imutáveis | hash deve corresponder aos bytes; alterações diretas em Bronze, versão, item, composição e auditoria falham; dados de teste são sintéticos |
| RT-014; RNF-014/017; TST-048/051 parciais | scripts/radar_ops.py e radar_schema.py | ensaio real da revisão explícita em cópia restaurada; inventário/estrutura/contagens e reversibilidade; manifestos e limpeza registrados em _implementacao |
| RT-015; RNF-012 | runner C0 reutilizado | suíte completa Docker sem fonte externa, credencial de teste sem privilégios nas tabelas operacionais; nenhuma modificação de testes legados |

Arquivo de testes novo: tests/test_procurement_foundation.py, **25 casos**
incluindo parametrizações. Rodada 20261013T170515Z_0ef2d0bc: **95 aprovados**
(70 anteriores + 25 novos), zero falhas/erros/ignorados, mesmo warning legado.
Teste primeiro: 20261013T165828Z_1d2b40fe falhou por ausência do módulo; implementação
resolveu sem mudar testes legados. A rodada inicial intermediária tinha 89 aprovados.

Encerramento: 20261013T223714Z_a4938bf7 repetiu a suíte completa, 95 aprovados em
19,40 s, 1 warning, nenhum teste ignorado. Ensaio real
20261013T223425Z_34e76a5d aprovado, restauração integral e limpeza confirmadas;
36 tabelas legadas com contagens/estruturas iguais em todas as etapas. Nove
tabelas novas, 19 FKs RESTRICT, oito gatilhos e quatro funções verificados;
ausência após downgrade e estrutura idêntica no segundo upgrade confirmadas.
Revisão principal reconfirmada 20260817_0009, zero schemas públicos novos no
principal, zero bancos/papéis temporários residuais. Contagens completas, hash
do backup e comandos no controle de implementação.

Modelos SQLAlchemy mapeiam as seis tabelas do domínio procurement. O DDL e suas
constraints pertencem à revisão explícita; não usar metadata.create_all como
instalador. As três tabelas de infraestrutura têm DDL aditivo para o adapter futuro.
JSON interno tem schema_version e hashing; validação de contratos/manifestos
específicos das APIs e autorizações comerciais permanece no incremento de ingestão.

Limites: sem UI/HTTP PNCP, sem coleta, sem serviço ativo do Radar, sem validação
oficial de CNPJ, sem análise/invalidação comercial nem leases/fencing operacionais.
Os campos correspondentes são preparação estrutural, não evidência de integração.
O bloqueio transacional do pai foi implementado; carga concorrente e desempenho
não foram medidos. RPO operacional, recuperação de documentos/Prefect e igualdade
de cada valor legado não são provados por contagens e fingerprints estruturais.

O novo ADR-022 documenta a tabela de observações, FKs compostas e sequência aditiva
de módulos. Esquema de documentos/evidências e FK evidence_id entram juntos depois;
o módulo atual não aceita uma evidência documental sem integridade referencial.
Aplicação no principal permanece bloqueada por permissão e por guarda da revisão.
