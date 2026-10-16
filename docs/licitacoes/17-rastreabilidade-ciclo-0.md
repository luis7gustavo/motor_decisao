# Rastreabilidade da fundação — Ciclo 0

2026-10-13. Adendo à matriz 17. Nenhuma funcionalidade RF foi implementada.
A matriz original é trabalho preexistente não rastreado; este arquivo novo
mantém evidências da implementação separadas, conforme ADR-021.

| Origem | Componente | Evidência e alcance |
|---|---|---|
| RT-011; RNF-019; parcela TST-053 | compose.radar-ops.yml; docker/Dockerfile.radar-ops; .gitattributes | Build/configuração executados no Docker Desktop, checkout Windows com espaços, scripts LF; segundo host e locks completos não validados |
| RT-015; RNF-012; parcelas TST-046/070 | scripts/radar_ops.py; scripts/radar_test_entry.py; tests/test_radar_ops.py | Recusa principal/alvo não criado, OID e servidor conferidos; pytest como UID 65534 com papel temporário sem privilégios nas tabelas principais; 16 testes novos, suíte completa 70 aprovados |
| RT-014; RNF-014/017; parcelas TST-048/051 | scripts/radar_ops.py, modo rehearse | Dump custom restaurado em transação única; revisão sintética fora da cadeia produtiva; upgrade/downgrade/upgrade até IDs explícitos, contagens e estrutura conferidas; manifesto e limpeza de bancos próprios |
| RT-012; RNF-002 (parcial) | runner e manifesto | Sem .env no processo de teste; sem credencial administrativa herdada; logs de restore suprimidos, relatórios só identificadores/contagens/hashes; não representa implementação de CSRF/downloader/parser |

O baseline contém 54 testes legados aprovados; nenhum teste legado foi editado.
O aviso de depreciação FastAPI/Starlette/httpx permanece sem mudança de dependências.
Falha inicial de infraestrutura: audit hook bloqueava `ldconfig -p` usado pelo
Trio; exceção estrita de leitura corrigida, sem liberar subprocessos arbitrários.
Todos os bancos/papéis daquela tentativa foram removidos. O teste novo começou
com erro de importação esperado antes da implementação (teste escrito primeiro).

Os testes novos usam RT-014 para migração; referências RT-013 dos registros
iniciais eram imprecisas: RT-013 corresponde à observabilidade. Este adendo
corrige o vínculo sem modificar retrospectivamente mensagens de commits.

Não declarar TST-048/051 completos para produto: não há migração pública real,
replay funcional de análises, backup de documentos/Prefect ou medida de RPO
operacional. Contagens iguais são verificação parcial, não prova de conteúdo
idêntico linha a linha. Os hashes estruturais cobrem colunas, constraints e
índices das tabelas, sem afirmar cobertura de todos os objetos PostgreSQL.
CONNECT herdado de PUBLIC e bypass nativo do audit hook são limitações descritas
em [ADR-021](19-adr-021-fundacao.md). Não usar o runner para código não confiável.

Comandos exatos, hashes, contagens e manifestos em
[_implementacao.md](_implementacao.md). Evidência bruta fica em backups/, fora
do Git. Especificação e testes dos ciclos de produto continuam propostos.
