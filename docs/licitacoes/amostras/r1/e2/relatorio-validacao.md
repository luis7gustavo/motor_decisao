# R1-E2 — Relatório de validação documental

Versão 0.3-R1-E2, 2026-10-13, America/Sao_Paulo. Revisado em R1.
Escopo: somente docs/licitacoes do checkout motor_decisao, branch docs/radar-licita-especificacao, HEAD/base 261b32107ff7b053c68c1df98e88de329c41c0f5.

## Evidência efetivamente produzida

- Baseline Python offline, UID 65534, exit 0, 0,322 s internos: [baseline.json](baseline.json), hashes iniciais e corpos 09/12/D10 antes da propagação; Git inicial [git-inicial.txt](git-inicial.txt).
- Primeira validação documental: exit 1, 0,509 s; duas âncoras com underscore tratadas incorretamente pelo verificador, sem link documental quebrado. Evidência [validate-01.json](validate-01.json); corrigida normalização do slug, preservados os links originais. Verificação final atual em [validate.json](validate.json).
- Mermaid CLI/Mermaid 11.12.0 em imagem upstream por digest, linux/amd64, Node v18.20.5/Puppeteer 23.11.1/Chromium 124.0.6367.78: 15 blocos de 13 fontes renderizados em SVG, zero falhas, exit 0, 55,529 s internos. [Relatório por bloco](renderizados/mermaid-report.json), fontes extraídas/manifest em mermaid e SVGs em renderizados. Não foi renderer público nem UI GitHub.
- Ensaio de caminhos com espaços/acentos no Docker Desktop: UID 65534 lê UTF-8, escreve somente na saída permitida; entrada/scripts/raiz rejeitam escrita. Três scripts LF confirmados; exit 0, 0,013 s. [Relatório de portabilidade](<saída com espaços e acentos/portabilidade.json>). Não equivale a segundo host ou portabilidade da aplicação.

## Ambiente e limites

[Procedência, comandos e reprodução](../../../ferramentas-r1/README.md). Python oficial: sha256:527c28b29498575b851ad88e7522ac7201bbd9e920d2c11b00ff2b39b315f5f8; Mermaid upstream: sha256:e8edd23a12b7939cabdb7fd9d2c6cbe7d7b8c37f68d62c782fd8e405b61529d6. Entrypoints explicitamente substituídos, containers efêmeros não root, rootfs RO, caps removidas, no-new-privileges, rede none, sem portas/segredos/socket/banco/volumes operacionais. Entrada documental RO, saída específica desta revisão RW.

Python: 1 CPU, 256 MiB, 64 PIDs, tmpfs 32 MiB, deadline 120 s (30 s portabilidade). Renderer: 1 CPU, 1 GiB, 256 PIDs, tmpfs 256 MiB, 45 s/bloco, 900 s total. Dependências embarcadas nas imagens, nenhuma instalação no host. Scripts/configuração e entradas com hash nos relatórios. Duração individual de aquisição de imagens não instrumentada; ambas concluíram com digest esperado/exit 0, sem amostra.

Inspeção inicial do renderer via sh/timeout terminou exit 1 porque mmdc não estava no PATH; duração ~4,9 s incluía consulta dos metadados de imagem. Corrigido caminho da ferramenta conforme entrypoint inspecionado; nenhuma biblioteca instalada. Warnings de fallback de fontes locais KaTeX durante render; rede do container desativada, sem chamadas externas. Exit zero do renderer confirma parse/render, não regra de negócio.

## Revisão humana e limites

Conferidos por leitura: responsabilidades de RF-028/035–038, prioridades e dependências condicionais, transições de 22 §9 separadas de RN-032/D10, entidades/FKs e migração aditiva, preços/custos/etapas, coortes/denominadores, ADRs, fontes por atributo, orçamento zero e gates. Fórmulas/exemplos antigos de 09/12 preservados; nenhum recálculo necessário. D01–D15 têm checklist de impacto no índice; D04/D05 atores/cardinalidades revisados textualmente.

**Não executado:** renderização PlantUML (ferramenta fixada não preparada), teste do renderer GitHub, inspeção visual exaustiva, aplicação/suíte TST, benchmark, banco/restore/migrations, amostragem/normalização de dados públicos, validação DV operacional ou qualquer contato comercial. Fixtures TST-056–071 são especificações, não resultados aprovados. Falha do Engine E1 é histórica; E2 acessível não libera coleta. QA-R1-01 apenas parcialmente resolvida para ferramenta documental, não runner de amostra/produto. QA-R1-02–05 continuam.

Relatórios originais validacao-documental.json e diagramas/validacao-mermaid.json mantêm datas/escopo de 12/10, com adendo explícito de que não abrangem R1. Nenhuma evidência amostral observada: ledger E1 permanece 0/30, E2 não faz novas chamadas. Fonte documental não comprova implementação/conversão/capacidade.

## Encerramento

Revisão documental concluída em 13/10; validação final sem erros e inspeção Git registradas no progresso. Não é aprovação de produto. A checagem final cobre 237 links e preservação dos 15 blocos já renderizados; metadados/duração/hash da última execução estão em validate.json. IDs e campos verificáveis pelo script têm resultado atual em validate.json; verificações semânticas/dependências condicionais são humanas, sem alegar parser universal de documentos.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: escopo/digests/evidências offline, falhas reais e limitações, separados dos relatórios originais.
