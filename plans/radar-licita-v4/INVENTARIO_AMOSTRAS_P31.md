---
id: INV-P31
tipo: evidencia
estado: em_revisao
classe_evidencia: E0_E2
atualizado_em: 2026-11-18
---

# P31 — inventário dos exemplos disponíveis

Fotografia **offline** da branch de produto no merge `a80cdfcb7fd397126d32c57b73fddcfc227096a9`, examinada em 18/11/2026. Este inventário descreve arquivos já versionados; não atesta a fonte PNCP hoje. Caminhos, hashes SHA-256 e classes por arquivo estão em [manifestos e fixtures](MANIFESTOS_E_FIXTURES.md). Campos e lacunas estão em [schema e exemplos](SCHEMA_E_EXEMPLOS.md). A [linha de base P00](LINHA_DE_BASE_P00.md) documenta separadamente o acervo local da coleta.

| Grupo | Formato e cardinalidade observada | Classe e uso nesta P31 | Limite |
| --- | --- | --- | --- |
| `radar_licita/examples/demo/fixtures/official/PNCP_TECNOLOGIA_HISTORICA_2026-10-15.json` | JSON, 2 registros `records`, com contratação, itens e resultados aninhados | `E0_exemplo` de snapshot histórico já versionado; inspeção estrutural | Janela histórica selecionada; não é amostra comercial de periféricos |
| `radar_licita/examples/demo/fixtures/official/PNCP_TECNOLOGIA_HISTORICA_EXPANDIDA_2026-10-15.json` | JSON, 7 registros `records` na mesma família de envelope | `E0_exemplo` já versionado; inspeção estrutural | Pode sobrepor o arquivo anterior; não somar 2+7 como identidades únicas |
| `radar_licita/examples/demo/fixtures/official/PNCP_VERIFICACAO_2026-10-15.json` | JSON, registro de verificação de contrato e chamadas | Contexto `E0`; não é coleção de contratações | Evidência datada de outubro, sem revalidação atual |
| `fixtures/radar_demo/*.json` | Cinco JSON de demonstração: 5 versões de oportunidade, 14 eventos, 11 observações de resultado, 2 oportunidades/4 ações de revisão e 1 cenário financeiro | `E2_sintetico`, já versionado; regressão de interface e regras | Campos de demonstração diferem do envelope PNCP; URL `example.invalid` e hash textual não são origem oficial |
| `fixtures/radar_p31/opportunities_boundary.json` | JSON novo, 3 versões sintéticas em 2 identidades | `E2_sintetico`; versão, prazo ausente e falso positivo lexical | Mesma estrutura **externa** de `fixtures/radar_demo/opportunities.json`; não contém item, lote ou edital |

## Acervo local existente, sem publicação nesta P31

A [linha de base P00](LINHA_DE_BASE_P00.md) registra uma exportação histórica de 20 leilões/alienações (2.748 itens, 40 PDFs), cinco candidatas de desenvolvimento cujo conteúdo já foi examinado, três reservas com processamento estrutural de corpos e uma quarta reserva por metadados. Esses grupos pertencem a fontes e momentos distintos; não formam uma única amostra de aceite. A consulta C2 descrita em P00 foi local, em 18/11. Nenhum corpo, identidade, anexo ou hash de arquivo privado desse acervo foi copiado para este PR. As reservas não foram abertas nem rotuladas aqui.

**D13 pendente:** Luis ainda precisa definir quais fontes existentes podem virar `E1_fixture` ou anexos versionados, com origem, titularidade e uso autorizados. O G0 permitiu uso **local** do inventário P00, não publicação desses dados. Assim, este PR não cria E1 a partir do acervo real. A validação de origem/uso e a aprovação de G1 seguem humanas; este documento não atribui `E3_real_revisado` nem `E4_real_monitorado`.

## Recorte e modo de desfazer

O inventário cobre os exemplos JSON acima e a referência agregada P00. Não é um censo de todos os arquivos de coleta ou de todas as descrições no projeto. A P31 não modificou o worker, a coleta, a agenda, o banco ou o PNCP. O PR pode ser revertido removendo a nota, o manifesto e o caso sintético, preservando as fontes preexistentes.
