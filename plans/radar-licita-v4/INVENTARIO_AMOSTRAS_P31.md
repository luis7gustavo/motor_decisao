---
id: INV-P31
tipo: evidencia
estado: aprovado_com_limites
classe_evidencia: E0_E1_E2
atualizado_em: 2026-11-23
---

# P31 — inventário dos exemplos disponíveis

Fotografia **offline** da branch de produto iniciada no merge `a80cdfcb7fd397126d32c57b73fddcfc227096a9` e atualizada em 23/11/2026 com a exportação real autorizada por D13/D14. Este inventário descreve arquivos versionados; não atesta a fonte PNCP hoje. Caminhos, hashes SHA-256 e classes por arquivo estão em [manifestos e fixtures](MANIFESTOS_E_FIXTURES.md). Campos e lacunas estão em [schema e exemplos](SCHEMA_E_EXEMPLOS.md). A [linha de base P00](LINHA_DE_BASE_P00.md) documenta separadamente o acervo local da coleta.

| Grupo | Formato e cardinalidade observada | Classe e uso nesta P31 | Limite |
| --- | --- | --- | --- |
| `radar_licita/examples/demo/fixtures/official/PNCP_TECNOLOGIA_HISTORICA_2026-10-15.json` | JSON, 2 registros `records`, com contratação, itens e resultados aninhados | `E0_exemplo` de snapshot histórico já versionado; inspeção estrutural | Janela histórica selecionada; não é amostra comercial de periféricos |
| `radar_licita/examples/demo/fixtures/official/PNCP_TECNOLOGIA_HISTORICA_EXPANDIDA_2026-10-15.json` | JSON, 7 registros `records` na mesma família de envelope | `E0_exemplo` já versionado; inspeção estrutural | Pode sobrepor o arquivo anterior; não somar 2+7 como identidades únicas |
| `radar_licita/examples/demo/fixtures/official/PNCP_VERIFICACAO_2026-10-15.json` | JSON, registro de verificação de contrato e chamadas | Contexto `E0`; não é coleção de contratações | Evidência datada de outubro, sem revalidação atual |
| `fixtures/radar_demo/*.json` | Cinco JSON de demonstração: 5 versões de oportunidade, 14 eventos, 11 observações de resultado, 2 oportunidades/4 ações de revisão e 1 cenário financeiro | `E2_sintetico`, já versionado; regressão de interface e regras | Campos de demonstração diferem do envelope PNCP; URL `example.invalid` e hash textual não são origem oficial |
| `fixtures/radar_p31/opportunities_boundary.json` | JSON novo, 3 versões sintéticas em 2 identidades | `E2_sintetico`; versão, prazo ausente e falso positivo lexical | Mesma estrutura **externa** de `fixtures/radar_demo/opportunities.json`; não contém item, lote ou edital |
| `fixtures/radar_real/pncp_complete_20261123/` | 100 identidades reais completas no índice observado; 100 payloads e 1 manifesto | `E1_fixture` autorizado; contrato estrutural, regressão e testes de borda | Completude relativa ao índice observado; não prova cobertura nacional, frescor, abertura ou elegibilidade comercial |

## Acervo local e promoção controlada

A [linha de base P00](LINHA_DE_BASE_P00.md) registra uma exportação histórica de 20 leilões/alienações (2.748 itens, 40 PDFs), cinco candidatas de desenvolvimento cujo conteúdo já foi examinado, três reservas com processamento estrutural de corpos e uma quarta reserva por metadados. Esses grupos pertencem a fontes e momentos distintos; não formam uma única amostra de aceite. A consulta C2 descrita em P00 foi local, em 18/11. Nenhum corpo, identidade, anexo ou hash de arquivo privado desse acervo foi copiado para este PR. As reservas não foram abertas nem rotuladas aqui.

**D13/D14 e G1 estrutural aprovados em 23/11/2026:** Luis autorizou a promoção controlada de dados públicos reais. O GitHub recebe o recorte E1 de 100 identidades, com hashes e limites; o snapshot integral permanece no [Kaggle privado](https://www.kaggle.com/datasets/luisgustavo27/radar-licita-pncp-integral-2026-11-23). Este aceite não atribui `E3_real_revisado` nem `E4_real_monitorado`, e não conclui cobertura, frescor ou viabilidade.

## Recorte e modo de desfazer

O inventário cobre os exemplos JSON acima e a referência agregada P00. Não é um censo de todos os arquivos de coleta ou de todas as descrições no projeto. A P31 não modificou o worker, a coleta, a agenda, o banco ou o PNCP. O PR pode ser revertido removendo a nota, o manifesto e o caso sintético, preservando as fontes preexistentes.
