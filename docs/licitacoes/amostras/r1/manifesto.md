# R1 — Manifesto da investigação

Versão 0.2-R1. Data 2026-10-12, America/Sao_Paulo. Revisado em R1-E1.

**Estado: amostragem não executada.** Engine Docker inacessível. Cumulativo consumido 0/30; nenhuma tentativa de dados nem resposta bruta persistida. [Ledger](ledger.csv), [perfil](perfil-estrutural.md), [relatório](relatorio.md), [fontes/rotas](matriz-fontes.md) e [procedimento](../../ferramentas-r1/README.md).

| Campo de execução | Registro |
|---|---|
| Código base | 261b32107ff7b053c68c1df98e88de329c41c0f5 |
| Ambiente efetivo | Host Windows para autoria/Git/leitura humana; Docker CLI 29.8.2, Compose v5.5.1; nenhum runtime de processamento executado |
| Imagem, digest, arquitetura, runtime/dependências | Não obtidos/não executados; não há digest fictício |
| Scripts e hashes | Nenhum script de processamento criado/executado; hash não aplicável. Configuração executável de amostragem ainda não existe |
| Contrato | Manual Compras 2.0; OpenAPI detalhado/hash pendentes. Não substituir por contrato histórico PNCP sem revalidação |
| Método de dados | Somente GET público com TLS, proposto; nenhuma chamada nesta sessão |
| Saída | Apenas documentação e ledger vazio neste diretório |
| Duração/código de saída de container | Não aplicáveis; nenhum container foi iniciado |

## Recorte exploratório provisório — proposto

Hipótese técnica: papel A4 branco para impressão, com gramatura, dimensões e quantidade por embalagem evidenciadas. Motivo: atributos que permitem formular uma comparação explícita e produto físico; não há evidência de abundância de resultados, diversidade comercial, rentabilidade ou logística favorável. Não é categoria aprovada do piloto.

Janela pretendida de resultados: 2026-09-01 a 2026-09-30. Sem filtro regional até esclarecer órgão/fornecedor/entrega. Formato das datas da requisição fica bloqueado até contrato confirmado; não enviar os literais ISO por suposição. Modalidade pregão eletrônico somente após domínio/campo verificado; classificação local se a rota não permitir filtrá-la. Categoria por atributos localmente; sem código CATMAT inventado. Páginas de 10 quando permitido pelo contrato, sem varrer o país para achar a categoria.

| Recurso | Reserva máxima de chamadas de dados | Propósito |
|---|---:|---|
| Resultados Compras | 8 | Identidade, preço/natureza/situação e datas |
| Itens/compra Compras | 8 | Vínculos, descrição, modalidade, unidade/embalagem |
| Contratos/itens | 4 | Testar ligação entre etapas, se contrato validado |
| Atas/itens | 2 | Distinguir registrado de contratado, se contrato validado |
| PNCP consulta/principal | 4 | Correspondência de IDs, apenas rotas públicas verificadas |
| CATMAT/unidade | 2 | Especificação e taxonomia sem inferir equivalência |
| Reserva para falhas/redirect/retry | 2 | Tentativas explicitamente contadas; não automática |

Total planejado máximo 30, não autorização para consumir todas. Recurso sem contrato fica bloqueado; redistribuição só mediante edição prévia do manifesto e controle cumulativo. Não abrir sessão nova para zerar orçamento. Limites adicionais: 1 requisição por vez, intervalo mínimo 1 s entre inícios ou maior exigido, 1.000 registros processados no total, 2 MiB por resposta, 20 MiB agregados, 15 min por execução auxiliar proposta. Requisição incerta consome orçamento; corpo excedente gera parcial, sem truncamento silencioso. Ao não encontrar categoria no recorte, relatar não observada, sem ampliar silenciosamente a janela.

## Histórico de revisões

2026-10-12 — 0.2-R1: criado manifesto sem execução, recorte provisório e orçamento preservado.
