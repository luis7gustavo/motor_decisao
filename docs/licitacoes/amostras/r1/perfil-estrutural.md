# R1 — Perfil estrutural

Versão 0.2-R1. 2026-10-12, America/Sao_Paulo. Revisado em R1.

**Não executado.** Não há registros observados. N=0 representa ausência de amostragem, não resposta vazia da fonte. Todas as proporções são **não calculáveis**, nunca 0% de qualidade. O dicionário documental proposto está em [22](../../22-inteligencia-de-mercado-fornecedores.md).

| Recurso/campo a observar | Tipos/domínios observados | Presentes/N | Nulos/N | Inválidos/N | Utilizáveis/N |
|---|---|---|---|---|---|
| Resultado — identidade fiscal/tipo de pessoa | não observados | n/c | n/c | n/c | n/c |
| Resultado — item, sequência e situação | não observados | n/c | n/c | n/c | n/c |
| Resultado — preços/quantidade homologados | não observados | n/c | n/c | n/c | n/c |
| Item — descrição, unidade, embalagem, código | não observados | n/c | n/c | n/c | n/c |
| Resultado/contrato — datas e vínculos | não observados | n/c | n/c | n/c | n/c |
| Ata/contrato — natureza, valores, quantidades | não observados | n/c | n/c | n/c | n/c |

No perfil futuro, N é o número de registros efetivamente processados daquele recurso, inclusive rejeitados. Presente significa chave existente, mesmo com null. Ausente, null, inválido, não aplicável justificado e válido são categorias mutuamente exclusivas; presença se sobrepõe a null/inválido e não deve ser somada a elas. Utilizável é válido para a finalidade e pode ser subconjunto dos válidos estruturais. Mostrar n/N e, quando pertinente, n/N_aplicáveis com definição explícita. Semântica não confirmada vai a pendente, não inválido arbitrário. Excesso antes de terminar uma página gera perfil parcial com bytes/limite e sem alegar cobertura da página inteira.

Não persistir valores de identificação/nomes, CPFs, contatos, corpos ou URLs sensíveis. Domínios categóricos só por allowlist não pessoal; campo livre não é domínio seguro por padrão. Campo não observado não prova falta de suporte.

## Histórico de revisões

2026-10-12 — 0.2-R1: perfil sem amostra e protocolo de denominadores.
