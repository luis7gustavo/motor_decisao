# 20 — Resumo executivo

Revisão atual: 0.3-R1-E2, 2026-10-13 (America/Sao_Paulo). Seções alteradas marcadas revisado em R1; datas anteriores preservam produção/histórico.

Versão: 0.1. Data: 2026-10-12 (America/Sao_Paulo).  
Commit base: `261b32107ff7b053c68c1df98e88de329c41c0f5`.  
Status: rascunho para revisão do operador; evolução [PROPOSTO], sem implementação.

## Problema e solução

O SILLO já organiza coleta, dados comerciais e revisão humana para compra e revenda. A evolução Radar Licita propõe transformar esse investimento em apoio à decisão de fornecimento ao governo: localizar contratação, entender edital, verificar fornecedor/produto, avaliar execução e caixa, priorizar e registrar a decisão humana. Ranking não resolve produto incompatível, lote incompleto, falta de capital ou documento ambíguo.

## Modelo de negócio e diferenciais

Uso inicial: ferramenta interna de um operador que compra de fornecedores e fornece produtos conforme condições da contratação. Receita potencial decorre da operação comercial efetivamente realizada; valor estimado, vitória e contrato não equivalem a recebimento. Estoque, compra após decisão, transportadora, envio direto ou prestador regional são alternativas dependentes de evidências, custos e permissões.

Diferenciais propostos: viabilidade por unidade completa de julgamento, logística A/B/C/D independente de score, custos Decimal, curva de caixa, histórico de versões e justificativa acessível. Preferência por remoto inclui pós-venda/garantia e não se confunde com participação eletrônica. Oportunidade de mercado é hipótese de utilidade para operação comercial individual; não foi produzido tamanho de mercado nem projeção financeira sem fonte.

## Base aproveitável e arquitetura

[CONFIRMADO] FastAPI/Jinja2, PostgreSQL, Alembic, camadas Bronze/Silver/Gold, Prefect/Crawlee e auditoria oferecem base reaproveitável. [PARCIAL] matching lexical, ML e BI não atendem integralmente o domínio novo. [NÃO-CONFIRMADO] operação saudável e desempenho atuais.

Recomendação: monólito modular FastAPI/Jinja2, mesmo PostgreSQL de domínio e infraestrutura Prefect existente; Redis mantém função de controle. Schemas procurement/sourcing/commercial, adapter PNCP e parser isolado são [PROPOSTO]. Evitar reescrita, novo banco/agendador e React sem necessidade demonstrada. Preservar história e volumes por migrations aditivas, backup e restore ensaiado.

## Roadmap recomendado

1. Validar destino, isolamento, recuperação e contratos de fontes; implantar fundação pública com versões e consulta local.
2. Acrescentar documentos/evidências/logística e revisão mínima.
3. Integrar fornecedores/cotações e matching técnico.
4. Implementar Decimal, caixa, lote completo, elegibilidade e ranking explicável.
5. Consolidar carteira, estados, fatos externos e recebimento; completar MVP com todos os Must.
6. Evoluir inteligência ampla, CNAE em escala, lances completos e ML somente com evidência própria. Histórico básico é antecipado opcionalmente no marco 2-H (revisado em R1).

Fase 0 é a especificação. Fases 1–5 contêm o MVP completo; RF-024/028 são incrementos Should. Detalhes e aceites: [15](15-roadmap-backlog.md), [16](16-plano-de-migracao.md) e [14](14-plano-de-testes.md).

## Riscos, decisões pendentes e critérios de sucesso

Riscos centrais: fonte/edital desatualizado, matching falso, frete/tributo subestimado, falta de capital, pagamento tardio, extração incompleta, PDF malicioso e excesso de confiança no score. Mitigações e residual em [13](13-seguranca-riscos-conformidade.md). Permanecem pendentes contratos PNCP detalhados/datas, parâmetros comerciais do operador, enquadramento fiscal/jurídico e ensaios operacionais. Não usar a especificação como parecer ou autorização automática de participação.

MN-01–10 propõem medir tempo de triagem, qualidade de descarte/matching, custo previsto versus realizado e caixa. Baselines desconhecidas; metas calibráveis no piloto. Integridade, evidência e ausência de pendência crítica em positivo são condições obrigatórias. Os três casos sintéticos distinguem hardware viável (score 82,98), lote sem capital (bloqueado) e presença mal definida (pendente); não são resultados comerciais reais.

## Entrega documental e leitura

Especificação com 38 RF, 21 RNF, 37 RN, 10 CP, 19 RT, 20 ADRs, 15 diagramas e 71 casos de teste planejados. Começar pelo [PRD](00-prd.md), [TRD](00-trd.md) e [índice](README.md). A matriz 17 liga produto, regra, componente, entidade, caso de uso, teste e fase. Conclusão documental não significa aplicação implementada; evidências e limitações da verificação estão no progresso.


## Descoberta de fornecedores — revisado em R1

[PROPOSTO] Marco 2-H, após fundação/identidade e junto da preparação da fase 3: resultados comparáveis → candidatos explicáveis → registro manual de contato → cotação atual por escopo → verificação antes da análise. RF-028 mantém apresentação básica/expansão futura; RF-035/036/037/038 separam ingestão, comparação, candidato e relacionamento. Não se afirma capacidade por histórico nem custo por preço público; filiais preservadas e fontes/etapas não somadas.

[23](23-especificacao-piloto-fornecedores.md) especifica hipótese, coorte e MN-11–18. Categoria/UF continuam pendentes; A4 só hipótese sem amostra. E1 leu documentação mas consumiu 0/30 chamadas, sem evidência de conversão ou diversidade. Contratos, quota/termos, dados e release do produto ainda bloqueiam ativação. Alternativa manual com origem continua; ausência de histórico não bloqueia análise suficiente, cotação obrigatória ausente mantém pendência. Fórmulas/score e estados comerciais preservados.

## Histórico de revisões

2026-10-13 — 0.3-R1-E2: escopo antecipado e síntese do piloto condicionados, contagens atualizadas; conclusão documental não significa pronto para operar.

