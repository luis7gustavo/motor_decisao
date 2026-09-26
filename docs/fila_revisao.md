# Fila de Revisao Humana

A Fila de Revisao e uma interface web server-side do FastAPI para julgar as
oportunidades atuais cuja recomendacao heuristica e `revisar`. Ela usa Jinja2,
CSS e JavaScript locais; nao existe frontend ou microservico separado.

A decisao continua sendo humana. `approved_test_purchase` autoriza somente uma
etapa futura de cotacao ou compra controlada. A interface nao executa compras,
nao altera a heuristica e nao permite que o ML aprove uma compra sozinho.

## Aplicar a migration

Na raiz de `motor_decisao`, com os containers ativos:

```powershell
docker compose exec -T api alembic upgrade head
docker compose exec -T api alembic current
```

O head esperado e:

```text
20260817_0009 (head)
```

A migration cria o schema `feedback` e a tabela
`feedback.opportunity_reviews`. Ela nao limpa nem recria as tabelas Bronze,
Silver ou Gold.

## Subir e acessar

Para construir e subir os servicos essenciais:

```powershell
docker compose up -d --build postgres redis selenium api
docker compose exec -T api alembic upgrade head
```

Validar a API e abrir a fila:

```powershell
Invoke-RestMethod http://127.0.0.1:8010/health
Start-Process "http://127.0.0.1:8010/review"
```

Rotas:

| Metodo | Rota | Uso |
| --- | --- | --- |
| `GET` | `/review` | Abre o primeiro item pendente segundo os filtros. |
| `GET` | `/review/{opportunity_id}` | Abre uma oportunidade atual. |
| `POST` | `/review/{opportunity_id}/decision` | Persiste um julgamento validado. |
| `GET` | `/review/history` | Lista decisoes e eventos de desfazer. |
| `POST` | `/review/{opportunity_id}/undo` | Desativa a decisao e registra o evento. |
| `GET` | `/review/summary` | Retorna os contadores em JSON. |

## Estados humanos

| Valor interno | Significado |
| --- | --- |
| `approved_test_purchase` | Aprovada para uma futura compra teste controlada; compra ainda nao realizada. |
| `needs_reanalysis` | Aguarda novo dado, matching, custo ou confirmacao de disponibilidade. |
| `rejected` | Oportunidade descartada com motivo obrigatorio. |

`needs_reanalysis` e `rejected` exigem um motivo valido. Quando o motivo e
`other`, a observacao tambem e obrigatoria. Preco maximo de compra e aceito
somente na aprovacao.

Depois do salvamento, a aplicacao usa redirect HTTP `303`, mostra a confirmacao,
atualiza os contadores e abre o proximo item pendente. O fluxo funciona com
submissao HTML tradicional mesmo sem JavaScript.

Atalhos locais:

| Tecla | Acao |
| --- | --- |
| `A` | Abre e foca a aprovacao. |
| `N` | Abre e foca a solicitacao de nova analise. |
| `D` | Abre e foca o descarte. |
| `S` | Pula para o proximo item sem salvar. |
| `Z` | Desfaz a ultima decisao indicada pela confirmacao atual. |

Os atalhos que exigem motivo apenas abrem o formulario; eles nao contornam a
validacao do navegador nem a validacao Pydantic no servidor.

## Fila e filtros

A fila parte de `gold.decision_opportunities`, usa o snapshot da mesma
`decision_run_id` e `supplier_product_id`, e exclui somente a decisao humana
ativa para esse par oportunidade/snapshot. Isso evita duplicacao por snapshots
historicos e permite revisar novamente uma oportunidade quando uma nova rodada
produzir um novo snapshot.

Filtros disponiveis:

- fornecedor;
- texto do produto;
- margem liquida minima em percentual;
- lucro liquido minimo;
- `decision_score` minimo;
- `match_confidence` minimo;
- flag de risco;
- ordenacao por score, margem, lucro ou data.

A ordenacao padrao e `decision_score DESC`. As consultas carregam a lista, o
item e as evidencias em um numero constante de consultas, sem uma consulta por
evidencia.

## Persistencia e auditoria

`feedback.opportunity_reviews` armazena:

- UUID da revisao;
- UUID da oportunidade atual e UUID do snapshot, quando existente;
- UUID do produto Silver e `run_id`;
- evento `decision` ou `undo`;
- decisao, motivo, observacao, preco maximo e revisor;
- recomendacao e score originais da heuristica;
- score e versao do ML disponiveis no momento;
- versao da heuristica;
- data/hora, estado ativo e referencias de substituicao/desfazer.

Um indice unico parcial impede duas decisoes ativas para o mesmo par
oportunidade/snapshot, inclusive quando o snapshot e nulo. A gravacao usa
transacao e bloqueio da oportunidade. Scores, versoes e recomendacoes sao
relidos do banco dentro da transacao; o navegador nao e fonte desses valores.

Desfazer nao executa `DELETE`. A decisao anterior recebe `is_active = false`,
`invalidated_at` e `invalidated_by`; uma segunda linha `event_type = 'undo'`
referencia a decisao desfeita por `undoes_review_id`.

## Seguranca local

- autoescape do Jinja2 permanece habilitado;
- formularios usam cookie `SameSite=Strict` e token CSRF assinado com HMAC;
- defina `SILLO_REVIEW_CSRF_SECRET` com um segredo local longo fora de
  desenvolvimento;
- URLs externas aceitam apenas `http` e `https`, sem credenciais embutidas;
- links abrem com `noopener noreferrer` e imagens usam placeholder local;
- o banco e a restricao unica protegem contra duplo clique e concorrencia;
- o corpo dos formularios tem limite de tamanho e todos os campos possuem
  limites e validacao no servidor.

## Executar os testes

Suite padrao, sem escrever no banco real:

```powershell
python -m pytest -q
docker compose exec -T api python -m pytest -q
```

Os testes de integracao recusam executar se o nome do banco nao contiver
`test`. Para valida-los em um banco dedicado ja migrado:

```powershell
$env:DATABASE_URL="postgresql+psycopg://motor:motor@localhost:55432/motor_review_test"
python -m alembic upgrade head
python -m pytest tests/test_review_integration.py -q
```

Nunca aponte esse comando para `motor_decisao`: as fixtures de integracao
limpam apenas o banco dedicado de teste.

## Uso futuro no Machine Learning

As linhas `event_type = 'decision'` formam rotulos humanos versionados. Um
dataset futuro pode selecionar a ultima decisao valida por
oportunidade/snapshot, juntar os scores e versoes capturados e excluir eventos
`undo`. Isso permite comparar a recomendacao heuristica, o score do ML e a
decisao real do operador sem reescrever o passado.

Antes de treinar, o pipeline de ML ainda deve definir politica de qualidade,
balanceamento de classes, separacao temporal e tratamento de decisoes
substituidas. A existencia do feedback nao autoriza aprovacao automatica nem
compra automatica.
