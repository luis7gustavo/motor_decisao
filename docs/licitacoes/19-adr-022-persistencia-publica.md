# ADR-022 — Persistência incremental do EP-01 e gate operacional

2026-10-13. Complemento de ADR-014/019/021; implementação isolada do TEC-02.

O ciclo 1 prepara persistência antes do adapter: a dependência HU-003→HU-001
impede declarar a história funcional completa somente por existir um repositório.
As nove tabelas criadas são os pré-requisitos usados neste incremento. Não se
criam ainda sourcing, documentos/evidências ou análises comerciais vazios. A FK
judgment_units.evidence_id será adicionada junto ao módulo de evidências; neste
incremento a proveniência passa pela versão e Bronze. Não se habilita evidência
documental por UUID sem FK. Esta sequência aditiva não muda o modelo alvo de 08.

08 exige observação auditável quando conteúdo anterior retorna, mas não nomeia
sua tabela. Adicionada procurement.version_observations, com pai/versão anterior,
versão observada, Bronze, instante, ator e correlation_id; UNIQUE por pai, Bronze
e instante da observação. Replays mantêm esse instante; observações novas precisam
de instantes próprios. Conflito da mesma chave é erro, nunca overwrite. Observação
atrasada fica no histórico sem regredir ponteiro. Instantes iguais de origens
diferentes não promovem automaticamente a segunda observação.

Alternativas: sobrescrever snapshot (rejeitada); deduplicar apenas por hash
(perde retorno temporal); guardar observação append-only (adotada). A associação
de unidade/item acrescenta version_id com FKs compostas, impedindo vínculo entre
versões inclusive fora do repositório. Publicação congela conteúdo/filhos por
triggers; publicação é apenas congelamento interno, não completude nem aptidão.
Manifesto participa do hash; obtenção local e UUID Bronze não participam. Dados
desconhecidos continuam NULL. CNPJ original é textual, validação pendente; este
ciclo não implementa DV nem emite identidade fiscal validada. Índice alternativo
somente considera identidade explicitamente validada com todos os componentes.

O repositório recebe snapshot interno preparado; não afirma contrato oficial nem
faz HTTP, análise, invalidação comercial ou confirmação de checkpoint. Transação
é do chamador; savepoint protege escrita parcial; bloqueio do pai serializa versões.
Todos os membros fornecidos neste contrato inicial são obrigatórios. Itens
opcionais e evidência documental exigem extensão explícita, sem inferência.

O prefect-init legado chama upgrade head e compartilha os arquivos. Como a
aplicação principal está desabilitada, a revisão 20261013_0010 recusa banco fora
dos nomes descartáveis do runner. Isso evita aplicação incidental em reinício;
nesta sessão o init não foi iniciado. Não se alterou serviço legado. Liberação
operacional futura exige implementação de gate explícito sob autorização, novo
backup/ensaio e janela controlada; remover a guarda silenciosamente é proibido.
Prefixo não prova posse: runner exige registro de criação, servidor e OID antes
de migrar/remover; downgrade admite apenas cópia de ensaio.

Runner de ensaio agora recusa revisão real sem verificador estrutural específico.
Para C1 verifica nove tabelas, FK RESTRICT, oito triggers, quatro funções, hashes
estruturais, ausência no downgrade e equivalência após segundo upgrade. Em todas
as etapas compara contagens e estruturas legadas, somente em cópia restaurada.
Nenhum teste escreve dados sintéticos nessa cópia.

Decisões são detalhamento de implementação; documentos originais não rastreados
permanecem preservados conforme ADR-021. Evidências e limites em _implementacao e
17-rastreabilidade-ciclo-1. HU-001/003/004 continuam incompletas como produto.
