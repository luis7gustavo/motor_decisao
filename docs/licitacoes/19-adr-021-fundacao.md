# ADR-021 — Runner de fundação e bancos descartáveis protegidos

2026-10-13. RT-011/012/014/015. Complemento ao registro 19, que permanece
como documentação preexistente não rastreada e fora dos commits desta missão.

O prompt de implementação permite bancos temporários no PostgreSQL existente,
enquanto 14/21 descrevem uma stack independente futura. Nesta fundação, seguir
a autorização explícita do prompt: bancos distintos, sem trocar volumes ou
alterar serviços existentes. O runner efêmero `radar-ops` entra em arquivo
Compose adicional e depende apenas de postgres saudável. Não executa bootstrap,
Prefect, coleta ou migration no principal. Não precisa de healthcheck porque
é tarefa pontual cujo resultado é o código de saída.

Alternativas: nova instância PostgreSQL (não necessária e não autorizada como
novo servidor); rodar pytest na API com credenciais operacionais (rejeitada);
runner que provisiona alvos temporários e separa o processo pytest (escolhida).

O coordenador recebe o ambiente PostgreSQL para backup/restore e provisionamento.
Guarda nomes exatos, OIDs e identidade do servidor; nomes já existentes são
recusados. A limpeza só alcança criações registradas pela execução corrente,
confirma identidade e remoção e reporta falhas sem ocultar a causa anterior.
Interrupção abrupta pode deixar recursos: o manifesto é evidência para inspeção,
não autorização automática de exclusão em outra execução.

Pytest roda como UID 65534, sem .env, sem herdar credenciais administrativas,
com papel temporário sem superuser/createdb/createrole e permissões somente nas
tabelas sintéticas. Antes da suíte, o runner verifica zero privilégios do papel
em tabelas operacionais. O PostgreSQL legado concede CONNECT via PUBLIC; não
revogamos esse privilégio global nem alteramos ACL operacional. Isso permite
conexão ao catálogo, mas não acesso às linhas operacionais. Não constitui
isolamento contra código malicioso arbitrário; é isolamento da suíte revisada.
O audit hook bloqueia sockets Python externos e subprocessos, preservando
HTTP simulado e acesso interno ao PostgreSQL. Bibliotecas nativas não são um
firewall; nenhum teste que faça coleta nativa está autorizado neste runner.

Restauração usa dump custom, transação única e erro fatal. Contagens vêm somente
da cópia restaurada. Ensaio sintético copia Alembic para /tmp e acrescenta revisão
vazia fora da cadeia versionada. Migração real futura exige revisão manual,
destino exato e verificações de estrutura específicas, além das guardas e
comparações de contagens. Downgrade nunca cruza a origem da cópia.

Imagem derivada da API instalada; cliente PostgreSQL 15 obtido do repositório
oficial. Esta fundação não fecha locks/digests de todas as dependências legadas,
metas RPO/RTO de produção, recuperação de Prefect/documentos ou isolamento
hostil. TST-046/048/051/053/070 permanecem parciais onde excedem este escopo.
Evidências executadas e limitações ficam em _implementacao.md e no adendo 17.
