# Prompt mestre para executar o MVP em várias sessões

Copie o conteúdo abaixo em uma nova conversa aberta neste workspace. Ele executa uma sessão por vez, usando arquivos para conservar o contexto. Para retomar uma sessão interrompida, preserve o mesmo STATUS.md.

## Prompt para copiar

Você é responsável por executar o MVP de apresentação do Radar Licita neste workspace:
<checkout-local-radar_licita>

Prazo de apresentação: 16/10/2026, America/Sao_Paulo. O pacote de planejamento está em plans/mvp-apresentacao-2026-10-16/.

Leia README.md, STATUS.md, CONTRATO_DEMO.md e os repasses já existentes dessa pasta. Leia também AGENTS.md aplicáveis. Escolha a primeira sessão ainda não concluída cujas dependências estejam satisfeitas e execute o respectivo SESSAO_XX.md. Se houver sessão em andamento, retome-a antes de começar outra. Exceção para o prazo: se faltar critério central, registre MVP incompleto, preserve as etapas faltantes e avance a S07/S08 somente para ensaio e material parcial. Orçamento e busca inicial de parceiros continuam obrigatórios; não descartá-los para chamar o MVP de concluído. Execute uma sessão nesta conversa, salvo se eu pedir expressamente mais.

O produto desta missão é backend/ + frontend/ da raiz, com FastAPI, React e SQLite local. Preserve motor_decisao/, o banco original e toda configuração sensível. A especificação ampla do legado é contexto de domínio; não é uma ordem para implementar todos os requisitos até amanhã.

Priorize a jornada: informar orçamento → receber oportunidades ordenadas que cabem → abrir item/evidência → buscar possíveis parceiros em resultados de pregão → cadastrar candidata → associar cotação atual → simular e registrar próximo passo. Leia também ESCOPO_ORCAMENTO_E_PARCEIROS.md; os dois novos recursos são obrigatórios. Prepare casos sintéticos identificados e um arquivo oficial histórico sem mudar datas reais. A S05 valida leitura pública limitada com adapter real; a apresentação funciona também com cache oficial identificado. Simulação manual sem cotação pode calcular valores, mas permanece pendente.

Trabalhe de forma autônoma nas correções reversíveis previstas. Use subagentes quando houver tarefas independentes; S04 S05 e S06 podem ocorrer em paralelo após S03, com propriedade de arquivos definida; S07 integra routers e os aceites. Execute oito sessões conforme o plano atualizado. Não crie novas conversas do usuário automaticamente. Não faça push, deploy, contato comercial ou envio de propostas.

Antes de qualquer escrita em banco, confira o caminho resolvido e exija destino de teste/demo separado. Desative scheduler. Evite atualizar dependências. Não trate os 19 testes históricos nem o README anterior como prova do estado final.

Execute as validações da sessão e registre comandos e resultados reais. Ao terminar, escreva repasses/SXX.md, atualize STATUS.md e informe o que foi concluído, o que falta e o nome da próxima sessão. Se falhar um critério crítico, mantenha a etapa pendente/bloqueada com reprodução e próxima ação. Não altere regras para fabricar resultado positivo. Aplique o plano reduzido quando o prazo exigir.
