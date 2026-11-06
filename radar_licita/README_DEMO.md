# Radar Licita — demonstração com dados históricos reais

Apresentação de 16/10/2026. A base ativa contém somente registros públicos capturados do PNCP: **7 pregões eletrônicos de tecnologia, 19 itens com resultado, 19 resultados PJ e 11 empresas distintas**. A trilha contratação → item → resultado conserva número de controle, CNPJ, URL e hash da fonte. A seleção é dirigida e funciona sem rede.

As propostas desses pregões estão encerradas. Resultado publicado identifica atuação histórica, sem comprovar contrato executado, equivalência técnica, estoque, interesse ou cotação atual. Os orçamentos informados pelo usuário são parâmetros de comparação, não dados de fornecedores.

## Abrir a instância preparada

Abra [http://127.0.0.1:5173/?demo=historico](http://127.0.0.1:5173/?demo=historico). A aba **Tecnologia** mostra primeiro a captura oficial. Em PowerShell, na pasta `radar_licita/`, confira uma instância já iniciada:

```powershell
.\scripts\demo\check-demo.ps1
```

Se os serviços estiverem parados, inicie assim:

```powershell
.\scripts\demo\start-demo.ps1
.\scripts\demo\check-demo.ps1
```

No primeiro uso após clonar o repositório, instale o backend com `python -m venv backend/.venv` e `backend/.venv/Scripts/python.exe -m pip install -e "./backend[dev]"`, depois execute `pnpm install` em `frontend/`. `start-demo.ps1` gera o snapshot isolado a partir da captura oficial versionada caso ainda não exista, cria uma cópia nova, valida o conteúdo e inicia API e interface nas portas 8000 e 5173. Ele recusa base fora de `backend/data/demo/` ou com registros divergentes da captura oficial. Para usar portas alternativas, passe `-BackendPort 8003 -FrontendPort 5176` e abra `http://127.0.0.1:5176/?demo=historico`. Os logs ficam em `output/demo/logs/`; a cópia ativa e os PIDs estão em `output/demo/run.json`.

O banco original `backend/radar_licita.db` permanece separado. O agendador da demonstração fica desligado. `backend/.venv`, `frontend/node_modules` e Node.js precisam estar disponíveis.

## O que mostrar

Siga [ROTEIRO_7_MIN.md](docs/demo/ROTEIRO_7_MIN.md) e [FICHA_APOIO.md](docs/demo/FICHA_APOIO.md). Comece pelas sete contratações no painel **Tecnologia**. Exemplos: microcomputadores do Exército com INFOG; licenças e acessórios da Câmara de Sorriso com MEDIATIZE e CDS SOLUTIONS; scanner com HKA; integração de sistemas com SYS4WEB; itens de informática da USP com quatro CNPJs identificados. Cada associação vem do resultado publicado do mesmo item.

A seção **Consulta adicional de propostas abertas** fica abaixo do acervo; ela exige rede e pode retornar uma amostra parcial. Não é necessária para a apresentação. As abas Radar e Acompanhamento usam a mesma base histórica real. Como os prazos estão encerrados e não há cotações atuais, não espere itens elegíveis ou margens calculadas.

## Origem e integridade

- Captura: [PNCP_TECNOLOGIA_HISTORICA_EXPANDIDA_2026-10-15.json](examples/demo/fixtures/official/PNCP_TECNOLOGIA_HISTORICA_EXPANDIDA_2026-10-15.json), obtida em **33 requisições públicas** em 15/10/2026, SHA-256 `503CE560E9A0EF8CA755CEC706AC0BE2A3170A78878777E337FA661BDA9F064D`.
- O arquivo guarda os payloads selecionados de compra, item e resultado, seus hashes canônicos e URLs oficiais. `GET /api/v1/technology/historical-exhibit` valida e lê a captura sem rede. **Reconsultar PNCP** faz nova leitura dos mesmos controles e não sobrescreve o acervo.
- [EVIDENCIAS_HISTORICO_REAL.json](docs/demo/EVIDENCIAS_HISTORICO_REAL.json) registra os hashes do arquivo de captura, do snapshot e da tela atual. [INDICE_EVIDENCIAS.md](docs/demo/INDICE_EVIDENCIAS.md) aponta o registro visual.
- `scripts/demo/check_demo_db.py` exige exatamente as compras, os itens, os CNPJs e os hashes de resultado da captura. Exige zero fornecedores comerciais, produtos, cotações, cenários financeiros, scores e documentos locais na cópia inicial.

As etapas anteriores permanecem descritas nos registros de planejamento. Os arquivos de casos sintéticos foram retirados deste pacote.

## Parar, repetir ou reconstruir

```powershell
.\scripts\demo\stop-demo.ps1
.\scripts\demo\start-demo.ps1
```

Se precisar reconstruir o snapshot oficial em **novo arquivo**, use `backend/.venv/Scripts/python.exe examples/demo/seed_real_history.py backend/data/demo/novo_snapshot.db`; o comando migra e valida uma base isolada, sem sobrescrever arquivo existente. Para obter uma captura nova, ajuste a seleção e o nome do fixture em `backend/app/technology/historical.py`, capture em novo arquivo e confira e atualize o hash fixado antes de criar outra base. O procedimento da apresentação usa a captura já conferida.

Nenhuma função faz proposta, contato comercial ou validação automática de fornecedor. O [estado técnico](plans/mvp-apresentacao-2026-10-16/STATUS.md) registra os limites.
