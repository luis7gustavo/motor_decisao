# Operação e retomada — Ciclo 2

Estado em 2026-10-14: parcial; política PNCP desabilitada. Executar comandos na
raiz motor_decisao. Aplicação, testes e scripts somente pelo Docker. Principal,
schedules e push não autorizados nesta execução; não iniciar prefect-init porque
seu bootstrap legado aplica upgrade head.

## Conferir saldo sem rede

```powershell
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps --entrypoint python radar-ops -m scripts.radar_pncp_contract --session c2-20261013
```

Esperado: consumed=4, remaining=46. Ledger e recibos estão no volume compartilhado
./backups/c2/c2-20261013. Preservar arquivos, inclusive após falhas. Não executar
--init de novo nem trocar session para renovar orçamento. Janela mantida:
2026-10-07 a 2026-10-13 America/Sao_Paulo, ainda sem semântica de bordas confirmada.

## Testes sintéticos

```powershell
docker compose -f docker-compose.yml -f compose.radar-ops.yml run --rm --no-deps radar-ops tests --origin 20261013_0010
```

Runner cria banco/papel temporários próprios, aplica revisão explícita, executa
suíte offline e remove somente os recursos com registro/OID/servidor confirmados.
Não apontar DATABASE_URL ao principal; não apagar recursos por prefixo. Resultado
validado: 128 aprovados, manifesto 20261013T230028Z_5aee84a9.json. Não há DDL C2;
o ensaio de restauração da revisão C1 está no histórico, separado das fixtures.

## Fechar bloqueios antes da coleta

1. Obter documentação oficial sobre campo filtrado, inclusão das bordas, fuso,
   modalidade e termos/limites. Requisições adicionais passam pelo mesmo script
   com --url e contam no ledger, inclusive redirects e falhas. Cada invocação faz
   no máximo uma tentativa, sem retry/redirect implícito. Não usar clientes externos
   que escapem à cota; não repetir manual 401 para contornar acesso.
2. Revisar config/contracts/pncp-publicacao-policy.json e registrar referências e
   justificativas na ADR-023. enabled=false não deve virar true sem evidências;
   snapshot/hashes e revisão da política precisam permanecer rastreáveis.
3. Preparar destino isolado persistente/recuperável para amostra real. Store atual
   aceita apenas identidade exata de banco/papel/OID criada pelo runner sintético.
   Não reutilizar fixtures nem remover essa proteção para coletar no principal.
4. Criar job com escopo fixo, janela original, hash validado, uma modalidade
   verificada e session c2-20261013. Executar amostra mínima e flow manual pelo
   motor Prefect, sem deployment/schedule, respeitando as 46 tentativas restantes.
   Ainda não há comando operacional pronto de coleta real para o estado atual.
5. Guardar contagens, cursor, status, recibos/hash e resultado de retomada; não
   registrar corpos integrais ou credenciais em logs do Prefect. Declarar a
   cobertura efetiva, sem confundir amostra de uma modalidade com cobertura nacional.

Falha parcial mantém cursor e bytes. Retomada não zera as três tentativas por
página/job; not_before deve ser respeitado. Repetir após cota esgotada não envia.
Não recriar job/ledger para ocultar consumo. Sem amostra real e validação do motor
Prefect, ciclo continua em andamento; retomar C2 antes de selecionar C3.
