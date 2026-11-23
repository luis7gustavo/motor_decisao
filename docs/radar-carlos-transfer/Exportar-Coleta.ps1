param(
    [Parameter(Mandatory)][string]$Destino,
    [switch]$Execute,
    [switch]$Final,
    [string]$StatusArquivo
)

$ErrorActionPreference = 'Stop'
$origem = 'C:\Users\luisg\OneDrive\Documentos\ChatGPT\motor publico'
$operacao = Join-Path $origem 'motor_decisao'
$coleta = Join-Path $origem 'motor_decisao_coleta_20261111'
$c2Credenciais = Join-Path $operacao 'backups/c2/c2-20261013/target-credentials.json'
$destinoCompleto = [IO.Path]::GetFullPath($Destino)
$containers = @(
    'motor_postgres',
    'motor_decisao-prefect-history-dedicated-postgres-1',
    'motor_decisao-prefect-history-dedicated-redis-1',
    'motor_decisao-prefect-history-dedicated-server-1',
    'motor_decisao-prefect-history-dedicated-services-1',
    'motor_decisao-radar-history-dedicated-worker-1'
)
$images = @('pgvector/pgvector:pg15', 'postgres:15-alpine', 'redis:7-alpine', 'radar-history-prefect:3.8.8-sa2.0.49')

function Invoke-Docker([string[]]$DockerArgs) {
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "docker $($DockerArgs -join ' ') falhou ($LASTEXITCODE)" }
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw 'Docker CLI não encontrado.' }
if (-not (Test-Path -LiteralPath $operacao) -or -not (Test-Path -LiteralPath $coleta)) { throw 'Checkouts de origem não encontrados.' }
if (-not (Test-Path -LiteralPath $c2Credenciais)) { throw 'Credenciais C2 não encontradas.' }
$c2 = Get-Content -Raw -LiteralPath $c2Credenciais | ConvertFrom-Json
$servidor = (& docker exec motor_postgres psql -U motor -d motor_decisao -Atc 'SELECT system_identifier FROM pg_control_system();').Trim()
if ($LASTEXITCODE -ne 0 -or $servidor -ne [string]$c2.server) { throw 'O alvo C2 não corresponde ao PostgreSQL identificado.' }
if ($Final -and -not $Execute) { throw '-Final exige -Execute.' }
if ($Final -and ([string]::IsNullOrWhiteSpace($StatusArquivo) -or -not (Test-Path -LiteralPath $StatusArquivo -PathType Leaf))) {
    throw 'No corte final, informe -StatusArquivo com o status salvo antes de parar o worker.'
}
if (Test-Path -LiteralPath $destinoCompleto) { throw "Destino já existe: $destinoCompleto" }

$estado = foreach ($nome in $containers) {
    $id = & docker ps -a --filter "name=^/$nome`$" --format '{{.Names}}|{{.Status}}'
    [pscustomobject]@{ Container = $nome; Estado = ($id -join '') }
}
$estado | Format-Table -AutoSize
Write-Host "Destino: $destinoCompleto"
Write-Host "Imagens: $($images -join ', ')"
Write-Host "Banco C2 identificado: $($c2.database) no servidor $servidor"
Write-Host 'Será copiado o diretório backups inteiro (cerca de 30 GB em 23/11), inclusive arquivos sensíveis.'
if (-not $Execute) { Write-Host 'Pré-conferência concluída. Execute com -Execute -Final após o corte coordenado.'; return }

if ($Final) {
    foreach ($nome in @('motor_decisao-radar-history-dedicated-worker-1','motor_decisao-prefect-history-dedicated-services-1','motor_decisao-prefect-history-dedicated-server-1','motor_decisao-prefect-history-dedicated-redis-1')) {
        if ((& docker inspect --format '{{.State.Running}}' $nome 2>$null) -ne 'false') {
            throw "Corte final recusado: $nome precisa estar parado."
        }
    }
}

New-Item -ItemType Directory -Path $destinoCompleto -ErrorAction Stop | Out-Null
$temporarios = @(
    @{ Container='motor_postgres'; Dentro='/tmp/transferencia-carlos-motor.dump'; Fora='motor_decisao.dump'; Banco='motor_decisao'; Usuario='motor' },
    @{ Container='motor_postgres'; Dentro='/tmp/transferencia-carlos-c2.dump'; Fora='c2-coleta.dump'; Banco=[string]$c2.database; Usuario='motor' },
    @{ Container='motor_decisao-prefect-history-dedicated-postgres-1'; Dentro='/tmp/transferencia-carlos-prefect.dump'; Fora='prefect-dedicado.dump'; Banco='prefect'; Usuario='prefect' }
)
try {
    $metadados = [ordered]@{
        criado_utc = (Get-Date).ToUniversalTime().ToString('o')
        tipo = $(if ($Final) { 'corte_final' } else { 'fotografia_preliminar' })
        origem_operacao = $operacao
        origem_coleta = $coleta
        containers = $estado
        imagens = $images
        c2_database = [string]$c2.database
        c2_role = [string]$c2.role
        c2_server = $servidor
        redis_incluido = [bool]$Final
        nota = 'O .env não foi copiado. Não iniciar outro worker sem conciliar a origem.'
    }
    $metadados | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $destinoCompleto 'INVENTARIO.json') -Encoding utf8
    if ($StatusArquivo) {
        Copy-Item -LiteralPath $StatusArquivo -Destination (Join-Path $destinoCompleto 'status-missao.json')
    } else {
        & docker exec motor_decisao-radar-history-dedicated-worker-1 python scripts/radar_pncp_history.py status |
            Set-Content -LiteralPath (Join-Path $destinoCompleto 'status-missao.json') -Encoding utf8
        if ($LASTEXITCODE -ne 0) { throw 'Falha ao ler status da missão.' }
    }
    git -C $operacao status --short --branch | Set-Content -LiteralPath (Join-Path $destinoCompleto 'git-operacao.txt') -Encoding utf8
    git -C $coleta status --short --branch | Set-Content -LiteralPath (Join-Path $destinoCompleto 'git-coleta.txt') -Encoding utf8

    foreach ($item in $temporarios) {
        Invoke-Docker -DockerArgs @('exec', $item.Container, 'pg_dump', '-U', $item.Usuario, '-d', $item.Banco, '-Fc', '-f', $item.Dentro)
        Invoke-Docker -DockerArgs @('cp', "$($item.Container):$($item.Dentro)", (Join-Path $destinoCompleto $item.Fora))
        Invoke-Docker -DockerArgs @('exec', $item.Container, 'rm', '-f', $item.Dentro)
    }

    $codigoDestino = Join-Path $destinoCompleto 'codigo-coleta'
    New-Item -ItemType Directory -Path $codigoDestino | Out-Null
    foreach ($nome in @('app','config','pipelines','scripts','tests','docs','alembic','docker')) {
        Copy-Item -LiteralPath (Join-Path $coleta $nome) -Destination (Join-Path $codigoDestino $nome) -Recurse -Force
    }
    foreach ($nome in @('pyproject.toml','requirements.txt','alembic.ini')) {
        Copy-Item -LiteralPath (Join-Path $coleta $nome) -Destination (Join-Path $codigoDestino $nome)
    }
    $configDestino = Join-Path $destinoCompleto 'compose-e-build'
    New-Item -ItemType Directory -Path $configDestino | Out-Null
    foreach ($nome in @('docker-compose.yml','compose.radar-history-dedicated.yml','.env.example','requirements.txt')) {
        Copy-Item -LiteralPath (Join-Path $operacao $nome) -Destination (Join-Path $configDestino $nome)
    }
    Copy-Item -LiteralPath (Join-Path $coleta 'compose.radar-history-enrichment-20261111.yml') -Destination $configDestino
    Copy-Item -LiteralPath (Join-Path $operacao 'docker/Dockerfile.radar-history-prefect') -Destination $configDestino
    Copy-Item -LiteralPath (Join-Path $operacao 'backups') -Destination (Join-Path $destinoCompleto 'evidencias') -Recurse -Force

    if ($Final) {
        Invoke-Docker -DockerArgs @('cp', 'motor_decisao-prefect-history-dedicated-redis-1:/data', (Join-Path $destinoCompleto 'redis-dedicado-data'))
    }
    Invoke-Docker -DockerArgs (@('save','-o',(Join-Path $destinoCompleto 'imagens-docker.tar')) + $images)

    $manifesto = Get-ChildItem -LiteralPath $destinoCompleto -Recurse -File |
        Where-Object { $_.Name -ne 'MANIFESTO-SHA256.csv' } |
        ForEach-Object {
            [pscustomobject]@{
                Caminho = [IO.Path]::GetRelativePath($destinoCompleto, $_.FullName)
                Bytes = $_.Length
                SHA256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
            }
        }
    $manifesto | Export-Csv -LiteralPath (Join-Path $destinoCompleto 'MANIFESTO-SHA256.csv') -NoTypeInformation -Encoding utf8
    Write-Host "Pacote concluído: $destinoCompleto ($($manifesto.Count) arquivos)"
} catch {
    Write-Error "Pacote INCOMPLETO em $destinoCompleto. $($_.Exception.Message)"
    throw
} finally {
    foreach ($item in $temporarios) {
        & docker exec $item.Container rm -f $item.Dentro 2>$null | Out-Null
    }
}

