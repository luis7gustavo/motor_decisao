param(
    [Parameter(Mandatory)][string]$Pacote,
    [switch]$Verificar,
    [switch]$Restaurar,
    [string]$Destino,
    [string]$EnvArquivo
)

$ErrorActionPreference = 'Stop'
$raiz = [IO.Path]::GetFullPath($Pacote)
$manifestoPath = Join-Path $raiz 'MANIFESTO-SHA256.csv'
if (-not (Test-Path -LiteralPath $manifestoPath)) { throw 'MANIFESTO-SHA256.csv não encontrado. Pacote incompleto.' }
$itens = Import-Csv -LiteralPath $manifestoPath
if (-not $itens) { throw 'Manifesto vazio.' }
$falhas = @()
foreach ($item in $itens) {
    $caminho = [IO.Path]::GetFullPath((Join-Path $raiz $item.Caminho))
    if (-not $caminho.StartsWith($raiz + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        $falhas += "Caminho fora do pacote: $($item.Caminho)"; continue
    }
    if (-not (Test-Path -LiteralPath $caminho -PathType Leaf)) {
        $falhas += "Ausente: $($item.Caminho)"; continue
    }
    $arquivo = Get-Item -LiteralPath $caminho
    if ($arquivo.Length -ne [long]$item.Bytes) {
        $falhas += "Tamanho diferente: $($item.Caminho)"; continue
    }
    $hash = (Get-FileHash -LiteralPath $caminho -Algorithm SHA256).Hash
    if ($hash -ne $item.SHA256) { $falhas += "SHA-256 diferente: $($item.Caminho)" }
}
if ($falhas.Count) { $falhas | ForEach-Object { Write-Error $_ }; throw "$($falhas.Count) falha(s) na verificação." }
Write-Host "$($itens.Count) arquivos verificados. SHA-256 íntegro."
if (-not $Restaurar) { Write-Host 'Verificação apenas: nenhum contêiner foi alterado.'; return }
if ($Verificar) { throw 'Use -Verificar ou -Restaurar, não ambos.' }
if ([string]::IsNullOrWhiteSpace($Destino) -or [string]::IsNullOrWhiteSpace($EnvArquivo)) {
    throw '-Restaurar exige -Destino (diretório novo) e -EnvArquivo (segredos recebidos separadamente).'
}
$inventario = Get-Content -Raw -LiteralPath (Join-Path $raiz 'INVENTARIO.json') | ConvertFrom-Json
if ($inventario.tipo -ne 'corte_final' -or -not $inventario.redis_incluido) {
    throw 'Restauração recusada: pacote não é um corte final com Redis dedicado.'
}
$destinoCompleto = [IO.Path]::GetFullPath($Destino)
if (Test-Path -LiteralPath $destinoCompleto) { throw "Destino já existe: $destinoCompleto" }
if (-not (Test-Path -LiteralPath $EnvArquivo -PathType Leaf)) { throw 'Arquivo .env privado não encontrado.' }
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw 'Docker CLI não encontrado.' }
$senhaLinha = ($envTexto -split "\r?\n" | Where-Object { $_ -like 'MOTOR_POSTGRES_PASSWORD=*' } | Select-Object -First 1)
if (-not $senhaLinha -or $senhaLinha -eq 'MOTOR_POSTGRES_PASSWORD=ALTERAR_PARA_SENHA_PRIVADA') { throw 'Preencha MOTOR_POSTGRES_PASSWORD no .env privado.' }
$envTexto = Get-Content -Raw -LiteralPath $EnvArquivo
if ($envTexto -match '(?m)^MOTOR_POSTGRES_USER=(?!motor\s*$)' -or $envTexto -match '(?m)^MOTOR_POSTGRES_DB=(?!motor_decisao\s*$)') {
    throw 'O importador exige MOTOR_POSTGRES_USER=motor e MOTOR_POSTGRES_DB=motor_decisao, como na origem.'
}
foreach ($volume in @('motor_postgres_data','motor_decisao_radar_history_prefect_dedicated_pg','motor_decisao_radar_history_prefect_dedicated_redis')) {
    & docker volume inspect $volume 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { throw "Volume já existe; destino deve ser novo: $volume" }
}
foreach ($container in @('motor_postgres','motor_decisao-prefect-history-dedicated-postgres-1','motor_decisao-prefect-history-dedicated-redis-1')) {
    & docker container inspect $container 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { throw "Contêiner já existe: $container" }
}

function Docker([string[]]$Argumentos) {
    & docker @Argumentos
    if ($LASTEXITCODE -ne 0) { throw "Docker falhou ($LASTEXITCODE): $($Argumentos -join ' ')" }
}

New-Item -ItemType Directory -Path $destinoCompleto -ErrorAction Stop | Out-Null
foreach ($nome in @('docker-compose.yml','compose.radar-history-dedicated.yml','.env.example')) {
    Copy-Item -LiteralPath (Join-Path $raiz "compose-e-build/$nome") -Destination (Join-Path $destinoCompleto $nome)
}
Copy-Item -LiteralPath $EnvArquivo -Destination (Join-Path $destinoCompleto '.env')
foreach ($nome in @('app','config','pipelines','scripts','tests','docs','alembic','docker')) {
    Copy-Item -LiteralPath (Join-Path $raiz "codigo-coleta/$nome") -Destination (Join-Path $destinoCompleto $nome) -Recurse -Force
}
foreach ($nome in @('pyproject.toml','requirements.txt','alembic.ini')) {
    Copy-Item -LiteralPath (Join-Path $raiz "codigo-coleta/$nome") -Destination (Join-Path $destinoCompleto $nome)
}
Copy-Item -LiteralPath (Join-Path $raiz 'evidencias') -Destination (Join-Path $destinoCompleto 'backups') -Recurse -Force
$compose = @('-p','motor_decisao','-f',(Join-Path $destinoCompleto 'docker-compose.yml'),'-f',(Join-Path $destinoCompleto 'compose.radar-history-dedicated.yml'),'--profile','radar-history-dedicated')
Push-Location $destinoCompleto
try {
    Docker -Argumentos @('load','-i',(Join-Path $raiz 'imagens-docker.tar'))
    Docker -Argumentos (@('compose') + $compose + @('config','--quiet'))
    Docker -Argumentos (@('compose') + $compose + @('up','-d','postgres','prefect-history-dedicated-postgres'))
    Docker -Argumentos @('cp',(Join-Path $raiz 'motor_decisao.dump'),'motor_postgres:/tmp/motor_decisao.dump')
    & docker exec motor_postgres pg_restore --list /tmp/motor_decisao.dump | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Dump do banco principal inválido.' }
    Docker -Argumentos @('exec','motor_postgres','pg_restore','-U','motor','-d','motor_decisao','--no-owner','--no-privileges','--exit-on-error','/tmp/motor_decisao.dump')
    Docker -Argumentos @('exec','motor_postgres','rm','-f','/tmp/motor_decisao.dump')
    $c2Credenciais = Get-Content -Raw -LiteralPath (Join-Path $raiz 'evidencias/c2/c2-20261013/target-credentials.json') | ConvertFrom-Json
    $c2Uri = [uri]($c2Credenciais.url -replace '^postgresql\+psycopg:','postgresql:')
    $c2UserInfo = $c2Uri.UserInfo -split ':', 2
    if ($c2Credenciais.database -ne $inventario.c2_database -or $c2Credenciais.role -ne $inventario.c2_role -or
        $c2UserInfo.Count -ne 2 -or $c2UserInfo[0] -ne $c2Credenciais.role -or
        $c2Credenciais.role -notmatch '^[a-z][a-z0-9_]+$' -or $c2Credenciais.database -notmatch '^[a-z][a-z0-9_]+$') {
        throw 'Credenciais C2 e inventário divergem.'
    }
    $c2Senha = [uri]::UnescapeDataString($c2UserInfo[1]).Replace("'", "''")
    $c2Sql = "CREATE ROLE `"$($c2Credenciais.role)`" LOGIN PASSWORD '$c2Senha';"
    $c2Sql | & docker exec -i motor_postgres psql -U motor -d postgres -v ON_ERROR_STOP=1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Falha ao criar o papel C2.' }
    $c2Senha = $null
    $c2Sql = $null
    Docker -Argumentos @('cp',(Join-Path $raiz 'c2-coleta.dump'),'motor_postgres:/tmp/c2-coleta.dump')
    & docker exec motor_postgres pg_restore --list /tmp/c2-coleta.dump | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Dump C2 inválido.' }
    Docker -Argumentos @('exec','motor_postgres','pg_restore','-U','motor','-d','postgres','--create','--exit-on-error','/tmp/c2-coleta.dump')
    Docker -Argumentos @('exec','motor_postgres','rm','-f','/tmp/c2-coleta.dump')
    Docker -Argumentos @('cp',(Join-Path $raiz 'prefect-dedicado.dump'),'motor_decisao-prefect-history-dedicated-postgres-1:/tmp/prefect.dump')
    & docker exec motor_decisao-prefect-history-dedicated-postgres-1 pg_restore --list /tmp/prefect.dump | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Dump do Prefect inválido.' }
    Docker -Argumentos @('exec','motor_decisao-prefect-history-dedicated-postgres-1','pg_restore','-U','prefect','-d','prefect','--no-owner','--no-privileges','--exit-on-error','/tmp/prefect.dump')
    Docker -Argumentos @('exec','motor_decisao-prefect-history-dedicated-postgres-1','rm','-f','/tmp/prefect.dump')
    Docker -Argumentos (@('compose') + $compose + @('create','prefect-history-dedicated-redis'))
    Docker -Argumentos @('cp',(Join-Path $raiz 'redis-dedicado-data/.'),'motor_decisao-prefect-history-dedicated-redis-1:/data')
    Write-Host 'Bancos restaurados e Redis dedicado copiado em contêiner parado. Servidor, serviços e worker não foram iniciados.'
    Write-Host 'Confira permissões/AOF, tabelas, ledger, hashes e bind mounts segundo TRANSFERENCIA_CARLOS.md antes de ligar o Prefect.'
} finally {
    Pop-Location
}

