param(
  [string]$ReleaseTag = "pre-format-2026-09-26",
  [string]$Repository = "luis7gustavo/motor_decisao",
  [string]$BackupDirectory,
  [switch]$VerifyOnly,
  [switch]$SkipPrefectHistory
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

function Assert-Command {
  param([Parameter(Mandatory = $true)][string]$Name)

  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Comando obrigatorio nao encontrado: $Name"
  }
}

function Wait-HttpOk {
  param(
    [Parameter(Mandatory = $true)][string]$Uri,
    [int]$Attempts = 60
  )

  for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
    try {
      $response = Invoke-WebRequest -Uri $Uri -UseBasicParsing -TimeoutSec 5
      if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
        return
      }
    } catch {
      Start-Sleep -Seconds 2
    }
  }

  throw "Servico nao respondeu em $Uri depois de $Attempts tentativas."
}

Assert-Command -Name "docker"
Assert-Command -Name "curl.exe"

docker info | Out-Null
if ($LASTEXITCODE -ne 0) {
  throw "Docker Desktop nao esta pronto. Abra o Docker Desktop e execute novamente."
}

if (-not (Test-Path -LiteralPath ".env")) {
  Copy-Item -LiteralPath ".env.example" -Destination ".env"
  Write-Warning "Foi criado .env a partir de .env.example. Preencha credenciais privadas antes de reativar Mercado Livre ou Discord."
}

$downloadDir = if ($BackupDirectory) {
  $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($BackupDirectory)
} else {
  Join-Path $ProjectRoot "backups\recovery_$ReleaseTag"
}
New-Item -ItemType Directory -Force -Path $downloadDir | Out-Null

$baseUrl = "https://github.com/$Repository/releases/download/$ReleaseTag"
$assetNames = @(
  "motor_decisao_pre_format_20260926.dump",
  "SHA256SUMS.txt",
  "recovery_manifest_20260926.json"
)
if (-not $SkipPrefectHistory) {
  $assetNames += "prefect_pre_format_20260926.dump"
}

foreach ($assetName in $assetNames) {
  $destination = Join-Path $downloadDir $assetName
  if (Test-Path -LiteralPath $destination) {
    Write-Host "Asset ja existe: $assetName"
    continue
  }

  Write-Host "Baixando $assetName..."
  & curl.exe -L --fail --retry 3 --output $destination "$baseUrl/$assetName"
  if ($LASTEXITCODE -ne 0) {
    throw "Falha ao baixar $assetName."
  }
}

$hashFile = Join-Path $downloadDir "SHA256SUMS.txt"
$expectedHashes = @{}
foreach ($line in Get-Content -LiteralPath $hashFile) {
  if ($line -match '^([A-Fa-f0-9]{64})\s+\*?(.+)$') {
    $expectedHashes[$Matches[2].Trim()] = $Matches[1].ToUpperInvariant()
  }
}

$dumpsToVerify = @("motor_decisao_pre_format_20260926.dump")
if (-not $SkipPrefectHistory) {
  $dumpsToVerify += "prefect_pre_format_20260926.dump"
}

foreach ($dumpName in $dumpsToVerify) {
  if (-not $expectedHashes.ContainsKey($dumpName)) {
    throw "Hash esperado ausente para $dumpName."
  }

  $dumpPath = Join-Path $downloadDir $dumpName
  $actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $dumpPath).Hash
  if ($actualHash -ne $expectedHashes[$dumpName]) {
    throw "Hash invalido para $dumpName. Apague o arquivo e execute novamente."
  }
  Write-Host "Integridade confirmada: $dumpName"
}

if ($VerifyOnly) {
  Write-Host "Verificacao concluida; nenhum banco foi modificado."
  exit 0
}

Write-Host "Parando servicos que podem escrever nos bancos..."
docker compose --profile control stop api daemon worker-http-pc1 worker-browser-pc1 worker-manual-pc1 prefect-server prefect-services 2>$null

Write-Host "Subindo somente os bancos..."
docker compose --profile control up -d --wait postgres prefect-postgres
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$motorDump = Join-Path $downloadDir "motor_decisao_pre_format_20260926.dump"
& "$PSScriptRoot\import_db.ps1" -DumpPath $motorDump
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if (-not $SkipPrefectHistory) {
  $prefectDump = Join-Path $downloadDir "prefect_pre_format_20260926.dump"
  $containerDump = "/tmp/prefect_restore.dump"
  docker cp $prefectDump "motor_decisao-prefect-postgres-1:$containerDump"
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

  docker exec motor_decisao-prefect-postgres-1 pg_restore -U prefect -d prefect --clean --if-exists --exit-on-error $containerDump
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

  docker exec motor_decisao-prefect-postgres-1 rm -f $containerDump | Out-Null
  Write-Host "Historico do Prefect restaurado."
}

Write-Host "Reconstruindo imagens, aplicando migrations e iniciando o ambiente..."
docker compose --profile control build
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker compose --profile control run --rm api alembic upgrade head
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker compose --profile control up -d
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Wait-HttpOk -Uri "http://127.0.0.1:8010/health"
Wait-HttpOk -Uri "http://127.0.0.1:4200/api/health"

$alembicVersion = docker compose exec -T postgres psql -U motor -d motor_decisao -Atc "SELECT version_num FROM alembic_version;"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Restauracao concluida."
Write-Host "API: http://127.0.0.1:8010"
Write-Host "Prefect: http://127.0.0.1:4200"
Write-Host "Alembic: $alembicVersion"
Write-Warning "Credenciais privadas nao ficam no GitHub. Revise .env antes de habilitar OAuth, Discord e agendamentos."
