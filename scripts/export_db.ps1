param(
  [string]$OutputPath
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

if (-not (Test-Path ".env")) {
  Copy-Item ".env.example" ".env"
}

docker compose up -d postgres

if (-not $OutputPath) {
  $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
  $backupDir = Join-Path $ProjectRoot "backups"
  New-Item -ItemType Directory -Force -Path $backupDir | Out-Null
  $OutputPath = Join-Path $backupDir "motor_decisao_$stamp.dump"
}

$resolvedOutput = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutputPath)
$containerPath = "/tmp/motor_decisao.dump"

Write-Host "Gerando dump no container..."
docker exec motor_postgres pg_dump -U motor -d motor_decisao -Fc -f $containerPath
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Copiando dump para $resolvedOutput"
docker cp "motor_postgres:$containerPath" "$resolvedOutput"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker exec motor_postgres rm -f $containerPath | Out-Null

Write-Host "Backup criado: $resolvedOutput"
exit 0
