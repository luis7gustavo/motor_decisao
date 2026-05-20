param(
  [Parameter(Mandatory = $true)]
  [string]$DumpPath
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

if (-not (Test-Path ".env")) {
  Copy-Item ".env.example" ".env"
}

$resolvedDump = (Resolve-Path $DumpPath).Path
$containerPath = "/tmp/motor_decisao_restore.dump"

docker compose up -d postgres

Write-Host "Copiando dump para o container..."
docker cp "$resolvedDump" "motor_postgres:$containerPath"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Restaurando banco motor_decisao..."
docker exec motor_postgres pg_restore -U motor -d motor_decisao --clean --if-exists $containerPath
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker exec motor_postgres rm -f $containerPath | Out-Null

Write-Host "Banco restaurado com sucesso."
exit 0
