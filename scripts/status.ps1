$ErrorActionPreference = "Stop"
$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

if (-not (Test-Path ".env")) {
  Copy-Item ".env.example" ".env"
  Write-Host "Arquivo .env criado a partir de .env.example. Revise credenciais se necessario."
}

docker compose up -d postgres redis
docker compose run --rm api python scripts/collect_status.py @args
exit $LASTEXITCODE
