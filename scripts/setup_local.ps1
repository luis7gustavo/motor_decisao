$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

if (-not (Test-Path ".env")) {
  Copy-Item ".env.example" ".env"
  Write-Host "Arquivo .env criado a partir de .env.example."
  Write-Host "Para Discord/Mercado Livre, preencha as credenciais depois do setup basico."
}

Write-Host "Subindo Postgres, Redis e Selenium..."
docker compose up -d postgres redis selenium

Write-Host "Aplicando migracoes..."
docker compose run --rm api alembic upgrade head
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Validando setup..."
docker compose run --rm api python scripts/validate_setup.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Subindo API..."
docker compose up -d api

Write-Host "Aguardando healthcheck da API..."
$healthy = $false
for ($i = 1; $i -le 30; $i++) {
  try {
    $health = Invoke-RestMethod -Uri "http://127.0.0.1:8010/health" -TimeoutSec 3
    if ($health.status -eq "ok" -and $health.database -eq $true) {
      $healthy = $true
      break
    }
  } catch {
    Start-Sleep -Seconds 2
  }
}

if (-not $healthy) {
  Write-Error "API nao respondeu saudavel em http://127.0.0.1:8010/health."
  exit 1
}

Write-Host "Setup concluido. API saudavel em http://127.0.0.1:8010/health"
exit 0
