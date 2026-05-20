param(
  [switch]$Sync
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

if (-not (Test-Path ".env")) {
  Copy-Item ".env.example" ".env"
  Write-Host "Arquivo .env criado a partir de .env.example. Revise credenciais se necessario."
}

Write-Host "Garantindo stack local..."
docker compose up -d postgres redis selenium api

Write-Host "Aguardando API..."
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

$asyncValue = if ($Sync) { "false" } else { "true" }
$url = "http://127.0.0.1:8010/ops/bronze-cycle?async_run=$asyncValue"

try {
  Write-Host "Acionando ciclo Bronze via API: $url"
  $response = Invoke-RestMethod -Method Post -Uri $url -TimeoutSec 20
  $response | ConvertTo-Json -Depth 12
  Write-Host "Use StatusMotor.cmd ou scripts\status.ps1 para acompanhar."
  exit 0
} catch {
  Write-Host "Falha ao acionar ciclo Bronze."
  if ($_.ErrorDetails.Message) {
    Write-Host $_.ErrorDetails.Message
  } else {
    Write-Host $_.Exception.Message
  }
  exit 1
}
