$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_common.ps1')
$python = Join-Path $DemoBackend '.venv\Scripts\python.exe'
$database = $DemoDatabase
if (Test-Path -LiteralPath $DemoRunFile) {
    $run = Get-Content -LiteralPath $DemoRunFile -Raw | ConvertFrom-Json
    $database = Assert-DemoDatabasePath -Path $run.database
    foreach ($target in @(@{ name='API'; port=[int]$run.backend_port },
                         @{ name='Interface'; port=[int]$run.frontend_port })) {
        $listeners = @(Get-DemoListener -Port $target.port)
        Write-Host "$($target.name) porta $($target.port): $(if ($listeners.Count) { 'aberta' } else { 'fechada' })"
    }
    if (Get-DemoListener -Port ([int]$run.backend_port)) {
        $health = Invoke-RestMethod -Uri "http://127.0.0.1:$($run.backend_port)/api/v1/health" -TimeoutSec 5
        Write-Host "Saúde da API: $($health.status)"
    }
} else {
    Write-Host 'Serviços da demo não registrados como ativos.'
}
& $python (Join-Path $PSScriptRoot 'check_demo_db.py') $database
if ($LASTEXITCODE -ne 0) { throw 'Banco de demonstração inválido.' }
