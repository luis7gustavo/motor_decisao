$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_common.ps1')
if (-not (Test-Path -LiteralPath $DemoRunFile)) {
    Write-Host 'Nenhuma execução iniciada por start-demo.ps1 foi registrada.'
    exit 0
}
$run = Get-Content -LiteralPath $DemoRunFile -Raw | ConvertFrom-Json
foreach ($entry in @(@{ id=[int]$run.frontend_pid; marker='vite.js' },
                     @{ id=[int]$run.backend_pid; marker='uvicorn app.main:app' })) {
    $process = Get-CimInstance Win32_Process -Filter "ProcessId=$($entry.id)" -ErrorAction SilentlyContinue
    if ($process -and $process.CommandLine -like "*$($entry.marker)*") {
        Stop-DemoProcessTree -ProcessId $entry.id
    } elseif ($process) {
        throw "PID $($entry.id) não corresponde ao processo da demo; parada recusada."
    }
}
Remove-Item -LiteralPath $DemoRunFile
Write-Host 'Serviços gerenciados da demo encerrados. Banco e logs preservados.'
