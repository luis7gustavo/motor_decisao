$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_common.ps1')
$python = Join-Path $DemoBackend '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw 'Dependência local ausente: backend/.venv. Instale o backend antes de iniciar a demonstração.'
}
if (-not (Test-Path -LiteralPath $DemoDatabase -PathType Leaf)) {
    New-Item -ItemType Directory -Path (Split-Path -Parent $DemoDatabase) -Force | Out-Null
    & $python (Join-Path $DemoWorkspace 'examples\demo\seed_real_history.py') $DemoDatabase | Out-Host
    if ($LASTEXITCODE -ne 0) { throw 'Não foi possível gerar o snapshot isolado a partir da captura oficial.' }
}
$source = Assert-DemoDatabasePath -Path $DemoDatabase
& $python (Join-Path $PSScriptRoot 'check_demo_db.py') $source | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'Snapshot da demo inválido.' }
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss-ffff'
$destination = Join-Path $DemoBackend "data\demo\radar_real_show_$stamp.db"
if (Test-Path -LiteralPath $destination) { throw "Cópia já existe: $destination" }
Copy-Item -LiteralPath $source -Destination $destination -ErrorAction Stop
& $python (Join-Path $PSScriptRoot 'check_demo_db.py') $destination | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'Cópia da demo não passou na validação.' }
Write-Output $destination
