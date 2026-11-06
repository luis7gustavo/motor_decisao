param(
    [int]$BackendPort = 8000,
    [int]$FrontendPort = 5173,
    [string]$DatabasePath = ''
)
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '_common.ps1')

if ($BackendPort -eq $FrontendPort -or $BackendPort -lt 1 -or $BackendPort -gt 65535 -or $FrontendPort -lt 1 -or $FrontendPort -gt 65535) {
    throw 'Informe portas distintas entre 1 e 65535.'
}
if (Test-Path -LiteralPath $DemoRunFile) {
    throw "Já existe registro de execução em $DemoRunFile. Use stop-demo.ps1 antes de iniciar novamente."
}
foreach ($port in @($BackendPort, $FrontendPort)) {
    if (Get-DemoListener -Port $port) { throw "Porta $port ocupada. Pare o serviço existente ou escolha outra porta." }
}
$python = Join-Path $DemoBackend '.venv\Scripts\python.exe'
$vite = Join-Path $DemoFrontend 'node_modules\vite\bin\vite.js'
$nodeCommand = Get-Command node.exe -ErrorAction SilentlyContinue
if (-not $nodeCommand) {
    $fallback = Join-Path $env:USERPROFILE '.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe'
    if (Test-Path -LiteralPath $fallback) { $nodeCommand = @{ Source = $fallback } }
}
if (-not (Test-Path -LiteralPath $python) -or -not (Test-Path -LiteralPath $vite) -or -not $nodeCommand) {
    throw 'Dependências locais ausentes: verifique backend/.venv, frontend/node_modules e node.exe.'
}
if (-not $DatabasePath) { $DatabasePath = & (Join-Path $PSScriptRoot 'new-demo-copy.ps1') | Select-Object -Last 1 }
$DatabasePath = Assert-DemoDatabasePath -Path $DatabasePath
if ($DatabasePath.Equals([System.IO.Path]::GetFullPath($DemoDatabase), [System.StringComparison]::OrdinalIgnoreCase)) {
    throw 'O snapshot oficial é somente leitura para a execução. Inicie sem DatabasePath para criar uma cópia.'
}
& $python (Join-Path $PSScriptRoot 'check_demo_db.py') $DatabasePath
if ($LASTEXITCODE -ne 0) { throw 'Banco de demonstração inválido.' }

$logDirectory = Join-Path $DemoWorkspace 'output\demo\logs'
New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$backendOutput = Join-Path $logDirectory "backend-$stamp.out.log"
$backendError = Join-Path $logDirectory "backend-$stamp.err.log"
$frontendOutput = Join-Path $logDirectory "frontend-$stamp.out.log"
$frontendError = Join-Path $logDirectory "frontend-$stamp.err.log"
$backendProcess = $null
$frontendProcess = $null
$saved = @{}
foreach ($key in @('DATABASE_URL','APP_ENV','AUTO_START_SCHEDULER','DOCUMENT_STORAGE_PATH','FRONTEND_ORIGIN','VITE_BACKEND_URL')) {
    $saved[$key] = [System.Environment]::GetEnvironmentVariable($key, 'Process')
}
try {
    $env:DATABASE_URL = 'sqlite:///' + $DatabasePath.Replace('\','/')
    $env:APP_ENV = 'demo'
    $env:AUTO_START_SCHEDULER = 'false'
    $env:DOCUMENT_STORAGE_PATH = (Join-Path $DemoBackend 'data\demo\documents')
    $env:FRONTEND_ORIGIN = "http://localhost:$FrontendPort"
    $env:VITE_BACKEND_URL = "http://127.0.0.1:$BackendPort"
    $backendProcess = Start-Process -FilePath $python -ArgumentList @('-m','uvicorn','app.main:app','--host','127.0.0.1','--port',"$BackendPort") -WorkingDirectory $DemoBackend -WindowStyle Hidden -RedirectStandardOutput $backendOutput -RedirectStandardError $backendError -PassThru
    for ($attempt=0; $attempt -lt 60 -and -not (Get-DemoListener -Port $BackendPort); $attempt++) { Start-Sleep -Milliseconds 250 }
    if (-not (Get-DemoListener -Port $BackendPort)) { throw "API não abriu a porta $BackendPort. Consulte $backendError" }
    $health = Invoke-RestMethod -Uri "http://127.0.0.1:$BackendPort/api/v1/health" -TimeoutSec 5
    if ($health.status -ne 'ok') { throw 'Saúde da API não retornou ok.' }
    $listing = Invoke-RestMethod -Uri "http://127.0.0.1:$BackendPort/api/v1/opportunities?open_only=false&page_size=100" -TimeoutSec 5
    if (@($listing.data).Count -ne 7 -or @($listing.data | Where-Object { $_.official_control_number -like 'SINTETICO-*' }).Count -ne 0) {
        throw 'A API não está usando exclusivamente a base histórica oficial.'
    }
    $historical = Invoke-RestMethod -Uri "http://127.0.0.1:$BackendPort/api/v1/technology/historical-exhibit" -TimeoutSec 5
    if ($historical.totals.procurements -ne 7 -or $historical.totals.items -ne 19 -or $historical.totals.results -ne 19 -or $historical.totals.distinct_companies -ne 11) {
        throw 'O acervo histórico de tecnologia não corresponde ao recorte verificado.'
    }
    $viteConfig = Join-Path $DemoFrontend 'vite.config.ts'
    $viteArg = '"' + $vite + '" --config "' + $viteConfig + '" --host 127.0.0.1 --port ' + $FrontendPort + ' --strictPort'
    $frontendProcess = Start-Process -FilePath $nodeCommand.Source -ArgumentList $viteArg -WorkingDirectory $DemoFrontend -WindowStyle Hidden -RedirectStandardOutput $frontendOutput -RedirectStandardError $frontendError -PassThru
    for ($attempt=0; $attempt -lt 60 -and -not (Get-DemoListener -Port $FrontendPort); $attempt++) { Start-Sleep -Milliseconds 250 }
    if (-not (Get-DemoListener -Port $FrontendPort)) { throw "Interface não abriu a porta $FrontendPort. Consulte $frontendError" }
    $response = Invoke-WebRequest -Uri "http://127.0.0.1:$FrontendPort/" -TimeoutSec 5 -UseBasicParsing
    if ($response.StatusCode -ne 200) { throw 'Interface não respondeu HTTP 200.' }
    $run = [ordered]@{ backend_pid=$backendProcess.Id; frontend_pid=$frontendProcess.Id;
        backend_port=$BackendPort; frontend_port=$FrontendPort; database=$DatabasePath;
        started_at=(Get-Date).ToString('o'); backend_log=$backendError; frontend_log=$frontendError }
    $run | ConvertTo-Json | Set-Content -LiteralPath $DemoRunFile -Encoding UTF8
    Write-Host "Demo pronta: http://127.0.0.1:$FrontendPort/?demo=historico | API: http://127.0.0.1:$BackendPort/api/v1/health"
    Write-Host "Banco isolado: $DatabasePath"
} catch {
    if ($frontendProcess) { Stop-DemoProcessTree -ProcessId $frontendProcess.Id }
    if ($backendProcess) { Stop-DemoProcessTree -ProcessId $backendProcess.Id }
    throw
} finally {
    foreach ($key in $saved.Keys) { [System.Environment]::SetEnvironmentVariable($key, $saved[$key], 'Process') }
}
