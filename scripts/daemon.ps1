# daemon.ps1 — Inicia o daemon de coleta continua dentro do container Docker.
#
# Uso:
#   .\scripts\daemon.ps1                   # inicia daemon (4h entre ciclos)
#   .\scripts\daemon.ps1 --cooldown 6      # ciclo a cada 6h
#   .\scripts\daemon.ps1 --once            # roda uma vez e sai
#
# Para registrar como tarefa automatica no Windows (roda ao ligar o PC):
#   .\scripts\daemon.ps1 --register
#
# Para remover a tarefa automatica:
#   .\scripts\daemon.ps1 --unregister

param(
    [switch]$Register,
    [switch]$Unregister,
    [switch]$Once,
    [double]$Cooldown = 4
)

$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
$TaskName = "MotorDecisaoDaemon"

# --- Registra tarefa no Task Scheduler ---
if ($Register) {
    $ScriptPath = Join-Path $ProjectRoot "scripts\daemon.ps1"
    $Action = New-ScheduledTaskAction `
        -Execute "powershell.exe" `
        -Argument "-NonInteractive -WindowStyle Hidden -File `"$ScriptPath`"" `
        -WorkingDirectory $ProjectRoot

    # Dispara ao fazer login E ao acordar do sono
    $TriggerLogon = New-ScheduledTaskTrigger -AtLogOn
    $TriggerWake  = New-ScheduledTaskTrigger -AtStartup

    $Settings = New-ScheduledTaskSettingsSet `
        -ExecutionTimeLimit (New-TimeSpan -Hours 0) `
        -RestartCount 3 `
        -RestartInterval (New-TimeSpan -Minutes 5) `
        -StartWhenAvailable `
        -WakeToRun

    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $Action `
        -Trigger $TriggerLogon, $TriggerWake `
        -Settings $Settings `
        -RunLevel Highest `
        -Force | Out-Null

    Write-Host "Tarefa '$TaskName' registrada com sucesso." -ForegroundColor Green
    Write-Host "O daemon iniciara automaticamente ao ligar o PC e ao fazer login."
    exit 0
}

# --- Remove tarefa do Task Scheduler ---
if ($Unregister) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Tarefa '$TaskName' removida." -ForegroundColor Yellow
    exit 0
}

# --- Inicia o daemon ---
Set-Location $ProjectRoot

# Garante que postgres e redis estao rodando
docker compose up -d postgres redis

# Monta os args do daemon Python
$DaemonArgs = @("scripts/daemon.py", "--cooldown", $Cooldown)
if ($Once) { $DaemonArgs += "--once" }

docker compose run --rm api python @DaemonArgs
