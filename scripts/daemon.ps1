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
$StartupDir = [Environment]::GetFolderPath("Startup")
$StartupLauncher = Join-Path $StartupDir "$TaskName.cmd"

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

    try {
        Register-ScheduledTask `
            -TaskName $TaskName `
            -Action $Action `
            -Trigger $TriggerLogon, $TriggerWake `
            -Settings $Settings `
            -Force `
            -ErrorAction Stop | Out-Null
    }
    catch {
        $LauncherContent = @"
@echo off
start "" /min powershell.exe -NoProfile -NonInteractive -WindowStyle Hidden -File "$ScriptPath"
"@
        try {
            Set-Content -LiteralPath $StartupLauncher -Value $LauncherContent -Encoding ASCII
        }
        catch {
            Write-Error "Nao foi possivel registrar a tarefa nem criar o inicializador: $($_.Exception.Message)"
            exit 1
        }
        Write-Warning "Agendador de Tarefas indisponivel. Inicializador criado em '$StartupLauncher'."
        Write-Host "O daemon iniciara automaticamente ao fazer login." -ForegroundColor Green
        exit 0
    }

    Write-Host "Tarefa '$TaskName' registrada com sucesso." -ForegroundColor Green
    Write-Host "O daemon iniciara automaticamente ao ligar o PC e ao fazer login."
    exit 0
}

# --- Remove tarefa do Task Scheduler ---
if ($Unregister) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $StartupLauncher -Force -ErrorAction SilentlyContinue
    Write-Host "Tarefa '$TaskName' removida." -ForegroundColor Yellow
    exit 0
}

# --- Inicia o daemon ---
Set-Location $ProjectRoot

# O modo Once continua util para diagnostico local sem manter servico ativo.
if ($Once) {
    docker compose --profile daemon run --rm daemon python scripts/daemon.py --cooldown $Cooldown --once
    exit $LASTEXITCODE
}

$env:MOTOR_DAEMON_COOLDOWN_HOURS = "$Cooldown"
docker compose --profile daemon up -d --build daemon
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Servico motor_daemon ativo. Cooldown: $Cooldown h." -ForegroundColor Green
docker compose --profile daemon ps daemon
exit $LASTEXITCODE
