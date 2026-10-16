$DemoWorkspace = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$DemoBackend = Join-Path $DemoWorkspace 'backend'
$DemoFrontend = Join-Path $DemoWorkspace 'frontend'
$DemoDatabase = Join-Path $DemoBackend 'data\demo\radar_real_history_snapshot.db'
$DemoRunFile = Join-Path $DemoWorkspace 'output\demo\run.json'

function Assert-DemoDatabasePath {
    param([Parameter(Mandatory)][string]$Path)
    $resolved = [System.IO.Path]::GetFullPath($Path)
    $approved = [System.IO.Path]::GetFullPath((Join-Path $DemoBackend 'data\demo'))
    $original = [System.IO.Path]::GetFullPath((Join-Path $DemoBackend 'radar_licita.db'))
    if ($resolved.Equals($original, [System.StringComparison]::OrdinalIgnoreCase) -or
        -not $resolved.StartsWith($approved + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase) -or
        [System.IO.Path]::GetExtension($resolved) -ne '.db') {
        throw "Destino fora da pasta isolada de demonstração: $resolved"
    }
    if (-not (Test-Path -LiteralPath $resolved -PathType Leaf)) {
        throw "Banco de demonstração ausente: $resolved"
    }
    return $resolved
}

function Get-DemoListener {
    param([Parameter(Mandatory)][int]$Port)
    @(Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique)
}

function Stop-DemoProcessTree {
    param([Parameter(Mandatory)][int]$ProcessId)
    $children = @(Get-CimInstance Win32_Process -Filter "ParentProcessId=$ProcessId" -ErrorAction SilentlyContinue)
    foreach ($child in $children) {
        Stop-DemoProcessTree -ProcessId ([int]$child.ProcessId)
    }
    Stop-Process -Id $ProcessId -ErrorAction SilentlyContinue
}
