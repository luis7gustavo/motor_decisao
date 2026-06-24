param(
    [string]$SqlPath = ".\scripts\setup_bi_final_project.sql",
    [string]$PostgresService = "postgres",
    [string]$Database = $env:MOTOR_POSTGRES_DB,
    [string]$User = $env:MOTOR_POSTGRES_USER
)

if ([string]::IsNullOrWhiteSpace($Database)) {
    $Database = "motor_decisao"
}

if ([string]::IsNullOrWhiteSpace($User)) {
    $User = "motor"
}

$resolvedSql = Resolve-Path -LiteralPath $SqlPath -ErrorAction Stop
Write-Host "Aplicando camada BI final em $Database com usuario $User..."
Get-Content -LiteralPath $resolvedSql -Raw | docker compose exec -T $PostgresService psql -U $User -d $Database -v ON_ERROR_STOP=1

if ($LASTEXITCODE -ne 0) {
    throw "Falha ao aplicar $resolvedSql"
}

Write-Host "Camada BI final criada: schemas repositorio, dw e datamart."
