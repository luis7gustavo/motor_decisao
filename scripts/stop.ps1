$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

docker compose stop
exit $LASTEXITCODE
