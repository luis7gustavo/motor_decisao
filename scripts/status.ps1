$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

docker compose up -d postgres redis
docker compose run --rm api python scripts/collect_status.py @args
