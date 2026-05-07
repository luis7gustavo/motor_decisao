$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

docker compose up -d postgres redis selenium
docker compose run --rm api python scripts/collect_all.py @args
