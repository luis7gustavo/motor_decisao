$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

if (-not (Test-Path ".env")) {
  Copy-Item ".env.example" ".env"
}

docker compose up -d selenium
docker compose run --rm api python scripts/validate_selenium.py

