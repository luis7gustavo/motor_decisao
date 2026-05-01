from __future__ import annotations

import argparse
from pathlib import Path

from playwright.sync_api import sync_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Abre login assistido e salva storage_state para fontes B2B."
    )
    parser.add_argument("--url", required=True)
    parser.add_argument("--state-path", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state_path = Path(args.state_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        context = browser.new_context(locale="pt-BR", timezone_id="America/Sao_Paulo")
        page = context.new_page()
        page.goto(args.url)
        print("Faça o login manual na janela do navegador.")
        print(f"Quando terminar, aguarde ou feche a janela. Timeout: {args.timeout_seconds}s")
        page.wait_for_timeout(args.timeout_seconds * 1000)
        context.storage_state(path=str(state_path))
        browser.close()
    print(f"Storage state salvo em: {state_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
