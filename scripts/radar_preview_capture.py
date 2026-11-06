"""Capture the isolated real search and detail with no credentials on screen."""

import os
from pathlib import Path

from playwright.sync_api import sync_playwright


base = os.environ.get("RADAR_PREVIEW_BASE_URL", "http://radar-preview-app-20261105:8000")
output = Path("docs/licitacoes/evidence")
output.mkdir(parents=True, exist_ok=True)

with sync_playwright() as playwright:
    browser = playwright.chromium.launch(headless=True, args=["--no-sandbox"])
    page = browser.new_page(viewport={"width": 1440, "height": 900}, device_scale_factor=1)
    page.goto(base + "/radar-demo")
    page.wait_for_load_state("networkidle")
    assert page.locator("h1").inner_text() == "Buscar licitações registradas"
    assert page.locator("tbody tr").count() > 0
    page.screenshot(path=str(output / "radar-real-inicio.png"), full_page=True)
    page.goto(base + "/radar-demo?q=papel&uf=SP")
    page.wait_for_load_state("networkidle")
    assert page.locator("h1").inner_text() == "Buscar licitações registradas"
    assert page.locator("tbody tr").count() > 0
    page.screenshot(path=str(output / "radar-real-busca.png"), full_page=True)
    page.locator("tbody tr a").first.click()
    page.wait_for_load_state("networkidle")
    assert page.locator("h2", has_text="Identidade e proveniência").count() == 1
    page.screenshot(path=str(output / "radar-real-detalhe.png"), full_page=True)
    print(f"search_and_detail_captured={output.resolve()}")
    browser.close()
