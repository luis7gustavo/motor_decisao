from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


@contextmanager
def chromium_page(
    *,
    headless: bool = True,
    locale: str = "pt-BR",
    timezone_id: str = "America/Sao_Paulo",
    navigation_timeout_seconds: int = 45,
    action_timeout_seconds: int = 15,
    storage_state_path: str | None = None,
) -> Iterator[Page]:
    selenium_remote_url = os.environ.pop("SELENIUM_REMOTE_URL", None)
    try:
        with sync_playwright() as playwright:
            browser: Browser = playwright.chromium.launch(
                headless=headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )
            context_kwargs = {
                "locale": locale,
                "timezone_id": timezone_id,
                "user_agent": DEFAULT_USER_AGENT,
                "viewport": {"width": 1366, "height": 768},
                "extra_http_headers": {
                    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                },
            }
            if storage_state_path and Path(storage_state_path).exists():
                context_kwargs["storage_state"] = storage_state_path
            context: BrowserContext = browser.new_context(**context_kwargs)
            context.set_default_navigation_timeout(navigation_timeout_seconds * 1000)
            context.set_default_timeout(action_timeout_seconds * 1000)
            page = context.new_page()
            try:
                yield page
            finally:
                context.close()
                browser.close()
    finally:
        if selenium_remote_url is not None:
            os.environ["SELENIUM_REMOTE_URL"] = selenium_remote_url
