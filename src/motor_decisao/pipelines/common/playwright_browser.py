from __future__ import annotations

import os
from itertools import cycle
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from typing import Iterator

from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

USER_AGENT_POOL = (
    DEFAULT_USER_AGENT,
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
)
_USER_AGENT_ROTATOR = cycle(USER_AGENT_POOL)
_USER_AGENT_LOCK = Lock()


def _next_user_agent() -> str:
    # 2026-05-01: rotate common Chrome fingerprints to reduce repeated 403/429 blocks.
    # 2026-05-02: protect the shared rotator now that Bronze scrapers can fan out
    # across threads and would otherwise race on the global iterator.
    with _USER_AGENT_LOCK:
        return next(_USER_AGENT_ROTATOR)


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
    with sync_playwright() as playwright:
        user_agent = _next_user_agent()
        launch_env = dict(os.environ)
        # 2026-05-02: keep Playwright on its local Chromium path even when the
        # container exports SELENIUM_REMOTE_URL for Selenium-only workflows.
        launch_env.pop("SELENIUM_REMOTE_URL", None)
        browser: Browser = playwright.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
            env=launch_env,
        )
        context_kwargs = {
            "locale": locale,
            "timezone_id": timezone_id,
            "user_agent": user_agent,
            "viewport": {"width": 1366, "height": 768},
            "extra_http_headers": {
                "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "DNT": "1",
                "Upgrade-Insecure-Requests": "1",
                "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not.A/Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
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
