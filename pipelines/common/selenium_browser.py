from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Iterator

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.webdriver import WebDriver

from app.core.settings import get_settings


def build_chrome_options() -> Options:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1366,768")
    options.add_argument("--lang=pt-BR")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"
    )
    return options


@contextmanager
def remote_chrome() -> Iterator[WebDriver]:
    settings = get_settings()
    driver = webdriver.Remote(
        command_executor=settings.selenium_remote_url,
        options=build_chrome_options(),
    )
    try:
        driver.set_page_load_timeout(45)
        yield driver
    finally:
        driver.quit()

