from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.common.selenium_browser import remote_chrome  # noqa: E402


def main() -> int:
    with remote_chrome() as driver:
        driver.get("https://www.buscape.com.br/search?q=notebook")
        title = driver.title
        current_url = driver.current_url
        page_length = len(driver.page_source)

    if page_length < 1000:
        print("Selenium validation failed: page source too small")
        return 1

    print("Selenium validation passed.")
    print(f"Title: {title}")
    print(f"URL: {current_url}")
    print(f"HTML bytes: {page_length}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

