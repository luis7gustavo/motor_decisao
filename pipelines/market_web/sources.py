from __future__ import annotations

import html
import re
import time
from dataclasses import dataclass, replace
from typing import Any
from urllib.parse import quote_plus, urljoin

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from pipelines.common.playwright_browser import chromium_page
from pipelines.market_web.base import MarketListingSnapshot
from pipelines.market_web.parsing import (
    clean_text,
    detect_block,
    first_price_from_text,
    parse_int_text,
    parse_sold_quantity,
)


@dataclass(frozen=True)
class SourceConfig:
    source_name: str
    source_role: str
    base_url: str
    search_url_template: str
    card_selector: str
    link_selector: str
    card_root_selector: str | None = None
    title_selector: str | None = None
    price_selector: str | None = None
    image_selector: str | None = "img"
    sold_quantity_pattern_required: bool = False
    demand_signal_type: str | None = None

    def search_url(self, query: str) -> str:
        return self.search_url_template.format(query=quote_plus(query))


SOURCE_CONFIGS: dict[str, SourceConfig] = {
    "shopee": SourceConfig(
        source_name="shopee",
        source_role="benchmark",
        base_url="https://shopee.com.br",
        search_url_template="https://shopee.com.br/search?keyword={query}",
        card_selector="a[href*='-i.'], a[data-sqe='link']",
        link_selector="self",
        demand_signal_type="sold_quantity_text",
    ),
    "magalu": SourceConfig(
        source_name="magalu",
        source_role="benchmark",
        base_url="https://www.magazineluiza.com.br",
        search_url_template="https://www.magazineluiza.com.br/busca/{query}/",
        card_selector="[data-testid*='product'], li:has(a[href*='/p/']), a[href*='/p/']",
        link_selector="a[href], self",
        demand_signal_type="reviews_text",
    ),
    "amazon": SourceConfig(
        source_name="amazon",
        source_role="benchmark",
        base_url="https://www.amazon.com.br",
        search_url_template="https://www.amazon.com.br/s?k={query}",
        card_selector="[data-component-type='s-search-result'][data-asin]",
        link_selector="a.a-link-normal.s-no-outline, h2 a, a[href*='/dp/']",
        title_selector="h2 span, [data-cy='title-recipe'] span",
        price_selector=".a-price .a-offscreen",
        demand_signal_type="bsr_or_reviews_proxy",
    ),
    "aliexpress": SourceConfig(
        source_name="aliexpress",
        source_role="supplier",
        base_url="https://pt.aliexpress.com",
        search_url_template="https://pt.aliexpress.com/w/wholesale-{query}.html?SearchText={query}",
        card_selector="a[href*='/item/'], a[href*='aliexpress.com/item']",
        link_selector="self",
        demand_signal_type="sold_quantity_text",
    ),
    "kabum": SourceConfig(
        source_name="kabum",
        source_role="benchmark",
        base_url="https://www.kabum.com.br",
        search_url_template="https://www.kabum.com.br/busca/{query}",
        card_selector="a[href*='/produto/']",
        link_selector="self",
        card_root_selector="a[href*='/produto/']",
        demand_signal_type="retailer_offer",
    ),
    "pichau": SourceConfig(
        source_name="pichau",
        source_role="benchmark",
        base_url="https://www.pichau.com.br",
        search_url_template="https://www.pichau.com.br/search?q={query}",
        card_selector=".MuiCard-root[class*='product_item'], [class*='product_item']",
        link_selector="closest",
        card_root_selector=".MuiCard-root[class*='product_item'], [class*='product_item']",
        demand_signal_type="retailer_offer",
    ),
    "terabyte": SourceConfig(
        source_name="terabyte",
        source_role="benchmark",
        base_url="https://www.terabyteshop.com.br",
        search_url_template="https://www.terabyteshop.com.br/busca?str={query}",
        card_selector="a.tss-result-card[href*='/produto/'], a[href*='/produto/'][data-tss-product]",
        link_selector="self",
        card_root_selector="a.tss-result-card[href*='/produto/'], a[href*='/produto/'][data-tss-product]",
        demand_signal_type="retailer_offer",
    ),
}


class PlaywrightMarketSource:
    def __init__(
        self,
        *,
        config: SourceConfig,
        headless: bool = True,
        locale: str = "pt-BR",
        timezone_id: str = "America/Sao_Paulo",
        navigation_timeout_seconds: int = 45,
        action_timeout_seconds: int = 15,
        scroll_steps: int = 4,
        scroll_pause_ms: int = 900,
        rate_limit_ms: int = 2500,
        storage_state_path: str | None = None,
    ) -> None:
        self.config = config
        self.source_name = config.source_name
        self.source_role = config.source_role
        self.headless = headless
        self.locale = locale
        self.timezone_id = timezone_id
        self.navigation_timeout_seconds = navigation_timeout_seconds
        self.action_timeout_seconds = action_timeout_seconds
        self.scroll_steps = scroll_steps
        self.scroll_pause_seconds = max(scroll_pause_ms, 0) / 1000
        self.rate_limit_seconds = max(rate_limit_ms, 0) / 1000
        self.storage_state_path = storage_state_path
        self._last_request_at = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        wait = self.rate_limit_seconds - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def _prepare_page(self, page: Page, url: str) -> tuple[bool, str | None]:
        self._throttle()
        try:
            page.goto(url, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except PlaywrightTimeoutError:
                pass
            for _ in range(self.scroll_steps):
                page.mouse.wheel(0, 1100)
                page.wait_for_timeout(int(self.scroll_pause_seconds * 1000))
        except PlaywrightTimeoutError as error:
            return True, f"navigation_timeout: {error}"

        block_reason = detect_block(page.locator("body").inner_text(timeout=5000)[:5000])
        return block_reason is not None, block_reason

    def _extract_cards(self, page: Page, *, query: str, max_results: int) -> list[MarketListingSnapshot]:
        raw_cards = page.evaluate(
            """
            ({ cardSelector, cardRootSelector, linkSelector, titleSelector, priceSelector, imageSelector, maxResults }) => {
                const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                const absUrl = (value) => {
                    if (!value) return null;
                    try { return new URL(value, window.location.href).href; } catch { return value; }
                };
                const nodeList = Array.from(document.querySelectorAll(cardSelector));
                const seen = new Set();
                const cards = [];
                for (const node of nodeList) {
                    let card = node;
                    if (cardRootSelector) {
                        card = (node.matches(cardRootSelector) ? node : node.closest(cardRootSelector)) || node;
                    } else {
                        card = node.closest('[data-component-type="s-search-result"], [data-testid*="product"], li, div') || node;
                    }
                    const text = normalize(card.innerText || node.innerText);
                    if (!text || !/R\\$|vendid|avalia|review|rating|frete|envio|sold|comprados/i.test(text)) {
                        continue;
                    }

                    let linkNode = null;
                    if (linkSelector.includes('self') && node.href) {
                        linkNode = node;
                    }
                    if (!linkNode && linkSelector.includes('closest')) {
                        linkNode = node.closest('a');
                    }
                    if (!linkNode) {
                        const selectors = linkSelector.split(',').map((item) => item.trim()).filter((item) => item && item !== 'self');
                        for (const selector of selectors) {
                            if (selector === 'closest') continue;
                            linkNode = card.querySelector(selector);
                            if (linkNode) break;
                        }
                    }
                    const href = absUrl(linkNode && linkNode.getAttribute('href'));
                    const key = href || text.slice(0, 180);
                    if (seen.has(key)) continue;
                    seen.add(key);

                    const titleNode = titleSelector ? card.querySelector(titleSelector) : null;
                    const priceNode = priceSelector ? card.querySelector(priceSelector) : null;
                    const imageNode = imageSelector ? card.querySelector(imageSelector) : null;
                    cards.push({
                        text,
                        titleText: normalize(titleNode ? titleNode.innerText : ''),
                        priceText: normalize(priceNode ? priceNode.innerText : ''),
                        href,
                        imageUrl: absUrl(imageNode && (imageNode.getAttribute('src') || imageNode.getAttribute('data-src'))),
                        html: card.outerHTML.slice(0, 3000),
                    });
                    if (cards.length >= maxResults) break;
                }
                return cards;
            }
            """,
            {
                "cardSelector": self.config.card_selector,
                "cardRootSelector": self.config.card_root_selector,
                "linkSelector": self.config.link_selector,
                "titleSelector": self.config.title_selector,
                "priceSelector": self.config.price_selector,
                "imageSelector": self.config.image_selector,
                "maxResults": max_results,
            },
        )
        snapshots: list[MarketListingSnapshot] = []
        for index, raw in enumerate(raw_cards or [], start=1):
            text = clean_text(raw.get("text"))
            title = _clean_title_candidate(raw.get("titleText"), text) or _guess_title_from_text(text)
            price = _extract_market_price(raw.get("priceText"), text, self.source_name)
            sold_text = _extract_sold_text(text) or _extract_line(text, ["vend", "sold"])
            reviews_text = _extract_reviews_text(text) or _extract_line(text, ["avalia", "review"])
            bsr_text = _extract_line(text, ["mais vendido", "best sellers", "ranking"])
            sold_quantity = parse_sold_quantity(sold_text)
            reviews_count = parse_int_text(reviews_text)
            demand_value = sold_quantity if sold_quantity is not None else reviews_count
            snapshots.append(
                MarketListingSnapshot(
                    source_name=self.source_name,
                    source_role=self.source_role,
                    query=query,
                    position=index,
                    title=title,
                    price=price,
                    old_price=None,
                    currency_id="BRL" if price is not None else None,
                    sold_quantity_text=sold_text,
                    sold_quantity=sold_quantity,
                    demand_signal_type=self.config.demand_signal_type,
                    demand_signal_value=float(demand_value) if demand_value is not None else None,
                    bsr_text=bsr_text,
                    rating_text=_extract_line(text, ["estrela", "rating", "classifica"]),
                    reviews_count=reviews_count,
                    seller_text=_extract_line(text, ["vendido por", "loja", "seller"]),
                    shipping_text=_extract_line(text, ["frete", "envio", "full", "prime"]),
                    installments_text=_extract_line(text, ["x de", "sem juros", "parcel"]),
                    item_url=raw.get("href"),
                    image_url=raw.get("imageUrl"),
                    is_sponsored=_contains(text, ["patrocinado", "sponsored"]),
                    is_full=_contains(text, ["full", "prime"]),
                    is_catalog=None,
                    blocked=False,
                    block_reason=None,
                    payload=raw,
                )
            )
        return snapshots

    def fetch(self, *, query: str, max_results: int) -> list[MarketListingSnapshot]:
        url = self.config.search_url(query)
        with chromium_page(
            headless=self.headless,
            locale=self.locale,
            timezone_id=self.timezone_id,
            navigation_timeout_seconds=self.navigation_timeout_seconds,
            action_timeout_seconds=self.action_timeout_seconds,
            storage_state_path=self.storage_state_path,
        ) as page:
            blocked, block_reason = self._prepare_page(page, url)
            if blocked:
                return [
                    MarketListingSnapshot(
                        source_name=self.source_name,
                        source_role=self.source_role,
                        query=query,
                        position=None,
                        title=None,
                        price=None,
                        old_price=None,
                        currency_id=None,
                        sold_quantity_text=None,
                        sold_quantity=None,
                        demand_signal_type="blocked",
                        demand_signal_value=None,
                        bsr_text=None,
                        rating_text=None,
                        reviews_count=None,
                        seller_text=None,
                        shipping_text=None,
                        installments_text=None,
                        item_url=url,
                        image_url=None,
                        is_sponsored=None,
                        is_full=None,
                        is_catalog=None,
                        blocked=True,
                        block_reason=block_reason,
                        payload={"url": url, "block_reason": block_reason},
                    )
                ]
            snapshots = self._extract_cards(page, query=query, max_results=max_results)
            if self.source_name == "shopee":
                snapshots = self._enrich_shopee_details(page, snapshots)
            return snapshots

    def _enrich_shopee_details(
        self,
        page: Page,
        snapshots: list[MarketListingSnapshot],
    ) -> list[MarketListingSnapshot]:
        enriched: list[MarketListingSnapshot] = []
        for snapshot in snapshots:
            if not snapshot.item_url:
                enriched.append(snapshot)
                continue
            detail_payload: dict[str, Any] = {"url": snapshot.item_url}
            try:
                self._throttle()
                page.goto(snapshot.item_url, wait_until="domcontentloaded")
                try:
                    page.wait_for_load_state("networkidle", timeout=8000)
                except PlaywrightTimeoutError:
                    pass
                page.wait_for_timeout(1200)
                html = page.content()
                body_text = page.locator("body").inner_text(timeout=5000)
                detail_payload.update(_extract_shopee_detail_values(html, body_text))
            except Exception as error:  # noqa: BLE001 - detail enrichment should not fail the listing.
                detail_payload["error"] = str(error)[:500]
                enriched.append(
                    replace(snapshot, payload={**snapshot.payload, "detail": detail_payload})
                )
                continue

            sold_quantity = snapshot.sold_quantity or detail_payload.get("historical_sold") or detail_payload.get("global_sold")
            sold_quantity_text = snapshot.sold_quantity_text
            if sold_quantity is not None and not sold_quantity_text:
                sold_quantity_text = f"{sold_quantity} vendidos"

            reviews_count = snapshot.reviews_count or detail_payload.get("total_rating_count")
            demand_signal_type = snapshot.demand_signal_type
            demand_signal_value = snapshot.demand_signal_value
            if sold_quantity is not None:
                demand_signal_type = "sold_quantity_text"
                demand_signal_value = float(sold_quantity)
            elif reviews_count is not None:
                demand_signal_type = "reviews_proxy"
                demand_signal_value = float(reviews_count)
            elif detail_payload.get("liked_count") is not None:
                demand_signal_type = "favorites_proxy"
                demand_signal_value = float(detail_payload["liked_count"])

            enriched.append(
                replace(
                    snapshot,
                    sold_quantity=sold_quantity,
                    sold_quantity_text=sold_quantity_text,
                    reviews_count=reviews_count,
                    demand_signal_type=demand_signal_type,
                    demand_signal_value=demand_signal_value,
                    payload={**snapshot.payload, "detail": detail_payload},
                )
            )
        return enriched


def build_market_source(source_name: str, **kwargs: Any) -> PlaywrightMarketSource:
    if source_name not in SOURCE_CONFIGS:
        raise ValueError(f"Unsupported market web source: {source_name}")
    return PlaywrightMarketSource(config=SOURCE_CONFIGS[source_name], **kwargs)


def _contains(text: str | None, needles: list[str]) -> bool | None:
    if not text:
        return None
    lowered = text.lower()
    return any(needle in lowered for needle in needles)


def _extract_line(text: str | None, needles: list[str]) -> str | None:
    if not text:
        return None
    chunks = [chunk.strip() for chunk in text.split("\n") if chunk.strip()]
    for chunk in chunks:
        lowered = chunk.lower()
        if any(needle in lowered for needle in needles):
            return clean_text(chunk)
    lowered = text.lower()
    for needle in needles:
        index = lowered.find(needle)
        if index >= 0:
            start = max(0, index - 60)
            end = min(len(text), index + 100)
            return clean_text(text[start:end])
    return None


def _clean_title_candidate(value: Any, text: str | None) -> str | None:
    title = clean_text(value)
    if not title:
        return None
    lowered = title.lower()
    if lowered in {"patrocinado", "sponsored", "anuncio", "anúncio", "escolha da amazon"}:
        return None
    return _trim_product_title(title) or _guess_title_from_text(text)


def _guess_title_from_text(text: str | None) -> str | None:
    if not text:
        return None
    text = re.sub(r"^(patrocinado|sponsored|an[uú]ncio|escolha da amazon)\s+", "", text, flags=re.I)
    trimmed = _trim_product_title(text)
    if trimmed:
        return trimmed
    chunks = [chunk.strip() for chunk in text.split("\n") if chunk.strip()]
    for chunk in chunks:
        if "R$" not in chunk and len(chunk) >= 8:
            return clean_text(chunk[:250])
    return clean_text(text[:250])


def _trim_product_title(text: str | None) -> str | None:
    if not text:
        return None
    candidate = clean_text(html.unescape(text))
    if not candidate:
        return None
    candidate = re.sub(r"^[^A-Za-z0-9À-ÿ]+", "", candidate)
    candidate = re.sub(r"^(SELO:\s*)?CUPOM\s+\S+\s+", "", candidate, flags=re.I)
    candidate = re.sub(r"^(Avalia[cç][aã]o\s+\d+(?:[.,]\d+)?\s+de\s+\d+(?:[.,]\d+)?\s*)", "", candidate, flags=re.I)
    candidate = re.sub(r"^\d+%\s+OFF\s+", "", candidate, flags=re.I)
    candidate = re.sub(r"^\d+\s+UNID\s+", "", candidate, flags=re.I)
    candidate = re.sub(r"^Frete\s+Gr[aá]tis:[^A-Z0-9]+", "", candidate, flags=re.I)
    candidate = candidate.lstrip("🔥 ").strip()
    candidate = re.split(r"\s+R\$\s*", candidate, maxsplit=1)[0]
    candidate = re.split(r"\s+de\s+R\$\s*", candidate, maxsplit=1, flags=re.I)[0]
    candidate = re.split(r"\s+De:\s*$", candidate, maxsplit=1, flags=re.I)[0]
    candidate = re.split(r"\s+\|\s+Terabyte\b", candidate, maxsplit=1, flags=re.I)[0]
    candidate = re.split(r"\s+\d[,.]\d\s+\d[,.]\d\s+de\s+5\s+estrelas", candidate, maxsplit=1, flags=re.I)[0]
    candidate = re.split(r"\s+mais\s+de\s+\d", candidate, maxsplit=1, flags=re.I)[0]
    candidate = re.sub(r"^(patrocinado|sponsored|an[uú]ncio|escolha da amazon)\s+", "", candidate, flags=re.I)
    candidate = clean_text(candidate)
    if not candidate or len(candidate) < 8:
        return None
    return candidate[:250]


def _extract_market_price(price_text: Any, text: str | None, source_name: str) -> float | None:
    explicit_price = first_price_from_text(price_text)
    if explicit_price is not None:
        return explicit_price
    clean = clean_text(text)
    if not clean:
        return None
    if source_name in {"kabum", "pichau", "terabyte"}:
        promo_match = re.search(r"\bpor\s+(R\$\s*[\d.,]+)", clean, flags=re.I)
        if promo_match:
            return first_price_from_text(promo_match.group(1))
        prices = re.findall(r"R\$\s*[\d.,]+", clean)
        if len(prices) >= 2 and re.search(r"\b(de|desconto|pix|promo[cç][aã]o)\b", clean, flags=re.I):
            return first_price_from_text(prices[1])
    return first_price_from_text(clean)


def _extract_sold_text(text: str | None) -> str | None:
    if not text:
        return None
    match = re.search(r"(\d[\d.,]*\s*(?:mil|k)?\+?\s*vendid[oa]s?(?:\(s\))?)", text, flags=re.I)
    if not match:
        match = re.search(
            r"((?:mais\s+de\s+)?\d[\d.,]*\s*(?:mil|k)?\+?\s+compras?\s+no\s+m[eê]s\s+passado)",
            text,
            flags=re.I,
        )
    return clean_text(match.group(1)) if match else None


def _extract_reviews_text(text: str | None) -> str | None:
    if not text:
        return None
    match = re.search(r"\((\d+(?:[.,]\d+)?\s*(?:mil|k)?)\)", text, flags=re.I)
    return clean_text(match.group(1)) if match else None


def _extract_shopee_detail_values(html: str, body_text: str) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for key in ["historical_sold", "global_sold", "total_rating_count", "liked_count", "cmt_count"]:
        values[key] = _extract_json_int(html, key)
    rating_match = re.search(r'"rating_star"\s*:\s*(\d+(?:\.\d+)?)', html)
    if rating_match:
        values["rating_star"] = float(rating_match.group(1))
    if values.get("total_rating_count") is None:
        reviews_match = re.search(r"(\d[\d.,]*)\s+avalia[cç][oõ]es", body_text, flags=re.I)
        if reviews_match:
            values["total_rating_count"] = parse_int_text(reviews_match.group(1))
    return {key: value for key, value in values.items() if value is not None}


def _extract_json_int(html: str, key: str) -> int | None:
    match = re.search(rf'"{re.escape(key)}"\s*:\s*(\d+|null)', html)
    if not match or match.group(1) == "null":
        return None
    return int(match.group(1))
