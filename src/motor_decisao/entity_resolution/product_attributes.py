from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class ProductAttributes:
    normalized_text: str
    product_types: tuple[str, ...] = ()
    brands: tuple[str, ...] = ()
    capacities: tuple[str, ...] = ()
    memory_generations: tuple[str, ...] = ()
    storage_interfaces: tuple[str, ...] = ()
    connector_interfaces: tuple[str, ...] = ()
    voltages: tuple[str, ...] = ()
    power_watts: tuple[str, ...] = ()
    colors: tuple[str, ...] = ()
    pack_quantities: tuple[str, ...] = ()
    model_codes: tuple[str, ...] = ()


BRAND_ALIASES: dict[str, tuple[str, ...]] = {
    "amd": ("amd",),
    "c3plus": ("c3plus", "c3 plus", "c3tech", "c3 tech", "c3tech gaming"),
    "crucial": ("crucial",),
    "energizer": ("energizer",),
    "hp": ("hp", "hp gaming"),
    "intel": ("intel",),
    "kingston": ("kingston",),
    "lehmox": ("lehmox",),
    "logitech": ("logitech",),
    "megatron": ("megatron",),
    "philips": ("philips",),
    "plus_cable": ("plus cable",),
    "sandisk": ("sandisk", "san disk"),
    "seagate": ("seagate",),
    "western_digital": ("western digital", "wd"),
}

PRODUCT_TYPE_PATTERNS: dict[str, tuple[str, ...]] = {
    "adapter": ("adaptador", "adapter"),
    "cable": ("cabo", "cable"),
    "case": ("gabinete",),
    "charger": ("carregador",),
    "combo": ("combo", "kit teclado mouse"),
    "cooler": ("cooler",),
    "cpu": ("processador", "ryzen", "core i3", "core i5", "core i7", "core i9"),
    "headset": ("headset", "headphone", "fone", "fones"),
    "hub": ("hub",),
    "memory_ram": ("memoria ram", "ddr3", "ddr4", "ddr5"),
    "microphone": ("microfone",),
    "motherboard": ("placa mae",),
    "mouse": ("mouse",),
    "mousepad": ("mousepad", "mouse pad"),
    "notebook_stand": ("suporte notebook", "base notebook", "cooler notebook"),
    "power_supply": ("fonte",),
    "ssd": ("ssd",),
    "keyboard": ("teclado", "keyboard"),
    "webcam": ("webcam", "camera web"),
}

COLOR_ALIASES: dict[str, tuple[str, ...]] = {
    "black": ("preto", "black"),
    "white": ("branco", "white"),
    "blue": ("azul", "blue"),
    "red": ("vermelho", "red"),
    "green": ("verde", "green"),
    "pink": ("rosa", "pink"),
    "gray": ("cinza", "grey", "gray"),
}

CONFLICT_FIELDS = (
    "product_types",
    "brands",
    "capacities",
    "memory_generations",
    "storage_interfaces",
    "connector_interfaces",
    "voltages",
    "power_watts",
    "colors",
    "pack_quantities",
)


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text_value = re.sub(r"\s+", " ", str(value).replace("\u00a0", " ")).strip().lower()
    text_value = unicodedata.normalize("NFKD", text_value)
    text_value = "".join(char for char in text_value if not unicodedata.combining(char))
    text_value = re.sub(r"[^a-z0-9]+", " ", text_value)
    return re.sub(r"\s+", " ", text_value).strip()


def extract_product_attributes(
    title: Any,
    *,
    payload: dict[str, Any] | None = None,
) -> ProductAttributes:
    text = _combined_text(title, payload)
    normalized = normalize_text(text)
    return ProductAttributes(
        normalized_text=normalized,
        product_types=_extract_product_types(normalized),
        brands=_extract_aliases(normalized, BRAND_ALIASES),
        capacities=_extract_capacities(normalized),
        memory_generations=_ordered_unique(re.findall(r"\bddr[345]\b", normalized)),
        storage_interfaces=_extract_storage_interfaces(normalized),
        connector_interfaces=_extract_connector_interfaces(normalized),
        voltages=_extract_voltages(normalized),
        power_watts=_extract_power_watts(normalized),
        colors=_extract_aliases(normalized, COLOR_ALIASES),
        pack_quantities=_extract_pack_quantities(normalized),
        model_codes=_extract_model_codes(normalized),
    )


def attribute_conflicts(left: ProductAttributes, right: ProductAttributes) -> list[str]:
    conflicts: list[str] = []
    for field_name in CONFLICT_FIELDS:
        left_values = set(getattr(left, field_name))
        right_values = set(getattr(right, field_name))
        if field_name == "product_types":
            left_values, right_values = _compatible_product_types(left_values, right_values)
        if field_name == "voltages" and ("bivolt" in left_values or "bivolt" in right_values):
            continue
        if left_values and right_values and left_values.isdisjoint(right_values):
            conflicts.append(f"{field_name}_conflict")
    return conflicts


def conflict_penalty(conflicts: Iterable[str]) -> float:
    conflict_set = set(conflicts)
    if not conflict_set:
        return 1.0
    high_risk = {
        "product_types_conflict",
        "capacities_conflict",
        "memory_generations_conflict",
        "storage_interfaces_conflict",
        "connector_interfaces_conflict",
        "voltages_conflict",
        "power_watts_conflict",
    }
    if conflict_set & high_risk:
        return 0.30
    if "brands_conflict" in conflict_set:
        return 0.55
    return 0.70


def _combined_text(title: Any, payload: dict[str, Any] | None) -> str:
    parts = [str(title or "")]
    if payload:
        parts.extend(_payload_attribute_values(payload))
    return " ".join(part for part in parts if part)


def _payload_attribute_values(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []

    def visit(value: Any, *, depth: int = 0) -> None:
        if depth > 4 or len(values) >= 80:
            return
        if isinstance(value, dict):
            for key in (
                "name",
                "title",
                "product_name",
                "description",
                "modelo",
                "model",
                "brand",
                "line",
                "product_type",
                "reference",
                "codigo",
                "value_name",
                "value_id",
            ):
                item = value.get(key)
                if isinstance(item, (str, int, float)) and str(item).strip():
                    values.append(str(item))
            for nested_key in ("product", "attributes", "catalog_sections"):
                if nested_key in value:
                    visit(value[nested_key], depth=depth + 1)
        elif isinstance(value, list):
            for item in value[:40]:
                visit(item, depth=depth + 1)

    visit(payload)
    return values


def _extract_aliases(text: str, aliases: dict[str, tuple[str, ...]]) -> tuple[str, ...]:
    found = []
    padded = f" {text} "
    for canonical, terms in aliases.items():
        for term in terms:
            if f" {normalize_text(term)} " in padded:
                found.append(canonical)
                break
    return _ordered_unique(found)


def _extract_product_types(text: str) -> tuple[str, ...]:
    found = []
    padded = f" {text} "
    for product_type, terms in PRODUCT_TYPE_PATTERNS.items():
        for term in terms:
            if f" {normalize_text(term)} " in padded:
                found.append(product_type)
                break
    if "mousepad" in found and "mouse" in found:
        found.remove("mouse")
    return _ordered_unique(found)


def _extract_capacities(text: str) -> tuple[str, ...]:
    values = []
    for amount, unit in re.findall(r"\b(\d+(?:[.,]\d+)?)\s*(tb|gb|mb)\b", text):
        normalized_amount = amount.replace(",", ".")
        if normalized_amount.endswith(".0"):
            normalized_amount = normalized_amount[:-2]
        values.append(f"{normalized_amount}{unit}")
    return _ordered_unique(values)


def _extract_storage_interfaces(text: str) -> tuple[str, ...]:
    found = []
    if re.search(r"\bnvme\b", text):
        found.append("nvme")
    if re.search(r"\bsata\b", text):
        found.append("sata")
    if re.search(r"\bm\s*2\b|\bm2\b", text):
        found.append("m2")
    if re.search(r"\bpcie\b|\bpci\s*e\b", text):
        found.append("pcie")
    return _ordered_unique(found)


def _extract_connector_interfaces(text: str) -> tuple[str, ...]:
    rules = {
        "usb_c": r"\busb\s*c\b|\btype\s*c\b|\btipo\s*c\b",
        "usb_3": r"\busb\s*3(?:\s*0)?\b",
        "usb_2": r"\busb\s*2(?:\s*0)?\b",
        "micro_usb": r"\bmicro\s*usb\b",
        "lightning": r"\blightning\b",
        "hdmi": r"\bhdmi\b",
        "vga": r"\bvga\b",
        "displayport": r"\bdisplay\s*port\b|\bdisplayport\b",
        "p2": r"\bp2\b",
        "p3": r"\bp3\b",
        "rj45": r"\brj\s*45\b|\brj45\b",
        "bluetooth": r"\bbluetooth\b",
        "wifi": r"\bwi\s*fi\b|\bwifi\b",
        "wireless": r"\bwireless\b|\bsem\s*fio\b",
    }
    found = [name for name, pattern in rules.items() if re.search(pattern, text)]
    return _ordered_unique(found)


def _extract_voltages(text: str) -> tuple[str, ...]:
    found = []
    if re.search(r"\bbivolt\b", text):
        found.append("bivolt")
    for voltage in re.findall(r"\b(110|127|220)\s*v\b", text):
        found.append(f"{voltage}v")
    return _ordered_unique(found)


def _extract_power_watts(text: str) -> tuple[str, ...]:
    return _ordered_unique(f"{value}w" for value in re.findall(r"\b(\d{2,4})\s*w\b", text))


def _extract_pack_quantities(text: str) -> tuple[str, ...]:
    values = []
    for amount in re.findall(r"\b(?:kit|pack|pacote|combo)\s*(?:com|de)?\s*(\d{1,3})\b", text):
        values.append(amount)
    for amount in re.findall(r"\b(\d{1,3})\s*(?:pecas|pcs|unidades|un)\b", text):
        values.append(amount)
    return _ordered_unique(values)


def _extract_model_codes(text: str) -> tuple[str, ...]:
    candidates = re.findall(r"\b[a-z]{1,8}[- ]?\d{2,5}[a-z0-9-]*\b", text)
    filtered = [
        candidate.replace(" ", "-")
        for candidate in candidates
        if not re.fullmatch(r"(usb|ddr|hdmi|sata|nvme)?\d+", candidate)
    ]
    return _ordered_unique(filtered)


def _compatible_product_types(
    left: set[str],
    right: set[str],
) -> tuple[set[str], set[str]]:
    if not left or not right:
        return left, right
    compatible_groups = (
        {"headset", "microphone"},
        {"adapter", "hub"},
        {"notebook_stand", "cooler"},
    )
    for group in compatible_groups:
        if left <= group and right <= group:
            return set(), set()
    return left, right


def _ordered_unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value))
