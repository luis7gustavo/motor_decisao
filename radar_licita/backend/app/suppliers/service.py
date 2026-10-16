import re
import unicodedata
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.time import local_today
from app.models import MatchClass, ProcurementItem, ProductMatch, SupplierProduct

STOPWORDS = {"de", "da", "do", "das", "dos", "com", "para", "em", "e", "a", "o", "unidade", "tipo"}
SPEC_VALUE = r"[A-Za-z]*\d+(?:[.,]\d+)?(?:\s*(?:GB|TB|MB))?"


def tokens(text: str) -> set[str]:
    normalized = unicodedata.normalize("NFKD", text.lower()).encode("ascii", "ignore").decode()
    return {part for part in re.findall(r"[a-z0-9]+", normalized) if len(part) > 2 and part not in STOPWORDS}


def match_product(db: Session, item: ProcurementItem, product: SupplierProduct) -> ProductMatch:
    required_text = f"{item.description} {item.complementary_description or ''}"
    product_text = " ".join(filter(None, [product.name, product.brand, product.model, product.manufacturer_code, product.technical_description]))
    required_tokens = tokens(required_text)
    product_tokens = tokens(product_text)
    overlap = len(required_tokens & product_tokens) / max(1, len(required_tokens))
    reasons = [f"Cobertura lexical das especificações: {overlap:.0%}"]
    checks: dict[str, dict] = {}
    incompatible = False
    for key, supplied_value in (product.specifications or {}).items():
        key_tokens = tokens(str(key))
        if key_tokens and key_tokens & required_tokens:
            supplied = str(supplied_value).lower()
            escaped_key = re.escape(str(key))
            mentioned_values = re.findall(rf"(?i)\b{escaped_key}\b\s*[:\-]?\s*({SPEC_VALUE})", required_text)
            mentioned_values += re.findall(rf"(?i)({SPEC_VALUE})\s+\b{escaped_key}\b", required_text)
            normalized_supplied = re.sub(r"\s+", "", supplied)
            matches = not mentioned_values or any(
                re.sub(r"\s+", "", value.lower()) in normalized_supplied
                or normalized_supplied in re.sub(r"\s+", "", value.lower())
                for value in mentioned_values
            )
            checks[key] = {"required": mentioned_values or "mentioned without normalized value", "supplied": supplied_value, "matches": matches}
            incompatible = incompatible or not matches
    if incompatible:
        classification, score = MatchClass.INCOMPATIBLE, max(0, int(overlap * 50))
        reasons.append("Ao menos uma especificação técnica informada diverge do requisito")
    elif checks and overlap >= 0.55:
        classification, score = MatchClass.COMPATIBLE, min(95, 70 + int(overlap * 25))
        reasons.append("Especificações cadastradas e descrição apresentam boa cobertura")
    elif overlap >= 0.40:
        classification, score = MatchClass.POSSIBLY_COMPATIBLE, int(45 + overlap * 35)
        reasons.append("Correspondência textual relevante; validar requisitos obrigatórios")
    else:
        classification, score = MatchClass.INSUFFICIENT, int(overlap * 60)
        reasons.append("Dados insuficientes para confirmar equivalência técnica")
    result = db.scalar(
        select(ProductMatch).where(
            ProductMatch.procurement_item_id == item.id,
            ProductMatch.supplier_product_id == product.id,
        )
    )
    if result is None:
        result = ProductMatch(procurement_item_id=item.id, supplier_product_id=product.id, classification=classification, score=score)
        db.add(result)
    result.classification = classification
    result.score = score
    result.reasons = reasons
    result.mandatory_spec_checks = checks
    db.flush()
    return result


def quote_is_valid(valid_until: date | None) -> bool:
    return valid_until is None or valid_until >= local_today()

