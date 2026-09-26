from __future__ import annotations

import re
import unicodedata
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode, urlsplit
from uuid import UUID

from bs4 import BeautifulSoup

from app.repositories.review_repository import ReviewRepository
from app.schemas.review import ReviewDecisionInput, ReviewFilters


DECISION_LABELS = {
    "approved_test_purchase": "Aprovado para compra teste",
    "needs_reanalysis": "Aguardando nova análise",
    "rejected": "Descartado",
}

REANALYSIS_REASON_LABELS = {
    "collect_prices_again": "Coletar preços novamente",
    "find_more_evidence": "Buscar mais evidências",
    "fix_matching": "Corrigir o matching",
    "review_technical_attributes": "Revisar atributos técnicos",
    "review_costs_and_fees": "Revisar custos e taxas",
    "confirm_availability": "Confirmar disponibilidade",
    "stale_data": "Dados desatualizados",
    "other": "Outro",
}

REJECTION_REASON_LABELS = {
    "insufficient_margin": "Margem insuficiente",
    "low_absolute_profit": "Lucro absoluto baixo",
    "incorrect_matching": "Matching incorreto",
    "conflicting_specifications": "Especificações conflitantes",
    "low_demand": "Baixa demanda",
    "few_market_evidences": "Poucas evidências de mercado",
    "unreliable_market_price": "Preço de mercado pouco confiável",
    "high_shipping_cost": "Frete elevado",
    "risky_product": "Produto de risco",
    "stale_data": "Dados desatualizados",
    "other": "Outro",
}

RISK_LABELS = {
    "sem_preco_mercado": "Sem preço de mercado",
    "match_fraco": "Matching fraco",
    "match_revisar": "Matching exige revisão",
    "demanda_fraca": "Demanda fraca",
    "demanda_incompleta": "Sinais de demanda incompletos",
    "margem_indisponivel": "Margem indisponível",
    "margem_baixa": "Margem abaixo do mínimo",
    "ticket_fornecedor_muito_baixo": "Ticket do fornecedor muito baixo",
    "titulo_generico_sem_identificador": "Título genérico sem identificador",
    "fontes_insuficientes": "Fontes insuficientes",
    "poucas_ofertas_mercado": "Poucas ofertas de mercado",
    "modelo_nao_confirmado": "Modelo não confirmado nas evidências",
    "preco_mercado_muito_disperso": "Preços de mercado muito dispersos",
}

ATTRIBUTE_LABELS = {
    "capacity": "Capacidade",
    "voltage": "Voltagem",
    "color": "Cor",
    "size": "Tamanho",
    "generation": "Geração/interface",
    "model": "Modelo",
}

COLOR_WORDS = {
    "azul",
    "branco",
    "cinza",
    "dourado",
    "laranja",
    "preto",
    "rosa",
    "roxo",
    "verde",
    "vermelho",
}


def safe_external_url(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value or len(value) > 2048:
        return None
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    if parsed.username or parsed.password:
        return None
    return value


def _normalized_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.lower())
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9.,+-]+", " ", normalized).strip()


def _technical_attributes(title: str) -> dict[str, set[str]]:
    normalized = _normalized_text(title)
    tokens = set(normalized.split())
    attributes: dict[str, set[str]] = {
        "capacity": set(re.findall(r"\b\d+(?:[.,]\d+)?\s*(?:tb|gb|mb)\b", normalized)),
        "voltage": set(re.findall(r"\b(?:110|127|220)\s*v\b|\bbivolt\b", normalized)),
        "color": tokens & COLOR_WORDS,
        "size": set(
            re.findall(
                r"\b\d+(?:[.,]\d+)?\s*(?:mm|cm|pol(?:egadas?)?)\b",
                normalized,
            )
        ),
        "generation": set(
            re.findall(
                r"\b(?:ddr\s*\d|usb\s*\d(?:[.,]\d)?|sata\s*(?:i{1,3}|\d)|gen\s*\d|wi-?fi\s*\d|bluetooth\s*\d(?:[.,]\d)?)\b",
                normalized,
            )
        ),
        "model": set(
            re.findall(r"\b[a-z]{1,8}[- ]?\d{2,}[a-z0-9-]*\b", normalized)
        ),
    }
    return {key: {item.replace(" ", "") for item in values} for key, values in attributes.items()}


def compare_titles(supplier_title: str, market_title: str) -> dict[str, Any]:
    supplier_attrs = _technical_attributes(supplier_title)
    market_attrs = _technical_attributes(market_title)
    matches: list[str] = []
    conflicts: list[dict[str, str]] = []
    for key, label in ATTRIBUTE_LABELS.items():
        supplier_values = supplier_attrs.get(key, set())
        market_values = market_attrs.get(key, set())
        shared = supplier_values & market_values
        if shared:
            matches.append(f"{label}: {', '.join(sorted(shared))}")
        elif supplier_values and market_values:
            conflicts.append(
                {
                    "attribute": label,
                    "supplier": ", ".join(sorted(supplier_values)),
                    "market": ", ".join(sorted(market_values)),
                }
            )

    supplier_tokens = set(_normalized_text(supplier_title).split())
    market_tokens = set(_normalized_text(market_title).split())
    common_tokens = sorted(
        token for token in supplier_tokens & market_tokens if len(token) >= 3
    )[:12]
    return {"matches": matches, "conflicts": conflicts, "common_tokens": common_tokens}


def _find_image(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    candidate_keys = (
        "image_url",
        "image",
        "thumbnail",
        "thumbnail_url",
        "picture",
        "picture_url",
    )
    for key in candidate_keys:
        candidate = payload.get(key)
        if isinstance(candidate, dict):
            candidate = candidate.get("url") or candidate.get("src")
        if isinstance(candidate, list) and candidate:
            candidate = candidate[0]
        safe_candidate = safe_external_url(candidate)
        if safe_candidate:
            return safe_candidate

    html = payload.get("html_excerpt") or payload.get("html")
    if isinstance(html, str) and html:
        soup = BeautifulSoup(html, "html.parser")
        source = soup.find("source", srcset=True)
        image = soup.find("img")
        candidate = source.get("srcset") if source else None
        if not candidate and image:
            candidate = (
                image.get("data-src")
                or image.get("data-amsrc")
                or image.get("src")
            )
        if isinstance(candidate, str):
            candidate = candidate.split(",", 1)[0].strip().split(" ", 1)[0]
        return safe_external_url(candidate)
    return None


def _availability(opportunity: dict[str, Any]) -> str | None:
    if opportunity.get("raw_stock") is not None:
        return f"{opportunity['raw_stock']} unidade(s)"
    payload = opportunity.get("supplier_payload")
    if isinstance(payload, dict):
        for key in ("availability", "stock_status", "raw_stock_text"):
            value = payload.get(key)
            if value not in (None, ""):
                return str(value)
    return None


def _decimal_value(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (ValueError, TypeError):
        return None


class ReviewService:
    def __init__(self, repository: ReviewRepository) -> None:
        self.repository = repository

    def summary(self) -> dict[str, int]:
        return self.repository.get_summary()

    def filter_options(self) -> dict[str, list[str]]:
        return self.repository.get_filter_options()

    def pending(self, filters: ReviewFilters) -> list[dict[str, Any]]:
        return self.repository.list_pending(filters)

    def prepare_opportunity(self, raw: dict[str, Any]) -> dict[str, Any]:
        opportunity = dict(raw)
        opportunity["source_url"] = safe_external_url(opportunity.get("source_url"))
        opportunity["image_url"] = _find_image(opportunity.get("supplier_payload"))
        opportunity["availability"] = _availability(opportunity)

        evidence_payload = opportunity.get("evidence") or {}
        raw_matches = evidence_payload.get("top_matches", []) if isinstance(evidence_payload, dict) else []
        raw_matches = [item for item in raw_matches if isinstance(item, dict)]
        enriched = self.repository.enrich_evidence(raw_matches)
        evidence: list[dict[str, Any]] = []
        for item in enriched:
            evidence_item = dict(item)
            evidence_item["item_url"] = safe_external_url(
                evidence_item.get("resolved_item_url") or evidence_item.get("item_url")
            )
            evidence_item["image_url"] = safe_external_url(evidence_item.get("image_url"))
            evidence_item["comparison"] = compare_titles(
                str(opportunity.get("product_title") or ""),
                str(evidence_item.get("title") or ""),
            )
            evidence.append(evidence_item)
        opportunity["evidence_items"] = evidence

        run_config = opportunity.get("run_config") or {}
        margin_config = run_config.get("margin", {}) if isinstance(run_config, dict) else {}
        market_price = _decimal_value(opportunity.get("estimated_market_price"))
        total_fee_pct = _decimal_value(opportunity.get("total_fee_pct"))
        fee_pct = _decimal_value(margin_config.get("ml_fee_pct"))
        tax_pct = _decimal_value(margin_config.get("tax_pct"))
        shipping_pct = _decimal_value(margin_config.get("shipping_pct"))

        def amount(rate: Decimal | None) -> Decimal | None:
            return market_price * rate if market_price is not None and rate is not None else None

        known_rates = [rate for rate in (fee_pct, tax_pct, shipping_pct) if rate is not None]
        other_pct = None
        if total_fee_pct is not None and len(known_rates) == 3:
            remainder = total_fee_pct - sum(known_rates, Decimal("0"))
            other_pct = remainder if remainder > 0 else Decimal("0")
        opportunity["financial_breakdown"] = {
            "fee_pct": fee_pct,
            "fee_amount": amount(fee_pct),
            "tax_pct": tax_pct,
            "tax_amount": amount(tax_pct),
            "shipping_pct": shipping_pct,
            "shipping_amount": amount(shipping_pct),
            "other_pct": other_pct,
            "other_amount": amount(other_pct),
            "total_fee_pct": total_fee_pct,
            "total_fee_amount": amount(total_fee_pct),
        }
        opportunity["risk_details"] = [
            {"code": flag, "label": RISK_LABELS.get(flag, flag.replace("_", " ").title())}
            for flag in (opportunity.get("risk_flags") or [])
        ]
        opportunity["matching_guardrails"] = [
            detail
            for detail in opportunity["risk_details"]
            if detail["code"]
            in {"match_fraco", "match_revisar", "modelo_nao_confirmado", "titulo_generico_sem_identificador"}
        ]
        return opportunity

    def get_opportunity(self, opportunity_id: UUID) -> dict[str, Any] | None:
        raw = self.repository.get_opportunity(opportunity_id)
        return self.prepare_opportunity(raw) if raw else None

    def queue_context(
        self,
        filters: ReviewFilters,
        opportunity_id: UUID | None = None,
    ) -> dict[str, Any]:
        pending = self.pending(filters)
        selected_id = opportunity_id or (pending[0]["id"] if pending else None)
        opportunity = self.get_opportunity(UUID(str(selected_id))) if selected_id else None

        position = None
        next_id = None
        if selected_id is not None:
            selected_text = str(selected_id)
            for index, item in enumerate(pending):
                if str(item["id"]) == selected_text:
                    position = index + 1
                    if index + 1 < len(pending):
                        next_id = pending[index + 1]["id"]
                    break
            if position is None and pending:
                next_id = pending[0]["id"]

        query_values = filters.active_values()
        query_string = urlencode(query_values)
        return {
            "summary": self.summary(),
            "filter_options": self.filter_options(),
            "filters": filters,
            "active_filters": query_values,
            "query_string": query_string,
            "pending": pending,
            "filtered_count": len(pending),
            "position": position,
            "next_id": next_id,
            "opportunity": opportunity,
        }

    def create_decision(
        self,
        opportunity_id: UUID,
        decision: ReviewDecisionInput,
    ) -> dict[str, Any]:
        return self.repository.create_decision(opportunity_id, decision)

    def undo(
        self,
        opportunity_id: UUID,
        *,
        reviewer: str | None = None,
    ) -> dict[str, Any]:
        return self.repository.undo_decision(opportunity_id, invalidated_by=reviewer)

    def history(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.repository.list_history(limit=limit)
        for row in rows:
            row["decision_label"] = DECISION_LABELS.get(
                row.get("human_decision"), "Decisão desfeita"
            )
            reason = row.get("reason_code")
            row["reason_label"] = (
                REANALYSIS_REASON_LABELS.get(reason)
                or REJECTION_REASON_LABELS.get(reason)
                or ("Desfazer" if reason == "undo" else reason)
            )
        return rows
