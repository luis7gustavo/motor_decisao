from __future__ import annotations

import re
import unicodedata
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from app.core.time import local_today
from app.ingestion.service import upsert_items, upsert_procurement
from app.partners.pncp_adapter import PartnerSourceError, PNCPPartnerAdapter

SAO_PAULO = ZoneInfo("America/Sao_Paulo")
CATEGORIES = {
    "computadores": ("notebook", "laptop", "computador", "desktop", "workstation", "tablet", "microcomputador", "minipc", "mini pc"),
    "perifericos": ("impressora", "scanner", "digitalizador", "teclado", "mouse", "webcam", "monitor de video", "monitor led", "monitor lcd", "projetor multimidia"),
    "infraestrutura": ("servidor de rede", "servidor rack", "storage", "switch", "roteador", "firewall", "access point", "nobreak", "cabeamento estruturado", "fibra optica", "datacenter", "data center"),
    "software": ("software", "licenca de uso", "licenciamento de sistema", "sistema de informacao", "antivirus", "windows", "microsoft 365", "office 365", "erp", "desenvolvimento de sistemas", "suporte tecnico em ti"),
    "telecom": ("telefonia ip", "voip", "link dedicado", "servico de internet", "acesso a internet", "banda larga", "telecomunicacoes"),
}
STOPWORDS = {"para", "com", "sem", "tipo", "unidade", "aquisicao", "contratacao", "servico", "servicos", "material", "materiais", "fornecimento", "modelo", "equipamento", "equipamentos", "marca", "tecnologia", "informacao", "diversos", "diversas", "lote", "contrato", "contratos", "ampla", "disputa", "incluindo", "nao"}
GENERIC_MATCH_TERMS = {"software", "licenca", "licenciamento", "sistema", "sistemas", "uso", "temporario", "computador", "suporte", "tecnico", "digital", "programa", "programas", "servico", "servicos", "solucao", "solucoes"}
NON_TECH_PRIMARY = re.compile(r"\b(limpeza|conservacao predial|vigilancia patrimonial|alimentacao escolar|servicos medicos|locacao de veiculos)\b")


def normalize(value: str) -> str:
    return unicodedata.normalize("NFKD", value.casefold()).encode("ascii", "ignore").decode()


def categories(value: str) -> set[str]:
    normalized = normalize(value)
    if "scanner" in normalized and ("raio x" in normalized or "raio-x" in normalized or "bagagens" in normalized):
        return set()
    primary_non_tech = NON_TECH_PRIMARY.search(normalized[:180])
    if primary_non_tech:
        first_tech = min((match.start() for values in CATEGORIES.values() for term in values
                          if (match := re.search(rf"(?<!\w){re.escape(term)}(?!\w)", normalized))),
                         default=len(normalized))
        if primary_non_tech.start() < first_tech:
            return set()
    return {name for name, terms in CATEGORIES.items() if any(re.search(rf"(?<!\w){re.escape(term)}(?!\w)", normalized) for term in terms)}


def terms(value: str) -> set[str]:
    return {part for part in re.findall(r"[a-z0-9]{3,}", normalize(value)) if part not in STOPWORDS}


def identity(row: dict[str, Any]) -> tuple[str, int, int] | None:
    cnpj, year, sequence = PNCPPartnerAdapter._publication_identity(row)
    if not cnpj or not year or not sequence:
        return None
    return cnpj, year, sequence


def still_open(row: dict[str, Any]) -> bool:
    raw = row.get("dataEncerramentoProposta")
    if not raw:
        return False
    try:
        closing = datetime.fromisoformat(str(raw))
    except ValueError:
        return False
    if closing.tzinfo is None:
        closing = closing.replace(tzinfo=SAO_PAULO)
    return closing > datetime.now(UTC)


def item_estimate(item: Any) -> tuple[Decimal | None, str]:
    if item.total_estimated_value is not None:
        return item.total_estimated_value, "valor total oficial do item"
    if item.unit_estimated_value is not None and item.quantity is not None:
        return item.unit_estimated_value * item.quantity, "preço unitário oficial × quantidade"
    return None, "não divulgado"


def historical_company(result: dict[str, Any]) -> bool:
    return (str(result.get("tipoPessoa") or "").upper() == "PJ"
            and bool(result.get("niFornecedor")) and bool(result.get("nomeRazaoSocialFornecedor"))
            and not result.get("dataCancelamento")
            and "cancel" not in normalize(str(result.get("situacaoCompraItemResultadoNome") or "")))


async def search_technology(db: Session, request: Any, adapter: PNCPPartnerAdapter | None = None) -> dict[str, Any]:
    today = local_today()
    history_end = request.history_end or today - timedelta(days=83)
    history_start = request.history_start or history_end - timedelta(days=7)
    if history_start > history_end:
        raise ValueError("O início do histórico deve ser anterior ao fim")
    limitations = [
        "A consulta varre apenas as primeiras páginas e compras configuradas; ausência de resultado não significa ausência no PNCP.",
        "Estimativa pública do item não representa custo de compra, capital necessário ou margem.",
        "Empresa em resultado homologado é apenas candidata histórica: capacidade atual, equivalência técnica, estoque, preço e interesse exigem confirmação.",
    ]
    coverage: dict[str, Any] = {"open_rows": 0, "open_purchases_checked": 0, "historical_rows": 0,
                               "historical_purchases_checked": 0, "historical_result_items_checked": 0,
                               "request_count": 0, "history_from": history_start.isoformat(),
                               "history_to": history_end.isoformat(), "open_until": (today + timedelta(days=request.open_horizon_days)).isoformat()}
    own_adapter = adapter is None
    adapter = adapter or PNCPPartnerAdapter(max_requests=70, deadline_seconds=110)
    opportunities: list[dict[str, Any]] = []
    historical: list[dict[str, Any]] = []
    status = "partial"
    try:
        open_rows = await adapter.open_proposal_rows(today + timedelta(days=request.open_horizon_days),
                                                     uf=request.uf, max_pages=request.open_pages)
        coverage["open_rows"] = len(open_rows)
        # Objects with a clear technology term go first; generic objects are also inspected.
        open_rows.sort(key=lambda row: bool(categories(str(row.get("objetoCompra") or ""))), reverse=True)
        seen: set[tuple[str, int, int]] = set()
        for row in open_rows:
            key = identity(row)
            if not key or key in seen or not still_open(row):
                continue
            seen.add(key)
            if coverage["open_purchases_checked"] >= request.max_open_purchases:
                break
            try:
                raw_items = await adapter.items(*key)
                coverage["open_purchases_checked"] += 1
                selected = []
                for raw in raw_items:
                    description = " ".join(str(raw.get(field) or "") for field in ("descricao", "informacaoComplementar"))
                    cats = categories(description)
                    if not cats or (request.keyword and normalize(request.keyword) not in normalize(description)):
                        continue
                    selected.append((raw, cats))
                if not selected:
                    continue
                procurement, _, _ = upsert_procurement(db, row)
                upsert_items(db, procurement, [raw for raw, _ in selected])
                db.commit()
                for raw, cats in selected:
                    item = next(i for i in procurement.items if i.item_number == int(raw.get("numeroItem") or 0))
                    estimate, basis = item_estimate(item)
                    opportunities.append({
                        "procurement_id": procurement.id, "item_id": item.id,
                        "control_number": procurement.official_control_number,
                        "object_description": procurement.object_description,
                        "item_number": item.item_number, "description": item.description,
                        "organization": procurement.organization_name, "uf": procurement.uf,
                        "proposal_close_at": row.get("dataEncerramentoProposta"),
                        "pncp_url": procurement.pncp_url, "categories": sorted(cats),
                        "public_estimate": str(estimate) if estimate is not None else None,
                        "estimate_basis": basis,
                        "budgets": [{"amount": str(b), "estimate_fits": estimate <= b if estimate is not None else None}
                                    for b in request.budgets],
                        "companies": [],
                    })
            except PartnerSourceError:
                raise
            except Exception as exc:  # noqa: BLE001 - one malformed official row must not abort the sample
                db.rollback()
                limitations.append(f"Compra {key[0]}/{key[1]}/{key[2]} não pôde ser processada: {str(exc)[:140]}")
        if opportunities:
            history_rows = await adapter.publication_rows(history_start, history_end,
                                                          uf=request.uf, max_pages=request.history_pages)
            coverage["historical_rows"] = len(history_rows)
            history_rows.sort(key=lambda row: bool(categories(str(row.get("objetoCompra") or ""))), reverse=True)
            seen.clear()
            for row in history_rows:
                key = identity(row)
                if not key or key in seen:
                    continue
                seen.add(key)
                if coverage["historical_purchases_checked"] >= request.max_history_purchases:
                    break
                try:
                    items = await adapter.items(*key)
                    coverage["historical_purchases_checked"] += 1
                    for raw in items:
                        cats = categories(str(raw.get("descricao") or "") + " " + str(raw.get("informacaoComplementar") or ""))
                        if not cats or not raw.get("temResultado"):
                            continue
                        if not any(cats.intersection(set(o["categories"])) for o in opportunities):
                            continue
                        number = int(raw.get("numeroItem") or 0)
                        results = await adapter.results(*key, number)
                        coverage["historical_result_items_checked"] += 1
                        for result in results:
                            if not historical_company(result):
                                continue
                            historical.append({
                                "tax_id": str(result["niFornecedor"]),
                                "legal_name": str(result["nomeRazaoSocialFornecedor"]),
                                "description": str(raw.get("descricao") or ""),
                                "categories": cats, "description_terms": terms(str(raw.get("descricao") or "")),
                                "result_date": result.get("dataResultado"),
                                "unit_price": str(result.get("valorUnitarioHomologado")) if result.get("valorUnitarioHomologado") is not None else None,
                                "source_url": f"https://pncp.gov.br/api/pncp/v1/orgaos/{key[0]}/compras/{key[1]}/{key[2]}/itens/{number}/resultados",
                                "control_number": str(row.get("numeroControlePNCP") or ""),
                            })
                            if len(historical) >= request.max_historical_results:
                                break
                        if len(historical) >= request.max_historical_results:
                            break
                    if len(historical) >= request.max_historical_results:
                        break
                except PartnerSourceError:
                    raise
                except Exception as exc:  # noqa: BLE001 - retain results from other purchases
                    limitations.append(f"Histórico {key[0]}/{key[1]}/{key[2]} indisponível: {str(exc)[:140]}")
        for opportunity in opportunities:
            current_terms = terms(opportunity["description"])
            by_company: dict[str, dict[str, Any]] = {}
            for record in historical:
                common_categories = record["categories"].intersection(opportunity["categories"])
                shared = current_terms.intersection(record["description_terms"])
                if not common_categories or not (shared - GENERIC_MATCH_TERMS):
                    continue
                score = round(len(shared) / max(1, len(current_terms.union(record["description_terms"]))), 3)
                company = by_company.setdefault(record["tax_id"], {
                    "tax_id": record["tax_id"], "legal_name": record["legal_name"],
                    "technical_match": "pending", "matched_terms": [], "similarity": 0.0,
                    "evidence": [],
                })
                company["similarity"] = max(company["similarity"], score)
                company["matched_terms"] = sorted(set(company["matched_terms"]).union(shared))[:8]
                if len(company["evidence"]) < 3:
                    company["evidence"].append({k: record[k] for k in
                                                ("description", "result_date", "unit_price", "source_url", "control_number")})
            opportunity["companies"] = sorted(by_company.values(), key=lambda c: c["similarity"], reverse=True)[:5]
        if not opportunities:
            limitations.append("Nenhum item de tecnologia apareceu no recorte consultado; amplie páginas ou altere UF/prazo.")
        elif not historical:
            limitations.append("Nenhum resultado homologado de empresa foi encontrado na janela histórica consultada.")
    except PartnerSourceError as exc:
        status = exc.kind
        limitations.append(str(exc))
    finally:
        coverage["request_count"] = adapter.request_count
        if own_adapter:
            await adapter.__aexit__(None, None, None)
    return {"status": status, "searched_at": datetime.now(UTC).isoformat(),
            "coverage": coverage, "limitations": limitations, "opportunities": opportunities}
