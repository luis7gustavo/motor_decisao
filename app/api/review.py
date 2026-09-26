from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError
from starlette.responses import Response

from app.core.csrf import csrf_context, set_csrf_cookie, verify_csrf
from app.core.database import engine
from app.repositories.review_repository import (
    ReviewConflictError,
    ReviewNotFoundError,
    ReviewRepository,
)
from app.schemas.review import ReviewDecisionInput, ReviewFilters
from app.services.review_service import (
    DECISION_LABELS,
    REANALYSIS_REASON_LABELS,
    REJECTION_REASON_LABELS,
    RISK_LABELS,
    ReviewService,
)


router = APIRouter(prefix="/review", tags=["review"])
PROJECT_ROOT = Path(__file__).resolve().parents[2]
templates = Jinja2Templates(directory=str(PROJECT_ROOT / "templates"))
review_service = ReviewService(ReviewRepository(engine))
MAX_FORM_BYTES = 32_768
FILTER_KEYS = ("supplier", "q", "min_margin", "min_profit", "min_score", "min_match", "risk", "sort")


def _format_number(value: Any, decimals: int = 2) -> str:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return "Não disponível"
    raw = f"{number:,.{decimals}f}"
    return raw.replace(",", "_").replace(".", ",").replace("_", ".")


def _currency(value: Any) -> str:
    formatted = _format_number(value)
    return "Não disponível" if formatted == "Não disponível" else f"R$ {formatted}"


def _percent(value: Any) -> str:
    try:
        number = Decimal(str(value)) * Decimal("100")
    except (InvalidOperation, TypeError, ValueError):
        return "Não disponível"
    return f"{_format_number(number)}%"


def _score(value: Any) -> str:
    return _format_number(value)


def _date_time(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone().strftime("%d/%m/%Y %H:%M")
    if isinstance(value, date):
        return value.strftime("%d/%m/%Y")
    return str(value) if value else "Não disponível"


templates.env.filters.update(
    currency=_currency,
    percent=_percent,
    score=_score,
    datetime=_date_time,
)


async def _parse_form(request: Request) -> dict[str, str]:
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/x-www-form-urlencoded":
        raise HTTPException(status_code=415, detail="Use um formulário URL encoded.")
    body = await request.body()
    if len(body) > MAX_FORM_BYTES:
        raise HTTPException(status_code=413, detail="Formulário excede o tamanho permitido.")
    try:
        parsed = parse_qs(
            body.decode("utf-8"),
            keep_blank_values=True,
            max_num_fields=40,
            strict_parsing=False,
        )
    except (UnicodeDecodeError, ValueError) as error:
        raise HTTPException(status_code=400, detail="Formulário inválido.") from error
    return {key: values[-1] for key, values in parsed.items() if values}


def _filters_from_mapping(mapping: Any, *, prefix: str = "") -> ReviewFilters:
    values = {
        key: mapping.get(f"{prefix}{key}")
        for key in FILTER_KEYS
        if mapping.get(f"{prefix}{key}") not in (None, "")
    }
    try:
        return ReviewFilters.model_validate(values)
    except ValidationError as error:
        raise HTTPException(status_code=422, detail=error.errors(include_context=False)) from error


def _validation_message(error: ValidationError) -> str:
    return " ".join(item["msg"] for item in error.errors(include_context=False))


def _redirect_url(path: str, filters: ReviewFilters, **extras: str) -> str:
    values = filters.active_values()
    values.update({key: value for key, value in extras.items() if value})
    query = urlencode(values)
    return f"{path}?{query}" if query else path


def _render_queue(
    request: Request,
    filters: ReviewFilters,
    *,
    opportunity_id: UUID | None = None,
    status_code: int = 200,
    form_error: str | None = None,
    form_values: dict[str, str] | None = None,
) -> HTMLResponse:
    context = review_service.queue_context(filters, opportunity_id)
    if opportunity_id is not None and context["opportunity"] is None:
        raise HTTPException(status_code=404, detail="Oportunidade em revisão não encontrada.")
    csrf = csrf_context(request)
    context.update(
        {
            "request": request,
            "csrf_token": csrf.token,
            "decision_labels": DECISION_LABELS,
            "reanalysis_reasons": REANALYSIS_REASON_LABELS,
            "rejection_reasons": REJECTION_REASON_LABELS,
            "risk_labels": RISK_LABELS,
            "form_error": form_error,
            "form_values": form_values or {},
            "saved": request.query_params.get("saved"),
            "undo_opportunity_id": request.query_params.get("undo"),
            "undone": request.query_params.get("undone"),
        }
    )
    response = templates.TemplateResponse(
        request=request,
        name="review/queue.html",
        context=context,
        status_code=status_code,
    )
    set_csrf_cookie(response, csrf)
    return response


@router.get("", response_class=HTMLResponse, name="review_queue")
@router.get("/", response_class=HTMLResponse, include_in_schema=False)
def review_queue(request: Request) -> HTMLResponse:
    filters = _filters_from_mapping(request.query_params)
    return _render_queue(request, filters)


@router.get("/history", response_class=HTMLResponse, name="review_history")
def review_history(request: Request) -> HTMLResponse:
    csrf = csrf_context(request)
    response = templates.TemplateResponse(
        request=request,
        name="review/history.html",
        context={
            "request": request,
            "csrf_token": csrf.token,
            "history": review_service.history(),
            "summary": review_service.summary(),
            "decision_labels": DECISION_LABELS,
        },
    )
    set_csrf_cookie(response, csrf)
    return response


@router.get("/summary", response_class=JSONResponse, name="review_summary")
def review_summary() -> dict[str, int]:
    return review_service.summary()


@router.get("/{opportunity_id}", response_class=HTMLResponse, name="review_opportunity")
def review_opportunity(
    request: Request,
    opportunity_id: UUID,
) -> HTMLResponse:
    filters = _filters_from_mapping(request.query_params)
    return _render_queue(request, filters, opportunity_id=opportunity_id)


@router.post("/{opportunity_id}/decision", name="review_decision")
async def save_review_decision(
    request: Request,
    opportunity_id: UUID,
) -> Response:
    form = await _parse_form(request)
    filters = _filters_from_mapping(form, prefix="filter_")
    if not verify_csrf(request, form.get("csrf_token")):
        return _render_queue(
            request,
            filters,
            opportunity_id=opportunity_id,
            status_code=403,
            form_error="A sessão do formulário expirou. Recarregue a página e tente novamente.",
            form_values=form,
        )
    try:
        decision = ReviewDecisionInput.model_validate(form)
    except ValidationError as error:
        return _render_queue(
            request,
            filters,
            opportunity_id=opportunity_id,
            status_code=422,
            form_error=_validation_message(error),
            form_values=form,
        )
    try:
        review_service.create_decision(opportunity_id, decision)
    except ReviewNotFoundError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ReviewConflictError as error:
        return _render_queue(
            request,
            filters,
            opportunity_id=opportunity_id,
            status_code=409,
            form_error=str(error),
            form_values=form,
        )
    return RedirectResponse(
        _redirect_url(
            "/review",
            filters,
            saved=decision.decision,
            undo=str(opportunity_id),
        ),
        status_code=303,
    )


@router.post("/{opportunity_id}/undo", name="review_undo")
async def undo_review_decision(
    request: Request,
    opportunity_id: UUID,
) -> RedirectResponse:
    form = await _parse_form(request)
    if not verify_csrf(request, form.get("csrf_token")):
        raise HTTPException(status_code=403, detail="A sessão do formulário expirou.")
    reviewer = (form.get("reviewer") or "").strip() or None
    if reviewer and len(reviewer) > 160:
        raise HTTPException(status_code=422, detail="Nome do revisor excede 160 caracteres.")
    try:
        review_service.undo(opportunity_id, reviewer=reviewer)
    except ReviewNotFoundError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error

    if form.get("return_to") == "history":
        return RedirectResponse("/review/history", status_code=303)
    filters = _filters_from_mapping(form, prefix="filter_")
    return RedirectResponse(
        _redirect_url(f"/review/{opportunity_id}", filters, undone="1"),
        status_code=303,
    )
