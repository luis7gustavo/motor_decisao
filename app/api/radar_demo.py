"""Read-only preview of Radar Licita with explicitly synthetic fixtures."""

import json
from datetime import date, datetime
from pathlib import Path
from typing import Annotated
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.radar_demo import PAGE_SIZE, grouped_versions, is_fresh, load_catalog, search


router = APIRouter(prefix="/radar-demo", tags=["radar-demo"])
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[2] / "templates"))


def _catalog_or_error(request: Request):
    try:
        return load_catalog(), None
    except (OSError, ValueError, KeyError, json.JSONDecodeError):
        return None, templates.TemplateResponse(
            request=request,
            name="radar_demo/error.html",
            context={"request": request},
            status_code=503,
        )


@router.get("", response_class=HTMLResponse, name="radar_demo_index")
@router.get("/", response_class=HTMLResponse, include_in_schema=False)
def index(
    request: Request,
    q: Annotated[str, Query(max_length=120)] = "",
    uf: Annotated[str, Query(max_length=2)] = "",
    category: Annotated[str, Query(max_length=80)] = "",
    state: Annotated[str, Query(pattern="^(|open|closed|unknown)$")] = "",
    deadline_from: date | None = None,
    deadline_to: date | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
) -> HTMLResponse:
    catalog, error = _catalog_or_error(request)
    if error is not None:
        return error
    now = datetime.now(ZoneInfo("America/Sao_Paulo"))
    rows, total = search(
        catalog, today=now.date(), q=q.strip(), uf=uf.strip(), category=category.strip(),
        state=state, deadline_from=deadline_from, deadline_to=deadline_to, page=page,
    )
    groups = grouped_versions(catalog)
    filters = {"q": q, "uf": uf, "category": category, "state": state,
               "deadline_from": deadline_from or "", "deadline_to": deadline_to or ""}

    def page_url(number: int) -> str:
        return f"{request.url.path}?{urlencode({**filters, 'page': number})}"

    return templates.TemplateResponse(
        request=request,
        name="radar_demo/index.html",
        context={
            "request": request, "rows": rows, "total": total, "page": page,
            "page_size": PAGE_SIZE, "next_url": page_url(page + 1) if page * PAGE_SIZE < total else None,
            "previous_url": page_url(page - 1) if page > 1 else None,
            "filters": filters, "observed_at": catalog.observed_at, "fresh": is_fresh(catalog, now),
            "source_available": catalog.source_available,
            "change_alerts": [rows[-1] for rows in groups.values() if len(rows) > 1],
        },
    )


@router.get("/alteracoes", response_class=HTMLResponse, name="radar_demo_changes")
def changes(request: Request) -> HTMLResponse:
    catalog, error = _catalog_or_error(request)
    if error is not None:
        return error
    groups = grouped_versions(catalog)
    changed = [(source_id, rows) for source_id, rows in sorted(groups.items()) if len(rows) > 1]
    return templates.TemplateResponse(
        request=request, name="radar_demo/changes.html",
        context={"request": request, "changed": changed, "observed_at": catalog.observed_at},
    )


@router.get("/{source_id}", response_class=HTMLResponse, name="radar_demo_detail")
def detail(request: Request, source_id: str) -> HTMLResponse:
    catalog, error = _catalog_or_error(request)
    if error is not None:
        return error
    rows = grouped_versions(catalog).get(source_id)
    if rows is None:
        return templates.TemplateResponse(
            request=request, name="radar_demo/not_found.html",
            context={"request": request}, status_code=404,
        )
    now = datetime.now(ZoneInfo("America/Sao_Paulo"))
    return templates.TemplateResponse(
        request=request, name="radar_demo/detail.html",
        context={"request": request, "row": rows[-1], "versions": rows,
                 "fresh": is_fresh(catalog, now), "observed_at": catalog.observed_at,
                 "source_available": catalog.source_available},
    )
