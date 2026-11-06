"""Isolated real-data discovery preview. No operational database connection."""

from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.exc import SQLAlchemyError

from app import radar_c2_reader as reader


router = APIRouter(prefix="/radar-demo", tags=["radar-demo"])
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[2] / "templates"))


def _unavailable(request: Request):
    return templates.TemplateResponse(request=request, name="radar_real/unavailable.html",
                                      context={}, status_code=503)


@router.get("", response_class=HTMLResponse, name="radar_real_index")
@router.get("/", response_class=HTMLResponse, include_in_schema=False)
def index(request: Request, q: str = Query(default="", max_length=80),
          uf: str = Query(default="", pattern=r"^(|[A-Za-z]{2})$"),
          modality: str = Query(default="", max_length=80),
          state: str = Query(default="", max_length=80),
          page: int = Query(default=1, ge=1, le=reader.MAX_PAGE)):
    filters = {"q": q.strip(), "uf": uf.upper(), "modality": modality.strip(),
               "state": state.strip()}
    try:
        rows, has_next = reader.search(**filters, page=page)
    except (reader.PreviewUnavailable, SQLAlchemyError, ValueError, OSError):
        return _unavailable(request)

    def page_url(number):
        return f"{request.url.path}?{urlencode({**filters, 'page': number})}"

    return templates.TemplateResponse(
        request=request, name="radar_real/index.html",
        context={"rows": rows, "filters": filters, "page": page,
                 "previous_url": page_url(page - 1) if page > 1 else None,
                 "next_url": page_url(page + 1) if has_next and page < reader.MAX_PAGE else None,
                 "snapshot_at": reader.SNAPSHOT_AT, "page_size": reader.PAGE_SIZE},
    )


@router.get("/{source_id:path}", response_class=HTMLResponse, name="radar_real_detail")
def detail(request: Request, source_id: str):
    if len(source_id) > 120:
        return templates.TemplateResponse(request=request, name="radar_real/not_found.html",
                                          context={}, status_code=404)
    try:
        row, versions = reader.detail(source_id)
    except (reader.PreviewUnavailable, SQLAlchemyError, ValueError, OSError):
        return _unavailable(request)
    if row is None:
        return templates.TemplateResponse(request=request, name="radar_real/not_found.html",
                                          context={}, status_code=404)
    return templates.TemplateResponse(request=request, name="radar_real/detail.html",
                                      context={"row": row, "versions": versions,
                                               "snapshot_at": reader.SNAPSHOT_AT,
                                               "backup_sha256": reader.BACKUP_SHA256})
