from fastapi import APIRouter
from sqlalchemy import text

from app.core.database import engine
from app.core.settings import get_settings

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict[str, object]:
    settings = get_settings()
    database_ok = False

    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        database_ok = True
    except Exception:
        database_ok = False

    return {
        "status": "ok" if database_ok else "degraded",
        "environment": settings.environment,
        "database": database_ok,
    }

