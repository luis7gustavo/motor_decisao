from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router
from app.core.config import settings
from app.core.logging import configure_logging
from app.partners.router import router as partners_router
from app.recommendations.router import router as recommendations_router
from app.tasks.scheduler import start_scheduler, stop_scheduler
from app.technology.router import router as technology_router

configure_logging(settings.log_level)


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings.document_storage_path.mkdir(parents=True, exist_ok=True)
    if settings.auto_start_scheduler and settings.app_env != "test":
        start_scheduler()
    yield
    stop_scheduler()


app = FastAPI(
    title="Radar Licita API",
    description="Inteligência comercial explicável para oportunidades de compras públicas.",
    version="0.1.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_origin],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)
app.include_router(recommendations_router)
app.include_router(partners_router)
app.include_router(technology_router)


@app.get("/")
def root():
    return {"name": settings.app_name, "docs": "/docs", "health": "/api/v1/health"}

