from fastapi import FastAPI

from app.api.health import router as health_router
from app.api.mercado_livre_oauth import router as mercado_livre_oauth_router
from app.api.operations import router as operations_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="Motor de Decisao de Compra",
        version="0.1.0",
        description="Backend local-first para recomendacao de portfolio de compra.",
    )
    app.include_router(health_router)
    app.include_router(mercado_livre_oauth_router)
    app.include_router(operations_router)
    return app


app = create_app()
