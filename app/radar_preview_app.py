"""Dedicated preview server: only the isolated real reader and synthetic demo."""

from fastapi import FastAPI

from app.api.radar_demo import router as synthetic_router
from app.api.radar_c2_preview import router as real_router


app = FastAPI(title="Radar Licita — prévia isolada")
app.include_router(synthetic_router)
app.include_router(real_router)
