from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    app_name: str = "Radar Licita"
    app_env: str = "development"
    secret_key: str = "local-development-only"
    database_url: str = "sqlite:///./radar_licita.db"
    frontend_origin: str = "http://localhost:5173"
    pncp_consulta_base_url: str = "https://pncp.gov.br/api/consulta"
    pncp_api_base_url: str = "https://pncp.gov.br/api/pncp"
    pncp_requests_per_second: float = Field(default=2.0, gt=0, le=10)
    pncp_page_size: int = Field(default=50, ge=10, le=50)
    pncp_sync_lookback_days: int = Field(default=3, ge=1, le=30)
    sync_interval_minutes: int = Field(default=60, ge=5)
    document_max_bytes: int = Field(default=25_000_000, ge=1_000_000)
    document_storage_path: Path = Path("./data/documents")
    auto_start_scheduler: bool = True
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

