from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    environment: str = Field(default="development", alias="ENVIRONMENT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    database_url: str = Field(
        default="postgresql+psycopg://motor:motor@localhost:55432/motor_decisao",
        alias="DATABASE_URL",
    )
    redis_url: str = Field(default="redis://localhost:6380/0", alias="REDIS_URL")
    selenium_remote_url: str = Field(
        default="http://localhost:4444/wd/hub",
        alias="SELENIUM_REMOTE_URL",
    )
    config_path: str = Field(default="config/config.yaml", alias="MOTOR_CONFIG_PATH")
    ml_api_base: str = Field(default="https://api.mercadolibre.com", alias="ML_API_BASE")
    ml_auth_base: str = Field(default="https://auth.mercadolivre.com.br", alias="ML_AUTH_BASE")
    ml_site_id: str = Field(default="MLB", alias="ML_SITE_ID")
    ml_client_id: str | None = Field(default=None, alias="ML_CLIENT_ID")
    ml_client_secret: str | None = Field(default=None, alias="ML_CLIENT_SECRET")
    ml_redirect_uri: str | None = Field(default=None, alias="ML_REDIRECT_URI")
    ml_access_token: str | None = Field(default=None, alias="ML_ACCESS_TOKEN")
    ml_refresh_token: str | None = Field(default=None, alias="ML_REFRESH_TOKEN")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    def load_project_config(self) -> dict[str, Any]:
        path = Path(self.config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with path.open("r", encoding="utf-8") as file:
            data = yaml.safe_load(file) or {}
        if not isinstance(data, dict):
            raise ValueError("Project config must be a YAML mapping")
        return data


@lru_cache
def get_settings() -> Settings:
    return Settings()
