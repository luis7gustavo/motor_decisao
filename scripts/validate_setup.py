from __future__ import annotations

import sys
from pathlib import Path

import redis
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.settings import get_settings  # noqa: E402


REQUIRED_SCHEMAS = {"control", "bronze", "silver", "gold"}
REQUIRED_TABLES = {
    ("control", "pipeline_runs"),
    ("control", "source_runs"),
    ("control", "data_quality_checks"),
    ("bronze", "mercado_livre_items_raw"),
    ("bronze", "price_history_raw"),
    ("bronze", "supplier_products_raw"),
}
REQUIRED_EXTENSIONS = {"pgcrypto", "vector"}


def main() -> int:
    settings = get_settings()
    project_config = settings.load_project_config()

    engine = create_engine(settings.database_url, pool_pre_ping=True, future=True)

    with engine.connect() as connection:
        extensions = {
            row[0]
            for row in connection.execute(
                text("SELECT extname FROM pg_extension WHERE extname = ANY(:names)"),
                {"names": list(REQUIRED_EXTENSIONS)},
            )
        }

        schemas = {
            row[0]
            for row in connection.execute(
                text("SELECT schema_name FROM information_schema.schemata")
            )
        }

        tables = {
            (row[0], row[1])
            for row in connection.execute(
                text(
                    """
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_type = 'BASE TABLE'
                    """
                )
            )
        }

    missing_extensions = REQUIRED_EXTENSIONS - extensions
    missing_schemas = REQUIRED_SCHEMAS - schemas
    missing_tables = REQUIRED_TABLES - tables

    redis_client = redis.Redis.from_url(settings.redis_url)
    redis_ok = bool(redis_client.ping())

    errors = []
    if missing_extensions:
        errors.append(f"Missing extensions: {sorted(missing_extensions)}")
    if missing_schemas:
        errors.append(f"Missing schemas: {sorted(missing_schemas)}")
    if missing_tables:
        errors.append(f"Missing tables: {sorted(missing_tables)}")
    if not redis_ok:
        errors.append("Redis ping failed")
    if "pipeline" not in project_config:
        errors.append("config.yaml missing 'pipeline'")

    if errors:
        print("Setup validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("Setup validation passed.")
    print(f"Environment: {settings.environment}")
    print(f"Initial category: {project_config['pipeline'].get('initial_category_id')}")
    print(f"Schemas: {', '.join(sorted(REQUIRED_SCHEMAS))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

