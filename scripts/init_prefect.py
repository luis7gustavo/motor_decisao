from __future__ import annotations

import asyncio

from alembic import command
from alembic.config import Config

from scripts.bootstrap_prefect import bootstrap
from scripts.deploy_collection import main as deploy_collection


if __name__ == "__main__":
    command.upgrade(Config("alembic.ini"), "head")
    asyncio.run(bootstrap())
    deploy_collection()
