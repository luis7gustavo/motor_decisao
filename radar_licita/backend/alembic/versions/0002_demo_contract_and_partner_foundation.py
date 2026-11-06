"""Additive contract fields and partner-search foundation.

The 0001 migration used metadata.create_all. This revision deliberately
inspects existing structures before adding columns and creates only tables
that are absent, so it can upgrade both an older SQLite copy and an empty DB.
"""
from alembic import op
from sqlalchemy import Boolean, Column, Date, Integer, JSON, Numeric, String, inspect

from app.db import Base
from app import models  # noqa: F401

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def add_column_if_missing(bind, table_name: str, column: Column) -> None:
    if table_name not in inspect(bind).get_table_names():
        return
    existing = {item["name"] for item in inspect(bind).get_columns(table_name)}
    if column.name not in existing:
        op.add_column(table_name, column)


def create_table_if_missing(bind, table_name: str) -> None:
    if table_name in inspect(bind).get_table_names():
        return
    Base.metadata.tables[table_name].create(bind=bind)


def upgrade() -> None:
    bind = op.get_bind()
    add_column_if_missing(bind, "supplier_quotes", Column("freight_scope", JSON, nullable=True))
    # SQLite cannot add a standalone FK constraint with ALTER TABLE. The
    # declarative model still carries the FK for fresh schemas; this additive
    # column is intentionally constraint-free on legacy SQLite copies.
    add_column_if_missing(bind, "financial_scenarios", Column("supplier_quote_id", Integer, nullable=True))
    add_column_if_missing(bind, "financial_scenarios", Column("supplier_quote_unit_price_snapshot", Numeric(18, 4), nullable=True))
    add_column_if_missing(bind, "financial_scenarios", Column("supplier_quote_freight_snapshot", Numeric(18, 2), nullable=True))
    add_column_if_missing(bind, "financial_scenarios", Column("supplier_quote_valid_until_snapshot", Date, nullable=True))
    add_column_if_missing(bind, "financial_scenarios", Column("capital_source", String(40), nullable=True))
    add_column_if_missing(bind, "financial_scenarios", Column("freight_confirmed", Boolean, nullable=False, server_default="0"))
    add_column_if_missing(bind, "financial_scenarios", Column("taxes_confirmed", Boolean, nullable=False, server_default="0"))
    add_column_if_missing(bind, "opportunity_scores", Column("eligibility", String(20), nullable=False, server_default="pending"))
    add_column_if_missing(bind, "opportunity_scores", Column("pending_checks", JSON, nullable=False, server_default="[]"))
    for table_name in ("partner_search_runs", "partner_candidates", "partner_observations"):
        create_table_if_missing(bind, table_name)


def downgrade() -> None:
    # The demo contract is intentionally additive. No destructive downgrade
    # is provided because old records must remain recoverable.
    pass
