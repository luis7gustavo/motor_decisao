"""Atomic persistence for already-normalized internal snapshots (not an adapter).

Caller owns the transaction. Source contracts, fiscal validation and analysis
invalidation are separate future capabilities. A published snapshot can still
have pending resource coverage and does not authorize a business decision.
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert

from .models import (Procurement, ProcurementVersion, ProcurementItem,
                     JudgmentUnit, JudgmentUnitItem, VersionObservation)


@dataclass(frozen=True)
class Item:
    key: str
    description: str
    quantity: Decimal | None
    unit: str | None
    estimated_unit_price: Decimal | None
    price_status: str
    attributes: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Unit:
    key: str
    kind: str
    item_keys: tuple[str, ...]
    composition_status: str
    criterion: str | None = None


@dataclass(frozen=True)
class Snapshot:
    source: str
    source_id: str
    raw_response_id: UUID
    snapshot: dict
    source_manifest: dict
    schema_version: str
    observed_at: datetime
    items: tuple[Item, ...] = ()
    units: tuple[Unit, ...] = ()
    source_updated_at: datetime | None = None
    source_time_text: str | None = None
    timezone_status: str = 'unknown'
    deadline_at: datetime | None = None
    official_status: str | None = None
    cnpj_original: str | None = None


def _json(value):
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError('Nonfinite decimal')
        return format(value, 'f')
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError('Naive instant requires unresolved source_time_text')
        return value.isoformat()
    raise ValueError('Unsupported snapshot value')


def validate(value):
    if not value.source.strip() or not value.source_id.strip() or not value.schema_version.strip():
        raise ValueError('Source identity/schema required')
    for instant in (value.observed_at, value.source_updated_at, value.deadline_at):
        if instant is not None:
            _json(instant)
    keys = [i.key for i in value.items]
    if len(set(keys)) != len(keys) or any(not key.strip() for key in keys):
        raise ValueError('Unique nonempty item keys required')
    for item in value.items:
        for number in (item.quantity, item.estimated_unit_price):
            if number is not None and (not isinstance(number, Decimal) or not number.is_finite()
                    or number <= 0 or number > Decimal('1000000') or number.as_tuple().exponent < -6):
                raise ValueError('Expected positive finite Decimal <= 1000000 with <= 6 places')
        if not item.price_status.strip() or not item.description.strip():
            raise ValueError('Description and price status required')
    if len({u.key for u in value.units}) != len(value.units):
        raise ValueError('Duplicate unit key')
    for unit in value.units:
        if not unit.key.strip() or unit.kind not in {'item','lote','grupo','global','desconhecido'}:
            raise ValueError('Invalid unit')
        if len(set(unit.item_keys)) != len(unit.item_keys) or not set(unit.item_keys) <= set(keys):
            raise ValueError('Unit membership must reference unique items of this version')
        if unit.kind == 'item' and len(unit.item_keys) != 1:
            raise ValueError('Item unit requires exactly one member')


def content(value):
    data = asdict(value)
    # Acquisition time/Bronze UUID are observation data. Semantic coverage,
    # resource hashes and schema changes in the manifest DO change the version.
    for key in ('observed_at','raw_response_id'):
        del data[key]
    data['items'] = sorted(data['items'], key=lambda item: item['key'])
    data['units'] = sorted(data['units'], key=lambda unit: unit['key'])
    for unit in data['units']:
        unit['item_keys'] = sorted(unit['item_keys'])
    encoded = json.dumps(data, default=_json, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


class ProcurementRepository:
    def __init__(self, session):
        self.session = session

    def record(self, value: Snapshot, *, actor: str, correlation_id: UUID):
        validate(value)
        if not actor.strip() or not isinstance(correlation_id, UUID):
            raise ValueError('Actor and correlation UUID required')
        fingerprint = content(value)
        # JSON roundtrip rejects non-JSON/custom values before acquiring locks.
        snapshot = json.loads(json.dumps(value.snapshot, default=_json, allow_nan=False))
        manifest = json.loads(json.dumps(value.source_manifest, default=_json, allow_nan=False))
        with self.session.begin_nested():
            raw = self.session.execute(text('SELECT source,http_status FROM bronze.pncp_responses WHERE id=:id'), {'id': value.raw_response_id}).first()
            if raw is None or raw.source != value.source or not 200 <= raw.http_status < 300:
                raise ValueError('Snapshot requires successful Bronze of the same source')
            self.session.execute(insert(Procurement).values(source=value.source, source_id=value.source_id,
                cnpj_original=value.cnpj_original).on_conflict_do_nothing(index_elements=['source','source_id']))
            parent = self.session.scalar(select(Procurement).where(Procurement.source==value.source,
                Procurement.source_id==value.source_id).with_for_update().execution_options(populate_existing=True))
            observed = self.session.scalar(select(VersionObservation).where(
                VersionObservation.procurement_id==parent.id, VersionObservation.raw_response_id==value.raw_response_id,
                VersionObservation.observed_at==value.observed_at))
            if observed:
                previous = self.session.get(ProcurementVersion, observed.version_id)
                if previous.content_hash != fingerprint:
                    raise ValueError('Same observation key with different content')
                return previous
            version = self.session.scalar(select(ProcurementVersion).where(
                ProcurementVersion.procurement_id==parent.id, ProcurementVersion.content_hash==fingerprint,
                ProcurementVersion.publication_state=='publicada'))
            if version is None:
                number = self.session.scalar(select(func.coalesce(func.max(ProcurementVersion.version_no),0)).where(
                    ProcurementVersion.procurement_id==parent.id)) + 1
                version = ProcurementVersion(procurement_id=parent.id, raw_response_id=value.raw_response_id,
                    version_no=number, content_hash=fingerprint, snapshot=snapshot, source_manifest=manifest,
                    schema_version=value.schema_version, observed_at=value.observed_at,
                    source_updated_at=value.source_updated_at, source_time_text=value.source_time_text,
                    timezone_status=value.timezone_status, deadline_at=value.deadline_at,
                    official_status=value.official_status, canonicalizer_version='radar-internal-1', publication_state='rascunho')
                self.session.add(version)
                self.session.flush()
                items = {}
                for item in value.items:
                    row = ProcurementItem(version_id=version.id, source_item_key=item.key, description=item.description,
                        quantity=item.quantity, unit=item.unit,
                        attributes=json.loads(json.dumps(item.attributes, default=_json, allow_nan=False)),
                        estimated_unit_price=item.estimated_unit_price, price_status=item.price_status)
                    self.session.add(row)
                    self.session.flush()
                    items[item.key] = row.id
                for unit in value.units:
                    row = JudgmentUnit(version_id=version.id, source_unit_key=unit.key, kind=unit.kind,
                        criterion=unit.criterion, composition_status=unit.composition_status)
                    self.session.add(row)
                    self.session.flush()
                    for key in unit.item_keys:
                        self.session.add(JudgmentUnitItem(unit_id=row.id,item_id=items[key],version_id=version.id,required=True))
                    self.session.flush()
                version.publication_state = 'publicada'
                self.session.flush()
            latest = self.session.scalar(select(func.max(VersionObservation.observed_at)).where(VersionObservation.procurement_id==parent.id))
            # Out-of-order observations are retained but cannot regress current.
            change = (latest is None or value.observed_at > latest) and parent.current_version_id != version.id
            self.session.add(VersionObservation(procurement_id=parent.id, version_id=version.id,
                previous_version_id=parent.current_version_id, raw_response_id=value.raw_response_id,
                observed_at=value.observed_at, actor=actor, correlation_id=correlation_id, changed=change))
            self.session.flush()
            if change:
                parent.current_version_id = version.id
                self.session.flush()
            return version
