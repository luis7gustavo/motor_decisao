"""Conservative metadata boundary for future PNCP C2 discovery integration.

The record intentionally does not infer an open opportunity or source
freshness from the listing payload. Those require separate Gate A evidence.
"""

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class DiscoveryMetadata:
    source_id: str
    version: int
    object: str
    uf: str
    modality: str | None
    official_state_raw: str | None
    deadline_text_unverified: str | None
    opening_state: str = 'unknown'
    freshness: str = 'unknown'


def from_current_version(*, source_id: str, version: int,
                         snapshot: Mapping[str, Any]) -> DiscoveryMetadata:
    """Validate identity/required fields and retain unverified source text."""
    if (not isinstance(source_id, str) or not source_id
            or isinstance(version, bool) or not isinstance(version, int) or version < 1
            or not isinstance(snapshot, Mapping)
            or snapshot.get('numeroControlePNCP') != source_id):
        raise ValueError('C2 identity or version mismatch')
    object_text = snapshot.get('objetoCompra')
    unit = snapshot.get('unidadeOrgao')
    uf = unit.get('ufSigla') if isinstance(unit, Mapping) else None
    if (not isinstance(object_text, str) or not object_text.strip()
            or not isinstance(uf, str) or len(uf) != 2 or not uf.isalpha()):
        raise ValueError('C2 discovery metadata is incomplete')
    modality = snapshot.get('modalidadeNome')
    official_state = snapshot.get('situacaoCompraNome')
    deadline = snapshot.get('dataEncerramentoProposta')
    if any(value is not None and not isinstance(value, str)
           for value in (modality, official_state, deadline)):
        raise ValueError('C2 source text has an unexpected type')
    return DiscoveryMetadata(
        source_id=source_id, version=version, object=object_text.strip(),
        uf=uf.upper(), modality=modality,
        official_state_raw=official_state,
        deadline_text_unverified=deadline,
    )
