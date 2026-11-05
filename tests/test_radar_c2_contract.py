"""Offline tests for the conservative real-data discovery boundary."""

import pytest

from app.radar_c2_contract import from_current_version


def snapshot(**overrides):
    row = {'numeroControlePNCP': 'ORG-2026-0001',
           'objetoCompra': 'Aquisição de papel',
           'unidadeOrgao': {'ufSigla': 'sp'},
           'modalidadeNome': 'Pregão',
           'situacaoCompraNome': 'Divulgada no PNCP',
           'dataEncerramentoProposta': '2026-11-06T09:00:00'}
    row.update(overrides)
    return row


@pytest.mark.parametrize('state', ['Divulgada no PNCP', 'Suspensa', 'Revogada', 'Anulada'])
def test_official_state_and_raw_deadline_never_imply_open_or_fresh(state):
    row = from_current_version(
        source_id='ORG-2026-0001', version=2,
        snapshot=snapshot(situacaoCompraNome=state),
    )
    assert row.object == 'Aquisição de papel'
    assert row.uf == 'SP'
    assert row.official_state_raw == state
    assert row.deadline_text_unverified == '2026-11-06T09:00:00'
    assert row.opening_state == 'unknown'
    assert row.freshness == 'unknown'


def test_identity_mismatch_or_missing_location_is_rejected():
    with pytest.raises(ValueError, match='identity'):
        from_current_version(source_id='ORG-2026-0001', version=1,
                             snapshot=snapshot(numeroControlePNCP='another'))
    with pytest.raises(ValueError, match='incomplete'):
        from_current_version(source_id='ORG-2026-0001', version=1,
                             snapshot=snapshot(unidadeOrgao={}))
