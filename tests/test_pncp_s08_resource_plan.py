"""Archived route checks for the read-only S08 enrichment plan."""

import pytest
import re

from scripts.radar_pncp_s08_resource_plan import ROUTES, contract_routes


def documents_with_required_gets():
    documents = {'consulta': {'paths': {}}, 'integracao': {'paths': {}}}
    for source, path in ROUTES.values():
        variables = re.findall(r'{([^{}]+)}', path)
        documents[source]['paths'][path] = {'get': {
            'parameters': [{'name': name, 'in': 'path', 'required': True}
                           for name in variables],
            'responses': {'200': {}}}}
    return documents


def test_resource_plan_requires_get_and_all_path_parameters():
    documents = documents_with_required_gets()
    assert set(contract_routes(documents)) == set(ROUTES)
    del documents['integracao']['paths'][ROUTES['results'][1]]['get']['parameters'][-1]
    with pytest.raises(ValueError, match='Incomplete archived GET contract: results'):
        contract_routes(documents)
    documents = documents_with_required_gets()
    del documents['integracao']['paths'][ROUTES['documents_binary'][1]]['get']
    with pytest.raises(ValueError, match='Missing archived GET contract: documents_binary'):
        contract_routes(documents)
