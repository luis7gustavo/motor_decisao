from __future__ import annotations

import pytest

from motor_decisao.app.core import mercado_livre_pkce


def test_pkce_verifier_is_stored_and_consumed_once(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        mercado_livre_pkce,
        "PKCE_STORE_PATH",
        tmp_path / "mercado_livre_pkce.json",
    )

    authorization = mercado_livre_pkce.create_pkce_authorization()
    verifier = mercado_livre_pkce.consume_code_verifier(authorization["state"])

    assert verifier
    assert authorization["code_challenge"]
    assert authorization["code_challenge_method"] == "S256"
    with pytest.raises(RuntimeError, match="ausente ou expirado"):
        mercado_livre_pkce.consume_code_verifier(authorization["state"])
